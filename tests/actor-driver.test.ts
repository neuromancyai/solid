import { describe, expect, it, vi } from "vitest";
import { createDriver, DriverPhase, type StepResponse } from "../src/actor/driver";
import {
  ActorPhase,
  MessageType,
  RuntimeFailureReasonType,
  RuntimePhase,
  RuntimeRequestType,
  RuntimeResultType,
  SystemCommandType,
  SystemMessageType,
  reducer,
  type ActorInstance,
  type Message,
  type RuntimeRequest,
  type RuntimeState,
} from "../src/actor/runtime";

type ReadCommand = { type: "READ_FILE"; path: string };

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}

function live(name: string, mailbox: Message[] = []): ActorInstance<number> {
  return { phase: ActorPhase.RUNNING, name, mailbox, state: 0, watchers: [] };
}

function state(instances: RuntimeState["instances"]): RuntimeState {
  return {
    phase: RuntimePhase.RUNNING,
    instances,
    ready: Object.entries(instances)
      .filter(([, actor]) => actor.phase !== ActorPhase.STOPPED && actor.mailbox.length > 0)
      .map(([id]) => id),
  };
}

const start: Message = { type: MessageType.START, parentId: null };
const exit = { type: SystemCommandType.EXIT };
const read: ReadCommand = { type: "READ_FILE", path: "input.txt" };
const reply = (id: string, message: unknown): RuntimeRequest => ({
  type: RuntimeRequestType.SEND_MESSAGE,
  sourceId: null,
  recipientId: id,
  message,
});

describe("actor driver", () => {
  it("processes queued work in FIFO order, one item per step", async () => {
    const seen: Message[] = [];
    const send = { type: SystemCommandType.SEND, id: "worker", message: "from command" };
    const driver = createDriver(
      reducer({
        worker: {
          initialState: 0,
          on: (previous: number, message: Message) => {
            seen.push(message);
            return {
              state: previous + 1,
              commands: message.type === MessageType.START ? [send] : [],
            };
          },
        },
      }),
      { initialState: state({ worker: live("worker", [start]) }) },
    );
    const initial = driver.inspect();
    const first = driver.request(reply("worker", "first"));
    const second = driver.request(reply("worker", "second"));
    await Promise.resolve();
    expect(driver.inspect()).toBe(initial);
    expect(seen).toEqual([]);

    const firstStep = driver.step();
    expect(firstStep).toMatchObject({
      request: reply("worker", "first"),
      command: null,
      sourceId: null,
    });
    await expect(first).resolves.toBe(firstStep.response);
    const secondStep = driver.step();
    expect(secondStep).toMatchObject({ request: reply("worker", "second") });
    await expect(second).resolves.toBe(secondStep.response);
    expect(seen).toEqual([]);

    expect(driver.step().response).toBeNull();
    expect(driver.step()).toMatchObject({ command: send, sourceId: "worker" });
    expect(seen).toEqual([start]);
    await driver.batch(3);
    expect(seen).toEqual([
      start,
      { type: MessageType.NEXT, sourceId: null, data: "first" },
      { type: MessageType.NEXT, sourceId: null, data: "second" },
      { type: MessageType.NEXT, sourceId: "worker", data: "from command" },
    ]);
  });

  it("interprets native lifecycle commands and preserves actor and runtime origins", async () => {
    const responses: StepResponse[] = [];
    const childMessages: Message[] = [];
    const driver = createDriver(
      reducer<never>({
        parent: {
          initialState: 0,
          on: (previous: number, message: Message) => {
            if (message.type === MessageType.START) {
              return {
                state: previous,
                commands: [{ type: SystemCommandType.START, name: "child" }],
              };
            }
            if (
              message.type === MessageType.NEXT &&
              message.data.type === SystemMessageType.STARTED
            ) {
              return {
                state: previous,
                commands: [
                  { type: SystemCommandType.WATCH, id: message.data.id },
                  { type: SystemCommandType.SEND, id: message.data.id, message: "payload" },
                  { type: SystemCommandType.STOP, id: message.data.id },
                ],
              };
            }
            return { state: previous, commands: [exit] };
          },
        },
        child: {
          initialState: 0,
          on: (previous: number, message: Message) => {
            childMessages.push(message);
            return { state: previous, commands: message.type === MessageType.STOP ? [exit] : [] };
          },
        },
      }),
      {
        initialState: state({ parent: live("parent", [start]) }),
        onResponse: (result) => responses.push(result),
      },
    );

    expect(await driver.batch(20)).toBe(DriverPhase.RUNNING);
    const childId = Object.keys(driver.inspect().instances).find((id) => id !== "parent");
    expect(childMessages).toEqual([
      { type: MessageType.START, parentId: "parent" },
      { type: MessageType.NEXT, sourceId: "parent", data: "payload" },
      { type: MessageType.STOP },
    ]);
    expect(responses.map(({ sourceId, command }) => [sourceId, command?.type])).toEqual([
      ["parent", SystemCommandType.START],
      [null, SystemCommandType.SEND],
      ["parent", SystemCommandType.WATCH],
      ["parent", SystemCommandType.SEND],
      ["parent", SystemCommandType.STOP],
      [childId, SystemCommandType.EXIT],
      [null, SystemCommandType.SEND],
      ["parent", SystemCommandType.EXIT],
    ]);
    expect(
      responses.every(({ response }) => response.result.type === RuntimeResultType.SUCCESS),
    ).toBe(true);
    expect(
      Object.values(driver.inspect().instances).every(
        (actor) => actor.phase === ActorPhase.STOPPED,
      ),
    ).toBe(true);
  });

  it("continues after refusals and lets callbacks enqueue replies", async () => {
    const refused = { type: SystemCommandType.STOP, id: "missing" };
    const responses: StepResponse[] = [];
    const on = vi.fn((previous: number, message: Message) => ({
      state: previous,
      commands: message.type === MessageType.START ? [refused] : [exit],
    }));
    const driver = createDriver(reducer<never>({ worker: { initialState: 0, on } }), {
      initialState: state({ worker: live("worker", [start]) }),
      onResponse: (result) => {
        responses.push(result);
        if (result.response.result.type === RuntimeResultType.FAILURE) {
          driver.request(reply("worker", result.response.result));
        }
      },
    });

    expect(await driver.batch(5)).toBe(DriverPhase.RUNNING);
    const failure = {
      type: RuntimeResultType.FAILURE,
      reason: { type: RuntimeFailureReasonType.ACTOR_NOT_FOUND, id: "missing" },
    };
    expect(responses[0]).toMatchObject({
      sourceId: "worker",
      command: refused,
      response: { result: failure },
    });
    expect(on).toHaveBeenLastCalledWith(0, {
      type: MessageType.NEXT,
      sourceId: null,
      data: failure,
    });
    expect(responses.map(({ response }) => response.result.type)).toEqual([
      RuntimeResultType.FAILURE,
      RuntimeResultType.SUCCESS,
      RuntimeResultType.SUCCESS,
    ]);
  });

  it("yields before batches and advances empty runtimes for the full budget", async () => {
    const reduce = vi.fn(reducer({}));
    const driver = createDriver(reduce);
    expect(await driver.batch(0)).toBe(DriverPhase.RUNNING);
    expect(reduce).not.toHaveBeenCalled();

    let timerRan = false;
    setTimeout(() => {
      timerRan = true;
    }, 0);
    const batch = driver.batch(3);
    await Promise.resolve();
    expect(reduce).not.toHaveBeenCalled();
    expect(await batch).toBe(DriverPhase.RUNNING);
    expect(timerRan).toBe(true);
    expect(reduce.mock.calls.map(([, request]) => request)).toEqual([null, null, null]);
    driver.step();
    expect(reduce).toHaveBeenCalledTimes(4);
  });

  it("runs effects without blocking actors and queues results as they settle", async () => {
    const slow = deferred<RuntimeRequest | null>();
    const fast = deferred<RuntimeRequest | null>();
    const received: unknown[] = [];
    const responses: StepResponse<ReadCommand>[] = [];
    const execute = vi.fn((command: ReadCommand) =>
      command.path === "slow" ? slow.promise : fast.promise,
    );
    const driver = createDriver(
      reducer<ReadCommand>({
        worker: {
          initialState: 0,
          on: (previous: number, message: Message) => {
            if (message.type === MessageType.START) {
              return {
                state: previous,
                commands: [
                  { type: "READ_FILE", path: "slow" },
                  { type: "READ_FILE", path: "fast" },
                ],
              };
            }
            if (message.type === MessageType.NEXT) {
              received.push(message.data);
            }
            return { state: previous, commands: [] };
          },
        },
        other: { initialState: 0, on: () => ({ state: 0, commands: [exit] }) },
      }),
      {
        initialState: state({ worker: live("worker", [start]), other: live("other", [start]) }),
        execute,
        onResponse: (result) => responses.push(result),
      },
    );

    expect(await driver.batch(5)).toBe(DriverPhase.RUNNING);
    expect(execute.mock.calls.map(([command]) => command.path)).toEqual(["slow", "fast"]);
    expect(driver.inspect().instances.other?.phase).toBe(ActorPhase.STOPPED);
    const beforeResults = driver.inspect();
    fast.resolve(reply("worker", "fast"));
    await Promise.resolve();
    slow.resolve(reply("worker", "slow"));
    await Promise.resolve();
    expect(driver.inspect()).toBe(beforeResults);
    responses.length = 0;

    await driver.batch(2);
    expect(responses.map(({ command, sourceId }) => [command, sourceId])).toEqual([
      [{ type: "READ_FILE", path: "fast" }, "worker"],
      [{ type: "READ_FILE", path: "slow" }, "worker"],
    ]);
    expect(received).toEqual([]);
    await driver.batch(2);
    expect(received).toEqual(["fast", "slow"]);
  });

  it.each(["throw", "reject"] as const)(
    "recovers from an executor %s and accepts an error callback's reply",
    async (mode) => {
      const error = new Error("read failed");
      const on = vi.fn((previous: number, message: Message) => ({
        state: previous,
        commands: message.type === MessageType.START ? [read] : [exit],
      }));
      const onExecutorError = vi.fn(() => {
        driver.request(reply("worker", "read failed"));
      });
      const driver = createDriver(reducer<ReadCommand>({ worker: { initialState: 0, on } }), {
        initialState: state({ worker: live("worker", [start]) }),
        execute: (_command: ReadCommand) => {
          if (mode === "throw") {
            throw error;
          }
          return Promise.reject(error);
        },
        onExecutorError,
      });

      await driver.batch(2);
      expect(await driver.batch(3)).toBe(DriverPhase.RUNNING);
      expect(onExecutorError).toHaveBeenCalledExactlyOnceWith({
        sourceId: "worker",
        command: read,
        error,
      });
      expect(on).toHaveBeenLastCalledWith(0, {
        type: MessageType.NEXT,
        sourceId: null,
        data: "read failed",
      });
    },
  );

  it("faults the driver when onExecutorError throws", async () => {
    const effect = deferred<RuntimeRequest | null>();
    const failure = new Error("error callback failed");
    const driver = createDriver(
      reducer<ReadCommand>({
        worker: { initialState: 0, on: () => ({ state: 0, commands: [read] }) },
      }),
      {
        initialState: state({ worker: live("worker", [start]) }),
        execute: (_command: ReadCommand) => effect.promise,
        onExecutorError: () => {
          throw failure;
        },
      },
    );
    await driver.batch(2);
    const batchRejected = expect(driver.batch(1)).rejects.toBe(failure);
    const receiptRejected = expect(driver.request(reply("worker", "queued"))).rejects.toBe(failure);
    effect.reject(new Error("executor failed"));

    await Promise.all([batchRejected, receiptRejected]);
    expect(() => driver.step()).toThrow(failure);
    await expect(driver.batch(1)).rejects.toBe(failure);
    await expect(driver.request(reply("worker", "later"))).rejects.toBe(failure);
  });

  it("rejects responses that do not match the request", async () => {
    const driver = createDriver((state) => ({
      state,
      response: {
        type: RuntimeRequestType.STOP_RUNTIME,
        result: { type: RuntimeResultType.SUCCESS },
      },
      commands: [],
    }));
    const rejected = expect(
      driver.request({ type: RuntimeRequestType.START_ACTOR, name: "worker", parentId: null }),
    ).rejects.toThrow("reducer returned no matching response to a request");

    expect(() => driver.step()).toThrow("reducer returned no matching response to a request");
    await rejected;
  });

  it("propagates reducer errors without discarding later requests", async () => {
    const driver = createDriver(reducer({}));
    const rejected = expect(
      driver.request({ type: RuntimeRequestType.FINALIZE_ACTOR, targetId: null }),
    ).rejects.toThrow();
    const shutdown = driver.request({ type: RuntimeRequestType.STOP_RUNTIME });

    expect(() => driver.step()).toThrow();
    await rejected;
    expect(await driver.batch(2)).toBe(DriverPhase.STOPPED);
    await expect(shutdown).resolves.toMatchObject({ result: { type: RuntimeResultType.SUCCESS } });
  });

  it.each(["request", "null", "reject"] as const)(
    "waits for pending effects and drains a late %s result after explicit shutdown",
    async (result) => {
      const effect = deferred<RuntimeRequest | null>();
      const onResponse = vi.fn();
      const driver = createDriver(
        reducer<ReadCommand>({
          worker: { initialState: 0, on: () => ({ state: 0, commands: [read, exit] }) },
        }),
        {
          initialState: state({ worker: live("worker", [start]) }),
          execute: (_command: ReadCommand) => effect.promise,
          onResponse,
        },
      );
      expect(await driver.batch(3)).toBe(DriverPhase.RUNNING);
      expect(driver.inspect().instances.worker?.phase).toBe(ActorPhase.STOPPED);
      expect(driver.inspect().phase).toBe(RuntimePhase.RUNNING);

      driver.request({ type: RuntimeRequestType.STOP_RUNTIME });
      expect(await driver.batch(2)).toBe(DriverPhase.RUNNING);
      expect(driver.inspect().phase).toBe(RuntimePhase.STOPPED);
      onResponse.mockClear();
      if (result === "reject") {
        effect.reject(new Error("late executor failure"));
      } else {
        effect.resolve(result === "null" ? null : reply("worker", "too late"));
      }
      await Promise.resolve();

      expect(await driver.batch(1)).toBe(DriverPhase.STOPPED);
      if (result === "request") {
        expect(onResponse).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({
            sourceId: "worker",
            command: read,
            request: reply("worker", "too late"),
            response: {
              type: RuntimeRequestType.SEND_MESSAGE,
              result: {
                type: RuntimeResultType.FAILURE,
                reason: { type: RuntimeFailureReasonType.ACTOR_STOPPED, id: "worker" },
              },
            },
          }),
        );
      } else {
        expect(onResponse).not.toHaveBeenCalled();
      }
    },
  );
});
