import { describe, expect, it, vi } from "vitest";
import {
  ActorPhase,
  MessageType,
  RuntimeFailureReasonType,
  RuntimePhase,
  RuntimeRequestType,
  RuntimeResultType,
  SystemCommandType,
  SystemMessageType,
  TerminationType,
  reducer,
  type ActorDefinitions,
  type LiveActorInstance,
  type Message,
  type Reduction,
  type RuntimeFailureReason,
  type RuntimeRequest,
  type RuntimeState,
} from "../src/actor/runtime";

function freeze<T>(value: T): T {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const nested of Object.values(value)) {
      freeze(nested);
    }
  }
  return value;
}

function live(mailbox: Message[] = [], watchers: string[] = []): LiveActorInstance<number> {
  return { phase: ActorPhase.RUNNING, name: "counter", mailbox, state: 0, watchers };
}

function state(instances: RuntimeState["instances"] = {}): RuntimeState {
  return freeze({
    phase: RuntimePhase.RUNNING,
    instances,
    ready: Object.entries(instances)
      .filter(([, actor]) => actor.phase !== ActorPhase.STOPPED && actor.mailbox.length > 0)
      .map(([id]) => id),
  });
}

function started(result: Reduction): string {
  if (
    result.response?.type !== RuntimeRequestType.START_ACTOR ||
    result.response.result.type !== RuntimeResultType.SUCCESS
  ) {
    throw new Error("Expected successful actor start");
  }
  return result.response.result.id;
}

const next = (data: unknown, sourceId: string | null = null): Message => ({
  type: MessageType.NEXT,
  sourceId,
  data,
});

const send = (
  recipientId: string,
  message: unknown,
  sourceId: string | null = null,
): RuntimeRequest => ({
  type: RuntimeRequestType.SEND_MESSAGE,
  recipientId,
  sourceId,
  message,
});

const watch = (watcherId: string, targetId: string): RuntimeRequest => ({
  type: RuntimeRequestType.WATCH_ACTOR,
  watcherId,
  targetId,
});

const counter = {
  counter: {
    initialState: 0,
    on: (previous: number) => ({
      state: previous + 1,
      commands: [],
    }),
  },
} satisfies ActorDefinitions;

describe("actor runtime", () => {
  it("starts fresh actors with unique IDs and automatic parent watching", () => {
    const on = vi.fn(counter.counter.on);
    const reduce = reducer({ counter: { initialState: 0, on } });
    const root = reduce(state(), {
      type: RuntimeRequestType.START_ACTOR,
      name: "counter",
      parentId: null,
    });
    const parentId = started(root);
    expect(root.state.instances[parentId]).toEqual(
      live([{ type: MessageType.START, parentId: null }]),
    );
    expect(on).not.toHaveBeenCalled();

    const advanced = reduce(freeze(root.state), null);
    const child = reduce(freeze(advanced.state), {
      type: RuntimeRequestType.START_ACTOR,
      name: null,
      parentId,
    });
    const childId = started(child);
    expect(childId).not.toBe(parentId);
    expect(child.state.instances[childId]).toEqual(
      live([{ type: MessageType.START, parentId }], [parentId]),
    );
    expect(child.state.instances[parentId]).toMatchObject({ state: 1, mailbox: [] });
    expect(child.state.ready).toEqual([childId]);
    expect(child.commands).toEqual([
      {
        sourceId: null,
        commands: [
          {
            type: SystemCommandType.SEND,
            id: parentId,
            message: { type: SystemMessageType.STARTED, id: childId },
          },
        ],
      },
    ]);
    expect(on).toHaveBeenCalledTimes(1);
  });

  it("handles requests without advancing, then processes one message per null step fairly", () => {
    const emitted = [
      { type: SystemCommandType.SEND, id: "idle", message: "forwarded" },
      { type: "save", value: 1 },
    ];
    const on = vi.fn((previous: number, message: Message) => ({
      state: previous + 1,
      commands: message.type === MessageType.NEXT && message.data === "first" ? emitted : [],
    }));
    const reduce = reducer({ counter: { initialState: 0, on } });
    const initial = state({
      a: live([next("first"), next("second")]),
      b: live([next("b")]),
      idle: live(),
    });
    const admitted = reduce(initial, send("a", "third", "b"));
    expect(on).not.toHaveBeenCalled();
    expect(admitted.state.ready).toEqual(["a", "b"]);
    expect(admitted.state.instances.a).toMatchObject({
      mailbox: [next("first"), next("second"), next("third", "b")],
    });

    const first = reduce(freeze(admitted.state), null);
    expect(first.response).toBeNull();
    expect(first.commands).toEqual([{ sourceId: "a", commands: emitted }]);
    expect(first.state.ready).toEqual(["b", "a"]);
    expect(first.state.instances.a).toMatchObject({ state: 1 });
    expect(first.state.instances.idle).toMatchObject({ mailbox: [] });

    const delivered = reduce(freeze(first.state), send("idle", "forwarded", "a"));
    expect(delivered.state.ready).toEqual(["b", "a", "idle"]);
    const second = reduce(freeze(delivered.state), null);
    const third = reduce(freeze(second.state), null);
    expect(on.mock.calls.map(([, message]) => message)).toEqual([
      next("first"),
      next("b"),
      next("second"),
    ]);
    expect(third.state.ready).toEqual(["idle", "a"]);
    expect(initial.instances.a).toMatchObject({
      state: 0,
      mailbox: [next("first"), next("second")],
    });
  });

  it("delivers STOP in FIFO order and allows cleanup replies before successful exit", () => {
    const on = vi.fn((previous: number, message: Message) => ({
      state: previous + 1,
      commands:
        message.type === MessageType.NEXT && message.data === "cleaned"
          ? [{ type: SystemCommandType.EXIT }]
          : [],
    }));
    const reduce = reducer({ counter: { initialState: 0, on } });
    const stopping = reduce(state({ worker: live([next("work")]) }), {
      type: RuntimeRequestType.STOP_ACTOR,
      targetId: "worker",
    });
    expect(stopping.state.instances.worker).toMatchObject({
      phase: ActorPhase.STOPPING,
      mailbox: [next("work"), { type: MessageType.STOP }],
    });
    const repeated = reduce(stopping.state, {
      type: RuntimeRequestType.STOP_ACTOR,
      targetId: "worker",
    });
    expect(repeated.response?.result).toEqual({
      type: RuntimeResultType.FAILURE,
      reason: { type: RuntimeFailureReasonType.ACTOR_NOT_RUNNING, id: "worker" },
    });

    const first = reduce(freeze(stopping.state), null);
    const waiting = reduce(freeze(first.state), null);
    expect(waiting.state.instances.worker).toMatchObject({
      phase: ActorPhase.STOPPING,
      mailbox: [],
    });
    const replied = reduce(freeze(waiting.state), send("worker", "cleaned"));
    const cleaned = reduce(freeze(replied.state), null);
    expect(on.mock.calls.map(([, message]) => message)).toEqual([
      next("work"),
      { type: MessageType.STOP },
      next("cleaned"),
    ]);
    expect(cleaned.commands).toEqual([
      { sourceId: "worker", commands: [{ type: SystemCommandType.EXIT }] },
    ]);
    const exited = reduce(freeze(cleaned.state), {
      type: RuntimeRequestType.FINALIZE_ACTOR,
      targetId: "worker",
    });
    expect(exited.state.instances.worker).toEqual({
      phase: ActorPhase.STOPPED,
      termination: { type: TerminationType.SUCCESS },
    });
  });

  it("notifies watchers once on natural exit and replays termination to late watchers", () => {
    const reduce = reducer(counter);
    const initial = state({ worker: live([next("discarded")]), observer: live() });
    const watched = reduce(initial, watch("observer", "worker"));
    const repeated = reduce(freeze(watched.state), watch("observer", "worker"));
    const exited = reduce(freeze(repeated.state), {
      type: RuntimeRequestType.FINALIZE_ACTOR,
      targetId: "worker",
    });
    const termination = { type: TerminationType.SUCCESS };
    expect(exited.state.instances.worker).toEqual({ phase: ActorPhase.STOPPED, termination });
    expect(exited.state.ready).toEqual([]);
    expect(exited.state.instances.observer).toMatchObject({ mailbox: [] });
    expect(exited.commands).toEqual([
      {
        sourceId: null,
        commands: [
          {
            type: SystemCommandType.SEND,
            id: "observer",
            message: { type: SystemMessageType.STOPPED, id: "worker", termination },
          },
        ],
      },
    ]);
    const late = reduce(freeze(exited.state), watch("observer", "worker"));
    expect(late.response?.result.type).toBe(RuntimeResultType.SUCCESS);
    expect(late.commands).toEqual(exited.commands);
  });

  it("isolates failed actors and normalizes their errors", () => {
    const thrown = { reason: "provider failed" };
    const on = vi.fn(() => {
      throw thrown;
    });
    const reduce = reducer({ ...counter, failing: { initialState: 0, on } });
    const initial = state({
      worker: { ...live([next("trigger"), next("discarded")], ["observer"]), name: "failing" },
      healthy: live([next("continue")]),
      observer: live(),
    });
    const failed = reduce(initial, null);
    const termination = {
      type: TerminationType.FAILURE,
      error: expect.objectContaining({ cause: thrown }),
    };
    expect(failed.state.instances.worker).toEqual({ phase: ActorPhase.STOPPED, termination });
    expect(failed.commands).toEqual([
      {
        sourceId: null,
        commands: [
          {
            type: SystemCommandType.SEND,
            id: "observer",
            message: { type: SystemMessageType.STOPPED, id: "worker", termination },
          },
        ],
      },
    ]);
    const continued = reduce(freeze(failed.state), null);
    expect(continued.state.instances.healthy).toMatchObject({ state: 1, mailbox: [] });
    expect(continued.state.ready).toEqual([]);
    expect(on).toHaveBeenCalledTimes(1);
  });

  it("returns structured refusals without changing state for invalid requests", () => {
    const reduce = reducer(counter);
    const initial = state({
      dead: { phase: ActorPhase.STOPPED, termination: { type: TerminationType.SUCCESS } },
    });
    const cases: { request: RuntimeRequest; reason: RuntimeFailureReason }[] = [
      {
        request: send("missing", null),
        reason: { type: RuntimeFailureReasonType.ACTOR_NOT_FOUND, id: "missing" },
      },
      {
        request: send("dead", null),
        reason: { type: RuntimeFailureReasonType.ACTOR_STOPPED, id: "dead" },
      },
      {
        request: { type: RuntimeRequestType.START_ACTOR, name: "unknown", parentId: null },
        reason: { type: RuntimeFailureReasonType.UNKNOWN_DEFINITION, name: "unknown" },
      },
      {
        request: { type: RuntimeRequestType.START_ACTOR, name: null, parentId: "dead" },
        reason: { type: RuntimeFailureReasonType.ACTOR_STOPPED, id: "dead" },
      },
    ];
    for (const { request, reason } of cases) {
      const result = reduce(initial, request);
      expect(result.response).toEqual({
        type: request.type,
        result: { type: RuntimeResultType.FAILURE, reason },
      });
      expect(result.state).toBe(initial);
      expect(result.commands).toEqual([]);
    }
  });

  it("throws for malformed actor requests", () => {
    const reduce = reducer(counter);
    const requests: RuntimeRequest[] = [
      { type: RuntimeRequestType.START_ACTOR, parentId: null, name: null },
      { type: RuntimeRequestType.FINALIZE_ACTOR, targetId: null },
      { type: RuntimeRequestType.WATCH_ACTOR, watcherId: null, targetId: "target" },
    ];
    for (const request of requests) {
      expect(() => reduce(state(), request)).toThrow(Error);
    }
  });

  it("keeps an empty or naturally exhausted runtime running until explicit shutdown", () => {
    const reduce = reducer(counter);
    expect(reduce(state(), null).state.phase).toBe(RuntimePhase.RUNNING);
    const exited = reduce(state({ worker: live() }), {
      type: RuntimeRequestType.FINALIZE_ACTOR,
      targetId: "worker",
    });
    const advanced = reduce(freeze(exited.state), null);
    expect(advanced.state.phase).toBe(RuntimePhase.RUNNING);
    const restarted = reduce(advanced.state, {
      type: RuntimeRequestType.START_ACTOR,
      name: "counter",
      parentId: null,
    });
    expect(restarted.response?.result.type).toBe(RuntimeResultType.SUCCESS);

    const stopping = reduce(state(), { type: RuntimeRequestType.STOP_RUNTIME });
    expect(stopping.state.phase).toBe(RuntimePhase.STOPPING);
    expect(reduce(freeze(stopping.state), null).state.phase).toBe(RuntimePhase.STOPPED);
  });

  it("closes admission on shutdown and waits for actor cleanup before stopping", () => {
    const reduce = reducer(counter);
    const stopping = reduce(state({ worker: live() }), { type: RuntimeRequestType.STOP_RUNTIME });
    expect(stopping.state.phase).toBe(RuntimePhase.STOPPING);
    expect(stopping.commands).toEqual([
      {
        sourceId: null,
        commands: [{ type: SystemCommandType.STOP, id: "worker" }],
      },
    ]);
    expect(stopping.state.instances.worker).toMatchObject({ phase: ActorPhase.RUNNING });
    const requests: RuntimeRequest[] = [
      { type: RuntimeRequestType.START_ACTOR, name: "counter", parentId: null },
      { type: RuntimeRequestType.STOP_RUNTIME },
    ];
    for (const request of requests) {
      expect(reduce(stopping.state, request).response?.result).toEqual({
        type: RuntimeResultType.FAILURE,
        reason: { type: RuntimeFailureReasonType.RUNTIME_STOPPING },
      });
    }

    const requested = reduce(stopping.state, {
      type: RuntimeRequestType.STOP_ACTOR,
      targetId: "worker",
    });
    const waiting = reduce(freeze(requested.state), null);
    expect(waiting.state.ready).toEqual([]);
    expect(reduce(freeze(waiting.state), null).state.phase).toBe(RuntimePhase.STOPPING);
    const reply = reduce(waiting.state, send("worker", "cleaned"));
    expect(reply.response?.result.type).toBe(RuntimeResultType.SUCCESS);
    const handled = reduce(freeze(reply.state), null);
    const exited = reduce(freeze(handled.state), {
      type: RuntimeRequestType.FINALIZE_ACTOR,
      targetId: "worker",
    });
    expect(exited.state.phase).toBe(RuntimePhase.STOPPING);
    const stopped = reduce(freeze(exited.state), null);
    expect(stopped.state.phase).toBe(RuntimePhase.STOPPED);
    for (const request of requests) {
      expect(reduce(stopped.state, request).response?.result).toEqual({
        type: RuntimeResultType.FAILURE,
        reason: { type: RuntimeFailureReasonType.RUNTIME_STOPPED },
      });
    }
  });
});
