import { expectTypeOf } from "vitest";
import {
  createDriver,
  reducer,
  RuntimePhase,
  RuntimeRequestType,
  RuntimeResultType,
  SystemCommandType,
  type ActorDefinition,
  type ActorDefinitions,
  type ActorId,
  type CommandBatch,
  type Driver,
  type Message,
  type Reducer,
  type RuntimeRequest,
  type RuntimeResponse,
  type SendMessageRequest,
  type SendMessageResponse,
  type StartActorResponse,
  type StopActorRequest,
  type StopActorResponse,
  type SystemCommand,
} from "../src/actor";

type ReadFile = { type: "read-file"; path: string };
type Log = { type: "log"; text: string };
type AppCommand = ReadFile | Log;

const reader: ActorDefinition<number, Message, SystemCommand | ReadFile> = {
  initialState: 0,
  on: (state) => ({ state, commands: [{ type: "read-file", path: "input.txt" }] }),
};
const logger: ActorDefinition<string[], { type: "LOG"; text: string }, Log> = {
  initialState: [],
  on: (state, message) => ({ state, commands: [{ type: "log", text: message.text }] }),
};
const definitions = { reader, logger } satisfies ActorDefinitions<AppCommand>;
const reduce = reducer<AppCommand>(definitions);
expectTypeOf(reduce).toEqualTypeOf<Reducer<AppCommand>>();

const reduction = reduce({ phase: RuntimePhase.RUNNING, instances: {}, ready: [] }, null);
expectTypeOf(reduction.commands).toEqualTypeOf<CommandBatch<SystemCommand | AppCommand>[]>();

const driver = createDriver(reduce, {
  execute: async (command) => {
    expectTypeOf(command).toEqualTypeOf<AppCommand>();
    return null;
  },
  onResponse: (result) => {
    expectTypeOf(result.command).toEqualTypeOf<SystemCommand | NoInfer<AppCommand> | null>();
  },
  onExecutorError: (failure) => {
    expectTypeOf(failure.command).toEqualTypeOf<AppCommand>();
  },
});
expectTypeOf(driver).toEqualTypeOf<Driver<AppCommand>>();

declare const unsupported: ActorDefinition<null, Message, { type: "fetch" }>;
declare const malformed: ActorDefinition<null, Message, { type: "read-file"; path: number }>;
// @ts-expect-error Actor commands must belong to the application union.
reducer<AppCommand>({ unsupported });
// @ts-expect-error A known command must have the correct payload.
reducer<AppCommand>({ malformed });

createDriver(reduce, {
  // @ts-expect-error An executor must accept every application command.
  execute: async (_command: ReadFile) => null,
});
createDriver(reduce, {
  // @ts-expect-error Driver options cannot replace the reducer's command type.
  execute: async (_command: { type: "fetch" }) => null,
});

const systemDefinitions: ActorDefinitions = {
  worker: {
    initialState: null,
    on: (state) => ({ state, commands: [{ type: SystemCommandType.EXIT }] }),
  },
};
const systemReduce = reducer<never>(systemDefinitions);
expectTypeOf(systemReduce).toEqualTypeOf<Reducer>();
expectTypeOf(createDriver(systemReduce)).toEqualTypeOf<Driver<never>>();
expectTypeOf(reducer({})).toEqualTypeOf<Reducer>();

const startReceipt = driver.request({
  type: RuntimeRequestType.START_ACTOR,
  name: "reader",
  parentId: null,
});
expectTypeOf(startReceipt).toEqualTypeOf<Promise<StartActorResponse>>();
startReceipt.then((response) => {
  if (response.result.type === RuntimeResultType.SUCCESS) {
    expectTypeOf(response.result.id).toEqualTypeOf<ActorId>();
  }
});

declare const request: SendMessageRequest | StopActorRequest;
expectTypeOf(driver.request(request)).toEqualTypeOf<
  Promise<SendMessageResponse | StopActorResponse>
>();
declare const runtimeRequest: RuntimeRequest;
expectTypeOf(driver.request(runtimeRequest)).toEqualTypeOf<Promise<RuntimeResponse>>();
