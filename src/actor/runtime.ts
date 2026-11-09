import Crypto from "node:crypto";
import { lookup } from "../utility";

const MessageType = {
  NEXT: "NEXT",
  START: "START",
  STOP: "STOP",
} as const;

type NextMessage = {
  type: typeof MessageType.NEXT;
  sourceId: ActorId | null;
  data: any;
};

type StartMessage = {
  type: typeof MessageType.START;
  parentId: ActorId | null;
};

type StopMessage = {
  type: typeof MessageType.STOP;
};

type Message = NextMessage | StartMessage | StopMessage;

const SystemCommandType = {
  SEND: "system:send",
  START: "system:start",
  STOP: "system:stop",
  EXIT: "system:exit",
  WATCH: "system:watch",
} as const;

type SystemSendCommand = {
  type: typeof SystemCommandType.SEND;
  id: ActorId;
  message: any;
};

type SystemStartCommand = {
  type: typeof SystemCommandType.START;
  name: string | null;
};

type SystemStopCommand = {
  type: typeof SystemCommandType.STOP;
  id: ActorId;
};

type SystemExitCommand = {
  type: typeof SystemCommandType.EXIT;
};

type SystemWatchCommand = {
  type: typeof SystemCommandType.WATCH;
  id: ActorId;
};

type SystemCommand =
  | SystemSendCommand
  | SystemStartCommand
  | SystemStopCommand
  | SystemExitCommand
  | SystemWatchCommand;

const SystemMessageType = {
  STARTED: "system:started",
  STOPPED: "system:stopped",
} as const;

type SystemStartedMessage = {
  type: typeof SystemMessageType.STARTED;
  id: ActorId;
};

type SystemStoppedMessage = {
  type: typeof SystemMessageType.STOPPED;
  id: ActorId;
  termination: Termination;
};

type SystemMessage = SystemStartedMessage | SystemStoppedMessage;

interface ActorTransitionResult<S, C> {
  commands: C[];
  state: S;
}

interface ActorTransitionFunction<S, M, C> {
  (state: S, message: M): ActorTransitionResult<S, C>;
}

interface ActorDefinition<S, M, C> {
  initialState: S;
  on: ActorTransitionFunction<S, M, C>;
}

type ActorDefinitions<C = never> = Readonly<
  Record<string, ActorDefinition<any, any, SystemCommand | C>>
>;

const RuntimeResultType = {
  SUCCESS: "SUCCESS",
  FAILURE: "FAILURE",
} as const;

const RuntimeFailureReasonType = {
  RUNTIME_STOPPING: "RUNTIME_STOPPING",
  RUNTIME_STOPPED: "RUNTIME_STOPPED",
  UNKNOWN_DEFINITION: "UNKNOWN_DEFINITION",
  ACTOR_NOT_FOUND: "ACTOR_NOT_FOUND",
  ACTOR_STOPPED: "ACTOR_STOPPED",
  ACTOR_NOT_RUNNING: "ACTOR_NOT_RUNNING",
} as const;

type RuntimeStoppingReason = {
  type: typeof RuntimeFailureReasonType.RUNTIME_STOPPING;
};

type RuntimeStoppedReason = {
  type: typeof RuntimeFailureReasonType.RUNTIME_STOPPED;
};

type UnknownDefinitionReason = {
  type: typeof RuntimeFailureReasonType.UNKNOWN_DEFINITION;
  name: string;
};

type ActorNotFoundReason = {
  type: typeof RuntimeFailureReasonType.ACTOR_NOT_FOUND;
  id: ActorId;
};

type ActorStoppedReason = {
  type: typeof RuntimeFailureReasonType.ACTOR_STOPPED;
  id: ActorId;
};

type ActorNotRunningReason = {
  type: typeof RuntimeFailureReasonType.ACTOR_NOT_RUNNING;
  id: ActorId;
};

type RuntimeFailureReason =
  | RuntimeStoppingReason
  | RuntimeStoppedReason
  | UnknownDefinitionReason
  | ActorNotFoundReason
  | ActorStoppedReason
  | ActorNotRunningReason;

type RuntimeSuccessResult = {
  type: typeof RuntimeResultType.SUCCESS;
};

type RuntimeFailureResult = {
  type: typeof RuntimeResultType.FAILURE;
  reason: RuntimeFailureReason;
};

type RuntimeRequestResult = RuntimeSuccessResult | RuntimeFailureResult;

type ActorStartSuccessResult = {
  type: typeof RuntimeResultType.SUCCESS;
  id: ActorId;
};

type ActorStartResult = ActorStartSuccessResult | RuntimeFailureResult;

const TerminationType = {
  SUCCESS: "SUCCESS",
  FAILURE: "FAILURE",
} as const;

type SuccessfulTermination = {
  type: typeof TerminationType.SUCCESS;
};

type FailedTermination = {
  type: typeof TerminationType.FAILURE;
  error: Error;
};

type Termination = SuccessfulTermination | FailedTermination;

function normalizeError(error: unknown): Error {
  return error instanceof Error
    ? error
    : new Error("Actor threw a non-Error value", { cause: error });
}

const ActorPhase = {
  RUNNING: "RUNNING",
  STOPPING: "STOPPING",
  STOPPED: "STOPPED",
} as const;

type ActorPhase = (typeof ActorPhase)[keyof typeof ActorPhase];

type LiveActorInstance<S> = {
  phase: typeof ActorPhase.RUNNING | typeof ActorPhase.STOPPING;
  name: string;
  mailbox: Message[];
  state: S;
  watchers: ActorId[];
};

type StoppedActorInstance = {
  phase: typeof ActorPhase.STOPPED;
  termination: Termination;
};

type ActorInstance<S> = LiveActorInstance<S> | StoppedActorInstance;

type ActorId = string;

const RuntimePhase = {
  RUNNING: "RUNNING",
  STOPPING: "STOPPING",
  STOPPED: "STOPPED",
} as const;

type RuntimePhase = (typeof RuntimePhase)[keyof typeof RuntimePhase];

interface RuntimeState {
  phase: RuntimePhase;
  instances: Record<ActorId, ActorInstance<unknown>>;
  ready: ActorId[];
}

const RuntimeRequestType = {
  SEND_MESSAGE: "SEND_MESSAGE",
  START_ACTOR: "START_ACTOR",
  STOP_ACTOR: "STOP_ACTOR",
  FINALIZE_ACTOR: "FINALIZE_ACTOR",
  WATCH_ACTOR: "WATCH_ACTOR",
  STOP_RUNTIME: "STOP_RUNTIME",
} as const;

type SendMessageRequest = {
  type: typeof RuntimeRequestType.SEND_MESSAGE;
  sourceId: ActorId | null;
  recipientId: ActorId;
  message: any;
};

type StartActorRequest = {
  type: typeof RuntimeRequestType.START_ACTOR;
  parentId: ActorId | null;
  name: string | null;
};

type StopActorRequest = {
  type: typeof RuntimeRequestType.STOP_ACTOR;
  targetId: ActorId;
};

type FinalizeActorRequest = {
  type: typeof RuntimeRequestType.FINALIZE_ACTOR;
  targetId: ActorId | null;
};

type WatchActorRequest = {
  type: typeof RuntimeRequestType.WATCH_ACTOR;
  watcherId: ActorId | null;
  targetId: ActorId;
};

type StopRuntimeRequest = {
  type: typeof RuntimeRequestType.STOP_RUNTIME;
};

type RuntimeRequest =
  | SendMessageRequest
  | StartActorRequest
  | StopActorRequest
  | FinalizeActorRequest
  | WatchActorRequest
  | StopRuntimeRequest;

type SendMessageResponse = {
  type: typeof RuntimeRequestType.SEND_MESSAGE;
  result: RuntimeRequestResult;
};

type StartActorResponse = {
  type: typeof RuntimeRequestType.START_ACTOR;
  result: ActorStartResult;
};

type StopActorResponse = {
  type: typeof RuntimeRequestType.STOP_ACTOR;
  result: RuntimeRequestResult;
};

type FinalizeActorResponse = {
  type: typeof RuntimeRequestType.FINALIZE_ACTOR;
  result: RuntimeRequestResult;
};

type WatchActorResponse = {
  type: typeof RuntimeRequestType.WATCH_ACTOR;
  result: RuntimeRequestResult;
};

type StopRuntimeResponse = {
  type: typeof RuntimeRequestType.STOP_RUNTIME;
  result: RuntimeRequestResult;
};

type RuntimeResponse =
  | SendMessageResponse
  | StartActorResponse
  | StopActorResponse
  | FinalizeActorResponse
  | WatchActorResponse
  | StopRuntimeResponse;

type RuntimeResponseFor<R extends RuntimeRequest> = Extract<RuntimeResponse, { type: R["type"] }>;

interface CommandBatch<C> {
  sourceId: ActorId | null;
  commands: C[];
}

interface Reduction<C = never> {
  state: RuntimeState;
  response: RuntimeResponse | null;
  commands: CommandBatch<SystemCommand | C>[];
}

interface Reducer<C = never> {
  (state: RuntimeState, request: RuntimeRequest | null): Reduction<C>;
}

function succeed(
  state: RuntimeState,
  type: Exclude<RuntimeRequest["type"], typeof RuntimeRequestType.START_ACTOR>,
  commands: CommandBatch<SystemCommand>[] = [],
): Reduction {
  return {
    state,
    response: { type, result: { type: RuntimeResultType.SUCCESS } },
    commands,
  };
}

function deny(
  state: RuntimeState,
  type: RuntimeRequest["type"],
  reason: RuntimeFailureReason,
): Reduction {
  return {
    state,
    response: { type, result: { type: RuntimeResultType.FAILURE, reason } },
    commands: [],
  };
}

function buildStoppedNotification(
  id: ActorId,
  watcherId: ActorId,
  termination: Termination,
): SystemSendCommand {
  return {
    type: SystemCommandType.SEND,
    id: watcherId,
    message: { type: SystemMessageType.STOPPED, id, termination },
  };
}

function advance<C>(state: RuntimeState, definitions: ActorDefinitions<C>): Reduction<C> {
  const [id, ...remaining] = state.ready;

  if (id === undefined) {
    if (
      state.phase === RuntimePhase.STOPPING &&
      Object.values(state.instances).every((actor) => actor.phase === ActorPhase.STOPPED)
    ) {
      return { state: { ...state, phase: RuntimePhase.STOPPED }, response: null, commands: [] };
    }
    return { state, response: null, commands: [] };
  }

  const actor = lookup(state.instances, id);

  if (!actor || actor.phase === ActorPhase.STOPPED) {
    throw new Error(`ready actor '${id}' is not live`);
  }

  const message = actor.mailbox[0];

  if (message === undefined) {
    throw new Error(`ready actor '${id}' has an empty mailbox`);
  }

  const definition = lookup(definitions, actor.name);

  if (!definition) {
    throw new Error(`unknown actor definition of name '${actor.name}'`);
  }

  let transition: ActorTransitionResult<unknown, SystemCommand | C>;

  try {
    transition = definition.on(actor.state, message);
  } catch (error) {
    const termination: Termination = {
      type: TerminationType.FAILURE,
      error: normalizeError(error),
    };

    return {
      state: {
        ...state,
        instances: {
          ...state.instances,
          [id]: { phase: ActorPhase.STOPPED, termination },
        },
        ready: remaining,
      },
      response: null,
      commands: [
        {
          sourceId: null,
          commands: actor.watchers.map((watcherId) =>
            buildStoppedNotification(id, watcherId, termination),
          ),
        },
      ],
    };
  }

  const mailbox = actor.mailbox.slice(1);

  return {
    state: {
      ...state,
      instances: {
        ...state.instances,
        [id]: {
          ...actor,
          state: transition.state,
          mailbox,
        },
      },
      ready: mailbox.length === 0 ? remaining : [...remaining, id],
    },
    response: null,
    commands: [{ sourceId: id, commands: transition.commands }],
  };
}

function startActor<C>(
  state: RuntimeState,
  request: StartActorRequest,
  definitions: ActorDefinitions<C>,
): Reduction {
  let name = request.name;

  if (request.parentId === null) {
    if (name === null) {
      throw new Error("START_ACTOR requires a name when parentId is null");
    }
  } else {
    const parent = lookup(state.instances, request.parentId);
    if (!parent) {
      return deny(state, request.type, {
        type: RuntimeFailureReasonType.ACTOR_NOT_FOUND,
        id: request.parentId,
      });
    }
    if (parent.phase === ActorPhase.STOPPED) {
      return deny(state, request.type, {
        type: RuntimeFailureReasonType.ACTOR_STOPPED,
        id: request.parentId,
      });
    }
    name ??= parent.name;
  }

  if (state.phase === RuntimePhase.STOPPING) {
    return deny(state, request.type, { type: RuntimeFailureReasonType.RUNTIME_STOPPING });
  }
  if (state.phase === RuntimePhase.STOPPED) {
    return deny(state, request.type, { type: RuntimeFailureReasonType.RUNTIME_STOPPED });
  }

  const definition = lookup(definitions, name);

  if (!definition) {
    return deny(state, request.type, {
      type: RuntimeFailureReasonType.UNKNOWN_DEFINITION,
      name,
    });
  }

  const id = Crypto.randomUUID();

  return {
    state: {
      ...state,
      instances: {
        ...state.instances,
        [id]: {
          phase: ActorPhase.RUNNING,
          name,
          mailbox: [{ type: MessageType.START, parentId: request.parentId }],
          state: definition.initialState,
          watchers: request.parentId === null ? [] : [request.parentId],
        },
      },
      ready: [...state.ready, id],
    },
    response: { type: request.type, result: { type: RuntimeResultType.SUCCESS, id } },
    commands: [
      {
        sourceId: null,
        commands:
          request.parentId === null
            ? []
            : [
                {
                  type: SystemCommandType.SEND,
                  id: request.parentId,
                  message: { type: SystemMessageType.STARTED, id },
                },
              ],
      },
    ],
  };
}

function sendMessage(state: RuntimeState, request: SendMessageRequest): Reduction {
  const actor = lookup(state.instances, request.recipientId);
  if (!actor) {
    return deny(state, request.type, {
      type: RuntimeFailureReasonType.ACTOR_NOT_FOUND,
      id: request.recipientId,
    });
  }
  if (actor.phase === ActorPhase.STOPPED) {
    return deny(state, request.type, {
      type: RuntimeFailureReasonType.ACTOR_STOPPED,
      id: request.recipientId,
    });
  }
  return succeed(
    {
      ...state,
      instances: {
        ...state.instances,
        [request.recipientId]: {
          ...actor,
          mailbox: [
            ...actor.mailbox,
            {
              type: MessageType.NEXT,
              sourceId: request.sourceId,
              data: request.message,
            },
          ],
        },
      },
      ready: actor.mailbox.length === 0 ? [...state.ready, request.recipientId] : state.ready,
    },
    request.type,
  );
}

function stopActor(state: RuntimeState, request: StopActorRequest): Reduction {
  const actor = lookup(state.instances, request.targetId);
  if (!actor) {
    return deny(state, request.type, {
      type: RuntimeFailureReasonType.ACTOR_NOT_FOUND,
      id: request.targetId,
    });
  }
  if (actor.phase !== ActorPhase.RUNNING) {
    return deny(state, request.type, {
      type: RuntimeFailureReasonType.ACTOR_NOT_RUNNING,
      id: request.targetId,
    });
  }
  return succeed(
    {
      ...state,
      instances: {
        ...state.instances,
        [request.targetId]: {
          ...actor,
          phase: ActorPhase.STOPPING,
          mailbox: [...actor.mailbox, { type: MessageType.STOP }],
        },
      },
      ready: actor.mailbox.length === 0 ? [...state.ready, request.targetId] : state.ready,
    },
    request.type,
  );
}

function finalizeActor(state: RuntimeState, request: FinalizeActorRequest): Reduction {
  const { targetId } = request;
  if (targetId === null) {
    throw new Error("FINALIZE_ACTOR requires a targetId");
  }

  const actor = lookup(state.instances, targetId);
  if (!actor) {
    return deny(state, request.type, {
      type: RuntimeFailureReasonType.ACTOR_NOT_FOUND,
      id: targetId,
    });
  }
  if (actor.phase === ActorPhase.STOPPED) {
    return deny(state, request.type, {
      type: RuntimeFailureReasonType.ACTOR_STOPPED,
      id: targetId,
    });
  }
  const termination: Termination = { type: TerminationType.SUCCESS };
  return succeed(
    {
      ...state,
      instances: {
        ...state.instances,
        [targetId]: { phase: ActorPhase.STOPPED, termination },
      },
      ready: state.ready.filter((id) => id !== targetId),
    },
    request.type,
    [
      {
        sourceId: null,
        commands: actor.watchers.map((watcherId) =>
          buildStoppedNotification(targetId, watcherId, termination),
        ),
      },
    ],
  );
}

function watchActor(state: RuntimeState, request: WatchActorRequest): Reduction {
  if (request.watcherId === null) {
    throw new Error("WATCH_ACTOR requires a watcherId");
  }

  const watcher = lookup(state.instances, request.watcherId);

  if (!watcher) {
    return deny(state, request.type, {
      type: RuntimeFailureReasonType.ACTOR_NOT_FOUND,
      id: request.watcherId,
    });
  }

  if (watcher.phase === ActorPhase.STOPPED) {
    return deny(state, request.type, {
      type: RuntimeFailureReasonType.ACTOR_STOPPED,
      id: request.watcherId,
    });
  }

  const target = lookup(state.instances, request.targetId);

  if (!target) {
    return deny(state, request.type, {
      type: RuntimeFailureReasonType.ACTOR_NOT_FOUND,
      id: request.targetId,
    });
  }

  if (target.phase === ActorPhase.STOPPED) {
    return succeed(state, request.type, [
      {
        sourceId: null,
        commands: [
          buildStoppedNotification(request.targetId, request.watcherId, target.termination),
        ],
      },
    ]);
  }
  if (target.watchers.includes(request.watcherId)) {
    return succeed(state, request.type);
  }
  return succeed(
    {
      ...state,
      instances: {
        ...state.instances,
        [request.targetId]: {
          ...target,
          watchers: [...target.watchers, request.watcherId],
        },
      },
    },
    request.type,
  );
}

function stopRuntime(state: RuntimeState, request: StopRuntimeRequest): Reduction {
  if (state.phase === RuntimePhase.STOPPING) {
    return deny(state, request.type, { type: RuntimeFailureReasonType.RUNTIME_STOPPING });
  }
  if (state.phase === RuntimePhase.STOPPED) {
    return deny(state, request.type, { type: RuntimeFailureReasonType.RUNTIME_STOPPED });
  }
  const commands: SystemStopCommand[] = Object.entries(state.instances)
    .filter(([, actor]) => actor.phase === ActorPhase.RUNNING)
    .map(([id]) => ({ type: SystemCommandType.STOP, id }));
  return succeed({ ...state, phase: RuntimePhase.STOPPING }, request.type, [
    { sourceId: null, commands },
  ]);
}

const reducer: <C = never>(definitions: ActorDefinitions<C>) => Reducer<C> =
  (definitions) => (state, request) => {
    if (request === null) {
      return advance(state, definitions);
    }

    switch (request.type) {
      case RuntimeRequestType.START_ACTOR:
        return startActor(state, request, definitions);
      case RuntimeRequestType.SEND_MESSAGE:
        return sendMessage(state, request);
      case RuntimeRequestType.STOP_ACTOR:
        return stopActor(state, request);
      case RuntimeRequestType.FINALIZE_ACTOR:
        return finalizeActor(state, request);
      case RuntimeRequestType.WATCH_ACTOR:
        return watchActor(state, request);
      case RuntimeRequestType.STOP_RUNTIME:
        return stopRuntime(state, request);
    }
  };

export {
  reducer,
  ActorPhase,
  RuntimePhase,
  RuntimeRequestType,
  RuntimeResultType,
  RuntimeFailureReasonType,
  TerminationType,
  MessageType,
  SystemCommandType,
  SystemMessageType,
};

export type {
  ActorId,
  ActorDefinition,
  ActorDefinitions,
  ActorInstance,
  LiveActorInstance,
  StoppedActorInstance,
  ActorTransitionFunction,
  ActorTransitionResult,
  Termination,
  SuccessfulTermination,
  FailedTermination,
  RuntimeState,
  RuntimeRequest,
  RuntimeResponse,
  RuntimeResponseFor,
  RuntimeRequestResult,
  RuntimeSuccessResult,
  RuntimeFailureResult,
  RuntimeFailureReason,
  RuntimeStoppingReason,
  RuntimeStoppedReason,
  UnknownDefinitionReason,
  ActorNotFoundReason,
  ActorStoppedReason,
  ActorNotRunningReason,
  ActorStartResult,
  ActorStartSuccessResult,
  SendMessageRequest,
  StartActorRequest,
  StopActorRequest,
  FinalizeActorRequest,
  WatchActorRequest,
  StopRuntimeRequest,
  SendMessageResponse,
  StartActorResponse,
  StopActorResponse,
  FinalizeActorResponse,
  WatchActorResponse,
  StopRuntimeResponse,
  Message,
  NextMessage,
  StartMessage,
  StopMessage,
  SystemCommand,
  SystemSendCommand,
  SystemStartCommand,
  SystemStopCommand,
  SystemExitCommand,
  SystemWatchCommand,
  SystemMessage,
  SystemStartedMessage,
  SystemStoppedMessage,
  CommandBatch,
  Reduction,
  Reducer,
};
