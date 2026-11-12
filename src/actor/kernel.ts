export type ActorId = string;

export const MessageType = {
  NEXT: "NEXT",
  START: "START",
  STOP: "STOP",
} as const;

export type NextMessage = {
  type: typeof MessageType.NEXT;
  sourceId: ActorId | null;
  data: any;
};

export type StartMessage = {
  type: typeof MessageType.START;
  childId: ActorId;
  parentId: ActorId | null;
  data: unknown;
};

export type StopMessage = {
  type: typeof MessageType.STOP;
};

export type Message = NextMessage | StartMessage | StopMessage;

export const ActorPhase = {
  RUNNING: "RUNNING",
  STOPPED: "STOPPED",
} as const;

export type ActorPhase = (typeof ActorPhase)[keyof typeof ActorPhase];

export const ActorTerminationType = {
  NORMAL: "NORMAL",
  ERROR: "ERROR",
} as const;

export type NormalActorTermination = {
  type: typeof ActorTerminationType.NORMAL;
};

export type ErrorActorTermination = {
  type: typeof ActorTerminationType.ERROR;
  error: string;
};

export type ActorTermination = NormalActorTermination | ErrorActorTermination;

export type RunningActorInstance<S> = {
  name: string;
  phase: typeof ActorPhase.RUNNING;
  state: S;
  mailbox: Message[];
};

export type StoppedActorInstance = {
  phase: typeof ActorPhase.STOPPED;
  termination: ActorTermination;
};

export type ActorInstance<S> = RunningActorInstance<S> | StoppedActorInstance;

export type StartActorRequest = {
  name: string;
  childId: ActorId;
  parentId: ActorId | null;
  data: unknown;
};

export type SendMessageRequest = {
  recipientId: string;
  message: Message;
};

export type ActorTransitionResult<S, C> = {
  commands: C[];
  state: S;
};

export interface ActorTransitionFunction<S, M, C> {
  (state: S, message: M): ActorTransitionResult<S, C>;
}

export type ActorDefinition<S, M, C> = {
  initialState: S;
  on: ActorTransitionFunction<S, M, C>;
};

export type ActorDefinitions = Readonly<Record<string, ActorDefinition<any, any, any>>>;
export type ActorInstances = Readonly<Record<ActorId, ActorInstance<any>>>;

export type State = {
  instances: ActorInstances;
  ready: ActorId[];
};

export const AdvanceResultType = {
  EMPTY: "EMPTY",
  ERROR: "ERROR",
  SUCCESS: "SUCCESS",
} as const;

export type EmptyAdvanceResult = {
  type: typeof AdvanceResultType.EMPTY;
};

export type ErrorAdvanceResult = {
  type: typeof AdvanceResultType.ERROR;
  actorId: ActorId;
  error: Error;
};

export type SuccessAdvanceResult = {
  type: typeof AdvanceResultType.SUCCESS;
  actorId: ActorId;
  state: State;
  commands: any[];
};

export type AdvanceResult = EmptyAdvanceResult | ErrorAdvanceResult | SuccessAdvanceResult;

export const SendMessageResultType = {
  ACCEPTED: "ACCEPTED",
  REJECTED: "REJECTED",
} as const;

export type StopActorRequest = {
  actorId: ActorId;
  termination: ActorTermination;
};

export const getActorDefinition = (
  definitions: ActorDefinitions,
  name: string,
): ActorDefinition<any, any, any> => {
  if (!Object.hasOwn(definitions, name)) {
    throw new Error(`unknown actor definition '${name}'`);
  }

  return definitions[name]!!;
};

export const getActorInstance = (instances: ActorInstances, id: ActorId): ActorInstance<any> => {
  if (!Object.hasOwn(instances, id)) {
    throw new Error(`actor instance with ID '${id}' not found`);
  }

  return instances[id]!!;
};

export const advance =
  (definitions: ActorDefinitions) =>
  (currentState: State): AdvanceResult => {
    const [actorId, ...ready] = currentState.ready;

    if (!actorId) {
      return { type: AdvanceResultType.EMPTY };
    }

    const instance = getActorInstance(currentState.instances, actorId);

    if (instance.phase === ActorPhase.STOPPED) {
      throw new Error(`can't advance actor in phase '${instance.phase}'`);
    }

    const [message, ...mailbox] = instance.mailbox;

    if (!message) {
      throw new Error("ready actor can't have an empty mailbox");
    }

    const definition = getActorDefinition(definitions, instance.name);

    try {
      const result = definition.on(instance.state, message);

      return {
        type: AdvanceResultType.SUCCESS,
        actorId,
        state: {
          instances: {
            ...currentState.instances,
            [actorId]: {
              ...instance,
              mailbox,
              state: result.state,
            },
          },
          ready: mailbox.length === 0 ? ready : [...ready, actorId],
        },
        commands: result.commands,
      };
    } catch (error) {
      return {
        type: AdvanceResultType.ERROR,
        actorId,
        error: error as Error,
      };
    }
  };

export const sendMessage =
  ({ recipientId, message }: SendMessageRequest) =>
  (currentState: State): State => {
    const instance = getActorInstance(currentState.instances, recipientId);

    if (instance.phase === ActorPhase.STOPPED) {
      return currentState;
    }

    return {
      instances: {
        ...currentState.instances,
        [recipientId]: {
          ...instance,
          mailbox: [...instance.mailbox, message],
        },
      },
      ready: currentState.ready.includes(recipientId)
        ? currentState.ready
        : [...currentState.ready, recipientId],
    };
  };

export const broadcastMessage =
  (message: Message) =>
  (currentState: State): State => {
    return Object.keys(currentState.instances).reduce(
      (state, recipientId) => sendMessage({ recipientId, message })(state),
      currentState,
    );
  };

export const startActor =
  (definitions: ActorDefinitions) =>
  ({ name, childId, parentId, data }: StartActorRequest) =>
  (currentState: State): State => {
    const definition = getActorDefinition(definitions, name);

    if (Object.hasOwn(currentState.instances, childId)) {
      throw new Error(`duplicate actor ID '${childId}'`);
    }

    return {
      instances: {
        ...currentState.instances,
        [childId]: {
          name,
          phase: ActorPhase.RUNNING,
          state: definition.initialState,
          mailbox: [{ type: MessageType.START, childId, parentId, data }],
        },
      },
      ready: [...currentState.ready, childId],
    };
  };

export const stopActor =
  ({ actorId, termination }: StopActorRequest) =>
  (currentState: State): State => {
    const instance = getActorInstance(currentState.instances, actorId);

    if (instance.phase === ActorPhase.STOPPED) {
      throw new Error(`actor with ID '${actorId}' has already stopped`);
    }

    return {
      instances: {
        ...currentState.instances,
        [actorId]: {
          phase: ActorPhase.STOPPED,
          termination,
        },
      },
      ready: currentState.ready.filter((id) => id !== actorId),
    };
  };

export const hasRunningInstances = (state: State): boolean => {
  return Object.values(state.instances).some((x) => x.phase === ActorPhase.RUNNING);
};
