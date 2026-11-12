import Crypto from "node:crypto";

import * as Kernel from "./kernel";

import {
  ActorDefinitions,
  ActorId,
  ActorPhase,
  ActorTerminationType,
  AdvanceResultType,
  Message,
  MessageType,
} from "./kernel";

import { sleep } from "../time";

export const Phase = {
  RUNNING: "RUNNING",
  SHUTTING_DOWN: "SHUTTING_DOWN",
  SHUTDOWN: "SHUTDOWN",
} as const;

export type Phase = (typeof Phase)[keyof typeof Phase];

export const CommandType = {
  EXIT: "system:exit",
  SEND_MESSAGE: "system:send-message",
  SHUTDOWN: "system:shutdown",
  START_ACTOR: "system:start-actor",
  STOP_ACTOR: "system:stop-actor",
} as const;

type ExitCommand = {
  type: typeof CommandType.EXIT;
};

type SendMessageCommand = {
  type: typeof CommandType.SEND_MESSAGE;
  recipientId: ActorId;
  message: Message;
};

type ShutdownCommand = {
  type: typeof CommandType.SHUTDOWN;
};

type StartActorCommand = {
  type: typeof CommandType.START_ACTOR;
  name: string | null;
  data: any;
};

type StopActorCommand = {
  type: typeof CommandType.STOP_ACTOR;
  actorId: ActorId;
};

type Command =
  | ExitCommand
  | SendMessageCommand
  | ShutdownCommand
  | StartActorCommand
  | StopActorCommand;

type CommandEnvelope<C> = [senderId: ActorId | null, content: C];

interface CommandExecutor<C> {
  (command: CommandEnvelope<C>): Promise<C | Command | void>;
}

type State<C> = {
  phase: Phase;
  shouldAdvance: boolean;

  kernel: Kernel.State;
  commands: CommandEnvelope<C | Command>[];
};

type Options<C> = {
  initialState: State<C>;
  definitions: ActorDefinitions;
  executor: CommandExecutor<C>;
  onExecutorError?: (error: unknown) => void;
};

interface System<C> {
  enqueue(command: Command | C): void;

  step(): boolean;
  batch(steps: number): Promise<boolean>;

  inspect(): State<C>;
}

const isSystemCommand = (command: any): command is Command => {
  if (typeof command !== "object") {
    return false;
  }

  if (!Object.hasOwn(command, "type")) {
    return false;
  }

  if (!Object.values(CommandType).includes(command.type)) {
    return false;
  }

  return true;
};

const advance =
  (definitions: ActorDefinitions) =>
  <C>(currentState: State<C>): State<C> => {
    if (!currentState.shouldAdvance) {
      throw new Error("illegal state error");
    }

    const result = Kernel.advance(definitions)(currentState.kernel);

    if (result.type === AdvanceResultType.EMPTY) {
      return { ...currentState, shouldAdvance: false };
    }

    if (result.type === AdvanceResultType.ERROR) {
      const { actorId, error } = result;
      const termination = {
        type: ActorTerminationType.ERROR,
        error: error.name,
      };

      const kernel = Kernel.stopActor({ actorId, termination })(currentState.kernel);

      return { ...currentState, kernel, shouldAdvance: false };
    }

    const { actorId: senderId, state: kernel, commands } = result;

    return {
      ...currentState,
      shouldAdvance: false,
      kernel,
      commands: [
        ...currentState.commands,
        ...commands.map<CommandEnvelope<C | Command>>((content) => [senderId, content]),
      ],
    };
  };

const executeSystemCommand =
  <C>(definitions: ActorDefinitions) =>
  (command: CommandEnvelope<Command>, commands: CommandEnvelope<C | Command>[]) =>
  (currentState: State<C>): State<C> => {
    const shouldAdvance = true;
    const [senderId, content] = command;

    if (content.type === CommandType.EXIT) {
      if (senderId === null) {
        throw new Error("exit command sent with empty actor ID");
      }

      const termination = { type: ActorTerminationType.NORMAL };
      const kernel = Kernel.stopActor({ actorId: senderId, termination })(currentState.kernel);

      return { ...currentState, commands, kernel, shouldAdvance };
    }

    if (content.type === CommandType.SEND_MESSAGE) {
      const kernel = Kernel.sendMessage(content)(currentState.kernel);

      return {
        ...currentState,
        shouldAdvance,
        kernel,
        commands,
      };
    }

    if (content.type === CommandType.SHUTDOWN) {
      if (currentState.phase !== Phase.RUNNING) {
        return { ...currentState, shouldAdvance, commands };
      }

      const message = { type: MessageType.STOP };
      const kernel = Kernel.broadcastMessage(message)(currentState.kernel);

      return {
        ...currentState,
        phase: Phase.SHUTTING_DOWN,
        shouldAdvance,
        kernel,
        commands,
      };
    }

    if (content.type === CommandType.START_ACTOR) {
      if (currentState.phase !== Phase.RUNNING) {
        return { ...currentState, commands, shouldAdvance };
      }

      const data = content.data;
      let name = content.name;

      if (name === null) {
        if (senderId === null) {
          throw new Error("can't start actor without definition name and parent ID");
        }

        const instance = currentState.kernel.instances[senderId]!!;

        if (instance.phase === ActorPhase.STOPPED) {
          throw new Error("can't start a fork of a stopped parent!");
        }

        name = instance.name;
      }

      const childId = Crypto.randomUUID();
      const kernel = Kernel.startActor(definitions)({ name, childId, data, parentId: senderId })(
        currentState.kernel,
      );

      return {
        ...currentState,
        shouldAdvance,
        kernel,
        commands,
      };
    }

    if (content.type === CommandType.STOP_ACTOR) {
      const message = { type: MessageType.STOP };
      const kernel = Kernel.sendMessage({ recipientId: content.actorId, message })(
        currentState.kernel,
      );

      return {
        ...currentState,
        shouldAdvance,
        kernel,
        commands,
      };
    }

    content satisfies never;
    throw new Error(`unexpected command type '${(command as any).type}'`);
  };

const execute =
  <C>(definitions: ActorDefinitions, executor: CommandExecutor<C>) =>
  (currentState: State<C>): [State<C>, Promise<C | Command | void> | null] => {
    if (currentState.shouldAdvance) {
      throw new Error("illegal state error");
    }

    const shouldAdvance = true;
    const [command, ...commands] = currentState.commands;

    if (!command) {
      return [{ ...currentState, commands, shouldAdvance }, null];
    }

    const [senderId, content] = command;

    if (isSystemCommand(content)) {
      return [
        executeSystemCommand<C>(definitions)([senderId, content], commands)(currentState),
        null,
      ];
    }

    return [{ ...currentState, commands, shouldAdvance }, executor([senderId, content])];
  };

export const system = <C>({
  initialState,
  executor,
  definitions,
  onExecutorError,
}: Options<C>): System<C> => {
  let currentState = initialState;
  let pendingTasks = 0;

  const enqueue = (command: C | Command) => {
    currentState = {
      ...currentState,
      commands: [...currentState.commands, [null, command]],
    };
  };

  const shouldShutdown = () => {
    return (
      currentState.phase === Phase.SHUTTING_DOWN &&
      !Kernel.hasRunningInstances(currentState.kernel) &&
      pendingTasks === 0 &&
      currentState.commands.length === 0
    );
  };

  const step = (): boolean => {
    if (currentState.phase === Phase.SHUTDOWN) {
      return false;
    }

    if (currentState.shouldAdvance) {
      currentState = advance(definitions)(currentState);
    } else {
      const [updatedState, task] = execute(definitions, executor)(currentState);

      if (task !== null) {
        pendingTasks += 1;

        task
          .then((result) => {
            if (result !== undefined) {
              enqueue(result);
            }
          })
          .catch((error) => onExecutorError?.(error))
          .finally(() => {
            pendingTasks -= 1;
          });
      }

      currentState = updatedState;
    }

    if (shouldShutdown()) {
      currentState = { ...currentState, phase: Phase.SHUTDOWN };
    }

    return true;
  };

  const batch = async (steps: number): Promise<boolean> => {
    for (let i = 0; i < steps; i++) {
      if (!step()) {
        return false;
      }
    }

    await sleep(0);

    return true;
  };

  return {
    enqueue,

    step,
    batch,

    inspect: () => currentState,
  };
};
