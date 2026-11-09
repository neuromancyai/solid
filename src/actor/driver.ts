import {
  RuntimePhase,
  RuntimeRequestType,
  SystemCommandType,
  type ActorId,
  type CommandBatch,
  type Reducer,
  type RuntimeRequest,
  type RuntimeResponse,
  type RuntimeResponseFor,
  type RuntimeState,
  type SystemCommand,
} from "./runtime";

const DriverPhase = {
  RUNNING: "RUNNING",
  STOPPED: "STOPPED",
} as const;

type DriverPhase = (typeof DriverPhase)[keyof typeof DriverPhase];

type StepProgress = {
  phase: DriverPhase;
  state: RuntimeState;
  response: null;
};

type StepResponse<C = never> = {
  phase: DriverPhase;
  state: RuntimeState;
  request: RuntimeRequest;
  response: RuntimeResponse;
  sourceId: ActorId | null;
  command: SystemCommand | C | null;
};

type StepResult<C = never> = StepProgress | StepResponse<C>;

type ExecutorError<C> = {
  sourceId: ActorId | null;
  command: C;
  error: unknown;
};

type CommandExecutor<C> = (command: C, sourceId: ActorId | null) => Promise<RuntimeRequest | null>;

interface DriverOptions<C> {
  initialState?: RuntimeState;
  execute?: CommandExecutor<C>;
  onResponse?: (result: StepResponse<C>) => void;
  onExecutorError?: (failure: ExecutorError<C>) => void;
}

interface Driver<C> {
  request<R extends RuntimeRequest>(request: R): Promise<RuntimeResponseFor<R>>;
  inspect(): RuntimeState;
  step(): StepResult<C>;
  batch(maxSteps: number): Promise<DriverPhase>;
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}

type Pending<T> = ReturnType<typeof deferred<T>>;

const WorkType = {
  COMMAND: "COMMAND",
  REQUEST: "REQUEST",
} as const;

type CommandWork<C> = {
  type: typeof WorkType.COMMAND;
  sourceId: ActorId | null;
  command: SystemCommand | C;
};

type RequestWork<C> = {
  type: typeof WorkType.REQUEST;
  request: RuntimeRequest;
  origin: CommandWork<C> | null;
  receipt: Pending<RuntimeResponse> | null;
};

type Work<C> = RequestWork<C> | CommandWork<C>;

function isSystemCommand(command: unknown): command is SystemCommand {
  return (
    typeof command === "object" &&
    command !== null &&
    "type" in command &&
    Object.values<unknown>(SystemCommandType).includes(command.type)
  );
}

function convertSystemCommand(command: SystemCommand, sourceId: ActorId | null): RuntimeRequest {
  switch (command.type) {
    case SystemCommandType.SEND:
      return {
        type: RuntimeRequestType.SEND_MESSAGE,
        sourceId,
        recipientId: command.id,
        message: command.message,
      };
    case SystemCommandType.START:
      return { type: RuntimeRequestType.START_ACTOR, parentId: sourceId, name: command.name };
    case SystemCommandType.STOP:
      return { type: RuntimeRequestType.STOP_ACTOR, targetId: command.id };
    case SystemCommandType.EXIT:
      return { type: RuntimeRequestType.FINALIZE_ACTOR, targetId: sourceId };
    case SystemCommandType.WATCH:
      return { type: RuntimeRequestType.WATCH_ACTOR, watcherId: sourceId, targetId: command.id };
  }
}

function createDriver<C = never>(
  reduce: Reducer<C>,
  options: DriverOptions<NoInfer<C>> = {},
): Driver<C> {
  let state = options.initialState ?? { phase: RuntimePhase.RUNNING, instances: {}, ready: [] };

  const work: Work<C>[] = [];

  let processing = 0;
  let failed = false;
  let failure: unknown;

  function phase(): DriverPhase {
    return state.phase === RuntimePhase.STOPPED && work.length === 0 && processing === 0
      ? DriverPhase.STOPPED
      : DriverPhase.RUNNING;
  }

  function fail(error: unknown) {
    if (failed) {
      return;
    }

    failed = true;
    failure = error;

    for (const item of work) {
      if (item.type === WorkType.REQUEST) {
        item.receipt?.reject(error);
      }
    }

    work.length = 0;
  }

  function append(batches: CommandBatch<SystemCommand | C>[]) {
    for (const batch of batches) {
      for (const command of batch.commands) {
        work.push({ type: WorkType.COMMAND, sourceId: batch.sourceId, command });
      }
    }
  }

  function commit(request: RuntimeRequest | null) {
    const reduction = reduce(state, request);
    state = reduction.state;
    append(reduction.commands);
    return reduction.response;
  }

  function applyRequest(request: RuntimeRequest, origin: CommandWork<C> | null): StepResponse<C> {
    const response = commit(request);
    if (response === null || response.type !== request.type) {
      throw new Error("reducer returned no matching response to a request");
    }
    return {
      phase: phase(),
      state,
      request,
      response,
      sourceId: origin?.sourceId ?? null,
      command: origin === null ? null : origin.command,
    };
  }

  function execute(item: CommandWork<C>) {
    if (!options.execute) {
      throw new Error("no interpreter supplied for a custom command");
    }

    processing++;

    let output: ReturnType<CommandExecutor<C>>;

    try {
      output = options.execute(item.command as C, item.sourceId);
    } catch (error) {
      output = Promise.reject(error);
    }

    void output
      .then(
        (request) => {
          processing--;
          if (failed) {
            return;
          }

          if (request !== null) {
            work.push({ type: WorkType.REQUEST, request, origin: item, receipt: null });
          }
        },
        (error) => {
          processing--;
          options.onExecutorError?.({
            sourceId: item.sourceId,
            command: item.command as C,
            error,
          });
        },
      )
      .catch(fail);
  }

  function step(): StepResult<C> {
    if (failed) {
      throw failure;
    }

    const item = work.shift();
    let result: StepResult<C>;

    try {
      if (item?.type === WorkType.REQUEST) {
        result = applyRequest(item.request, item.origin);
        item.receipt?.resolve(result.response);
      } else if (item?.type === WorkType.COMMAND && isSystemCommand(item.command)) {
        result = applyRequest(convertSystemCommand(item.command, item.sourceId), item);
      } else {
        if (item?.type === WorkType.COMMAND) {
          execute(item);
        } else {
          commit(null);
        }
        result = { phase: phase(), state, response: null };
      }
    } catch (error) {
      if (item?.type === WorkType.REQUEST) {
        item.receipt?.reject(error);
      }
      throw error;
    }

    if (result.response !== null) {
      options.onResponse?.(result);
    }
    return result;
  }

  async function batch(maxSteps: number): Promise<DriverPhase> {
    if (failed) {
      throw failure;
    }
    if (!Number.isSafeInteger(maxSteps) || maxSteps < 0) {
      throw new RangeError("maxSteps must be a nonnegative safe integer");
    }
    if (maxSteps === 0 || phase() === DriverPhase.STOPPED) {
      return phase();
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    if (failed) {
      throw failure;
    }
    for (let count = 0; count < maxSteps && phase() !== DriverPhase.STOPPED; count++) {
      step();
    }
    return phase();
  }

  return {
    request<R extends RuntimeRequest>(request: R) {
      if (failed) {
        return Promise.reject(failure);
      }
      const receipt = deferred<RuntimeResponse>();
      work.push({ type: WorkType.REQUEST, request, origin: null, receipt });
      // Each receipt receives the response to its own queued request.
      return receipt.promise as Promise<RuntimeResponseFor<R>>;
    },
    inspect: () => state,
    step,
    batch,
  };
}

export { createDriver, DriverPhase };
export type {
  Driver,
  DriverOptions,
  CommandExecutor,
  ExecutorError,
  StepResult,
  StepResponse,
  StepProgress,
};
