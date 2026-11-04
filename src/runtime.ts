import type { Actor } from './definition';
import { getActorTarget, getDefinition, type Definition } from './internal';
import type { ErrorData, Message } from './message';
import { createReference, getReference, type ActorRef } from './reference';
import { copyData, validateValue, type Data, type Schema } from './schema';

export type RuntimeStatus = 'running' | 'stopping' | 'closed' | 'failed';
export interface RuntimeOptions { readonly maxStepsPerTurn?: number; readonly now?: number }
export interface RuntimeStartOptions<M> { readonly id?: string; readonly message?: M }
export interface RunOptions { readonly maxSteps?: number }
export interface RunResult { readonly processed: number; readonly pending: number; readonly exhausted: boolean }
export type SendResult = { readonly status: 'accepted' | 'rejected' };
export interface StopOptions { readonly gracePeriodMs?: number }
export interface StopReport { readonly status: 'completed' | 'forced'; readonly forcedActorIds: readonly string[] }
export interface ActorInspection {
  readonly id: string; readonly definition: string; readonly state: Data;
  readonly status: 'running' | 'stopping' | 'stopped'; readonly queued: number;
  readonly error?: ErrorData;
}
export interface RuntimeInspection {
  readonly status: RuntimeStatus; readonly now: number; readonly deadline?: number;
  readonly actors: readonly ActorInspection[]; readonly error?: RuntimeError;
}
export interface Runtime {
  start<M, S>(actor: Actor<M, S>, options?: RuntimeStartOptions<NoInfer<M>>): ActorRef<M>;
  send<M>(to: ActorRef<M>, message: NoInfer<M>): SendResult;
  /** Broadcast Stop through ordinary mailboxes, then wait for actors to stop themselves. */
  stop(options?: StopOptions): Promise<StopReport>;
  inspect(): RuntimeInspection;
}
export interface TestRuntime extends Runtime {
  flush(options?: RunOptions): RunResult;
  /** Advance the test clock, including the shared stop deadline. */
  advanceTo(time: number, options?: RunOptions): RunResult;
}
export class RuntimeError extends Error {
  readonly code = 'EXECUTION_FAILED';
  constructor(message: string, readonly actorId?: string, cause?: unknown) {
    super(message, { cause }); this.name = 'RuntimeError';
  }
}

/** Unbounded FIFO with amortized constant-time admission and consumption. */
class Queue<T> {
  private items: (T | undefined)[] = [];
  private head = 0;
  get length(): number { return this.items.length - this.head; }
  push(value: T): void { this.items.push(value); }
  peek(): T | undefined { return this.items[this.head]; }
  shift(): T | undefined {
    if (!this.length) return undefined;
    const value = this.items[this.head]; this.items[this.head++] = undefined;
    if (this.head >= 1_024 && this.head * 2 >= this.items.length) { this.items = this.items.slice(this.head); this.head = 0; }
    return value;
  }
  clear(): void { this.items = []; this.head = 0; }
}
type Instance = {
  ref: ActorRef<Data>; definition: Definition; state: Data;
  status: ActorInspection['status']; mailbox: Queue<Message<Data>>; scheduled: boolean;
  creator?: { actor: Instance; name: string }; error?: ErrorData;
};
type PreparedCommand =
  | { kind: 'send'; target: Instance; message: Data }
  | { kind: 'start'; ref: ActorRef<Data>; name: string; definition: Definition; state: Data; message: Data }
  | { kind: 'stop'; target: Instance | null };

class ActorFailure {
  constructor(readonly cause: unknown) {}
}
const runtimeFaults = new WeakSet<object>();

/** Thrown values need not be Errors; do not invoke their getters or string conversions. */
function errorData(cause: unknown): ErrorData {
  function property(name: string): string | undefined {
    if ((typeof cause !== 'object' && typeof cause !== 'function') || cause === null) return;
    try {
      let current: object | null = cause;
      for (let depth = 0; current !== null && depth < 16; depth++) {
        const descriptor = Object.getOwnPropertyDescriptor(current, name);
        if (descriptor) return 'value' in descriptor && typeof descriptor.value === 'string' ? descriptor.value : undefined;
        current = Object.getPrototypeOf(current);
      }
    } catch { /* A hostile object cannot prevent actor retirement. */ }
    return undefined;
  }
  const name = property('name') ?? 'Error';
  const fallback = cause === null ? 'null'
    : typeof cause === 'object' || typeof cause === 'function' ? 'Actor threw a non-Error value.' : String(cause);
  const message = property('message') ?? fallback;
  const stack = property('stack');
  return Object.freeze({ name, message, ...(stack === undefined ? {} : { stack }) });
}

function time(value: number, label: string): void {
  if (!Number.isFinite(value) || value < 0) throw new Error(`${label} must be a finite nonnegative number.`);
}
function steps(value: number): void {
  if (!Number.isSafeInteger(value) || value < 1) throw new Error('maxSteps must be a positive safe integer.');
}
function key(value: unknown, label: string): asserts value is string {
  if (typeof value !== 'string' || !value.length || value.length > 256) throw new Error(`Invalid ${label}.`);
}
function record(value: unknown, fields: readonly string[], label: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value) ||
      ![Object.prototype, null].includes(Object.getPrototypeOf(value)) || Object.getOwnPropertySymbols(value).length) {
    throw new Error(`${label} must be a synchronous plain object.`);
  }
  for (const [name, descriptor] of Object.entries(Object.getOwnPropertyDescriptors(value))) {
    if (!fields.includes(name) || !('value' in descriptor) || !descriptor.enumerable) throw new Error(`Invalid ${label} field '${name}'.`);
  }
  return value as Record<string, unknown>;
}

function create(automatic: boolean, options: RuntimeOptions): TestRuntime {
  const maxStepsPerTurn = options.maxStepsPerTurn ?? 1_024;
  steps(maxStepsPerTurn);
  let now = options.now ?? 0;
  time(now, 'Initial time');
  const owner = {}, initialTime = now, hostStart = performance.now();
  const actors = new Map<string, Instance>(), ready = new Queue<Instance>();
  let nextId = 1, status: RuntimeStatus = 'running', transitioning = false, pumping = false;
  let timer: ReturnType<typeof setTimeout> | undefined, changeScheduled = false;
  let deadline: number | undefined, error: RuntimeError | undefined;
  let stopPromise: Promise<StopReport> | undefined;
  let resolveStop: ((report: StopReport) => void) | undefined, rejectStop: ((error: RuntimeError) => void) | undefined;
  const clock = () => automatic ? Math.max(now, initialTime + performance.now() - hostStart) : now;
  const finished = () => status === 'closed' || status === 'failed';
  function outsideTransition(): void {
    if (transitioning) {
      const fault = new Error('Actor transitions cannot mutate their runtime. Return commands instead.');
      runtimeFaults.add(fault); throw fault;
    }
  }
  function clearWakeup(): void { if (timer !== undefined) { clearTimeout(timer); timer = undefined; } }
  function localReferences(value: Data): void {
    const reference = getReference(value);
    if (reference) {
      if (reference.owner !== owner) throw new Error('Actor reference belongs to another runtime.');
    } else if (value && typeof value === 'object') {
      for (const child of Object.values(value)) localReferences(child as Data);
    }
  }
  function checked(schema: Schema, value: unknown, label: string): Data {
    const errors = validateValue(schema, value);
    if (errors.length) throw new Error(`Invalid ${label}: ${errors.join(' ')}`);
    localReferences(value as Data); return copyData(value as Data);
  }
  function checkedMessage(schema: Schema, value: unknown, label: string): Data {
    const message = checked(schema, value, label);
    if (message !== null && typeof message === 'object' && Object.hasOwn(message, 'type')) {
      const type = (message as Record<string, Data>).type;
      if (typeof type === 'string' && type.startsWith('system:')) {
        throw new Error('The system: message namespace is reserved for runtime notifications.');
      }
    }
    return message;
  }
  function resolve(value: unknown): Instance {
    const reference = getReference(value);
    if (!reference) throw new Error('Expected an actor reference issued by this runtime.');
    if (reference.owner !== owner) throw new Error('Actor reference belongs to another runtime.');
    const instance = actors.get(reference.id);
    if (!instance || instance.ref !== value) throw new Error('Unknown actor reference.');
    return instance;
  }
  function enqueue(instance: Instance, message: Message<Data>): SendResult {
    if (instance.status === 'stopped') return { status: 'rejected' };
    instance.mailbox.push(Object.freeze(message));
    if (!instance.scheduled) { instance.scheduled = true; ready.push(instance); }
    return { status: 'accepted' };
  }
  function notifyCreator(instance: Instance, type: 'system:started' | 'system:stopped'): void {
    if (!instance.creator || finished()) return;
    const value = copyData({ type, actor: instance.creator.name, ref: instance.ref,
      ...(type === 'system:stopped' && instance.error ? { error: instance.error } : {}) });
    enqueue(instance.creator.actor, { type: 'Next', value });
  }
  function retire(instance: Instance, failure?: ErrorData): void {
    if (instance.status === 'stopped') return;
    instance.status = 'stopped'; instance.mailbox.clear(); instance.scheduled = false;
    if (failure) instance.error = failure;
    notifyCreator(instance, 'system:stopped');
  }
  function finish(forced: boolean): void {
    if (status !== 'stopping') return;
    const forcedActorIds = forced ? [...actors.values()].filter(actor => actor.status !== 'stopped').map(actor => actor.ref.id) : [];
    status = 'closed'; clearWakeup();
    if (forced) for (const actor of actors.values()) retire(actor);
    ready.clear();
    resolveStop?.(Object.freeze({ status: forced ? 'forced' : 'completed', forcedActorIds: Object.freeze(forcedActorIds) }));
  }
  function fail(failure: RuntimeError): void {
    if (finished()) return;
    status = 'failed'; error = failure; clearWakeup();
    for (const actor of actors.values()) retire(actor);
    ready.clear(); rejectStop?.(failure);
  }
  function settle(): void {
    if (status !== 'stopping') return;
    if ([...actors.values()].every(actor => actor.status === 'stopped')) finish(false);
    else if (deadline !== undefined && clock() >= deadline) finish(true);
  }
  function schedule(): void {
    if (!automatic || pumping || finished()) return;
    clearWakeup(); settle();
    if (finished() || (!ready.length && deadline === undefined)) return;
    const delay = ready.length ? 0 : Math.max(0, deadline! - clock());
    timer = setTimeout(() => {
      timer = undefined;
      try { drive(clock(), { maxSteps: maxStepsPerTurn }); } catch { /* Failure is available through inspect(). */ }
    }, Math.min(2_147_483_647, delay));
  }
  function changed(): void {
    if (changeScheduled || finished()) return;
    changeScheduled = true;
    queueMicrotask(() => { changeScheduled = false; settle(); schedule(); });
  }
  function reserve(definition: Definition, requestedId?: string): ActorRef<Data> {
    let id = requestedId;
    if (id === undefined) { do { id = `actor-${nextId++}`; } while (actors.has(id)); }
    return createReference<Data>(id, owner, definition.message);
  }
  function install(definition: Definition, state: Data, message: { value: Data } | undefined, ref: ActorRef<Data>, creator?: Instance['creator']): Instance {
    const instance: Instance = {
      ref, definition, state, creator,
      status: 'running', mailbox: new Queue(), scheduled: false,
    };
    actors.set(ref.id, instance); enqueue(instance, { type: 'Start', parentId: creator?.actor.ref.id ?? null });
    if (message) enqueue(instance, { type: 'Next', value: message.value });
    if (status === 'stopping') enqueue(instance, { type: 'Stop' });
    notifyCreator(instance, 'system:started');
    return instance;
  }
  function prepare(instance: Instance, message: Message<Data>): { state: Data; commands: PreparedCommand[] } {
    let raw: unknown;
    try { raw = instance.definition.on(Object.freeze({ message, state: instance.state, self: instance.ref })); }
    catch (cause) {
      if (cause !== null && typeof cause === 'object' && runtimeFaults.has(cause)) throw cause;
      throw new ActorFailure(cause);
    }
    if (raw instanceof Promise) {
      // Invalid async handlers still need their rejection observed after the runtime fails.
      void Promise.prototype.then.call(raw, undefined, () => undefined);
      throw new Error('Actor results must be synchronous; promises are not supported.');
    }
    const result = raw === undefined ? {} : record(raw, ['state', 'commands'], 'Actor result');
    const state = Object.hasOwn(result, 'state') ? checked(instance.definition.state, result.state, 'actor state') : instance.state;
    if (Object.hasOwn(result, 'commands') && !Array.isArray(result.commands)) throw new Error('Actor commands must be an array.');
    const commands: PreparedCommand[] = [], batch = (result.commands ?? []) as unknown[];
    for (let index = 0; index < batch.length; index++) {
      const command = record(batch[index], ['kind', 'to', 'message', 'actor', 'actorId'], 'Actor command');
      switch (command.kind) {
        case 'send': {
          record(command, ['kind', 'to', 'message'], 'send command');
          const target = resolve(command.to);
          commands.push({ kind: 'send', target, message: checkedMessage(target.definition.message, command.message, 'sent message') });
          break;
        }
        case 'start': {
          record(command, ['kind', 'actor', 'message'], 'start command');
          const target = getActorTarget(command.actor);
          if (!Object.hasOwn(instance.definition.actors, target.name) || instance.definition.actors[target.name] !== target.actor) {
            throw new Error(`Undeclared actor '${target.name}'.`);
          }
          const definition = getDefinition(target.actor);
          const childState = checked(definition.state, definition.initialState, 'initial actor state');
          const initialMessage = checkedMessage(definition.message, command.message, 'initial actor message');
          const ref = reserve(definition);
          commands.push({ kind: 'start', ref, name: target.name, definition, state: childState, message: initialMessage });
          break;
        }
        case 'stop': {
          record(command, ['kind', 'actorId'], 'stop command');
          let target: Instance | null = null;
          if (command.actorId === null) {
            if (index !== batch.length - 1) throw new Error('Self-stop must be the last command in a transition.');
          } else {
            key(command.actorId, 'stop actor id');
            target = actors.get(command.actorId) ?? null;
            if (!target) throw new Error(`Unknown actor '${command.actorId}'.`);
          }
          commands.push({ kind: 'stop', target }); break;
        }
        default: throw new Error('Unknown actor command.');
      }
    }
    return { state, commands };
  }
  function result(processed: number, maxSteps: number): RunResult {
    let pending = 0;
    for (const actor of actors.values()) pending += actor.mailbox.length;
    return { processed, pending, exhausted: processed === maxSteps && pending > 0 };
  }
  function drive(target: number, options: RunOptions = {}): RunResult {
    outsideTransition(); time(target, 'Target time');
    if (target < now) throw new Error('Runtime time cannot move backwards.');
    const maxSteps = options.maxSteps ?? maxStepsPerTurn;
    steps(maxSteps);
    if (status === 'failed') throw error!;
    if (pumping) throw new Error('A runtime run is already in progress.');
    if (status === 'closed') return result(0, maxSteps);
    // Explicit clock advancement must reach the stop deadline even with a busy mailbox.
    now = target;
    let processed = 0, current: Instance | undefined;
    pumping = true;
    try {
      settle();
      while (!finished() && ready.length && processed < maxSteps) {
        current = ready.shift()!;
        const message = current.mailbox.peek();
        if (!message || current.status === 'stopped') { current.scheduled = false; continue; }
        let prepared: ReturnType<typeof prepare> | undefined;
        transitioning = true;
        try { prepared = prepare(current, message); }
        catch (cause) {
          if (!(cause instanceof ActorFailure)) throw cause;
          retire(current, errorData(cause.cause));
        } finally { transitioning = false; }
        if (prepared) {
          current.mailbox.shift(); current.state = prepared.state;
          if (message.type === 'Stop') current.status = 'stopping';
          for (const command of prepared.commands) {
            if (command.kind === 'send') enqueue(command.target, { type: 'Next', value: command.message });
            else if (command.kind === 'start') {
              install(command.definition, command.state, { value: command.message }, command.ref, { actor: current, name: command.name });
            } else if (command.target === null) retire(current);
            else enqueue(command.target, { type: 'Stop' });
          }
        }
        if (current.mailbox.length) ready.push(current);
        else current.scheduled = false;
        processed++;
        if (automatic && deadline !== undefined && clock() >= deadline) break;
      }
      settle(); return result(processed, maxSteps);
    } catch (cause) {
      const failure = new RuntimeError('An actor transition failed; the runtime was stopped.', current?.ref.id, cause);
      fail(failure); throw failure;
    } finally { pumping = false; schedule(); }
  }
  return Object.freeze({
    start<M, S>(actor: Actor<M, S>, options: RuntimeStartOptions<NoInfer<M>> = {}): ActorRef<M> {
      outsideTransition();
      if (status !== 'running') throw new Error('Runtime no longer accepts new actors.');
      if (options.id !== undefined) {
        key(options.id, 'actor id');
        if (actors.has(options.id)) throw new Error(`Actor '${options.id}' already exists.`);
      }
      const definition = getDefinition(actor);
      const state = checked(definition.state, definition.initialState, 'initial actor state');
      const message = Object.hasOwn(options, 'message') ? { value: checkedMessage(definition.message, options.message, 'initial message') } : undefined;
      const instance = install(definition, state, message, reserve(definition, options.id));
      changed(); return instance.ref as unknown as ActorRef<M>;
    },
    send<M>(to: ActorRef<M>, message: NoInfer<M>): SendResult {
      outsideTransition();
      if (status !== 'running') throw new Error('Runtime is shutting down; external admissions are closed.');
      const instance = resolve(to);
      if (instance.status === 'stopped') return { status: 'rejected' };
      const result = enqueue(instance, { type: 'Next', value: checkedMessage(instance.definition.message, message, 'actor message') });
      changed(); return result;
    },
    stop(options: StopOptions = {}): Promise<StopReport> {
      outsideTransition();
      if (stopPromise) return stopPromise;
      if (status === 'failed') return Promise.reject(error);
      const grace = options.gracePeriodMs ?? 5_000;
      time(grace, 'Stop grace period');
      const nextDeadline = clock() + grace;
      time(nextDeadline, 'Stop deadline');
      status = 'stopping'; deadline = nextDeadline;
      stopPromise = new Promise((resolve, reject) => { resolveStop = resolve; rejectStop = reject; });
      for (const actor of actors.values()) enqueue(actor, { type: 'Stop' });
      settle(); changed(); return stopPromise;
    },
    inspect(): RuntimeInspection {
      return Object.freeze({ status, now: clock(), ...(deadline === undefined ? {} : { deadline }),
        actors: Object.freeze([...actors.values()].map(actor => Object.freeze({ id: actor.ref.id, definition: actor.definition.id,
          state: actor.state, status: actor.status, queued: actor.mailbox.length,
          ...(actor.error ? { error: actor.error } : {}) }))), ...(error ? { error } : {}) });
    },
    flush: (options?: RunOptions) => drive(clock(), options),
    advanceTo: (target: number, options?: RunOptions) => {
      if (automatic) throw new Error('Live runtime time is automatic. Use createTestRuntime.');
      return drive(target, options);
    },
  });
}

export function createRuntime(options: RuntimeOptions = {}): Runtime { return create(true, options); }
export function createTestRuntime(options: RuntimeOptions = {}): TestRuntime { return create(false, options); }
