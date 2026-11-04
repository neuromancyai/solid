import { isSafeKey, validateSchema, validateValue, type Schema, type Infer, type Data } from './schema';
import { getDefinition, makeActor, makeActorTarget } from './internal';
import type { ActorRef } from './reference';
import type { Message } from './message';

// Protected retains the generic protocol in emitted declarations while preventing structural copies.
declare class ActorIdentity<M, S> {
  protected readonly actorTypes: (message: M) => S;
}
/** A reusable, schema-checked blueprint. Only defineActor can create one. */
export interface Actor<M, S = Data> extends ActorIdentity<M, S> {
  readonly id: string;
}
export type ActorMap = Readonly<Record<string, Actor<any, any>>>;
export type ActorMessage<A> = A extends Actor<infer M, any> ? M : never;
export type ActorState<A> = A extends Actor<any, infer S> ? S : never;

declare class TargetIdentity<C> {
  protected readonly targetTypes: (child: C) => C;
}
/** An inert declaration of a permitted child and its message protocol. */
export interface ActorTarget<C, K extends string = string> extends TargetIdentity<C> {
  readonly name: K;
}
export type ActorTargets<A extends ActorMap = {}> = {
  readonly [K in keyof A & string]: ActorTarget<ActorMessage<A[K]>, K>;
};

declare class SendIdentity {
  private readonly checkedSend: true;
}
/** Command.send checks the receiver protocol before erasing the heterogeneous payload. */
export interface SendCommand extends SendIdentity {
  readonly kind: 'send';
  readonly to: ActorRef<never>;
  readonly message: unknown;
}
type StartDescription<C, K extends string> = {
  readonly kind: 'start';
  readonly actor: ActorTarget<C, K>;
  readonly message: C;
};
export type StartCommand<A extends ActorMap> = {
  [K in keyof A & string]: StartDescription<ActorMessage<A[K]>, K>;
}[keyof A & string];
export type StopCommand = { readonly kind: 'stop'; readonly actorId: string | null };
export type Command<A extends ActorMap = {}> = SendCommand | StartCommand<A> | StopCommand;

/** Pure descriptions; the runtime validates and commits a transition's entire batch. */
export const Command = {
  send: <M>(to: ActorRef<M>, message: NoInfer<M>): SendCommand =>
    Object.freeze({ kind: 'send', to, message }) as unknown as SendCommand,
  start: <C, K extends string>(
    actor: ActorTarget<C, K>,
    message: NoInfer<C>,
  ): StartDescription<C, K> => Object.freeze({ kind: 'start', actor, message }),
  /** Null ends this actor; an ID requests that actor's graceful shutdown through Stop. */
  stop: (actorId: string | null = null): StopCommand => Object.freeze({ kind: 'stop', actorId }),
};

export interface ActorResult<S, A extends ActorMap = {}> {
  readonly state?: S;
  readonly commands?: readonly Command<A>[];
}
export interface ActorContext<M, S, A extends ActorMap = {}> {
  readonly message: Message<M, A>;
  readonly state: S;
  readonly self: ActorRef<M>;
  /** Immutable child declarations for Command.start; these expose no runtime operations. */
  readonly actors: ActorTargets<A>;
}
export interface ActorSpec<M extends Schema, S extends Schema, A extends ActorMap = {}> {
  readonly id: string;
  readonly message: M;
  readonly state: S;
  readonly initialState: Infer<NoInfer<S>>;
  /** The complete, statically typed vocabulary of child blueprints this actor may start. */
  readonly actors?: A;
  readonly on: (context: ActorContext<Infer<NoInfer<M>>, Infer<NoInfer<S>>, NoInfer<A>>) =>
    ActorResult<Infer<NoInfer<S>>, NoInfer<A>> | void;
}

function identifier(value: string): void {
  if (typeof value !== 'string' || value.length > 128 || !/^[A-Za-z0-9_][A-Za-z0-9_.:@-]*$/.test(value) || !isSafeKey(value)) {
    throw new Error(`Invalid actor identifier '${value}'.`);
  }
}

/** A pure actor specification independent of any executor or visual frontend. */
export function defineActor<const M extends Schema, const S extends Schema, const A extends ActorMap = {}>(spec: ActorSpec<M, S, A>): Actor<Infer<M>, Infer<S>> {
  identifier(spec.id);
  for (const schema of [spec.message, spec.state]) {
    const errors = validateSchema(schema);
    if (errors.length) throw new Error(`Invalid actor schema: ${errors.join(' ')}`);
  }
  const errors = validateValue(spec.state, spec.initialState);
  if (errors.length) throw new Error(`Invalid initial actor state: ${errors.join(' ')}`);
  if (typeof spec.on !== 'function') throw new Error('Actor on must be a function.');
  const actors: Record<string, Actor<any, any>> = {};
  if (spec.actors !== undefined) {
    const declarations = spec.actors;
    if (typeof declarations !== 'object' || declarations === null || Array.isArray(declarations) ||
      (Object.getPrototypeOf(declarations) !== Object.prototype && Object.getPrototypeOf(declarations) !== null) ||
      Object.getOwnPropertySymbols(declarations).length) throw new Error('Actor actors must be a plain blueprint map.');
    for (const [name, descriptor] of Object.entries(Object.getOwnPropertyDescriptors(declarations))) {
      identifier(name);
      if (!('value' in descriptor) || !descriptor.enumerable) throw new Error('Actor blueprint declarations must be plain data properties.');
      const actor: Actor<any, any> = descriptor.value;
      getDefinition(actor);
      actors[name] = actor;
    }
  }
  const targets = Object.freeze(Object.fromEntries(Object.entries(actors).map(([name, actor]) =>
    [name, makeActorTarget(name, actor)]))) as ActorTargets<A>;
  const on = spec.on;
  return makeActor<Infer<M>, Infer<S>>({
    id: spec.id,
    message: spec.message,
    state: spec.state,
    initialState: spec.initialState as Data,
    actors,
    on: context => on(Object.freeze({ ...context, actors: targets }) as unknown as ActorContext<Infer<M>, Infer<S>, A>),
  });
}
