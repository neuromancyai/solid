// Historical, unapproved hot-only draft; cold execution and API decisions are under review.
/**
 * PROPOSED API — declaration-only design spike, not an executable runtime.
 * This module intentionally imports no actor types or implementation.
 */

export interface ErrorData {
  readonly name: string;
  readonly message: string;
  readonly stack?: string;
}

/** Inert capabilities can be sent and stored; imperative host handles cannot. */
declare abstract class StreamIdentity<O> {
  protected readonly streamOutput: O;
}
export interface StreamRef<out O> extends StreamIdentity<O> {
  readonly id: string;
}

declare abstract class SubjectIdentity<I, O> {
  protected readonly subjectInput: (value: I) => void;
  protected readonly subjectOutput: O;
}
export interface SubjectRef<in I, out O> extends SubjectIdentity<I, O> {
  readonly id: string;
  /** Inert read capability for this subject's output; no host subscription methods. */
  readonly stream: StreamRef<O>;
}

export type Data = null | boolean | number | string | readonly Data[]
  | { readonly [key: string]: Data } | StreamRef<unknown> | SubjectRef<never, unknown>;

/** Only the language's schema constructors issue schema values. */
declare abstract class SchemaIdentity<T> {
  protected readonly schemaValue: T;
}
export interface Schema<out T> extends SchemaIdentity<T> {}
export type Infer<S extends Schema<unknown>> = S extends Schema<infer T> ? T : never;
type Shape = Readonly<Record<string, Schema<unknown>>>;
type ObjectValue<P extends Shape, O extends keyof P = never> =
  { readonly [K in Exclude<keyof P, O>]: Infer<P[K]> }
  & { readonly [K in O]?: Infer<P[K]> };

export declare const s: {
  readonly number: Schema<number>;
  readonly string: Schema<string>;
  readonly boolean: Schema<boolean>;
  readonly null: Schema<null>;
  readonly data: Schema<Data>;
  literal<const V extends null | boolean | number | string>(value: V): Schema<V>;
  array<S extends Schema<unknown>>(items: S): Schema<readonly Infer<S>[]>;
  object<const P extends Shape>(properties: P): Schema<ObjectValue<P>>;
  object<const P extends Shape, const O extends readonly (keyof P)[]>(properties: P, optional: O): Schema<ObjectValue<P, O[number]>>;
  union<const V extends readonly [Schema<unknown>, ...Schema<unknown>[]]>(...variants: V): Schema<Infer<V[number]>>;
};

export type StreamEvent<O> =
  | { readonly type: 'Next'; readonly value: O }
  | { readonly type: 'Stopped'; readonly error?: ErrorData };

export declare function eventSchema<S extends Schema<unknown>>(value: S): Schema<StreamEvent<Infer<S>>>;
export declare function streamSchema<S extends Schema<unknown>>(value: S): Schema<StreamRef<Infer<S>>>;
export declare function subjectSchema<I extends Schema<unknown>, O extends Schema<unknown>>(
  input: I, output: O,
): Schema<SubjectRef<Infer<I>, Infer<O>>>;

export type Message<I> =
  | { readonly type: 'Start'; readonly value?: I }
  | { readonly type: 'Next'; readonly value: I }
  /** Synchronous cleanup; the executor closes managed bindings and finalizes afterward. */
  | { readonly type: 'Stop' };

/**
 * A definition contains code; only explicit start creates its hot execution.
 * A composed definition owns its internal stages. Final output termination seals
 * the composite input and requests Stop for its remaining owned stages.
 */
declare abstract class DefinitionIdentity<I, O> {
  protected readonly definitionInput: (value: I) => I;
  protected readonly definitionOutput: O;
}
export interface SubjectDefinition<I, O> extends DefinitionIdentity<I, O> {
  readonly id: string;
  pipe(): SubjectDefinition<I, O>;
  pipe<A>(a: Operator<O, A>): SubjectDefinition<I, A>;
  pipe<A, B>(a: Operator<O, A>, b: Operator<A, B>): SubjectDefinition<I, B>;
  pipe<A, B, C>(a: Operator<O, A>, b: Operator<A, B>, c: Operator<B, C>): SubjectDefinition<I, C>;
  pipe<A, B, C, D>(a: Operator<O, A>, b: Operator<A, B>, c: Operator<B, C>, d: Operator<C, D>): SubjectDefinition<I, D>;
}
type AnySubject = SubjectDefinition<any, any>;
type SubjectMap = Readonly<Record<string, AnySubject>>;
export type InputOf<D extends AnySubject> = D extends SubjectDefinition<infer I, any> ? I : never;
export type OutputOf<D extends AnySubject> = D extends SubjectDefinition<any, infer O> ? O : never;

/** Targets are issued from the subject's declared children, never from IDs. */
declare abstract class TargetIdentity<I, O, Name extends string> {
  protected readonly targetInput: (value: I) => I;
  protected readonly targetOutput: (value: O) => O;
  protected readonly targetName: Name;
}
export interface SubjectTarget<I, O, Name extends string> extends TargetIdentity<I, O, Name> {}
export type SubjectTargets<C extends SubjectMap> = {
  readonly [K in keyof C]: SubjectTarget<InputOf<C[K]>, OutputOf<C[K]>, K & string>;
};

/** Static pure routing code maps every stream event into the subject input. */
declare abstract class RouteIdentity<O, I> {
  protected readonly routeEvent: (event: StreamEvent<O>) => void;
  protected readonly routeInput: I;
}
export interface Route<in O, out I> extends RouteIdentity<O, I> {}
export declare function route<S extends Schema<unknown>, I>(
  valueSchema: S,
  project: (event: StreamEvent<Infer<S>>, context: { readonly key: string }) => I,
): Route<Infer<S>, I>;
type RouteMap = Readonly<Record<string, Route<any, any>>>;
type RouteValue<R> = R extends Route<infer O, any> ? O : never;

/**
 * A slot is a typed family of bindings. Each string key has a private generation.
 * Omitting a command's key selects the fixed default key ''. Unsubscribe without
 * a key affects only that binding; subject termination releases every binding.
 */
declare abstract class SlotIdentity<O, Name extends string> {
  protected readonly slotValue: (value: O) => void;
  protected readonly slotName: Name;
}
export interface SubscriptionSlot<in O, Name extends string> extends SlotIdentity<O, Name> {}
export type SubscriptionSlots<R extends RouteMap> = {
  readonly [K in keyof R]: SubscriptionSlot<RouteValue<R[K]>, K & string>;
};

/** These descriptors belong to Streams and cannot be mixed with actor commands. */
declare const commandBrand: unique symbol;
declare abstract class StreamCommand { protected readonly streamsCommand: typeof commandBrand }
declare abstract class NextIdentity<O> extends StreamCommand { protected readonly emitted: O }
declare abstract class StoppedIdentity extends StreamCommand { protected readonly terminal: true }
declare abstract class SendIdentity extends StreamCommand { protected readonly sent: true }
declare abstract class StopIdentity extends StreamCommand { protected readonly requestedStop: true }
declare abstract class SubscribeIdentity<K extends string> extends StreamCommand { protected readonly subscription: K }
declare abstract class UnsubscribeIdentity<K extends string> extends StreamCommand { protected readonly unsubscription: K }
declare abstract class StartIdentity<I, O, K extends string, Into extends string | undefined> extends StreamCommand {
  protected readonly started: SubjectTarget<I, O, K>;
  protected readonly into: Into;
}
export interface NextCommand<O> extends NextIdentity<O> {}
export interface StoppedCommand extends StoppedIdentity {}
export interface SendCommand extends SendIdentity {}
export interface StopCommand extends StopIdentity {}
export interface SubscribeCommand<K extends string> extends SubscribeIdentity<K> {}
export interface UnsubscribeCommand<K extends string> extends UnsubscribeIdentity<K> {}
export interface StartCommand<I, O, K extends string, Into extends string | undefined> extends StartIdentity<I, O, K, Into> {}

export declare const Command: {
  /** Proposes a value. Emission during Stop or after Stopped is invalid at execution. */
  next<O>(value: O): NextCommand<O>;
  /** Commits the terminal event, then schedules Stop cleanup; worker retirement is private. */
  stopped(error?: ErrorData): StoppedCommand;
  send<I, O>(to: SubjectRef<I, O>, value: NoInfer<I>): SendCommand;
  /** Requests graceful Stop; it does not synchronously terminate the subject. */
  stop<I, O>(to: SubjectRef<I, O>): StopCommand;
  /** Independent fire-and-forget creation; no reference reply in this proposal. */
  start<I, O, K extends string>(target: SubjectTarget<I, O, K>, value: NoInfer<I>): StartCommand<I, O, K, undefined>;
  /** Owned execution, attached before Start; replacing the binding requests Stop. */
  start<I, O, K extends string, Slot extends string>(
    target: SubjectTarget<I, O, K>, value: NoInfer<I>,
    options: { readonly into: SubscriptionSlot<NoInfer<O>, Slot>; readonly key?: string },
  ): StartCommand<I, O, K, Slot>;
  /** Borrowed producer: replacement or removal detaches without stopping it. */
  subscribe<O, K extends string>(
    source: StreamRef<O>, slot: SubscriptionSlot<NoInfer<O>, K>, options?: { readonly key?: string },
  ): SubscribeCommand<K>;
  unsubscribe<O, K extends string>(slot: SubscriptionSlot<O, K>, options?: { readonly key?: string }): UnsubscribeCommand<K>;
};

type Starts<C extends SubjectMap, R extends RouteMap> = {
  [K in keyof C]: StartCommand<InputOf<C[K]>, OutputOf<C[K]>, K & string, (keyof R & string) | undefined>;
}[keyof C];
export type Result<S, O, C extends SubjectMap, R extends RouteMap> = {
  readonly state?: S;
  readonly commands?: readonly (
    NextCommand<O> | StoppedCommand | SendCommand | StopCommand | Starts<C, R>
    | SubscribeCommand<keyof R & string> | UnsubscribeCommand<keyof R & string>
  )[];
};
export interface Context<I, O, S, C extends SubjectMap, R extends RouteMap> {
  readonly message: Message<I>;
  readonly state: S;
  readonly self: SubjectRef<I, O>;
  readonly subjects: SubjectTargets<C>;
  readonly subscriptions: SubscriptionSlots<R>;
}

export declare function defineSubject<
  const I extends Schema<unknown>, const O extends Schema<unknown>, const S extends Schema<unknown>,
  const C extends SubjectMap = {}, const R extends RouteMap = {},
>(spec: {
  readonly id: string;
  readonly input: I;
  readonly output: O;
  readonly state: S;
  readonly initialState: NoInfer<Infer<S>>;
  readonly subjects?: C;
  readonly subscriptions?: R & { readonly [K in keyof R]: Route<any, NoInfer<Infer<I>>> };
  /** Pure synchronous transition. Purity and phase-dependent command legality need runtime checks. */
  readonly on: (context: Context<Infer<I>, Infer<O>, Infer<S>, C, R>) => Result<Infer<S>, Infer<O>, C, R> | void;
}): SubjectDefinition<Infer<I>, Infer<O>>;

export interface Observer<O> {
  readonly next?: (value: O) => void;
  readonly stopped?: (error?: ErrorData) => void;
}
export interface Subscription { readonly unsubscribe: () => void }
declare abstract class OperatorIdentity<I, O> {
  protected readonly operatorInput: (value: I) => void;
  protected readonly operatorOutput: O;
}
export interface Operator<in I, out O> extends OperatorIdentity<I, O> {}

/**
 * Host wrapper around one live hot output. Each pipe call creates a live branch
 * that borrows this source and owns its newly installed operator stages.
 * Stopping the branch releases those stages without stopping its borrowed source.
 */
export interface Stream<O> {
  readonly ref: StreamRef<O>;
  pipe(): Stream<O>;
  pipe<A>(a: Operator<O, A>): Stream<A>;
  pipe<A, B>(a: Operator<O, A>, b: Operator<A, B>): Stream<B>;
  pipe<A, B, C>(a: Operator<O, A>, b: Operator<A, B>, c: Operator<B, C>): Stream<C>;
  pipe<A, B, C, D>(a: Operator<O, A>, b: Operator<A, B>, c: Operator<B, C>, d: Operator<C, D>): Stream<D>;
  readonly subscribe: (observer: Observer<O>) => Subscription;
}
export interface Subject<I, O> {
  readonly ref: SubjectRef<I, O>;
  readonly stream: Stream<O>;
  readonly next: (value: I) => void;
  readonly stop: () => void;
}
export interface Streams {
  /** Connections and initial observer are installed before the producer's Start. */
  start<D extends AnySubject>(definition: D, options?: {
    readonly value?: NoInfer<InputOf<D>>;
    readonly observer?: Observer<NoInfer<OutputOf<D>>>;
  }): Subject<InputOf<D>, OutputOf<D>>;
  subject<S extends Schema<unknown>>(schema: S): Subject<Infer<S>, Infer<S>>;
  /** Stops the runtime, or requests Stop for one execution/branch without stopping borrowed inputs. */
  readonly stop: (target?: StreamRef<unknown> | SubjectRef<never, unknown>) => Promise<void>;
}
export declare function createStreams(): Streams;

export declare function map<I, S extends Schema<unknown>>(
  output: S, project: (value: I, index: number) => NoInfer<Infer<S>>,
): Operator<I, Infer<S>>;
export declare function filter<I>(predicate: (value: I, index: number) => boolean): Operator<I, I>;
export declare function scan<I, S extends Schema<unknown>>(
  state: S, seed: NoInfer<Infer<S>>, reduce: (state: NoInfer<Infer<S>>, value: I, index: number) => NoInfer<Infer<S>>,
): Operator<I, Infer<S>>;
export declare function take<I>(count: number): Operator<I, I>;

/** Owned child definition: each accepted outer value explicitly starts an execution. */
export declare function mergeMap<I, D extends AnySubject>(
  definition: D, initial: (value: I, index: number) => NoInfer<InputOf<D>>, concurrency?: number,
): Operator<I, OutputOf<D>>;
/** Borrowed live stream: observes existing work and never starts its producer. */
export declare function mergeMap<I, S extends Schema<unknown>>(
  output: S, select: (value: I, index: number) => StreamRef<NoInfer<Infer<S>>>, concurrency?: number,
): Operator<I, Infer<S>>;
export declare function concatMap<I, D extends AnySubject>(
  definition: D, initial: (value: I, index: number) => NoInfer<InputOf<D>>,
): Operator<I, OutputOf<D>>;
export declare function concatMap<I, S extends Schema<unknown>>(
  output: S, select: (value: I, index: number) => StreamRef<NoInfer<Infer<S>>>,
): Operator<I, Infer<S>>;
export declare function switchMap<I, D extends AnySubject>(
  definition: D, initial: (value: I, index: number) => NoInfer<InputOf<D>>,
): Operator<I, OutputOf<D>>;
export declare function switchMap<I, S extends Schema<unknown>>(
  output: S, select: (value: I, index: number) => StreamRef<NoInfer<Infer<S>>>,
): Operator<I, Infer<S>>;
