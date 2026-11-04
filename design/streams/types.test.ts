// Historical, unapproved hot-only draft; cold execution and API decisions are under review.
/** PROPOSED API — compile-only examples and rejection checks; never execute this file. */
import {
  Command, createStreams, defineSubject, eventSchema, map, filter, scan, take,
  mergeMap, concatMap, switchMap, route, s, streamSchema, subjectSchema,
  type Data, type ErrorData, type Infer, type Schema, type Stream, type StreamRef,
  type Subject, type SubjectDefinition, type SubjectRef, type SubjectTarget,
} from './api';
import {
  Command as ActorCommand, createTestRuntime as createActorRuntime, defineActor, s as actorSchema,
} from '../../src/index';

const counter = defineSubject({
  id: 'counter', input: s.number, output: s.number, state: s.number, initialState: 0,
  on: ({ message, state, self }) => {
    const current: number = state;
    const ref: SubjectRef<number, number> = self;
    void current; void ref;
    if (message.type === 'Start') {
      const initial: number | undefined = message.value;
      return { state: initial ?? 0, commands: [Command.next(initial ?? 0)] };
    }
    if (message.type === 'Stop') return;
    const input: number = message.value;
    // @ts-expect-error Subject inputs retain the schema's exact value type.
    const wrong: string = message.value;
    void wrong;
    return { state: state + input, commands: [Command.next(state + input)] };
  },
});
const textWorker = defineSubject({
  id: 'text-worker', input: s.string, output: s.string, state: s.null, initialState: null,
  on: ({ message }) => {
    if (message.type === 'Start' && message.value !== undefined) {
      return { commands: [Command.next(message.value.toUpperCase()), Command.stopped()] };
    }
  },
});

defineSubject({
  id: 'bad-initial-state', input: s.number, output: s.number, state: s.number,
  // @ts-expect-error Initial state cannot widen the state schema.
  initialState: 'wrong',
  on: () => undefined,
});
defineSubject({
  id: 'bad-next', input: s.number, output: s.number, state: s.number, initialState: 0,
  // @ts-expect-error Emitted values must match the declared output schema.
  on: () => ({ commands: [Command.next('wrong')] }),
});
defineSubject({
  id: 'bad-state', input: s.number, output: s.number, state: s.number, initialState: 0,
  // @ts-expect-error A transition cannot commit a different state shape.
  on: () => ({ state: 'wrong' }),
});
defineSubject({
  id: 'bad-async-handler', input: s.number, output: s.number, state: s.number, initialState: 0,
  // @ts-expect-error Subject transitions must be synchronous.
  on: async () => ({ state: 1 }),
});

const app = createStreams();
const run = app.start(counter, { value: 1, observer: {
  next: value => { const n: number = value; void n; },
  stopped: error => { const e: ErrorData | undefined = error; void e; },
} });
const running: Subject<number, number> = run;
void running;
run.next(2);
run.stop();
Command.send(run.ref, 3);
Command.stop(run.ref);
const outputOfSubjectRef: StreamRef<number> = run.ref.stream;
void outputOfSubjectRef;
// @ts-expect-error Extracting a subject's readable output preserves its output schema.
const wrongOutputOfSubjectRef: StreamRef<string> = run.ref.stream;
void wrongOutputOfSubjectRef;
// @ts-expect-error A readable output capability cannot receive application input.
Command.send(run.ref.stream, 3);
app.stop(run.ref);
app.stop(run.stream.ref);
app.stop();
// @ts-expect-error Host initialization follows the definition's input.
app.start(counter, { value: 'wrong' });
// @ts-expect-error Host sends cannot widen their target subject's protocol.
run.next('wrong');
// @ts-expect-error Typed command sends also cannot widen that protocol.
Command.send(run.ref, 'wrong');
// @ts-expect-error Commands carry inert capabilities, not imperative host wrappers.
Command.send(run, 1);
// @ts-expect-error A definition is not an already running reference.
Command.send(counter, 1);
// @ts-expect-error Host stop cannot accept a structural fabricated reference.
app.stop({ id: 'fake' });

const input = app.subject(s.number);
const strings = app.subject(s.string);
const mixed = app.subject(s.union(s.number, s.string));
input.next(1);
// @ts-expect-error Subject ingress preserves its schema type.
input.next('wrong');
const readable: StreamRef<number | string> = input.stream.ref;
const narrowerInput: SubjectRef<number, number | string> = mixed.ref;
void readable; void narrowerInput;
// @ts-expect-error A stream that may emit strings cannot promise only numbers.
const narrowedOutput: StreamRef<number> = mixed.stream.ref;
void narrowedOutput;
// @ts-expect-error A number-only subject cannot accept arbitrary strings.
const widenedInput: SubjectRef<number | string, number> = input.ref;
void widenedInput;

const transformed = input.stream.pipe(
  map(s.number, (value, index) => value + index),
  filter(value => value > 0),
  scan(s.string, '', (total, value, index) => total + String(value + index)),
  take(3),
);
const exactTransform: Stream<string> = transformed;
void exactTransform;
transformed.subscribe({ next: value => { const text: string = value; void text; } });
// @ts-expect-error Observers must accept the stream's actual output.
transformed.subscribe({ next: (value: number) => void value });
// @ts-expect-error Transform output follows its output schema.
input.stream.pipe(map(s.string, value => value));
// @ts-expect-error A callback cannot change the input stream's type.
input.stream.pipe(map(s.number, (value: string) => value.length));
// @ts-expect-error Async callbacks cannot supply atomic output values.
input.stream.pipe(map(s.number, async value => value));
// @ts-expect-error Predicates must return booleans, not a Promise.
input.stream.pipe(filter(async value => value > 0));
// @ts-expect-error The reducer result must match the accumulator schema.
input.stream.pipe(scan(s.string, '', (_total, value) => value));
// @ts-expect-error The initial accumulator must match its schema.
input.stream.pipe(scan(s.string, 0, total => total));

const composed = counter.pipe(map(s.string, value => String(value)), take(2));
const exactDefinition: SubjectDefinition<number, string> = composed;
void exactDefinition;
const composedRun: Subject<number, string> = app.start(composed, { value: 4, observer: {
  next: value => { const text: string = value; void text; },
} });
void composedRun;
// @ts-expect-error A definition cannot be subscribed before an explicit start.
counter.subscribe({ next: (_value: number) => undefined });
// @ts-expect-error Definitions are not live stream references and cannot be selected as borrowed work.
input.stream.pipe(switchMap(s.number, () => counter));

const owned = input.stream.pipe(mergeMap(textWorker, value => String(value), 4));
const ownedOutput: Stream<string> = owned;
void ownedOutput;
input.stream.pipe(concatMap(counter, value => value));
input.stream.pipe(switchMap(counter, value => value));
input.stream.pipe(switchMap(s.string, () => strings.stream.ref));
input.stream.pipe(mergeMap(s.number, () => input.stream.ref, 2));
input.stream.pipe(concatMap(s.number, () => input.stream.ref));
// @ts-expect-error Owned inner startup data must match its definition.
input.stream.pipe(switchMap(textWorker, value => value));
// @ts-expect-error Borrowed inner output must match the explicit schema.
input.stream.pipe(switchMap(s.string, () => input.stream.ref));
// @ts-expect-error Borrowed callbacks return inert StreamRefs, not host Stream wrappers.
input.stream.pipe(mergeMap(s.number, () => input.stream));
// @ts-expect-error Inner selection is synchronous and cannot return a Promise.
input.stream.pipe(concatMap(s.string, async () => strings.stream.ref));

const inbox = s.union(
  s.object({ type: s.literal('request'), value: s.number }),
  s.object({ type: s.literal('number-result'), key: s.string, event: eventSchema(s.number) }),
  s.object({ type: s.literal('text-result'), key: s.string, event: eventSchema(s.string) }),
);
defineSubject({
  id: 'coordinator', input: inbox, output: s.number, state: s.number, initialState: 0,
  subjects: { counter, textWorker },
  subscriptions: {
    numbers: route(s.number, (event, { key }) => ({ type: 'number-result' as const, key, event })),
    texts: route(s.string, (event, { key }) => ({ type: 'text-result' as const, key, event })),
  },
  on: ({ message, state, self, subjects, subscriptions }) => {
    if (message.type === 'Start') return { commands: [
      Command.start(subjects.counter, 1),
      Command.start(subjects.counter, 2, { into: subscriptions.numbers, key: 'job-2' }),
      Command.start(subjects.textWorker, 'hello', { into: subscriptions.texts, key: 'job-3' }),
      Command.subscribe(input.stream.ref, subscriptions.numbers, { key: 'borrowed' }),
      Command.subscribe(self.stream, subscriptions.numbers, { key: 'self-output' }),
      Command.unsubscribe(subscriptions.numbers, { key: 'previous' }),
    ] };
    if (message.type !== 'Next') return;
    if (message.value.type === 'number-result' && message.value.event.type === 'Next') {
      const value: number = message.value.event.value;
      return { state: state + value, commands: [Command.next(value)] };
    }
    // @ts-expect-error The child initialization protocol cannot be widened.
    Command.start(subjects.counter, 'wrong');
    // @ts-expect-error Declared targets are distinct from subject definitions.
    Command.start(counter, 1);
    // @ts-expect-error The declaration map exposes only the permitted children.
    Command.start(subjects.missing, 1);
    // @ts-expect-error Child outputs must fit the receiving subscription route.
    Command.start(subjects.counter, 1, { into: subscriptions.texts });
    // @ts-expect-error Borrowed source outputs must fit their receiving route.
    Command.subscribe(input.stream.ref, subscriptions.texts);
    // @ts-expect-error Stream output refs are distinct from writable subject refs.
    Command.subscribe(input.ref, subscriptions.numbers);
    // @ts-expect-error Route handles are restricted to this definition's declarations.
    Command.unsubscribe(subscriptions.missing);
    const target = Math.random() ? subjects.counter : subjects.textWorker;
    // @ts-expect-error A union of incompatible child targets must be narrowed before initialization.
    Command.start(target, 1);
  },
});

defineSubject({
  id: 'bad-route', input: s.number, output: s.number, state: s.null, initialState: null,
  subscriptions: {
    // @ts-expect-error Routes must map to the declared input protocol; they cannot widen it.
    values: route(s.number, event => event.type === 'Next' ? String(event.value) : 'stopped'),
  },
  on: () => undefined,
});

const refSchema = streamSchema(s.number);
const inertRef: Infer<typeof refSchema> = input.stream.ref;
const subjectRefSchema = subjectSchema(s.number, s.number);
const inertSubject: Infer<typeof subjectRefSchema> = input.ref;
const storedOutput: StreamRef<number> = inertSubject.stream;
void storedOutput;
const storedStream: Data = inertRef;
const storedSubject: Data = inertSubject;
void storedStream; void storedSubject;
// @ts-expect-error Host execution handles are not message/state data.
const storedHandle: Data = input;
void storedHandle;
// @ts-expect-error Host stream handles contain operations and cannot become actor state.
const storedHostStream: Data = input.stream;
void storedHostStream;
// @ts-expect-error Stream refs preserve their value protocol through schema inference.
const badStoredRef: Infer<typeof refSchema> = strings.stream.ref;
void badStoredRef;
// @ts-expect-error Schema objects are descriptions, not stream references.
const schemaAsRef: StreamRef<number> = s.number;
void schemaAsRef;
// @ts-expect-error Plain objects cannot fabricate nominal stream capabilities.
const forgedRef: StreamRef<number> = { id: 'pretend' };
void forgedRef;
// @ts-expect-error Copies lose nominal runtime-issued reference identity.
const copiedRef: StreamRef<number> = { ...input.stream.ref };
void copiedRef;
// @ts-expect-error Writable subject capabilities cannot be fabricated either.
const forgedSubject: SubjectRef<number, number> = { id: 'pretend' };
void forgedSubject;
// @ts-expect-error Schema tokens cannot be fabricated from arbitrary data.
const forgedSchema: Schema<number> = { kind: 'number' };
void forgedSchema;
// @ts-expect-error Child targets are not arbitrary string identifiers or objects.
const forgedTarget: SubjectTarget<number, number, 'counter'> = {};
void forgedTarget;
// @ts-expect-error Capability names are types only; there is no public constructor to subclass.
class FabricatedStream extends StreamRef<number> {}
void FabricatedStream;

const actor = defineActor({
  id: 'raw-actor', message: actorSchema.number, state: actorSchema.null, initialState: null, on: () => undefined,
});
const actorRef = createActorRuntime().start(actor);
const actorCommand = ActorCommand.send(actorRef, 1);
// @ts-expect-error Raw actor refs cannot be substituted for Streams capabilities.
const leakedActorRef: StreamRef<number> = actorRef;
void leakedActorRef;
// @ts-expect-error Raw actor refs are not writable SubjectRefs.
Command.send(actorRef, 1);
// @ts-expect-error This language's Data excludes raw actor capabilities.
const leakedActorData: Data = actorRef;
void leakedActorData;
// @ts-expect-error A raw actor definition is not a Subject definition.
app.start(actor);
defineSubject({
  id: 'bad-actor-command', input: s.number, output: s.number, state: s.null, initialState: null,
  // @ts-expect-error Actor commands cannot cross the sealed Streams command boundary.
  on: () => ({ commands: [actorCommand] }),
});
defineSubject({
  id: 'bad-copied-command', input: s.number, output: s.number, state: s.null, initialState: null,
  // @ts-expect-error Copying an opaque command loses its command identity.
  on: () => ({ commands: [{ ...Command.next(1) }] }),
});
