import { Command, createTestRuntime, defineActor, isSystemMessage, s, type ActorContext, type ActorTarget, type ActorRef, type Infer, type Data, type Message, type SystemMessage, type ErrorData } from '@neuromancy/solid';

const protocol = s.union(
  s.object({ type: s.literal('increment'), amount: s.number }),
  s.object({ type: s.literal('reset') }),
);
const counter = defineActor({
  id: 'counter', message: protocol, state: s.number, initialState: 0,
  on: ({ message, state, self }) => {
    const ref: ActorRef<Infer<typeof protocol>> = self;
    void ref;
    if (message.type !== 'Next') return;
    if (message.value.type === 'reset') return { state: 0 };
    const amount: number = message.value.amount;
    // @ts-expect-error Message schemas preserve the exact payload type.
    const wrong: string = message.value.amount;
    void wrong;
    return { state: state + amount };
  },
});

const runtime = createTestRuntime();
const ref = runtime.start(counter, { message: { type: 'reset' } });
runtime.send(ref, { type: 'increment', amount: 2 });
Command.send(ref, { type: 'reset' });
// @ts-expect-error External sends cannot omit required fields.
runtime.send(ref, { type: 'increment' });
// @ts-expect-error External sends cannot widen the referenced actor protocol.
runtime.send(ref, { type: 'increment', amount: '2' });
// @ts-expect-error Command.send uses the reference's protocol, not a union inferred from its second argument.
Command.send(ref, { type: 'increment', amount: '2' });
// @ts-expect-error Unknown application variants are rejected.
Command.send(ref, { type: 'unknown' });
// @ts-expect-error Runtime lifecycle envelopes are not application payloads.
runtime.send(ref, { type: 'Start' });
// @ts-expect-error Start initialization must match the definition's protocol.
runtime.start(counter, { message: { type: 'increment', amount: '2' } });
// @ts-expect-error References cannot be fabricated from an ID.
const forgedRef: ActorRef<number> = { id: 'pretend' };
void forgedRef;
// @ts-expect-error Definitions must be created by defineActor, not structural anonymous objects.
runtime.start({ id: 'fabricated', message: s.number, state: s.number, initialState: 0, on: () => undefined });
// @ts-expect-error Copying an opaque definition does not preserve its nominal identity.
runtime.start({ ...counter });
// @ts-expect-error References do not expose imperative send handles.
ref.send({ type: 'reset' });
// @ts-expect-error References do not expose subscriptions.
ref.subscribe(() => undefined);

const numbers = defineActor({ id: 'numbers', message: s.number, state: s.null, initialState: null, on: () => undefined });
const strings = defineActor({ id: 'strings', message: s.string, state: s.null, initialState: null, on: () => undefined });
const numberRef = runtime.start(numbers);
const stringRef = runtime.start(strings);
// @ts-expect-error Copying an address does not issue a genuine reference capability.
runtime.send({ ...numberRef }, 1);
// @ts-expect-error A narrow mailbox cannot be widened to receive arbitrary data.
const widened: ActorRef<Data> = numberRef;
void widened;
// @ts-expect-error Incompatible references cannot be substituted.
const badRef: ActorRef<number> = stringRef;
void badRef;

const versatile = defineActor({ id: 'versatile', message: s.union(s.number, s.string), state: s.null, initialState: null, on: () => undefined });
const numberOnlyView: ActorRef<number> = runtime.start(versatile);
Command.send(numberOnlyView, 42);

Command.stop();
Command.stop(null);
Command.stop(numberRef.id);
// @ts-expect-error Stop requests use an actor ID or null, never a number.
Command.stop(42);
// @ts-expect-error Opaque references must be addressed through their readable actor ID.
Command.stop(numberRef);

const request = s.object({ replyTo: s.ref(s.number) });
const responder = defineActor({
  id: 'responder', message: request, state: s.null, initialState: null,
  on: ({ message }) => {
    if (message.type !== 'Next') return;
    Command.send(message.value.replyTo, 42);
    // @ts-expect-error References inside payloads retain their protocol.
    Command.send(message.value.replyTo, '42');
  },
});
const responderRef = runtime.start(responder);
runtime.send(responderRef, { replyTo: numberRef });
// @ts-expect-error Ref schemas constrain embedded reference protocols.
runtime.send(responderRef, { replyTo: stringRef });

const ownerProtocol = s.object({ type: s.literal('work'), value: s.number });
const owner = defineActor({
  id: 'owner', message: ownerProtocol, state: s.null, initialState: null, actors: { numbers, strings },
  on: ({ message, actors, self }) => {
    if (message.type === 'Start') {
      const parentId: string | null = message.parentId;
      void parentId;
      return { commands: [Command.start(actors.numbers, 1), Command.start(actors.strings, 'hello')] };
    }
    if (message.type !== 'Next') return;
    if (isSystemMessage(message.value)) {
      if (message.value.actor === 'numbers') {
        const child: ActorRef<number> = message.value.ref;
        void child;
        Command.send(message.value.ref, 1);
        // @ts-expect-error System notification narrowing preserves the exact child protocol.
        Command.send(message.value.ref, 'wrong');
      } else {
        const child: ActorRef<string> = message.value.ref;
        void child;
        Command.send(message.value.ref, 'hello');
        // @ts-expect-error Each declared child retains its own message protocol.
        Command.send(message.value.ref, 1);
      }
      if (message.value.type === 'system:stopped') {
        const error: ErrorData | undefined = message.value.error;
        const name: string | undefined = error?.name;
        const text: string | undefined = error?.message;
        const stack: string | undefined = error?.stack;
        void name; void text; void stack;
        // @ts-expect-error Errors are serializable data with string messages.
        const wrong: number | undefined = error?.message;
        void wrong;
      } else {
        // @ts-expect-error A successful start notification does not contain a failure.
        message.value.error;
      }
      // @ts-expect-error Only names from the declared child map appear in notifications.
      const unknown: 'missing' = message.value.actor;
      void unknown;
      // @ts-expect-error Self references accept application messages, not system notifications.
      Command.send(self, message.value);
      return;
    }
    const value: number = message.value.value;
    Command.send(self, { type: 'work', value });
    // @ts-expect-error Application-only references cannot inject lifecycle notifications.
    Command.send(self, { type: 'system:stopped', actor: 'numbers', ref: numberRef });
  },
});
const ownerRef = runtime.start(owner);
// @ts-expect-error External callers cannot impersonate the system through the parent's reference.
runtime.send(ownerRef, { type: 'system:started', actor: 'numbers', ref: numberRef });

const lifecycle: Message<number> = { type: 'Next', value: 1 };
const start: Message<number> = { type: 'Start', parentId: null };
void lifecycle; void start;
// @ts-expect-error Every Start explicitly identifies its parent or the root system.
const missingParent: Message<number> = { type: 'Start' };
void missingParent;
// @ts-expect-error Actors with no declared children have no system notification variants.
const unexpectedChild: Message<number> = { type: 'Next', value: { type: 'system:started', actor: 'numbers', ref: numberRef } };
void unexpectedChild;
// @ts-expect-error Child lifecycle notifications use Next, never a separate lifecycle variant.
const spawned: Message<number> = { type: 'Spawned', actor: 'numbers', key: 'job', ref: numberRef };
void spawned;
// @ts-expect-error Start commands require a typed declaration target, not a string name.
Command.start('numbers', 1);
// @ts-expect-error A blueprint is not a creator's declared start target.
Command.start(numbers, 1);
// @ts-expect-error Declaration targets cannot be fabricated from structural objects.
const fabricatedTarget: ActorTarget<number, 'numbers'> = {};
void fabricatedTarget;

defineActor({
  id: 'extracted-start', message: s.ref(s.number), state: s.null, initialState: null, actors: { numbers },
  on: context => {
    // @ts-expect-error Starting a child is exclusively a command, not an actor-context operation.
    context.start;
    const createChild = Command.start;
    const target = context.actors.numbers;
    const command = createChild(target, 1);
    // @ts-expect-error Child lifecycle notifications are automatic; custom reply callbacks are removed.
    createChild(target, 1, (child: ActorRef<number>) => child);
    // @ts-expect-error The declaration map contains only the creator's permitted child names.
    createChild(context.actors.strings, 'hello');
    // @ts-expect-error Extracted targets retain the exact child initialization type.
    createChild(target, 'wrong');
    // @ts-expect-error Copying a target does not preserve its opaque declaration identity.
    createChild({ ...target }, 1);
    // @ts-expect-error A declaration target cannot be used as an actor definition.
    runtime.start(target);
    return { commands: [command] };
  },
});

declare const numberTarget: ActorContext<number, null, { numbers: typeof numbers }>['actors']['numbers'];
const extractedCommand = Command.start(numberTarget, 1);
defineActor({
  id: 'compatible-extracted-command', message: s.string, state: s.null, initialState: null, actors: { numbers },
  on: () => ({ commands: [extractedCommand] }),
});
defineActor({
  id: 'wrong-blueprint-for-extracted-command', message: s.number, state: s.null, initialState: null,
  actors: { numbers: strings },
  // @ts-expect-error Matching declaration names do not erase incompatible child protocols.
  on: () => ({ commands: [extractedCommand] }),
});
defineActor({
  id: 'wrong-alias-for-extracted-command', message: s.number, state: s.null, initialState: null,
  actors: { anotherName: numbers },
  // @ts-expect-error Commands retain their exact declared target name.
  on: () => ({ commands: [extractedCommand] }),
});

defineActor({
  id: 'dynamic-child-name', message: s.union(s.null, s.literal('numbers'), s.literal('strings')),
  state: s.null, initialState: null, actors: { numbers, strings },
  on: ({ message, actors }) => {
    if (message.type !== 'Next' || message.value === null || isSystemMessage(message.value)) return;
    const name = message.value;
    // @ts-expect-error A union target must be narrowed before selecting its initialization protocol.
    Command.start(actors[name], 1);
    if (name === 'numbers') return { commands: [Command.start(actors[name], 1)] };
    return { commands: [Command.start(actors[name], 'hello')] };
  },
});

defineActor({
  id: 'primitive-protocol', message: s.number, state: s.null, initialState: null, actors: { numbers, strings },
  on: ({ message }) => {
    if (message.type !== 'Next') return;
    if (isSystemMessage(message.value)) {
      if (message.value.actor === 'numbers') {
        Command.send(message.value.ref, 1);
        // @ts-expect-error Guarding primitive protocols retains exact child-reference types.
        Command.send(message.value.ref, 'wrong');
      }
      return;
    }
    const amount: number = message.value;
    void amount;
  },
});
defineActor({
  id: 'broad-protocol', message: s.data, state: s.null, initialState: null, actors: { numbers, strings },
  on: ({ message }) => {
    if (message.type !== 'Next') return;
    if (isSystemMessage(message.value)) {
      if (message.value.actor === 'numbers') {
        Command.send(message.value.ref, 1);
        // @ts-expect-error Broad Data application schemas do not erase known child protocols.
        Command.send(message.value.ref, 'wrong');
      }
      return;
    }
    const data: Data = message.value;
    void data;
  },
});

defineActor({
  id: 'unknown-child', message: s.null, state: s.null, initialState: null, actors: { numbers },
  // @ts-expect-error Only statically declared child definitions can be started.
  on: ({ actors }) => ({ commands: [Command.start(actors.missing, 1)] }),
});
defineActor({
  id: 'wrong-child-payload', message: s.null, state: s.null, initialState: null, actors: { numbers },
  // @ts-expect-error Child initialization is checked against the declaration's message schema.
  on: ({ actors }) => ({ commands: [Command.start(actors.numbers, 'wrong')] }),
});
defineActor({
  id: 'no-declared-children', message: s.null, state: s.null, initialState: null,
  // @ts-expect-error Actors without declared children cannot start arbitrary definitions.
  on: ({ actors }) => ({ commands: [Command.start(actors.numbers, 1)] }),
});
defineActor({
  id: 'wrong-state', message: s.number, state: s.number, initialState: 0,
  // @ts-expect-error Returned state is checked against the state schema.
  on: () => ({ state: 'wrong' }),
});
defineActor({
  id: 'wrong-initial-state', message: s.number, state: s.number,
  // @ts-expect-error Initial state is checked against the state schema.
  initialState: 'wrong',
  on: () => undefined,
});
defineActor({
  id: 'async-transition', message: s.number, state: s.number, initialState: 0,
  // @ts-expect-error Actor transitions are synchronous; async handlers are not part of this core.
  on: async () => ({ state: 1 }),
});
defineActor({
  id: 'fabricated-command', message: s.number, state: s.number, initialState: 0,
  // @ts-expect-error A send descriptor must be made by the type-safe constructor.
  on: () => ({ commands: [{ kind: 'send', to: numberRef, message: 'wrong' }] }),
});
defineActor({
  id: 'modified-command', message: s.number, state: s.number, initialState: 0,
  // @ts-expect-error Spreading a checked command cannot bypass the send protocol check.
  on: () => ({ commands: [{ ...Command.send(numberRef, 1), message: 'wrong' }] }),
});
defineActor({
  id: 'copied-command', message: s.number, state: s.number, initialState: 0,
  // @ts-expect-error Copying a command loses its proof that Command.send checked the payload.
  on: () => ({ commands: [{ ...Command.send(numberRef, 1) }] }),
});

// @ts-expect-error Actor creation is expressed with Command.start; the former alias is removed.
Command.spawn(numberTarget, 1);

function genericProtocol<M>(message: Message<M, { numbers: typeof numbers }>): void {
  if (message.type !== 'Next') return;
  if (isSystemMessage(message.value)) {
    Command.send(message.value.ref, 1);
    // @ts-expect-error Generic application types cannot widen a declared child's protocol.
    Command.send(message.value.ref, 'wrong');
  } else {
    const application: M = message.value;
    void application;
  }
}
void genericProtocol;

declare const untypedNotification: SystemMessage;
const childId: string = untypedNotification.ref.id;
void childId;
// @ts-expect-error Erasing child definitions permits observation, never untyped messaging.
Command.send(untypedNotification.ref, 1);
