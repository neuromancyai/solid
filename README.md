# Solid

Solid is a minimal TypeScript actor language and local runtime, packaged as `@neuromancy/solid`. An actor owns state and an unbounded FIFO mailbox. A pure synchronous handler describes state changes and commands. Actors communicate explicitly through typed references.

There is no output port. A message can carry a reply address, and an actor can send to any reference it holds. Child creation is declared and typed: an actor can only start definitions listed in its `actors` map.

```sh
npm install
npm run example
npm test
npm run typecheck
npm run typecheck:package # Builds and checks the published declarations too.
```

The [headless example](examples/headless.ts) demonstrates request/reply, declared dynamic workers, and automatic child lifecycle notifications. The [programming model](docs/programming-model.md) explains the core and its precedents; [lifecycle and execution](docs/lifecycle-and-execution.md) specifies scheduling and shutdown. **Solidify** will be a graph programming interface built on this model.

## Streams design status

The Streams implementation has been removed so its API can be reviewed one decision at a time. The package currently exports only Actor Core. See the [current design status](docs/streams.md). The earlier [design draft](design/streams/README.md) is retained for reference, not as an adopted specification; its hot-only assumption has been superseded by the requirement to support cold execution as well.

## A typed conversation

```ts
import { Command, createTestRuntime, defineActor, s } from '@neuromancy/solid';

const result = s.object({ type: s.literal('result'), value: s.number });
const work = s.object({ value: s.number, replyTo: s.ref(result) });

const collector = defineActor({
  id: 'collector',
  message: result,
  state: s.array(s.number),
  initialState: [],
  on: ({ message, state }) => {
    if (message.type === 'Stop') return { commands: [Command.stop()] };
    if (message.type === 'Next') return { state: [...state, message.value.value] };
  },
});

const worker = defineActor({
  id: 'worker',
  message: work,
  state: s.null,
  initialState: null,
  on: ({ message }) => {
    if (message.type === 'Stop') return { commands: [Command.stop()] };
    if (message.type !== 'Next') return;
    return {
      commands: [Command.send(message.value.replyTo, {
        type: 'result',
        value: message.value.value * 2,
      })],
    };
  },
});

const runtime = createTestRuntime();
const replies = runtime.start(collector);
const processor = runtime.start(worker);
runtime.send(processor, { value: 21, replyTo: replies });
runtime.flush();
console.log(runtime.inspect().actors); // Collector state is [42].

const stopped = runtime.stop();
runtime.flush();
await stopped;
```

`message` and `state` schemas infer the handler's TypeScript types and validate runtime boundaries. `s.ref(schema)` describes a reference accepting messages with that schema. `Command.send(ref, payload)` checks the destination's message type; a reference is an opaque runtime-issued token with a readable `id`, not a mutable actor object. References can be carried in explicitly typed message and state fields.

The handler sees lifecycle envelopes as well as application messages:

| Envelope | Meaning |
| --- | --- |
| `Start { parentId }` | Queued once before the initial application message; the creator's ID, or `null` for a host-created root. |
| `Next { value }` | An application message or a runtime-generated child lifecycle notification. |
| `Stop` | A graceful stop request, from another command or whole-runtime shutdown. The actor ends when it issues `Command.stop()`. |

Application messages can use names such as `Error`, `Complete`, `Request`, or `Ack`; these have no built-in lifecycle meaning. The top-level payload `type` prefix `system:` is reserved for runtime notifications; nested application data can use it. `on({ message, state, self, actors })` returns `{ state?, commands? }` or nothing. Omitting `state` preserves it. `self` is a reference to this instance. `actors` is a readonly map of typed child declarations. Command arrays form batches; `Command.stop()` or `Command.stop(null)`, if present, must be last. `Command.stop(actorId)` instead queues a graceful `Stop` request at that actor and lets the sender continue. There is no separate batch primitive.

## Declared dynamic actors

A definition lists the kinds of actors it may create. `Command.start` describes creation using one of the handler's typed child declarations and its initial message. The runtime automatically notifies the creator through `Next`:

```ts
const coordinator = defineActor({
  id: 'coordinator',
  message: s.object({ type: s.literal('work'), work }),
  state: s.number,
  initialState: 0,
  actors: { worker },
  on: ({ message, state, actors }) => {
    if (message.type === 'Stop') return { commands: [Command.stop()] };
    if (message.type !== 'Next') return;
    if (message.value.type === 'work') return {
      commands: [Command.start(actors.worker, message.value.work)],
    };
    if (message.value.type === 'system:started') {
      // actor is 'worker'; ref accepts the worker's exact message type.
      return { state: state + 1 };
    }
    if (message.value.type === 'system:stopped') {
      // error is absent for normal termination.
      return { commands: [Command.stop()] };
    }
  },
});
```

Each entry in the handler's `actors` map is an inert typed declaration, with no executable methods. `Command.start` checks the initial message and constructs a description; the runtime alone creates the child when the transition commits. A command cannot supply an arbitrary actor implementation, an undeclared target, or an incompatible initial message. The host creates root actors using actual definitions through `runtime.start(definition, { id?, message? })`.

The runtime queues `Next({ type: 'system:started', actor: 'worker', ref })` to the creator after the child is installed, before that child processes `Start`. The child runs independently and may already have stopped by the time the creator handles this notification. On termination it receives `Next({ type: 'system:stopped', actor: 'worker', ref, error? })`. Here `Next(payload)` abbreviates `{ type: 'Next', value: payload }`. `actor` identifies the declaration and `ref` identifies the instance. Both notifications preserve the child's exact reference type. They are included in the handler's types automatically; the application's schema does not need to declare them. For application payloads without a `type` field, use the exported `isSystemMessage(value)` guard to distinguish notifications.

A child's `Start` includes `parentId`, the creator's ID; host-created roots receive `parentId: null`. This ID can address a stop request, but application messages still require a typed actor reference. Lifecycle notifications use ordinary FIFO delivery. A stopped creator receives no further notifications, and forced runtime closure cannot deliver them.

Creating a child does not own its lifetime. The child has its own mailbox, state, and scheduler turns, and may outlive its creator. `Command.stop(childRef.id)` asks it to stop through its ordinary FIFO mailbox; the child may clean up and then return `Command.stop()` itself. The request imposes no individual timeout. The subsequent `system:stopped` notification confirms termination. Sending to a stopped actor is rejected. Stopping a creator does not cancel already committed sends or stop unrelated actors.

## Execution

`createRuntime()` schedules automatically. `createTestRuntime()` uses the same transitions with explicit `flush()` and `advanceTo(time)` calls. Every actor receives one transition per round-robin turn; the live driver yields to the JavaScript event loop after a configurable batch, defaulting to 1,024 transitions. A slow synchronous handler still blocks that JavaScript thread.

Mailboxes are logically unbounded and FIFO, without priorities or automatic demand negotiation. Sending queues a message; it does not wait for processing. Requests, acknowledgements, subscriptions, and concurrency limits can be application protocols built from explicit sends. Physical memory is finite; executor resource quotas are not implemented.

The runtime validates a transition before committing its state and commands. Actor code must remain pure and synchronous. A thrown handler stops that actor, preserves its last committed state, and notifies its creator with `system:stopped` and serializable error data. Other actors continue. Invalid returned state or commands, illegal runtime mutations, and runtime defects fail the runtime with `EXECUTION_FAILED`. Application error messages remain ordinary values.

`runtime.stop({ gracePeriodMs: 5_000 })` seals external start and send calls, queues `Stop` to every live actor, and keeps scheduling actor commands during the grace period. Actors may still create declared children; each new child receives `Start`, its initial `Next`, then `Stop`, and shares the original shutdown deadline. Shutdown completes when every actor stops, or forces remaining actors to stop at that deadline. `Stop` stays behind existing mailbox work. An idle actor is still alive.

## Scope

The core contains actor definitions, schemas, typed references, messages, three commands (`send`, `start`, `stop`), per-actor scheduling, inspection, and runtime shutdown. References are local capabilities; persistence and transport are not implemented.

Streams is pending a new design above this core. The previous block and port model has no compatibility layer. Timers, effects, presentation APIs, durability, distribution, restart supervision, resource accounting, and billing remain future work.
