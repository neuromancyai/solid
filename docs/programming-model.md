# Solid programming model

Status: experimental implementation, 4 November 2026. Solid is a minimal TypeScript actor language and local runtime. **Solidify** is its future graph frontend. The package is `@neuromancy/solid`.

The model consists of reusable definitions, independent actor instances, typed references, messages, and commands. Each actor owns state and a FIFO mailbox. Its handler runs one message at a time:

```text
on({ message, state, self, actors }) → { state?, commands? } | void
```

This is the current core. The previous input/output stream model, pipelines, timers, effect outboxes, subscription registries, and presentation APIs have been removed rather than retained as compatibility layers. The Streams implementation has also been removed pending a decision-by-decision redesign, including cold execution. See the [Streams design status](streams.md).

## Definitions and instances

`defineActor` accepts an identity, `message` and `state` schemas, `initialState`, an optional `actors` declaration map, and a synchronous `on` handler. A definition contains no running state. Each call to `runtime.start(definition)` creates a fresh instance and reference; a dynamic `start` command does the same through a declared child definition.

Runtime schemas infer TypeScript message and state types. They also validate JavaScript callers and runtime boundaries. The initial state must match its schema. Each transition's proposed state and commands must validate before they are committed. Omitting `state` preserves the previous state; omitting commands sends nothing.

Handlers are pure by contract: they compute from their arguments and return descriptions. They must not perform I/O, mutate closures, read a wall clock, or return a promise. JavaScript's type system cannot prove purity. Actor state and message values are copied and frozen at execution boundaries; issued reference tokens retain their identity.

Ordinary data remains finite and JSON-compatible. `Data` and `s.data` also accept genuine opaque actor references. Use `s.ref(messageSchema)` to preserve a reference's accepted message type in a schema; `s.data` alone does not retain that protocol information. A value containing a reference is not a durable serialized actor address.

## References and communication

`ActorRef<M>` denotes an actor that accepts application messages of type `M`. It is opaque, locally issued, and has a readable `id`. It does not expose methods for mutating an actor, reading its state, or subscribing to output. Having an identifier string is not equivalent to having a reference for sending application messages. The separate control command `Command.stop(actorId)` does accept an ID.

The host uses `runtime.send(ref, payload)`. An actor returns `Command.send(ref, payload)`. Both validate the destination's accepted message schema. A message may contain a reference, for example a `replyTo` field declared with `s.ref(responseSchema)`. Actors thereby express request/reply, demand, acknowledgements, and conversation IDs in their own application protocols.

References preserve the accepted message type: a sender cannot send an arbitrary payload because it possesses an address. An actor accepting a broader message union can serve as a reply destination accepting one variant. A reference carried through an `s.ref` field is checked against the actual destination's accepted schema.

There is no global output type, output port, mandatory channel, or implicit reverse control protocol. An actor sends to zero or more destinations, including itself. Communication is bidirectional when both participants possess the appropriate references. Separate channel or stream abstractions can be implemented later using these operations.

Sending accepts work into a mailbox; it does not acknowledge that the handler ran or that the requested work finished. Sending to a stopped actor is rejected. References belong to their issuing runtime; cross-runtime transport is not implemented.

## Lifecycle envelopes

The runtime distinguishes application payloads from lifecycle events:

| Handler message | Contract |
| --- | --- |
| `{ type: 'Start', parentId }` | Once before the initial application message; the creator's ID or `null` for a host-created root. |
| `{ type: 'Next', value }` | A schema-validated application payload or a runtime-generated child lifecycle notification. |
| `{ type: 'Stop' }` | A graceful stop request from an actor command or global runtime shutdown. |

Callers send raw application payloads, which the runtime wraps in `Next`. The runtime also uses `Next` for `system:started` and `system:stopped` child notifications. The top-level application payload `type` prefix `system:` is reserved; callers cannot impersonate these notifications. Nested application data can use the same strings. `Start` and runtime `Stop` are delivered by the runtime.

`on` has no separate start or stop callback. The startup `parentId` identifies who created the actor. It can address a stop request, but is not a reference for sending application messages or a general message-origin field. The handler can act immediately on `Start`, without waiting for an application request. Receiving `Stop` allows the actor to perform cleanup through further messages. `Command.stop()` or `Command.stop(null)` ends the actor itself; receiving `Stop` alone does not. `Command.stop(actorId)` asks the addressed actor to stop and lets the sender continue. A string containing the sender's own ID still queues a request rather than terminating it immediately.

`Error` and `Complete` are application protocol choices. A worker can report an error without dying, finish one request while serving another, or stop after replying. The actor's lifetime is not coupled to a stream terminal value.

## Commands and dynamic actors

There are three command forms:

| Command | Meaning |
| --- | --- |
| `Command.send(ref, payload)` | Enqueue an application message at a typed destination. |
| `Command.start(actors.worker, initialMessage)` | Create one actor from a declared child definition. |
| `Command.stop()` / `Command.stop(null)` | End the current actor. |
| `Command.stop(actorId)` | Queue a graceful `Stop` request at the actor with this ID. |

An `actors: { worker: workerDefinition }` declaration gives the handler permission to request creation of that definition. The handler receives a readonly `actors` map of inert typed declarations, without executable methods. Each declaration preserves the child's initial message and reference types. `Command.start` constructs a description for the runtime; it does not create an actor. The handler cannot pass an arbitrary implementation to a start command. An undeclared target or incompatible initialization payload is rejected by TypeScript and runtime validation.

The runtime automatically queues child lifecycle notifications to the creator:

```ts
{ type: 'Next', value: { type: 'system:started', actor: 'worker', ref } }
{ type: 'Next', value: { type: 'system:stopped', actor: 'worker', ref } }
{ type: 'Next', value: { type: 'system:stopped', actor: 'worker', ref,
  error: { name: 'Error', message: 'Work failed' } } }
```

`error` is omitted on normal termination and carries serializable failure data when the handler throws. `actor` is the declaration's alias and `ref` is the particular child's typed reference. The handler's input type includes these variants automatically, derived from its declared children. Narrowing the alias narrows the reference to that child's accepted protocol. The application's message schema and `ActorRef<M>` still describe only application payloads, so senders gain no capability to construct system notifications. For application payloads without a `type` field, the exported `isSystemMessage(value)` guard separates runtime notifications from application values.

`system:started` means installation committed and the child's mailbox exists. It does not mean its `Start` handler succeeded or that application initialization finished. The child receives `Start` before its initial `Next`; it runs independently, without waiting for its creator to handle the notification, and may already have stopped by the time that notification is handled. `Start.parentId` is the creator's ID, or `null` when the host created the instance. The creator receives `system:stopped` after the child retires, after any final messages that child already committed to the creator. A child that fails during `Start` therefore produces `system:started` followed by `system:stopped` with an error.

Notifications join the creator's ordinary FIFO mailbox. A stopped creator discards pending notifications and receives no new ones. Root actors have no creator to notify. Forced closure of the entire runtime cannot deliver further notifications. Application correlation IDs can be carried in work messages and replies; there is no creation-reply mapper or special correlation field in the start command.

The parent/child relationship records creation, not lifetime ownership. A child can outlive its creator. Stopping an actor does not follow references, cancel all its conversations, or recursively stop actors it created. An actor can explicitly request its children to stop using `Command.stop(childRef.id)`, or coordinate shutdown through its own application protocol. Global shutdown reaches every live actor directly. External start and send calls close when shutdown starts, while actor commands may continue during the grace period. A child created then receives `Start`, its initial `Next`, and `Stop`, and shares the original shutdown deadline.

A targeted stop request uses the destination's ordinary FIFO mailbox and imposes no individual timeout. It does not guarantee the actor will obey or finish cleanup. `system:stopped` reports actual retirement to that actor's creator, which need not be the sender of the stop request. Global shutdown separately applies its shared deadline to every live actor.

A transition returns an array of commands. `Command.stop()` or `Command.stop(null)`, if present, must be last; an addressed `Command.stop(actorId)` does not end the sender and need not be last. The runtime validates the complete result before committing it; invalid state, an invalid command, or a thrown handler produces no partial state or command commit. A thrown handler terminates only that actor and sends its creator `system:stopped` with `ErrorData`: a plain `name` and `message`, with an optional `stack`. Its last committed state and error remain visible through inspection. Invalid returned state or commands, illegal runtime mutations, and runtime defects fail the local runtime with `EXECUTION_FAILED`. Already committed work from earlier transitions is not rolled back.

## Mailboxes and flow control

Each instance has its own logically unbounded FIFO mailbox. No message priority, automatic admission backpressure, or built-in credit protocol is present. The scheduler gives actors independent turns even when one mailbox has a large backlog.

An application can limit outstanding work explicitly:

```text
consumer → producer: Request(4)
producer → consumer: Item(a), Item(b), Item(c), Item(d)
consumer finishes a → producer: Request(1)
```

Replenishing credit after finishing work bounds in-progress work; replenishing it immediately on receipt only bounds delivery up to that point. Multiple producers must share an appropriate capacity policy if they compete for the same resource.

This works for cooperative senders. A receiver cannot prevent allocation of already admitted messages by rejecting them inside `on`. Physical memory remains finite, and unbounded queues do not provide overload protection. Resource quotas and explicit exhaustion policies belong to future executor work. No persistence or exactly-once guarantee follows from an accepted local send.

## Precedents and lessons

Solid deliberately uses established ideas. Its present shape is **typed actors with pure transitions and explicit command descriptions**. The following comparisons are design lessons, not claims that Solid implements those runtimes' guarantees.

| Precedent | Shared idea | Lesson for Solid |
| --- | --- | --- |
| [Elm architecture](https://guide.elm-lang.org/architecture/) and [commands](https://guide.elm-lang.org/effects/) | A pure function of message and state returns the next state and descriptions of work. | Keep the transition small. Actors additionally need identities and communication rules. `Start` supplies a place to issue initial commands. |
| [Erlang processes](https://www.erlang.org/doc/system/ref_man_processes.html) and [Elixir GenServer](https://hexdocs.pm/elixir/GenServer.html) | Independent state, references, and sequential handling of messages. | Mailbox growth and slow work require policy. Solid's synchronous JavaScript scheduler does not provide BEAM's execution isolation or preemption. |
| [Akka Typed interaction patterns](https://doc.akka.io/libraries/akka-core/current/typed/interaction-patterns.html) | Typed actor references and explicit reply addresses. | Preserve message types through references and child declarations. Request/reply is a protocol; a send alone is not a processing acknowledgement. |
| [Elixir GenStage](https://hexdocs.pm/gen_stage/GenStage.html) | Stateful producers and consumers with demand. | Model demand explicitly. A small actor core can support several application flow-control protocols. |
| [RxJS concatMap](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/concatMap.ts) | Compose work that proceeds over time. | Concurrency limits can move overload into an unbounded queue. Stream operators can be a library over actors without defining every actor's lifetime. |
| [Temporal workflow execution](https://docs.temporal.io/workflow-execution) | Long-lived work and explicit external interactions. | Durability requires recovery and external-effect contracts. Pure transitions and serializable data are useful foundations but are insufficient by themselves. |

Supervision, durable recovery, distribution, timers, subscriptions, external I/O adapters, and metering remain separate design work. The current implementation establishes the typed communication and execution core on which those can be built.
