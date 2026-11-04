# Solid lifecycle and execution

This document specifies the current in-process actor runtime. It does not promise durable delivery, distributed ordering, preemptive execution, or external resource cleanup.

## Instance lifecycle

A host calls `runtime.start(definition, { id?, message? })`, or an actor returns `Command.start(actors.worker, initialMessage)` for the runtime to execute. Each instance starts with a copy of its definition's initial state and an independent mailbox. `Start` is queued first with `parentId` set to the creator's ID, or `null` for a host-created root. An initial application message follows as `Next`. The parent ID identifies the creator and can address a stop request, but does not replace a typed reference for sending application messages.

A dynamically created actor is immediately an independent scheduled instance. At creation commit, the runtime queues `Next({ type: 'system:started', actor, ref })` to its creator. `actor` is the child declaration's alias; `ref` is the new instance's typed reference. The notification means the child is installed, before its `Start` handler runs. The creator need not process it before the child starts. By the time the creator handles this notification, the child may already have stopped; it is a lifecycle fact, not a guarantee of current liveness. Here `Next(payload)` abbreviates `{ type: 'Next', value: payload }`.

On retirement, the runtime queues `Next({ type: 'system:stopped', actor, ref, error? })` to the creator. `error` is absent for normal termination. Notifications are delivered at most once through ordinary FIFO mailboxes. Previously committed child sends to the creator precede its stopped notification. A child that stops or throws on `Start` still produces started before stopped. The creator's handler types automatically include each declared child's notifications and preserve its exact reference type.

Stopped creators receive no new notifications, and their queued notifications are discarded with the rest of their mailbox. Root actors have no creator to notify. Whole-runtime failure or forced closure prevents any remaining notifications from being processed.

Actors are `running`, `stopping`, or `stopped`. A `Stop` request gives an actor an opportunity to finish. The actor terminates by committing `Command.stop()` or `Command.stop(null)`. This self-termination command must be the final command in its transition. It can send final messages before stopping; already committed deliveries survive its termination. Its remaining mailbox is discarded when it stops, and subsequent sends to its reference are rejected.

`Command.stop(actorId)` queues `Stop` through the addressed actor's ordinary FIFO mailbox. It leaves the sender alive and need not be the last command in a transition. Passing the sender's own ID as a string still queues a graceful request; only `null` or an omitted argument means immediate self-termination at commit. The destination can exchange cleanup messages and eventually return `Command.stop()` itself. There is no per-request timeout or forced termination attached to this command.

The request and notification have different meanings: `Stop` asks the destination to finish; `Next` containing `system:stopped` tells its creator that it has finished. A requester other than the creator does not automatically receive a termination notification.

An actor does not automatically stop when its mailbox is empty, its creator stops, or another actor no longer holds its reference. There are no output subscriptions to count. An application error or completion payload does not implicitly end an instance.

## Transition boundary

One scheduling step processes one actor message using the synchronous `on({ message, state, self, actors })` handler. Its optional result contains a new state and an array of command descriptions. The readonly `actors` map contains inert typed declarations for the permitted children. `Command.start` only constructs a command. The runtime validates the proposed result before applying its state or commands. Only a successful commit installs a child and queues its started notification.

A thrown handler terminates that actor, discards its remaining mailbox, and notifies its creator with `system:stopped` and serializable `ErrorData`. The error contains `name` and `message`, and may include `stack`; it is not a native JavaScript `Error` object. Other actors continue running. A root's error is visible through inspection even though it has no creator. The failing transition does not partially update state, deliver messages, or create children; the actor retains its last committed state.

Invalid returned state or commands, forbidden runtime mutation from a handler, and runtime defects fail the runtime with `EXECUTION_FAILED` and stop all its actors. This is an in-memory commit boundary, not a durable transaction. It cannot roll back direct JavaScript side effects; performing such effects inside a supposedly pure handler violates the contract.

An actor can also report an application failure without terminating: define a typed failure payload and send it to the interested actor. That recipient chooses whether to recover, retry, or stop. There is no built-in `Error` or `Complete` terminal envelope, and lifecycle notifications do not restart actors automatically.

Deferred design: an explicit failed self-termination command, tentatively `Command.fail(error)`, could commit final state and outgoing messages before producing `system:stopped` with an error. This would differ from throwing, which discards the transition. It is not part of the current API.

Input, state, and command payloads are validated and copied at boundaries. Runtime-issued references retain their identity. References contain a readable ID but are opaque capabilities; ordinary objects or IDs cannot be substituted for them. Their schemas constrain which application messages they can accept. The top-level payload `type` prefix `system:` is reserved for runtime-generated notifications; nested application data is unaffected. Use `isSystemMessage(value)` when an application payload needs a type guard to distinguish it from lifecycle notifications.

## Scheduling

Each actor has a logically unbounded FIFO mailbox. Sending enqueues a message without synchronously calling the destination's handler. There is no priority or automatic readiness handshake.

The runtime schedules ready actors round-robin, taking one transition at a time from each. Fairness is per actor, including dynamically created children. A busy mailbox does not consume every scheduling turn while another actor is ready.

`createRuntime({ maxStepsPerTurn? })` drives scheduling automatically. It yields to the JavaScript event loop between batches; the default batch budget is 1,024 transitions. When there is no ready work, it remains idle until a message arrives or a pending shutdown deadline requires action.

A transition budget controls how much cooperative work runs in a host turn. It does not cap retained messages or interrupt a running handler. A handler performing expensive synchronous work blocks all actors on the same JavaScript thread. Stronger CPU isolation requires another execution mechanism.

FIFO ordering is local enqueue order. It does not establish ordering across a future distributed transport. No transport, retry, deduplication, or durable message log is implemented.

## Test execution

`createTestRuntime({ now?, maxStepsPerTurn? })` uses explicit logical time and manual scheduling. `flush({ maxSteps? })` processes currently queued work up to the budget. `advanceTo(time, { maxSteps? })` advances the clock before driving work. Reaching an active shutdown deadline forces remaining actors to stop before another message is processed, even when mailboxes remain busy.

Advancing time does not generate application timer messages: timers are not part of this minimal core. Neither operation waits for external I/O. The live runtime requires no manual flushing.

A test can stop cooperatively by driving the requests it just queued:

```ts
const stopping = runtime.stop();
runtime.flush();
await stopping;
```

If actors ignore the stop request, advance through the deadline before awaiting settlement. Flushing an empty mailbox alone cannot finish shutdown while an actor remains alive.

## Whole-runtime shutdown

`runtime.stop({ gracePeriodMs: 5_000 })` performs these steps:

1. Seal external `runtime.start` and `runtime.send` calls.
2. Queue one `Stop` for every live actor, including children whose creators have already stopped.
3. Continue scheduling actor commands, including sends and declared dynamic starts.
4. Settle when every actor has stopped, or force the remaining actors to stop at the shared deadline.

Repeated stop calls return the same pending outcome and do not extend its deadline. Cleanup may be a state machine: receive `Stop`, send requests to peers, handle their replies, then issue `Command.stop()`. Queued work and cleanup may still start declared children during the grace period. A new child receives `Start`, its initial `Next`, then `Stop`, and shares the original shutdown deadline. Creating another child does not extend that deadline.

Stop uses ordinary FIFO delivery. It does not overtake old messages, reserve queue capacity, or clear a mailbox to make room. A sufficiently large backlog can prevent an actor from seeing `Stop` before forced termination. The deadline also cannot interrupt an infinitely running synchronous handler.

No parent-to-child propagation is required. Each actor receives the runtime request directly; no actor must forward it for the global shutdown mechanism to work. An actor may also queue targeted requests with `Command.stop(actorId)`. These requests have no independent deadline. Application-specific stop messages remain ordinary payloads and may have different semantics.

There is no effect host, subscription cleanup registry, or asynchronous disposer tracking in this core. A future external adapter must specify its own resource lifetime and cancellation contract. Stopping an actor currently ends its local execution and retained mailbox; it does not prove that an external resource was released.

## Inspection and limits

`runtime.inspect()` exposes runtime status and logical time, any active shutdown deadline or error, and actor records containing `id`, `definition`, `state`, `status`, queued count, and optional actor failure `error`. Inspection is diagnostic state, not a restorable checkpoint or an output subscription.

Stopped actors retain their final state and definition in this runtime's registry for inspection. Their mailboxes are discarded, but registry retention is currently unbounded; a host that creates actors indefinitely will need a retirement policy.

Logically unbounded mailboxes consume finite host memory. Application demand protocols can bound cooperative traffic, but actors cannot reject a message before it consumes mailbox memory merely by handling it later. The scheduling budget does not impose memory, actor-count, or execution-cost quotas.

References are local to a runtime. Runtime-issued tokens are not serialized addresses, and actor definitions retain JavaScript callbacks. Recovery, node placement, authenticated transport, resource quotas, billing, and restart supervision are not implemented.
