# Proposed Streams backend contract

> Historical, unapproved draft. The hot-only assumptions below must be reconsidered for cold execution. See the [current design status](../../docs/streams.md); this contract is not adopted.

This is a design proposal, not the behavior of an implemented executor. The current Solid runtime is an in-memory, local actor runtime; it does not provide durable storage, distributed execution, leases, or recovery. The proposed Subjects language hides that backend: application code sees typed Subject definitions, handles, events, and commands, never actor references, actor targets, monitoring primitives, or actor commands.

## Logical identity and execution

A Subject has a stable logical identity and an executor-owned record. Its executing worker is replaceable. Logical states are `Installing`, `Running`, `Closing`, and `Terminal`; `Closing` covers a requested host stop awaiting its cleanup transition. Worker cleanup can continue after logical termination. Recovery is an execution condition, not a new Subject identity.

Only the current worker lease or generation may commit transitions. An old worker cannot publish after replacement. A temporary disconnect triggers the executor's recovery policy, not an immediate `Stopped(error)`: recoverable work resumes under the same Subject identity and committed state. Exhausting the recovery policy, explicit forced termination, or an unrecoverable failure commits a terminal failure.

The first committed terminal result wins. It is an immutable `Stopped` event with an optional error. Retirement of the worker happens after this logical result is recorded; retirement does not erase the result. Initially, terminal records remain until executor disposal. Reclamation, expired handles, and retention limits require a future explicit policy.

## Installation and activation

Defining or composing a definition does not execute its handlers. Explicit instantiation creates a hot Subject: subscribing later attaches to that existing execution and never restarts it. Composing an already-running stream instead installs a new live branch and borrows that existing source.

The executor installs the requested initial connections before delivering the language's `Start` event. Static programs may install their topology before activation. Dynamically created children with an `into` binding use one create-and-attach operation with the same guarantee. Thus an emission from `on(Start)` can reach its intended initial consumers.

Separate operations that start a producer and attach later may miss values; that is ordinary hot behavior. The primary construction API must not depend on host timing or a worker being slow enough for subsequent wiring. Raw backend startup messages are private and need not correspond directly to the language's `Start`.

## Attachment and terminal publication

Attachment has one authoritative ordering point relative to publication and closure:

- For a running Subject, register the connection before confirming attachment. It receives future committed values, not earlier values.
- For a closing Subject, register a pending terminal recipient. It receives the eventual terminal result.
- For a terminal Subject, deliver its recorded terminal result without starting a worker.

If the worker disappears before or during attachment, the executor retains responsibility for the request. Recovery may finish the attachment; permanent failure must settle it with `Stopped(error)`. A liveness precheck, mailbox admission result, or producer acknowledgment alone is insufficient.

Terminal commit records the outcome and the delivery obligations to existing and pending consumers together. A durable backend needs an atomic commit and reliable outbox or an equivalent mechanism. It cannot acknowledge terminal completion and forget consumers because a worker dies before sending notifications.

## Delivery and cancellation

Each connection has a private identity and generation. Applications may use logical connection names; the executor owns wire identities. Delivery preserves a Subject's committed order within a connection, including all accepted values before its terminal event. There is no promised total order across independent producers.

Retries carry stable delivery identities. Duplicate transport deliveries must not cause duplicate logical handler transitions or a second terminal event. Durable replay may execute pure handler code again; this is not a promise that JavaScript or external side effects execute physically once. External effects need a separate idempotency contract.

Unsubscribe is queued. Its effective boundary is the executor's processing of the cancellation, not merely the host call returning. Values processed before that boundary remain observable. After it, the connection gate suppresses later deliveries, including messages already in transport. Cancellation does not invoke a completion callback, and it cannot retract an output already committed by its consumer.

Detachment from the producer may finish later. Obsolete unsubscribe requests and old generations are harmless. Reattaching to the same producer gets a new generation, preventing old deliveries from being mistaken for the new connection. A live registration receives at most one terminal event; cancellation and terminal delivery are resolved by their processing order.

## Terminal commit and worker cleanup

The language uses one `on` handler with `Start`, `Next`, and `Stop`; it adds no `Finalize` callback or public worker self-stop command.

`Command.stopped(error?)` commits a terminal result as part of the current valid transition. That commit seals application input and output, records delivery obligations, and detaches managed subscriptions. Prior values precede the terminal result. It also schedules one logical `Stop` event for best-effort user cleanup if a worker remains available. Observable completion does not wait for that cleanup; the worker retires afterward.

A host stop request first closes application admission and invokes `on(Stop)` once. Its synchronous transition commits normal `Stopped` unless the handler selects an error or throws. If it returns `Command.stopped`, the executor must not schedule a second `Stop`. Cleanup may describe requests to existing Subjects, but cannot publish application values, reopen the Subject, or await asynchronous work. Asynchronous cleanup waiting is not part of this proposal.

If a normal transition throws or proposes invalid state or output, discard that proposal, commit terminal failure, and schedule cleanup from the last committed state. Managed connection release does not depend on user cleanup succeeding. If cleanup fails before any terminal result exists, commit failure; if terminal was already committed, record the cleanup failure only as a diagnostic. Never replace a published result or recursively retry a failed cleanup handler.

If no worker can run cleanup, the executor still closes managed links and publishes the applicable failure. Arbitrary application cleanup is not guaranteed after worker loss. A backend must isolate or otherwise handle a stuck worker; a synchronous cleanup contract does not imply that arbitrary JavaScript is preemptible.

Recovery may reexecute the same pure cleanup transition after an interrupted attempt; its committed effects are deduplicated. A successfully committed cleanup is not invoked again by later worker notifications.

## Borrowed and owned producers

Hot producer lifetime is independent of listener count. Removing the last listener does not implicitly stop a Subject or reset an operator's state.

When `switchMap` selects an existing Subject, it borrows that producer. Switching closes the old connection, suppresses its old generation, and attaches to the new producer; the old producer continues independently.

Creating a child with an `into` binding makes that execution owned through the binding; closing or replacing it requests the child's Stop. Starting without `into` creates an independent execution; host-created roots are independent. Ownership is language metadata, not inferred from backend actor parentage.

A composed definition owns every internal stage it creates. The final output's termination seals composite input and requests Stop for remaining stages. A live branch owns its new stages and borrows the existing source. Scope closure propagates cleanup requests to owned work, but never to borrowed sources. Cleanup may overlap replacement work; inner concurrency limits count active streams, not all workers still cleaning up.

The executor owns the composite admission gate and terminal record. Before publishing the composite terminal result, it must atomically seal that gate and record Stop obligations for remaining owned stages. A tail's terminal notification can be delivered to this authority asynchronously, but the public result cannot race ahead of the gate closure. Late subscriptions resolve against the composite terminal record. Actual worker cleanup remains asynchronous.

The same ownership rule applies to custom slot commands: replacing or unsubscribing an owned `into` binding requests its execution's Stop; removing a borrowed binding only detaches. Every Subject terminal commit, including normal completion, closes all its remaining managed bindings and records Stop obligations for owned work.

Replacing an owned inner also requests its graceful Stop. New work need not wait for old cleanup. Normal outer completion waits for the selected inner to finish; operator failure or explicit termination detaches borrowed inputs and requests closure of owned executions. A late failure from an obsolete connection cannot fail the replacement.

## Acceptance scenarios

These are required outcomes for future implementation tests, not claims of existing coverage.

| Scenario | Required outcome |
| --- | --- |
| New child emits in `Start` | Initial consumers are installed before activation and receive the value. |
| Attach after normal completion or failure | Receive the immutable terminal result once; no worker restart. |
| Worker dies before handling an accepted attach | Recover or settle attachment with failure; never silently discard it. |
| Worker dies after attachment | Recover transparently, or notify the connection of logical failure. |
| Worker dies after terminal commit but before notification | Deliver the recorded outcome from durable obligations. |
| Duplicate terminal or retired-worker publication | Ignore duplicate or obsolete work; committed result remains unchanged. |
| Unsubscribe races with a queued value | Processing order decides; nothing passes the closed connection gate. |
| Switch away and back to the same Subject | Fresh generation rejects deliveries from the earlier attachment. |
| Borrowed producer loses its final listener | Producer remains running. |
| Owned inner is replaced | Close its connection and request Stop; replacement proceeds independently. |
| Host stop cleanup throws before terminal commit | Release managed links and publish terminal failure. |
| Cleanup throws after natural terminal commit | Retain the published outcome; record a diagnostic. |

## Initial adapter and future backends

An initial sealed adapter can use private coordinator and worker actors. Creating workers as coordinator children provides the current core's `system:stopped` notices without exposing monitoring publicly. The coordinator records logical identities, pending attachments, generations, and outcomes; worker activation follows installation. Physical actor creation and logical ownership remain separate.

This is an implementation option, not a requirement for one global coordinator. Future executors may partition records and scheduling while preserving the per-Subject ordering and commit guarantees above. They may replace actors entirely. The language contract depends on lifecycle observation, reliable transition commits, and connection admission—not on a particular actor API.
