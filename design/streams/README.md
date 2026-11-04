# Solid Streams: proposed language and API

> Historical, unapproved draft. The API is being reviewed one decision at a time. Cold execution must also be supported, so the hot-only assumptions below are superseded. These declarations and examples are reference material, not the replacement specification.

Design proposal, 4 November 2026. These APIs are **not implemented or exported** by the current package. [api.ts](api.ts) contains declarations, [examples.ts](examples.ts) contains consumer examples, and [types.test.ts](types.test.ts) checks the proposed type boundary. They are not runnable programs. The earlier implementation has been removed; see the [current design status](../../docs/streams.md).

Check this proposal with:

```sh
npx tsc --noEmit -p design/streams/tsconfig.json
```

## The language boundary

Streams owns its programming model. Subject authors use schemas, state, one `on` handler, and Streams commands. Their definitions expose no `ActorRef`, actor targets, actor commands, `sink`, wire subscription IDs, or monitoring calls. A private adapter compiles their transitions and topology into execution on an actor backend. Another backend can satisfy the same contract.

The API has three everyday concepts:

| Concept | Meaning |
| --- | --- |
| Subject definition | Reusable behavior: input and output schemas, state, a pure handler, and permitted child definitions. It is not running. |
| Running Subject | One hot execution. Its host handle can send inputs and request Stop. |
| Stream | Read access to one hot output. Composition and subscriptions observe that execution. |

A subscription identifies a particular attachment. It never starts or restarts a Subject. Two subscribers observe the same execution and state; operators continue with zero listeners until they terminate or are explicitly stopped.

Host handles have methods such as `.next()`, `.pipe()`, and `.subscribe()`. Subject state and messages carry **inert references**, not those imperative handles. `SubjectRef<I, O>` permits typed input and lifecycle commands; `StreamRef<O>` permits observing output. They are distinct capabilities issued by the Streams executor. `subjectSchema` and `streamSchema` retain those types at data boundaries.

## One handler for a Subject

```ts
const Counter = defineSubject({
  id: 'counter.v1',
  input: s.number,
  output: s.number,
  state: s.number,
  initialState: 0,

  on({ message, state }) {
    if (message.type === 'Start') {
      const initial = message.value ?? state;
      return { state: initial, commands: [Command.next(initial)] };
    }
    if (message.type === 'Next') {
      const next = state + message.value;
      return { state: next, commands: [Command.next(next)] };
    }
    // Stop: optional synchronous cleanup descriptions; retirement is automatic.
  },
});
```

`Command` here belongs to Streams. Commands are descriptions returned for validation and commit, not immediate calls into the executor. Output, state, input, references, and child targets remain typed. The handler is synchronous and pure by contract. Returning actor commands is rejected by the public types and must also be rejected by the implementation.

`Start.value` uses the same input schema as `Next.value`. Initialization does not require a separate configuration type, and an initial value is not delivered again as `Next`. These are language lifecycle messages; they need not match the backend actor's startup events.

The basic command vocabulary is:

| Command | Meaning |
| --- | --- |
| `Command.next(value)` | Publish an output value. |
| `Command.stopped(error?)` | Commit this output's terminal result. |
| `Command.send(subjectRef, input)` | Send typed input to another Subject. |
| `Command.stop(subjectRef)` | Request another Subject's graceful Stop. |
| `Command.subscribe(streamRef, slot, { key? })` | Bind a borrowed stream to a declared input route. |
| `Command.unsubscribe(slot, { key? })` | Close that managed binding. |
| `Command.start(subjects.child, input, { into, key? })` | Create declared work and attach its output before it starts. |

`Command.start(subjects.child, input)` without `into` creates independent work that may outlive its creator. This proposal does not expose a resulting reference receipt for that form. Use a managed output binding when the creator needs to consume results; a later design can add typed creation receipts without exposing actor lifecycle messages.

## Composition and startup

Retain familiar `.pipe(...)`, `map`, `filter`, `scan`, `take`, `mergeMap`, `concatMap`, and `switchMap` vocabulary. Keep the chosen `Next | Stopped(error?)` event protocol and queued execution.

```ts
const Counts = Counter.pipe(
  map(s.string, count => `Count: ${count}`),
  take(10),
);

const app = createStreams();
const counter = app.start(Counts, {
  value: 5,
  observer: {
    next: console.log,
    stopped: error => console.log(error ?? 'Finished'),
  },
});

counter.next(2);
```

Composing a **definition** is inert. `app.start` installs the internal connections and the initial observer before any language `Start` handler runs. Starting the same definition twice creates two explicitly requested executions. Subscription does not create another execution.

A composed definition owns the internal stages it creates. Before publishing its terminal result, the executor seals the composite's input and records Stop obligations for any still-running internal stages. Individual cleanup can finish afterward. This gives the returned handle one coherent lifetime, even if an internal `take` completes before its upstream computation.

Composing a **running stream** creates a new live branch attached to it:

```ts
const inbox = app.subject(emailSchema);
const totals = inbox.stream.pipe(
  scan(s.number, 0, count => count + 1),
);
totals.subscribe({ next: console.log });
```

That branch observes values after attachment; it cannot recover startup or historical values. Use definition composition and an initial observer when startup output matters. Calling host constructors from a handler or an operator projection is forbidden by the pure-transition contract; use declared creation commands instead.

A live branch owns its new operator stages and borrows the existing source. Its termination cleans up those stages and detaches from the source; the shared source continues.

Inert references can appear in Subject data; host observer functions cannot. `.subscribe({ next, stopped })` is an output adapter at the host boundary, not a durable computation. Consumers that need durable state are Subjects connected through managed subscriptions. A host observer throwing closes/reports its own observer failure and must not fail a shared producer.

`app.stop(stream.ref)` explicitly stops a live branch without stopping borrowed upstream producers. A running Subject also has `.stop()`. Whole-executor `app.stop()` requests shutdown of all executions. Details of deadlines and host observation acknowledgements remain executor concerns.

## Higher-order work without actor leakage

There are two explicit forms of higher-order composition:

```ts
// Observe an existing hot producer.
choices.stream.pipe(
  switchMap(s.string, choice => choice.stream),
);

// Start declared work for each request.
queries.stream.pipe(
  switchMap(Search, query => query),
);

// Keep up to four inner streams active.
emails.stream.pipe(
  mergeMap(EmailAgent, email => email, 4),
);
```

The first form returns an inert `StreamRef`; switching only detaches the previous binding. The definition overload declares the permitted child behavior before execution. The mapper returns typed initialization data, never an arbitrary implementation or a host call to `app.start`.

The definition overload owns its created executions. Replacement closes the old binding, rejects queued deliveries from its old generation, and requests that execution to Stop. New work need not wait for old cleanup. Its output binding is installed before its first `Start`, so startup output cannot race ahead of the consumer. This is explicit creation on an outer event; subscriptions themselves remain hot.

`mergeMap` limits active inner subscriptions. For owned work it also limits creation: buffered outer values are projected and started only when capacity becomes available. A terminated inner frees a slot even while its worker finishes cleanup, so this is not a bound on all physical workers or cleanup tasks. Borrowed work was already running; limiting observation does not limit its production. `concatMap` uses concurrency one; queued references to existing hot streams may have missed earlier values, but their retained terminal result prevents indefinite waiting.

Normal outer completion waits for the active inner work and pending admitted work. An outer or active-inner error fails the operator, detaches borrowed inputs, and requests Stop for owned work. Other branches of a shared producer continue. A superseded inner's late failure cannot fail its replacement.

The email examples cover one continuing aggregate and a fresh execution for each email. The sample agents are deterministic placeholders; AI calls, timers, and external effects need separate executor-backed adapters.

## Custom subscriptions remain explicit and typed

A custom Subject declares static routing functions:

```ts
subscriptions: {
  replies: route(resultSchema, (event, { key }) => ({
    type: 'Reply' as const,
    key,
    event,
  })),
}
```

The route's result must match that Subject's input schema. It maps both values and `Stopped(error?)` into the existing `on(Next)` input protocol, so the Subject chooses whether an input ending should end its own output. Routes are definition code, never closures returned in a command or saved in state.

The handler receives inert typed route slots. `Command.subscribe(source, subscriptions.replies, { key })` replaces the binding for that slot/key; distinct keys permit concurrent work. Omitted keys use the default key `''`; `unsubscribe(slot)` closes only that binding, while Subject termination releases every binding. The executor maintains private generations and filters obsolete deliveries before routing them. `Command.start(subjects.agent, input, { into: subscriptions.replies, key })` creates and attaches owned work in one operation.

These are managed relationships: no `sink`, string-built actor addresses, cleanup callback, or explicit monitoring. Closing a Subject releases its subscriptions even if its handler fails. Subscription identities, generations, and ownership records belong to executor state and are available to inspection rather than hidden in mutable closures.

Replacing or unsubscribing an `into` binding also requests Stop for the execution it owns. Borrowed bindings only detach. Every Subject completion or failure closes its remaining bindings and requests Stop for its owned work; independently started work may continue.

## Completion, cleanup, and failure

A logical Subject retains a terminal result independently of its executing worker. A completed worker can retire; late subscribers receive the saved `Stopped(error?)`, with no value replay or new execution. The initial retention policy keeps terminal metadata until executor disposal.

`Command.stopped` records the result, closes managed bindings, and schedules one `on(Stop)` cleanup transition when possible. It does not wait for cleanup to publish the terminal result. A later cleanup failure is a diagnostic and cannot change the published result. For an external Stop request, the executor closes input when processing the request, runs `on(Stop)`, then commits normal termination unless that handler chooses an error or fails. Returning from Stop is sufficient to finish; no public worker self-stop command is needed.

Cleanup may describe messages to existing Subjects. It cannot publish new application values, reopen subscriptions, or await asynchronous work. The backend always releases managed relationships. Arbitrary user cleanup cannot be guaranteed after an unrecoverable worker failure.

Subscribers automatically receive `Stopped(error)` when their logical producer permanently disappears without a terminal result. This includes a producer failing before it processes an attachment. The first terminal result wins; an ordinary worker retirement cannot turn previously recorded success into failure. Unsubscription remains silent and does not terminate a borrowed producer.

The [backend contract](backend-contract.md) specifies ordering, pending attachments, retries, cancellation, and acceptance scenarios. Those scenarios are requirements for implementation tests, not tests of the current runtime.

## Durability and the actor backend

Logical Subject identity must be distinct from a particular worker or server. A recoverable worker failure can resume the same execution; temporary loss of contact alone must not produce an irrevocable stream failure. Permanent logical failure does.

Durable execution needs versioned behavior identities, explicit initialization data, committed state and commands, persisted connection generations, terminal records and delivery obligations, and protection against obsolete workers committing. The public language must not depend on synchronous cross-subject calls or in-process identity. JavaScript closures do not become portable merely by having TypeScript types; code registration and captured configuration need an explicit deployment contract.

An initial adapter can implement these language responsibilities with private coordinator and worker actors, using existing child lifecycle notices. This does not require exposing a watch API or immediately changing the core's `Start` shape. It also does not require a single global coordinator in a distributed backend.

This proposal makes no claim that the current runtime supplies recovery, distribution, durable effects, bounded resource use, or exactly-once external effects. The first implementation slice should exercise a custom Subject, both `switchMap` forms, startup delivery, terminal replay, and unexpected producer death before expanding the operator set.

Solidify can construct these definitions and compositions and inspect their executions. The graph UI remains a programming interface to Streams; the actor core remains an execution backend.
