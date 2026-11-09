# Solid

A small actor reducer and driver with extensible commands.
The package exports the runtime and driver from `@neuromancy/solid`.

## Core

- `src/actor/runtime.ts`: `reducer(definitions)` interprets one runtime request, or
  advances one ready actor by one message when passed `null`. It returns state,
  a response, and command batches for the environment to execute.
- `src/actor/driver.ts`: `createDriver(reduce, options)` owns the work queue.
  `request(request)` queues work and returns a promise of its matching response type. `step()` processes
  one queued item, or advances the reducer with `null` when the queue is empty.
  `batch(maxSteps)` yields to the event loop, then executes a bounded number of steps.
- `src/utility.ts`: own-property lookup for actor and definition records.

Actors return their next state and commands. Built-in commands support starting,
sending, stopping, exiting, and watching actors. Custom commands go to the driver's
`execute(command, sourceId)` function, which returns `Promise<RuntimeRequest | null>`.
The driver does not await executors during a step; resolved requests join its queue.

Declare the custom-command union once with `reducer<AppCommand>(definitions)`.
Definitions can emit `SystemCommand | AppCommand`, and the driver infers `AppCommand`
for its executor and callbacks. Use `reducer<never>(definitions)` for an explicitly
system-only runtime. Each actor keeps its own state and message types.

The host drives execution explicitly. Queue a request, call `step()` or `batch()`,
then await its response. Loop over `await driver.batch(maxSteps)` to keep advancing
while allowing asynchronous work to settle.

## Lifecycle

Actors process messages in FIFO order, with ready actors taking turns. A stop request
queues a stop message; the actor can receive cleanup replies before exiting. An actor
may also exit naturally. A thrown actor handler immediately stops that actor with a
failure reason. Watchers receive termination notifications, including on a late watch.

An empty runtime remains running. The host requests `STOP_RUNTIME` to begin shutdown;
new actors are then refused while existing actors finish cleanup. `DriverPhase.STOPPED`
requires the runtime to be stopped, the work queue to be empty, and all executors to settle.

Expected request refusals are response data, observable through `onResponse`.
Executor failures are recoverable and optionally reported through `onExecutorError`.
Malformed requests and broken runtime invariants throw. An exception thrown by
`onExecutorError` faults the driver and is surfaced on subsequent driver calls.

## Development

```sh
npm ci
npm test
npm run typecheck
npm run format:check
npm run build
```

Behavior tests cover the reducer and driver scheduling, effects, and shutdown.
`npm run typecheck` also checks the command types across definitions, reductions, and executors.
