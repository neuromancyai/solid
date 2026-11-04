// Historical, unapproved hot-only draft; cold execution and API decisions are under review.
/**
 * Proposed Streams API examples. This is a typechecked design artifact, not a
 * runnable program: ./api contains declarations without an implementation.
 * None of the APIs illustrated here are claimed to ship in the current runtime.
 */
import {
  Command,
  createStreams,
  defineSubject,
  eventSchema,
  map,
  mergeMap,
  route,
  s,
  scan,
  streamSchema,
  switchMap,
} from './api';

const app = createStreams();

// A Subject author writes one pure handler. The backend owns actor addresses,
// subscription bookkeeping, scheduling, and producer-failure notification.
const counter = defineSubject({
  id: 'example.counter',
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
    // Stop is the one cleanup transition; the executor finalizes it itself.
    return;
  },
});

// Composing a definition is inert. Installation connects this topology and its
// initial observer before any Start handler runs, so startup output is visible.
const counterLabels = counter.pipe(
  map(s.string, value => `Count: ${value}`),
);

// Host observers are output adapters. Their callbacks are not durable Subject
// handlers and are never stored in a Subject's state.
// Start.value is initialization delivered once, not an additional Next message.
const count = app.start(counterLabels, {
  value: 10,
  observer: {
    next: label => console.log(label),
    stopped: error => console.log(error ? error.message : 'Counter finished.'),
  },
});

// This extra live observer shares the same execution. It may miss startup or
// earlier values; attaching it does not create another counter.
const display = count.stream.subscribe({
  next: label => console.log('Additional observer:', label),
});
count.next(2);
count.next(3);

// These are separate capabilities. A readable stream reference cannot be used
// to send the Subject arbitrary inputs; the type tests cover that distinction.
const countInput = count.ref;
const countOutput = count.stream.ref;
void countInput;
void countOutput;

// One stateful computation consumes the whole email sequence.
const emailSchema = s.object({
  id: s.string,
  sender: s.string,
  subject: s.string,
});
const summarySchema = s.object({
  count: s.number,
  text: s.string,
});
const emails = app.subject(emailSchema);
const aggregate = emails.stream.pipe(
  scan(s.array(emailSchema), [], (previous, email) => [...previous, email]),
  map(summarySchema, batch => ({
    count: batch.length,
    text: batch.map(email => email.subject).join(' | '),
  })),
);
aggregate.subscribe({ next: summary => console.log('Aggregate:', summary) });

// A fresh execution per email uses a typed Subject definition. This deliberately
// deterministic placeholder performs no AI calls or external I/O.
const emailAgent = defineSubject({
  id: 'example.email-agent-placeholder',
  input: emailSchema,
  output: summarySchema,
  state: s.null,
  initialState: null,
  on({ message }) {
    if (message.type === 'Stop') return;
    const email = message.value;
    if (!email) return {
      commands: [Command.stopped({ name: 'MissingEmail', message: 'An email is required.' })],
    };
    return {
      commands: [
        Command.next({ count: 1, text: `Preview: ${email.subject}` }),
        Command.stopped(),
      ],
    };
  },
});

const individual = emails.stream.pipe(
  mergeMap(emailAgent, email => email, 4),
);
individual.subscribe({ next: summary => console.log('Per email:', summary) });

// A custom coordinator can also create declared work using only Streams commands.
// Its mailbox includes ordinary inputs and statically routed child output events.
const emailCoordinator = defineSubject({
  id: 'example.email-coordinator',
  input: s.union(
    s.object({ type: s.literal('Email'), email: emailSchema }),
    s.object({ type: s.literal('Result'), job: s.string, event: eventSchema(summarySchema) }),
  ),
  output: summarySchema,
  state: s.number,
  initialState: 0,
  subjects: { agent: emailAgent },
  subscriptions: {
    results: route(summarySchema, (event, { key }) => ({ type: 'Result' as const, job: key, event })),
  },
  on({ message, state, subjects, subscriptions }) {
    if (message.type !== 'Next') return;
    if (message.value.type === 'Email') return {
      state: state + 1,
      commands: [Command.start(subjects.agent, message.value.email, {
        into: subscriptions.results,
        key: String(state),
      })],
    };
    const event = message.value.event;
    if (event.type === 'Next') return { commands: [Command.next(event.value)] };
    if (event.error) return { commands: [Command.stopped(event.error)] };
    // One successful child ending does not end this continuing coordinator.
  },
});
const coordinator = app.start(emailCoordinator);
coordinator.next({ type: 'Email', email: { id: 'c', sender: 'casey@example.test', subject: 'Follow-up' } });

emails.next({ id: 'a', sender: 'alice@example.test', subject: 'Meeting notes' });
emails.next({ id: 'b', sender: 'bob@example.test', subject: 'Release checklist' });

// Borrowed producers are already hot. Switching detaches the old connection;
// it does not stop either independent source.
const alpha = app.subject(s.string);
const beta = app.subject(s.string);
const selections = app.subject(s.union(s.literal('alpha'), s.literal('beta')));
const borrowed = selections.stream.pipe(
  switchMap(s.string, selected => selected === 'alpha' ? alpha.stream.ref : beta.stream.ref),
);
borrowed.subscribe({ next: value => console.log('Selected source:', value) });

// Selecting a definition instead creates an execution owned by the operator.
// Its output connection is established before Start is processed. Replacing it
// requests cleanup; obsolete output is ignored while cleanup finishes.
const search = defineSubject({
  id: 'example.search-placeholder',
  input: s.string,
  output: s.string,
  state: s.null,
  initialState: null,
  on({ message }) {
    if (message.type === 'Stop') return;
    const query = message.value;
    if (query === undefined) return { commands: [Command.stopped()] };
    return {
      commands: [Command.next(`Preview for ${query}`), Command.stopped()],
    };
  },
});
const queries = app.subject(s.string);
const owned = queries.stream.pipe(switchMap(search, query => query));
owned.subscribe({ next: value => console.log('Latest execution:', value) });

// A custom operator uses the same single-handler Subject contract. Its managed
// subscription maps upstream events into its declared application input type.
// Routing functions are part of the definition, not closures stored in commands.
const upstreamNumber = s.object({
  type: s.literal('Upstream'),
  event: eventSchema(s.number),
});
const attachNumber = s.object({
  type: s.literal('Attach'),
  source: streamSchema(s.number),
});
const runningTotal = defineSubject({
  id: 'example.running-total',
  input: s.union(attachNumber, upstreamNumber),
  output: s.number,
  state: s.number,
  initialState: 0,
  subscriptions: {
    source: route(s.number, event => ({ type: 'Upstream' as const, event })),
  },
  on({ message, state, subscriptions }) {
    if (message.type === 'Start') {
      if (message.value?.type === 'Attach') return {
        commands: [Command.subscribe(message.value.source, subscriptions.source)],
      };
      return;
    }
    if (message.type === 'Stop') return;
    if (message.value.type !== 'Upstream') return;
    const event = message.value.event;
    if (event.type === 'Stopped') return { commands: [Command.stopped(event.error)] };
    const total = state + event.value;
    return { state: total, commands: [Command.next(total)] };
  },
});

const amounts = app.subject(s.number);
const totals = app.start(runningTotal, {
  value: { type: 'Attach', source: amounts.stream.ref },
});
totals.stream.subscribe({ next: total => console.log('Running total:', total) });

// Detaching a host observer does not terminate the shared counter execution.
display.unsubscribe();
count.stop();
await app.stop();
