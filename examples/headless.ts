import assert from 'node:assert/strict';
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
  state: s.number,
  initialState: 0,
  on: ({ message, state }) => {
    if (message.type === 'Stop') return { commands: [Command.stop()] };
    if (message.type !== 'Next') return;
    return {
      state: state + 1,
      commands: [Command.send(message.value.replyTo, {
        type: 'result',
        value: message.value.value * 2,
      })],
    };
  },
});

const coordinator = defineActor({
  id: 'coordinator',
  message: s.object({ type: s.literal('work'), work }),
  state: s.union(s.null, work),
  initialState: null,
  actors: { worker },
  on: ({ message, state, actors }) => {
    if (message.type === 'Stop') return { commands: [Command.stop()] };
    if (message.type !== 'Next') return;
    if (message.value.type === 'work') return {
      state: message.value.work,
      commands: [Command.start(actors.worker, message.value.work)],
    };
    if (message.value.type === 'system:started' && state !== null) {
      // The declared child determines this reference's exact message type.
      // The child processes its initial job before this second job.
      return {
        commands: [
          Command.send(message.value.ref, { ...state, value: state.value + 1 }),
          // Stop is queued after the work; the worker terminates when it handles it.
          Command.stop(message.value.ref.id),
        ],
      };
    }
    if (message.value.type === 'system:stopped') {
      // The worker sent its results before it terminated.
      return { commands: [Command.stop()] };
    }
  },
});

const runtime = createTestRuntime();
const replies = runtime.start(collector, { id: 'results' });
runtime.start(coordinator, {
  id: 'coordinator',
  message: { type: 'work', work: { value: 20, replyTo: replies } },
});
runtime.flush();

const actors = runtime.inspect().actors;
assert.deepEqual(actors.find(actor => actor.id === replies.id)?.state, [40, 42]);
assert.equal(actors.find(actor => actor.id === 'coordinator')?.status, 'stopped');
assert.equal(actors.find(actor => actor.definition === 'worker')?.status, 'stopped');
assert.equal(actors.find(actor => actor.definition === 'worker')?.state, 2);
console.log('Received [40, 42]. The coordinator observed its worker start and stop.');

// The collector remains alive until whole-runtime shutdown requests Stop.
const stopping = runtime.stop();
runtime.flush();
await stopping;
assert(runtime.inspect().actors.every(actor => actor.status === 'stopped'));
console.log('All actors stopped.');
