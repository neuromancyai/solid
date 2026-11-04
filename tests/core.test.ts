import { describe, expect, it, vi } from 'vitest';
import { Command, createRuntime, createTestRuntime, defineActor, isSystemMessage, s, type ActorRef, type ActorTarget } from '@neuromancy/solid';

const counter = defineActor({
  id: 'counter', message: s.number, state: s.number, initialState: 0,
  on: ({ message, state }) => {
    if (message.type === 'Next') return { state: state + message.value };
    if (message.type === 'Stop') return { commands: [Command.stop()] };
  },
});

describe('actors and mailboxes', () => {
  it('retains the original handler when the specification is later modified', () => {
    const spec = { id: 'snapshot-handler', message: s.null, state: s.number, initialState: 0, on: () => ({ state: 1 }) };
    const actor = defineActor(spec);
    spec.on = () => ({ state: 2 });
    const runtime = createTestRuntime();
    runtime.start(actor);
    runtime.flush();
    expect(runtime.inspect().actors[0]?.state).toBe(1);
  });

  it('queues Start before initial input without running handlers synchronously', () => {
    const events: string[] = [];
    const actor = defineActor({
      id: 'started', message: s.string, state: s.null, initialState: null,
      on: ({ message }) => { events.push(message.type === 'Next' ? message.value : message.type); },
    });
    const runtime = createTestRuntime();
    const ref = runtime.start(actor, { message: 'initial' });
    expect(events).toEqual([]);
    expect(runtime.send(ref, 'later')).toEqual({ status: 'accepted' });
    expect(events).toEqual([]);
    runtime.flush();
    expect(events).toEqual(['Start', 'initial', 'later']);
  });

  it('retains every admitted message without deduplicating identical values', () => {
    const runtime = createTestRuntime();
    const ref = runtime.start(counter);
    for (let index = 0; index < 12_000; index++) expect(runtime.send(ref, 1).status).toBe('accepted');
    const result = runtime.flush({ maxSteps: 20_000 });
    expect(result.processed).toBe(12_001);
    expect(result.pending).toBe(0);
    expect(result.exhausted).toBe(false);
    expect(runtime.inspect().actors.find(actor => actor.id === ref.id)?.state).toBe(12_000);
  });

  it('round-robins ready actors while preserving FIFO within each mailbox', () => {
    const received: string[] = [];
    const actor = defineActor({
      id: 'ordered', message: s.string, state: s.null, initialState: null,
      on: ({ message }) => { if (message.type === 'Next') received.push(message.value); },
    });
    const runtime = createTestRuntime();
    const a = runtime.start(actor);
    const b = runtime.start(actor);
    runtime.flush();
    runtime.send(a, 'a1'); runtime.send(a, 'a2'); runtime.send(a, 'a3');
    runtime.send(b, 'b1'); runtime.send(b, 'b2');
    runtime.flush();
    expect(received).toEqual(['a1', 'b1', 'a2', 'b2', 'a3']);
  });

  it('bounds work per flush without bounding mailbox capacity', () => {
    const runtime = createTestRuntime();
    const ref = runtime.start(counter);
    runtime.send(ref, 1); runtime.send(ref, 2);
    expect(runtime.flush({ maxSteps: 2 })).toMatchObject({ processed: 2, pending: 1, exhausted: true });
    expect(runtime.inspect().actors[0]?.state).toBe(1);
    expect(runtime.flush()).toMatchObject({ processed: 1, pending: 0, exhausted: false });
    expect(runtime.inspect().actors[0]?.state).toBe(3);
  });

  it('uses the default scheduling budget for an actor that keeps messaging itself', () => {
    const loop = defineActor({
      id: 'loop', message: s.number, state: s.number, initialState: 0,
      on: ({ message, self, state }) => {
        if (message.type === 'Start') return { commands: [Command.send(self, 1)] };
        if (message.type === 'Next') return { state: state + 1, commands: [Command.send(self, 1)] };
      },
    });
    const runtime = createTestRuntime();
    runtime.start(loop);
    expect(runtime.flush()).toMatchObject({ processed: 1024, pending: 1, exhausted: true });
  });

  it('clones and freezes mailbox payloads and committed state', () => {
    const schema = s.object({ values: s.array(s.number) });
    const initial = { values: [0] };
    let delivered: unknown;
    const actor = defineActor({
      id: 'immutable', message: schema, state: schema, initialState: initial,
      on: ({ message, state }) => {
        expect(Object.isFrozen(state)).toBe(true);
        expect(Object.isFrozen(state.values)).toBe(true);
        if (message.type === 'Next') {
          delivered = message.value;
          expect(Object.isFrozen(message.value)).toBe(true);
          expect(Object.isFrozen(message.value.values)).toBe(true);
          return { state: message.value };
        }
      },
    });
    const runtime = createTestRuntime();
    runtime.start(actor);
    initial.values.push(100);
    const payload = { values: [1] };
    const recipient = runtime.start(actor);
    runtime.send(recipient, payload);
    payload.values.push(200);
    runtime.flush();
    expect(delivered).toEqual({ values: [1] });
    expect(delivered).not.toBe(payload);
    expect(runtime.inspect().actors[0]?.state).toEqual({ values: [0] });
  });

  it('treats application fields named Complete and Error as ordinary payloads', () => {
    const actor = defineActor({
      id: 'ordinary-data', message: s.object({ type: s.string }), state: s.number, initialState: 0,
      on: ({ message, state }) => message.type === 'Next' ? { state: state + 1 } : undefined,
    });
    const runtime = createTestRuntime();
    const ref = runtime.start(actor);
    runtime.send(ref, { type: 'Complete' }); runtime.send(ref, { type: 'Error' });
    runtime.flush();
    expect(runtime.inspect().actors[0]).toMatchObject({ state: 2, status: 'running' });
  });
});

describe('typed references and application protocols', () => {
  it('replies to a reference carried in the application payload', () => {
    const request = s.object({ amount: s.number, replyTo: s.ref(s.number) });
    let receivedRef: unknown;
    const worker = defineActor({
      id: 'worker', message: request, state: s.null, initialState: null,
      on: ({ message }) => {
        if (message.type !== 'Next') return;
        receivedRef = message.value.replyTo;
        return { commands: [Command.send(message.value.replyTo, message.value.amount * 2)] };
      },
    });
    const runtime = createTestRuntime();
    const recipient = runtime.start(counter);
    const responder = runtime.start(worker);
    runtime.send(responder, { amount: 21, replyTo: recipient });
    runtime.flush();
    expect(receivedRef).toBe(recipient);
    expect(runtime.inspect().actors.find(actor => actor.id === recipient.id)?.state).toBe(42);
    expect(Object.keys(recipient)).toEqual(['id']);
    expect(Object.isFrozen(recipient)).toBe(true);
  });

  it('rejects forged refs, refs from other runtimes, and incompatible nested refs', () => {
    const runtime = createTestRuntime();
    const ref = runtime.start(counter);
    const foreign = createTestRuntime().start(counter);
    expect(() => runtime.send({ id: ref.id } as never, 1)).toThrow();
    expect(() => runtime.send(foreign, 1)).toThrow();
    const receiver = defineActor({
      id: 'receiver', message: s.ref(s.number), state: s.null, initialState: null, on: () => undefined,
    });
    const recipient = runtime.start(receiver);
    expect(() => runtime.send(recipient, foreign)).toThrow();
    expect(() => runtime.send(recipient, { id: ref.id } as never)).toThrow();
    const strings = defineActor({
      id: 'strings', message: s.string, state: s.null, initialState: null, on: () => undefined,
    });
    expect(() => runtime.send(recipient, runtime.start(strings) as never)).toThrow();
    expect(runtime.send(recipient, ref)).toEqual({ status: 'accepted' });
  });

  it('rejects fabricated actor definitions and invalid external payloads', () => {
    const runtime = createTestRuntime();
    expect(() => runtime.start({ ...counter } as never)).toThrow();
    const ref = runtime.start(counter);
    expect(() => runtime.send(ref, 'wrong' as never)).toThrow();
    expect(() => runtime.send(ref, NaN)).toThrow();
    expect(() => runtime.start(counter, { message: 'wrong' as never })).toThrow();
    expect(runtime.inspect().actors).toHaveLength(1);
  });

  it('permits a narrower view of a reference that accepts a broader protocol', () => {
    const broad = defineActor({
      id: 'broad', message: s.union(s.number, s.string), state: s.null, initialState: null, on: () => undefined,
    });
    const receiver = defineActor({
      id: 'narrow-view', message: s.ref(s.number), state: s.null, initialState: null,
      on: ({ message }) => message.type === 'Next' ? { commands: [Command.send(message.value, 42)] } : undefined,
    });
    const runtime = createTestRuntime();
    const broadRef = runtime.start(broad);
    const recipient = runtime.start(receiver);
    expect(runtime.send(recipient, broadRef)).toEqual({ status: 'accepted' });
    expect(() => runtime.flush()).not.toThrow();
  });

  it('delivers committed sends when the sender stops and discards its remaining mailbox', () => {
    const runtime = createTestRuntime();
    const target = runtime.start(counter);
    const sender = defineActor({
      id: 'send-and-stop', message: s.null, state: s.number, initialState: 0,
      on: ({ message, state }) => message.type === 'Next' ? {
        state: state + 1, commands: [Command.send(target, 10), Command.stop()],
      } : undefined,
    });
    const source = runtime.start(sender);
    runtime.send(source, null); runtime.send(source, null);
    runtime.flush();
    expect(runtime.inspect().actors.find(actor => actor.id === source.id)).toMatchObject({ state: 1, status: 'stopped', queued: 0 });
    expect(runtime.inspect().actors.find(actor => actor.id === target.id)).toMatchObject({ state: 10, status: 'running' });
    expect(runtime.send(source, null)).toEqual({ status: 'rejected' });
  });

  it('only creates children when the runtime executes a returned start command', () => {
    const owner = defineActor({
      id: 'declarative-start', message: s.literal('go'),
      state: s.null, initialState: null, actors: { counter },
      on: context => {
        const { message, actors } = context;
        expect(context).not.toHaveProperty('start');
        expect(Object.isFrozen(actors)).toBe(true);
        expect(Object.isFrozen(actors.counter)).toBe(true);
        if (message.type === 'Next' && isSystemMessage(message.value)) return;
        const command = Command.start(actors.counter, 3);
        expect(Object.isFrozen(command)).toBe(true);
        expect(runtime.inspect().actors).toHaveLength(1);
        if (message.type === 'Next' && message.value === 'go') return { commands: [command] };
      },
    });
    const runtime = createTestRuntime();
    const parent = runtime.start(owner);
    runtime.flush();
    expect(runtime.inspect().actors).toHaveLength(1);
    runtime.send(parent, 'go');
    runtime.flush();
    expect(runtime.inspect().actors.find(actor => actor.definition === 'counter')?.state).toBe(3);
  });

  it('creates declared children and reports their typed refs to the creator', () => {
    const workerMessage = s.object({ amount: s.number, replyTo: s.ref(s.number) });
    const worker = defineActor({
      id: 'child-worker', message: workerMessage, state: s.number, initialState: 0,
      on: ({ message }) => {
        if (message.type === 'Next') return {
          state: message.value.amount,
          commands: [Command.send(message.value.replyTo, message.value.amount * 2)],
        };
      },
    });
    const owner = defineActor({
      id: 'owner', message: s.number,
      state: s.object({ total: s.number, child: s.union(s.null, s.ref(workerMessage)) }),
      initialState: { total: 0, child: null }, actors: { worker },
      on: ({ message, state, self, actors }) => {
        if (message.type === 'Start') return { commands: [
          Command.start(actors.worker, { amount: 21, replyTo: self }),
        ] };
        if (message.type !== 'Next') return;
        if (!isSystemMessage(message.value)) return { state: { ...state, total: message.value } };
        if (message.value.type === 'system:started') return { state: { ...state, child: message.value.ref } };
      },
    });
    const runtime = createTestRuntime();
    const parent = runtime.start(owner);
    runtime.flush();
    const state = runtime.inspect().actors.find(actor => actor.id === parent.id)?.state as { total: number; child: { id: string } };
    expect(state.total).toBe(42);
    expect(runtime.inspect().actors.find(actor => actor.id === state.child.id)).toMatchObject({ state: 21, status: 'running' });
  });

  it('lets a child outlive its creator and leaves unrelated actors running', () => {
    let child: ActorRef<number> | undefined;
    const owner = defineActor({
      id: 'short-lived-owner', message: s.null, state: s.null, initialState: null, actors: { counter },
      on: ({ message, actors }) => {
        if (message.type === 'Start') return { commands: [Command.start(actors.counter, 3)] };
        if (message.type === 'Next' && isSystemMessage(message.value) && message.value.type === 'system:started') {
          child = message.value.ref; return { commands: [Command.stop()] };
        }
      },
    });
    const runtime = createTestRuntime();
    const parent = runtime.start(owner);
    const unrelated = runtime.start(counter);
    runtime.flush();
    expect(runtime.send(parent, null)).toEqual({ status: 'rejected' });
    expect(child).toBeDefined();
    runtime.send(child!, 4);
    runtime.send(unrelated, 8);
    runtime.flush();
    expect(runtime.inspect().actors.find(actor => actor.id === child?.id)).toMatchObject({ state: 7, status: 'running' });
    expect(runtime.inspect().actors.find(actor => actor.id === unrelated.id)).toMatchObject({ state: 8, status: 'running' });
  });

  it('delivers system:started as an immutable Next value behind older messages', () => {
    const events: string[] = [];
    let received: ActorRef<number> | undefined;
    const owner = defineActor({
      id: 'fifo-owner', message: s.string,
      state: s.null, initialState: null, actors: { counter },
      on: ({ message, actors }) => {
        if (message.type === 'Start') return { commands: [Command.start(actors.counter, 3)] };
        if (message.type !== 'Next') return;
        if (!isSystemMessage(message.value)) { events.push(message.value); return; }
        if (message.value.type !== 'system:started') return;
        events.push('started'); received = message.value.ref;
        expect(Object.isFrozen(message.value)).toBe(true);
        const childId = message.value.ref.id;
        expect(runtime.inspect().actors.some(actor => actor.id === childId)).toBe(true);
        return { commands: [Command.send(message.value.ref, 4)] };
      },
    });
    const runtime = createTestRuntime();
    const parent = runtime.start(owner, { message: 'initial' });
    runtime.send(parent, 'queued');
    runtime.flush();
    expect(events).toEqual(['initial', 'queued', 'started']);
    expect(runtime.send(received!, 5)).toEqual({ status: 'accepted' });
    runtime.flush();
    expect(runtime.inspect().actors.find(actor => actor.id === received?.id)?.state).toBe(12);
  });

  it('automatically reports the new child to its creator', () => {
    const events: string[] = [];
    const owner = defineActor({
      id: 'fire-and-forget', message: s.null, state: s.null, initialState: null, actors: { counter },
      on: ({ message, actors }) => {
        events.push(message.type === 'Next' && isSystemMessage(message.value) ? message.value.type : message.type);
        if (message.type === 'Start') return { commands: [Command.start(actors.counter, 7)] };
      },
    });
    const runtime = createTestRuntime();
    runtime.start(owner);
    runtime.flush();
    expect(events).toEqual(['Start', 'system:started']);
    expect(runtime.inspect().actors.find(actor => actor.definition === 'counter')).toMatchObject({ state: 7, status: 'running' });
  });

  it('allows the creator to stop before handling its reply without stopping the child', () => {
    const replies: unknown[] = [];
    const owner = defineActor({
      id: 'stop-before-reply', message: s.null, state: s.null, initialState: null, actors: { counter },
      on: ({ message, actors }) => {
        if (message.type === 'Next') replies.push(message.value);
        if (message.type === 'Start') return { commands: [
          Command.start(actors.counter, 3), Command.stop(),
        ] };
      },
    });
    const runtime = createTestRuntime();
    const parent = runtime.start(owner);
    runtime.flush();
    expect(replies).toEqual([]);
    expect(runtime.inspect().actors.find(actor => actor.id === parent.id)).toMatchObject({ status: 'stopped', queued: 0 });
    expect(runtime.inspect().actors.find(actor => actor.definition === 'counter')).toMatchObject({ status: 'running', state: 3 });
  });
});

describe('shutdown and failure', () => {
  it.each(['fabricated', 'copied'] as const)('rejects a %s actor target before committing any commands', kind => {
    const runtime = createTestRuntime();
    const destination = runtime.start(counter);
    const owner = defineActor({
      id: 'invalid-target', message: s.null, state: s.number, initialState: 0, actors: { counter },
      on: ({ actors }) => {
        const target = kind === 'fabricated' ? { name: 'counter' } : { ...actors.counter };
        return { state: 99, commands: [Command.send(destination, 10), Command.start(target as never, 3)] };
      },
    });
    const parent = runtime.start(owner);
    expect(() => runtime.flush()).toThrow();
    const inspection = runtime.inspect();
    expect(inspection.status).toBe('failed');
    expect(inspection.actors).toHaveLength(2);
    expect(inspection.actors.find(actor => actor.id === parent.id)?.state).toBe(0);
    expect(inspection.actors.find(actor => actor.id === destination.id)?.state).toBe(0);
    expect((inspection.error?.cause as Error).message).toContain('Expected an actor target');
  });

  it('rejects a genuine target for a different blueprint under the same declared name', () => {
    let foreign: ActorTarget<number, 'counter'> | undefined;
    const otherCounter = defineActor({
      id: 'other-counter', message: s.number, state: s.number, initialState: 0, on: () => undefined,
    });
    const source = defineActor({
      id: 'other-owner', message: s.null, state: s.null, initialState: null, actors: { counter: otherCounter },
      on: ({ actors }) => { foreign = actors.counter; },
    });
    const otherRuntime = createTestRuntime();
    otherRuntime.start(source);
    otherRuntime.flush();
    const owner = defineActor({
      id: 'owner', message: s.null, state: s.number, initialState: 0, actors: { counter },
      on: () => ({ state: 99, commands: [Command.start(foreign!, 3)] }),
    });
    const runtime = createTestRuntime();
    runtime.start(owner);
    expect(() => runtime.flush()).toThrow();
    const inspection = runtime.inspect();
    expect(inspection.status).toBe('failed');
    expect(inspection.actors).toHaveLength(1);
    expect(inspection.actors[0]?.state).toBe(0);
    expect((inspection.error?.cause as Error).message).toContain("Undeclared actor 'counter'");
  });

  it('broadcasts Stop through ordinary FIFO queues to every live actor', async () => {
    const received: string[] = [];
    const actor = defineActor({
      id: 'shutdown-aware', message: s.string, state: s.null, initialState: null,
      on: ({ message, self }) => {
        received.push(`${self.id}:${message.type === 'Next' ? message.value : message.type}`);
        if (message.type === 'Stop') return { commands: [Command.stop()] };
      },
    });
    const runtime = createTestRuntime();
    const a = runtime.start(actor, { id: 'a' }); const b = runtime.start(actor, { id: 'b' });
    runtime.flush(); received.length = 0;
    runtime.send(a, 'queued');
    const stopping = runtime.stop();
    runtime.flush();
    expect(received).toEqual(['a:queued', 'b:Stop', 'a:Stop']);
    expect(await stopping).toEqual({ status: 'completed', forcedActorIds: [] });
    expect(runtime.inspect().status).toBe('closed');
    expect(runtime.inspect().actors.every(actor => actor.status === 'stopped')).toBe(true);
    expect(() => runtime.send(b, 'late')).toThrow();
  });

  it('allows actor messages during shutdown so cleanup protocols can finish', async () => {
    const cleanup = defineActor({
      id: 'cleanup', message: s.number, state: s.number, initialState: 0,
      on: ({ message, self }) => {
        if (message.type === 'Stop') return { commands: [Command.send(self, 42)] };
        if (message.type === 'Next') return { state: message.value, commands: [Command.stop()] };
      },
    });
    const runtime = createTestRuntime();
    const ref = runtime.start(cleanup);
    runtime.flush();
    const stopping = runtime.stop();
    expect(() => runtime.send(ref, 100)).toThrow();
    runtime.flush();
    expect(await stopping).toEqual({ status: 'completed', forcedActorIds: [] });
    expect(runtime.inspect().actors[0]?.state).toBe(42);
  });

  it('forces uncooperative actors to stop at the deadline', async () => {
    const stubborn = defineActor({
      id: 'stubborn', message: s.null, state: s.null, initialState: null, on: () => undefined,
    });
    const runtime = createTestRuntime({ now: 100 });
    const ref = runtime.start(stubborn);
    runtime.flush();
    const stopping = runtime.stop({ gracePeriodMs: 5000 });
    expect(runtime.stop({ gracePeriodMs: 1 })).toBe(stopping);
    runtime.flush();
    expect(runtime.inspect().actors[0]?.status).toBe('stopping');
    runtime.advanceTo(5099);
    expect(runtime.inspect().status).toBe('stopping');
    runtime.advanceTo(5100);
    expect(await stopping).toEqual({ status: 'forced', forcedActorIds: [ref.id] });
  });

  it('advances to the shutdown deadline even when a self-messaging actor always has work', async () => {
    const busy = defineActor({
      id: 'busy-shutdown', message: s.number, state: s.number, initialState: 0,
      on: ({ message, state, self }) => {
        if (message.type === 'Start') return { commands: [Command.send(self, 1)] };
        if (message.type === 'Next') return { state: state + 1, commands: [Command.send(self, 1)] };
      },
    });
    const runtime = createTestRuntime();
    const ref = runtime.start(busy);
    runtime.flush({ maxSteps: 2 });
    const stopping = runtime.stop({ gracePeriodMs: 5000 });
    expect(runtime.advanceTo(5000, { maxSteps: 1 })).toMatchObject({ processed: 0, pending: 0, exhausted: false });
    expect(runtime.inspect()).toMatchObject({ status: 'closed', now: 5000 });
    expect(runtime.inspect().actors[0]?.state).toBe(1);
    expect(await stopping).toEqual({ status: 'forced', forcedActorIds: [ref.id] });
  });

  it('rejects new external actor creation while stopping', async () => {
    const runtime = createTestRuntime();
    runtime.start(counter);
    const stopping = runtime.stop();
    expect(() => runtime.start(counter)).toThrow();
    runtime.flush();
    await stopping;
  });

  it('validates the whole transition before committing state or sends', () => {
    const runtime = createTestRuntime();
    const target = runtime.start(counter);
    const invalid = defineActor({
      id: 'invalid-transition', message: s.null, state: s.number, initialState: 0,
      on: ({ message }) => message.type === 'Next' ? {
        state: 99,
        commands: [Command.send(target, 10), Command.send(target, 'invalid' as never)],
      } : undefined,
    });
    const source = runtime.start(invalid);
    runtime.send(source, null);
    expect(() => runtime.flush()).toThrow();
    const inspection = runtime.inspect();
    expect(inspection.status).toBe('failed');
    expect(inspection.error).toMatchObject({ code: 'EXECUTION_FAILED' });
    expect(inspection.actors.find(actor => actor.id === source.id)?.state).toBe(0);
    expect(inspection.actors.find(actor => actor.id === target.id)?.state).toBe(0);
    expect(inspection.actors.every(actor => actor.status === 'stopped')).toBe(true);
  });

  it.each([
    ['an invalid state', () => ({ state: 'wrong' })],
    ['an asynchronous transition', async () => ({ state: 1 })],
  ] as const)('fails before committing %s even when TypeScript is bypassed', (_name, on) => {
    const runtime = createTestRuntime();
    const actor = defineActor({
      id: 'invalid-runtime-transition', message: s.null, state: s.number, initialState: 0,
      on: on as never,
    });
    runtime.start(actor);
    expect(() => runtime.flush()).toThrow();
    expect(runtime.inspect()).toMatchObject({ status: 'failed', error: { code: 'EXECUTION_FAILED' } });
    expect(runtime.inspect().actors[0]?.state).toBe(0);
  });

  it('finishes queued startup and backlogged child starts before stopping their children', async () => {
    const worker = defineActor({
      id: 'shutdown-child', message: s.string, state: s.array(s.string), initialState: [],
      on: ({ message, state }) => {
        const next = [...state, message.type === 'Next' ? message.value : message.type];
        return { state: next, commands: message.type === 'Stop' ? [Command.stop()] : [] };
      },
    });
    const owner = defineActor({
      id: 'shutdown-owner', message: s.string, state: s.null, initialState: null, actors: { worker },
      on: ({ message, actors }) => {
        if (message.type === 'Start') return { commands: [Command.start(actors.worker, 'startup')] };
        if (message.type === 'Next' && !isSystemMessage(message.value)) return { commands: [Command.start(actors.worker, message.value)] };
        if (message.type === 'Stop') return { commands: [Command.stop()] };
      },
    });
    const runtime = createTestRuntime();
    const parent = runtime.start(owner);
    runtime.send(parent, 'backlog');
    const stopping = runtime.stop();
    runtime.flush();
    expect(await stopping).toEqual({ status: 'completed', forcedActorIds: [] });
    const children = runtime.inspect().actors.filter(actor => actor.definition === 'shutdown-child');
    expect(children).toHaveLength(2);
    expect(children.map(child => child.state)).toEqual([
      ['Start', 'startup', 'Stop'], ['Start', 'backlog', 'Stop'],
    ]);
    expect(runtime.inspect().actors.every(actor => actor.status === 'stopped')).toBe(true);
  });

  it('includes children created during shutdown in the original grace deadline', async () => {
    const stubborn = defineActor({
      id: 'stubborn-child', message: s.null, state: s.null, initialState: null, on: () => undefined,
    });
    const owner = defineActor({
      id: 'cleanup-owner', message: s.null, state: s.null, initialState: null, actors: { stubborn },
      on: ({ message, actors }) => message.type === 'Stop'
        ? { commands: [Command.start(actors.stubborn, null), Command.stop()] } : undefined,
    });
    const runtime = createTestRuntime({ now: 100 });
    runtime.start(owner);
    const stopping = runtime.stop({ gracePeriodMs: 5000 });
    runtime.advanceTo(4100);
    const child = runtime.inspect().actors.find(actor => actor.definition === 'stubborn-child');
    expect(child).toMatchObject({ status: 'stopping', queued: 0 });
    expect(runtime.inspect().deadline).toBe(5100);
    runtime.advanceTo(5099);
    expect(runtime.inspect().status).toBe('stopping');
    runtime.advanceTo(5100);
    expect(await stopping).toEqual({ status: 'forced', forcedActorIds: [child!.id] });
  });
});

describe('live scheduling', () => {
  it('runs queued transitions automatically and remains alive while idle', async () => {
    const runtime = createRuntime({ maxStepsPerTurn: 2 });
    const ref = runtime.start(counter);
    runtime.send(ref, 1); runtime.send(ref, 2); runtime.send(ref, 3);
    await vi.waitFor(() => expect(runtime.inspect().actors[0]?.state).toBe(6));
    expect(runtime.inspect().status).toBe('running');
    runtime.send(ref, 4);
    await vi.waitFor(() => expect(runtime.inspect().actors[0]?.state).toBe(10));
    expect(await runtime.stop()).toEqual({ status: 'completed', forcedActorIds: [] });
  });
});
