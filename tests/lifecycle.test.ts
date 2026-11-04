import { describe, expect, it } from 'vitest';
import { Command, createTestRuntime, defineActor, isSystemMessage, s, type ActorRef, type Data } from '@neuromancy/solid';

describe('creator lifecycle notifications', () => {
  it('identifies root and child creators in Start even when the creator has already stopped', () => {
    const starts: { actor: string; parentId: string | null }[] = [];
    const child = defineActor({
      id: 'orphan', message: s.null, state: s.null, initialState: null,
      on: ({ message, self }) => {
        if (message.type === 'Start') starts.push({ actor: self.id, parentId: message.parentId });
      },
    });
    const creator = defineActor({
      id: 'short-lived-root', message: s.null, state: s.null, initialState: null, actors: { child },
      on: ({ message, self, actors }) => {
        if (message.type !== 'Start') return;
        starts.push({ actor: self.id, parentId: message.parentId });
        return { commands: [Command.start(actors.child, null), Command.stop()] };
      },
    });
    const runtime = createTestRuntime();
    const root = runtime.start(creator, { id: 'root' });
    runtime.flush({ maxSteps: 1 });
    expect(runtime.inspect().actors.find(actor => actor.id === root.id)?.status).toBe('stopped');
    runtime.flush();
    const childId = runtime.inspect().actors.find(actor => actor.definition === 'orphan')!.id;
    expect(starts).toEqual([{ actor: root.id, parentId: null }, { actor: childId, parentId: root.id }]);
    expect(runtime.inspect().actors.find(actor => actor.id === childId)?.status).toBe('running');
  });

  it('queues authentic immutable started and stopped messages outside the application schema', () => {
    const events: unknown[] = [];
    const child = defineActor({
      id: 'one-shot', message: s.null, state: s.null, initialState: null,
      on: ({ message }) => message.type === 'Next' ? { commands: [Command.stop()] } : undefined,
    });
    const creator = defineActor({
      id: 'creator', message: s.string, state: s.null, initialState: null, actors: { job: child },
      on: ({ message, actors }) => {
        if (message.type === 'Start') return { commands: [Command.start(actors.job, null)] };
        if (message.type !== 'Next') return;
        events.push(message.value);
        if (isSystemMessage(message.value)) {
          expect(Object.isFrozen(message)).toBe(true);
          expect(Object.isFrozen(message.value)).toBe(true);
          expect(Object.isFrozen(message.value.ref)).toBe(true);
          expect(message.value.actor).toBe('job');
          if (message.value.type === 'system:stopped') {
            const childId = message.value.ref.id;
            expect(runtime.inspect().actors.find(actor => actor.id === childId)?.status).toBe('stopped');
          }
        }
      },
    });
    const runtime = createTestRuntime();
    const parent = runtime.start(creator, { message: 'initial' });
    runtime.send(parent, 'backlog');
    runtime.flush({ maxSteps: 1 });
    // Start committed, but no child handler or creator notification has run yet.
    expect(events).toEqual([]);
    expect(runtime.inspect().actors.find(actor => actor.definition === 'one-shot')?.queued).toBe(2);
    runtime.flush();
    expect(events.slice(0, 2)).toEqual(['initial', 'backlog']);
    expect(events.slice(2)).toMatchObject([
      { type: 'system:started', actor: 'job' },
      { type: 'system:stopped', actor: 'job' },
    ]);
    const started = events[2] as { ref: ActorRef<null> };
    const stopped = events[3] as { ref: ActorRef<null>; error?: unknown };
    expect(stopped.ref).toBe(started.ref);
    expect(stopped).not.toHaveProperty('error');
    expect(runtime.send(started.ref, null)).toEqual({ status: 'rejected' });
    expect(runtime.inspect().actors.find(actor => actor.id === parent.id)?.status).toBe('running');
  });

  it('appends stopped after the child final result without waiting for its creator to finish', () => {
    const events: string[] = [];
    const child = defineActor({
      id: 'replying-child', message: s.ref(s.string), state: s.null, initialState: null,
      on: ({ message }) => message.type === 'Next' ? {
        commands: [Command.send(message.value, 'result'), Command.stop()],
      } : undefined,
    });
    const creator = defineActor({
      id: 'reply-owner', message: s.string, state: s.null, initialState: null, actors: { child },
      on: ({ message, self, actors }) => {
        if (message.type === 'Start') return { commands: [Command.start(actors.child, self)] };
        if (message.type === 'Next') events.push(isSystemMessage(message.value) ? message.value.type : message.value);
      },
    });
    const runtime = createTestRuntime();
    runtime.start(creator);
    runtime.flush();
    expect(events).toEqual(['system:started', 'result', 'system:stopped']);
    expect(runtime.inspect().status).toBe('running');
  });

  it('announces a child that fails its Start before reporting its error', () => {
    const events: unknown[] = [];
    const failure = new Error('startup failed');
    Object.defineProperty(failure, 'stack', { value: 'Error: startup failed\n    at child-start' });
    const child = defineActor({
      id: 'failed-start', message: s.null, state: s.null, initialState: null,
      on: () => { throw failure; },
    });
    const creator = defineActor({
      id: 'observing-parent', message: s.null, state: s.null, initialState: null, actors: { child },
      on: ({ message, actors }) => {
        if (message.type === 'Start') return { commands: [Command.start(actors.child, null)] };
        if (message.type === 'Next') events.push(message.value);
      },
    });
    const runtime = createTestRuntime();
    runtime.start(creator);
    expect(() => runtime.flush()).not.toThrow();
    expect(events).toMatchObject([
      { type: 'system:started', actor: 'child' },
      { type: 'system:stopped', actor: 'child', error: { name: 'Error', message: 'startup failed' } },
    ]);
    const stopped = events[1] as { error: { name: string; message: string; stack?: string } };
    expect(stopped.error).not.toBe(failure);
    expect(Object.isFrozen(stopped.error)).toBe(true);
    expect(stopped.error.stack).toBe(failure.stack);
    expect(runtime.inspect().actors.find(actor => actor.definition === 'failed-start'))
      .toMatchObject({ status: 'stopped', queued: 0, error: { message: 'startup failed' } });
    expect(runtime.inspect()).toMatchObject({ status: 'running' });
    expect(runtime.inspect()).not.toHaveProperty('error');
  });

  it('keeps children independent and drops notifications after their creator has stopped', () => {
    const received: unknown[] = [];
    let childRef: ActorRef<null> | undefined;
    const child = defineActor({
      id: 'independent-child', message: s.null, state: s.number, initialState: 0,
      on: ({ message, state }) => message.type === 'Next'
        ? { state: state + 1, commands: state === 0 ? [] : [Command.stop()] } : undefined,
    });
    const creator = defineActor({
      id: 'already-gone', message: s.null, state: s.null, initialState: null, actors: { child },
      on: ({ message, actors }) => {
        if (message.type === 'Start') return { commands: [Command.start(actors.child, null)] };
        if (message.type === 'Next' && isSystemMessage(message.value)) {
          received.push(message.value.type);
          childRef = message.value.ref;
          return { commands: [Command.stop()] };
        }
      },
    });
    const runtime = createTestRuntime();
    runtime.start(creator);
    runtime.flush();
    expect(runtime.inspect().actors.find(actor => actor.definition === 'independent-child'))
      .toMatchObject({ status: 'running', state: 1 });
    runtime.send(childRef!, null);
    runtime.flush();
    expect(received).toEqual(['system:started']);
    expect(runtime.inspect().actors.every(actor => actor.status === 'stopped' && actor.queued === 0)).toBe(true);
    expect(runtime.inspect().status).toBe('running');
  });

  it('runs no notification handlers after forcefully closing the runtime', async () => {
    const events: string[] = [];
    const child = defineActor({
      id: 'forced-child', message: s.null, state: s.null, initialState: null, on: () => undefined,
    });
    const creator = defineActor({
      id: 'forced-owner', message: s.null, state: s.null, initialState: null, actors: { child },
      on: ({ message, actors }) => {
        if (message.type === 'Start') return { commands: [Command.start(actors.child, null)] };
        if (message.type === 'Next' && isSystemMessage(message.value)) events.push(message.value.type);
      },
    });
    const runtime = createTestRuntime();
    runtime.start(creator);
    runtime.flush();
    expect(events).toEqual(['system:started']);
    const stopping = runtime.stop({ gracePeriodMs: 5 });
    runtime.advanceTo(5);
    expect(await stopping).toMatchObject({ status: 'forced' });
    expect(runtime.flush()).toMatchObject({ processed: 0, pending: 0 });
    expect(events).toEqual(['system:started']);
    expect(runtime.inspect().actors.every(actor => actor.status === 'stopped' && actor.queued === 0)).toBe(true);
  });
});

describe('addressed stop requests', () => {
  it('queues Stop in FIFO order and lets the target finish its cleanup before terminating', () => {
    const target = defineActor({
      id: 'cleanup-target', message: s.string, state: s.array(s.string), initialState: [],
      on: ({ message, state, self }) => {
        if (message.type === 'Start') return;
        if (message.type === 'Stop') return {
          state: [...state, 'Stop'], commands: [Command.send(self, 'cleanup')],
        };
        return {
          state: [...state, message.value],
          commands: message.value === 'cleanup' ? [Command.stop(null)] : [],
        };
      },
    });
    const runtime = createTestRuntime();
    const recipient = runtime.start(target);
    runtime.flush();
    runtime.send(recipient, 'queued');
    const requester = defineActor({
      id: 'requester', message: s.null, state: s.number, initialState: 0,
      on: ({ message }) => message.type === 'Start' ? {
        state: 1, commands: [Command.stop(recipient.id), Command.send(recipient, 'after-request')],
      } : undefined,
    });
    const sender = runtime.start(requester);
    runtime.flush({ maxSteps: 2 });
    expect(runtime.inspect().actors.find(actor => actor.id === recipient.id))
      .toMatchObject({ status: 'running', state: ['queued'], queued: 2 });
    runtime.flush({ maxSteps: 1 });
    expect(runtime.inspect().actors.find(actor => actor.id === recipient.id)?.status).not.toBe('stopped');
    expect(runtime.inspect().actors.find(actor => actor.id === recipient.id))
      .toMatchObject({ state: ['queued', 'Stop'], queued: 2 });
    runtime.flush();
    expect(runtime.inspect().actors.find(actor => actor.id === recipient.id))
      .toMatchObject({ status: 'stopped', state: ['queued', 'Stop', 'after-request', 'cleanup'] });
    expect(runtime.inspect().actors.find(actor => actor.id === sender.id))
      .toMatchObject({ status: 'running', state: 1 });
  });

  it('treats an explicit self ID as a Stop request and applies no automatic deadline', () => {
    const events: string[] = [];
    const actor = defineActor({
      id: 'self-request', message: s.string, state: s.null, initialState: null,
      on: ({ message, self }) => {
        if (message.type === 'Start') return { commands: [Command.stop(self.id), Command.send(self, 'after')] };
        events.push(message.type === 'Next' ? message.value : message.type);
        if (message.type === 'Next' && message.value === 'done') return { commands: [Command.stop(null)] };
      },
    });
    const runtime = createTestRuntime();
    const ref = runtime.start(actor);
    runtime.flush();
    expect(events).toEqual(['Stop', 'after']);
    expect(runtime.inspect().actors[0]?.status).not.toBe('stopped');
    runtime.advanceTo(60_000);
    expect(runtime.inspect().status).toBe('running');
    expect(runtime.inspect()).not.toHaveProperty('deadline');
    expect(runtime.inspect().actors[0]?.status).not.toBe('stopped');
    runtime.send(ref, 'done');
    runtime.flush();
    expect(runtime.inspect().actors[0]?.status).toBe('stopped');
  });

  it('lets a child request its creator to stop using Start.parentId without stopping itself', () => {
    const events: string[] = [];
    const child = defineActor({
      id: 'child-requester', message: s.null, state: s.null, initialState: null,
      on: ({ message }) => message.type === 'Start' && message.parentId !== null
        ? { commands: [Command.stop(message.parentId)] } : undefined,
    });
    const creator = defineActor({
      id: 'cooperative-creator', message: s.null, state: s.null, initialState: null, actors: { child },
      on: ({ message, actors }) => {
        events.push(message.type);
        if (message.type === 'Start') return { commands: [Command.start(actors.child, null)] };
        if (message.type === 'Stop') return { commands: [Command.stop(null)] };
      },
    });
    const runtime = createTestRuntime();
    runtime.start(creator);
    runtime.flush();
    expect(events).toEqual(['Start', 'Next', 'Stop']);
    expect(runtime.inspect().actors.find(actor => actor.definition === 'cooperative-creator')?.status).toBe('stopped');
    expect(runtime.inspect().actors.find(actor => actor.definition === 'child-requester')?.status).toBe('running');
  });

  it('allows an addressed request before a start command while keeping the requester alive', () => {
    const leaf = defineActor({
      id: 'new-child', message: s.null, state: s.null, initialState: null, on: () => undefined,
    });
    const runtime = createTestRuntime();
    const recipient = runtime.start(leaf);
    const creator = defineActor({
      id: 'continuing-requester', message: s.null, state: s.number, initialState: 0, actors: { leaf },
      on: ({ message, actors }) => message.type === 'Start' ? {
        state: 1, commands: [Command.stop(recipient.id), Command.start(actors.leaf, null)],
      } : undefined,
    });
    const parent = runtime.start(creator);
    runtime.flush();
    expect(runtime.inspect().actors).toHaveLength(3);
    expect(runtime.inspect().actors.find(actor => actor.id === parent.id)).toMatchObject({ state: 1, status: 'running' });
    expect(runtime.inspect().actors.find(actor => actor.id === recipient.id)?.status).not.toBe('stopped');
  });

  it('drops requests to stopped actors without duplicate stopped notifications', () => {
    let stops = 0;
    const child = defineActor({
      id: 'already-stopped-child', message: s.null, state: s.null, initialState: null,
      on: ({ message }) => message.type === 'Start' ? { commands: [Command.stop(null)] } : undefined,
    });
    const creator = defineActor({
      id: 'repeat-requester', message: s.null, state: s.null, initialState: null, actors: { child },
      on: ({ message, actors }) => {
        if (message.type === 'Start') return { commands: [Command.start(actors.child, null)] };
        if (message.type === 'Next' && isSystemMessage(message.value) && message.value.type === 'system:stopped') {
          stops++;
          if (stops === 1) return { commands: [Command.stop(message.value.ref.id), Command.stop(message.value.ref.id)] };
        }
      },
    });
    const runtime = createTestRuntime();
    runtime.start(creator);
    runtime.flush();
    expect(stops).toBe(1);
    expect(runtime.inspect().status).toBe('running');
    expect(runtime.inspect().actors.find(actor => actor.definition === 'repeat-requester')?.status).toBe('running');
  });

  it('rejects an unknown addressed target before committing state or earlier commands', () => {
    const child = defineActor({
      id: 'not-started', message: s.null, state: s.null, initialState: null, on: () => undefined,
    });
    const creator = defineActor({
      id: 'unknown-target', message: s.null, state: s.number, initialState: 0, actors: { child },
      on: ({ message, actors }) => message.type === 'Start' ? {
        state: 99, commands: [Command.start(actors.child, null), Command.stop('missing')],
      } : undefined,
    });
    const runtime = createTestRuntime();
    runtime.start(creator);
    expect(() => runtime.flush()).toThrow();
    expect(runtime.inspect()).toMatchObject({ status: 'failed', error: { code: 'EXECUTION_FAILED' } });
    expect(runtime.inspect().actors).toHaveLength(1);
    expect(runtime.inspect().actors[0]).toMatchObject({ state: 0, status: 'stopped' });
  });

  it('still requires immediate self-stop to be the final command', () => {
    const actor = defineActor({
      id: 'non-final-self-stop', message: s.null, state: s.number, initialState: 0,
      on: ({ message, self }) => message.type === 'Start' ? {
        state: 99, commands: [Command.stop(null), Command.send(self, null)],
      } : undefined,
    });
    const runtime = createTestRuntime();
    runtime.start(actor);
    expect(() => runtime.flush()).toThrow();
    expect(runtime.inspect()).toMatchObject({ status: 'failed', error: { code: 'EXECUTION_FAILED' } });
    expect(runtime.inspect().actors[0]?.state).toBe(0);
  });
});

describe('actor-local failures', () => {
  it('reports a child handler exception while its creator and unrelated actors keep working', () => {
    const errors: string[] = [];
    const child = defineActor({
      id: 'failing-child', message: s.number, state: s.number, initialState: 0,
      on: ({ message }) => { if (message.type === 'Next') throw new TypeError('bad job'); },
    });
    const counter = defineActor({
      id: 'unrelated-counter', message: s.number, state: s.number, initialState: 0,
      on: ({ message, state }) => message.type === 'Next' ? { state: state + message.value } : undefined,
    });
    const runtime = createTestRuntime();
    const unrelated = runtime.start(counter);
    const creator = defineActor({
      id: 'recovering-parent', message: s.null, state: s.number, initialState: 0, actors: { child },
      on: ({ message, actors }) => {
        if (message.type === 'Start') return { commands: [Command.start(actors.child, 1)] };
        if (message.type === 'Next' && isSystemMessage(message.value) && message.value.type === 'system:stopped') {
          errors.push(message.value.error!.message);
          return { state: 1, commands: [Command.send(unrelated, 7)] };
        }
      },
    });
    const parent = runtime.start(creator);
    runtime.send(unrelated, 3);
    runtime.flush();
    expect(errors).toEqual(['bad job']);
    expect(runtime.inspect()).toMatchObject({ status: 'running' });
    expect(runtime.inspect().actors.find(actor => actor.id === parent.id)).toMatchObject({ state: 1, status: 'running' });
    expect(runtime.inspect().actors.find(actor => actor.id === unrelated.id)).toMatchObject({ state: 10, status: 'running' });
  });

  it('commits no state, sends, or children when a handler throws before returning its transition', () => {
    const runtime = createTestRuntime();
    const counter = defineActor({
      id: 'atomic-counter', message: s.number, state: s.number, initialState: 0,
      on: ({ message, state }) => message.type === 'Next' ? { state: state + message.value } : undefined,
    });
    const destination = runtime.start(counter);
    const creator = defineActor({
      id: 'throwing-owner', message: s.number, state: s.number, initialState: 0, actors: { counter },
      on: ({ message, actors }) => {
        if (message.type !== 'Start') return;
        const transition = { state: 99, commands: [Command.send(destination, 100), Command.start(actors.counter, 1)] };
        expect(transition.commands).toHaveLength(2);
        throw new Error('handler failed');
      },
    });
    const parent = runtime.start(creator);
    expect(() => runtime.flush()).not.toThrow();
    expect(runtime.inspect()).toMatchObject({ status: 'running' });
    expect(runtime.inspect().actors).toHaveLength(2);
    expect(runtime.inspect().actors.find(actor => actor.id === parent.id))
      .toMatchObject({ status: 'stopped', state: 0, error: { name: 'Error', message: 'handler failed' } });
    expect(runtime.inspect().actors.find(actor => actor.id === destination.id)).toMatchObject({ status: 'running', state: 0 });
    runtime.send(destination, 1);
    runtime.flush();
    expect(runtime.inspect().actors.find(actor => actor.id === destination.id)?.state).toBe(1);
  });

  it.each([
    ['a string', 'plain failure'],
    ['null', null],
    ['an object with hostile accessors', Object.defineProperties({}, {
      name: { get() { throw new Error('name getter'); } },
      message: { get() { throw new Error('message getter'); } },
      stack: { get() { throw new Error('stack getter'); } },
      toString: { value() { throw new Error('coercion'); } },
    })],
    ['a hostile proxy', new Proxy({}, { get() { throw new Error('proxy get'); }, getPrototypeOf() { throw new Error('proxy prototype'); } })],
  ])('safely records a root failure thrown as %s', (_label, thrown) => {
    const actor = defineActor({
      id: 'odd-throwable', message: s.null, state: s.null, initialState: null,
      on: () => { throw thrown; },
    });
    const runtime = createTestRuntime();
    runtime.start(actor);
    expect(() => runtime.flush()).not.toThrow();
    const inspection = runtime.inspect();
    expect(inspection.status).toBe('running');
    expect(inspection.actors[0]?.status).toBe('stopped');
    expect(inspection.actors[0]?.error).toMatchObject({ name: expect.any(String), message: expect.any(String) });
    expect(Object.isFrozen(inspection.actors[0]?.error)).toBe(true);
  });

  it('keeps reentrant runtime mutation fatal even from a handler', () => {
    const runtime = createTestRuntime();
    const actor = defineActor({
      id: 'reentrant-handler', message: s.null, state: s.null, initialState: null,
      on: ({ self }) => { runtime.send(self, null); },
    });
    runtime.start(actor);
    expect(() => runtime.flush()).toThrow();
    expect(runtime.inspect()).toMatchObject({ status: 'failed', error: { code: 'EXECUTION_FAILED' } });
  });

  it('still treats invalid command batches as fatal and commits no earlier child or state', () => {
    const child = defineActor({
      id: 'uncommitted-child', message: s.number, state: s.number, initialState: 0, on: () => undefined,
    });
    const creator = defineActor({
      id: 'invalid-batch', message: s.null, state: s.number, initialState: 0, actors: { child },
      on: ({ message, actors }) => message.type === 'Start' ? { state: 99, commands: [
        Command.start(actors.child, 1), Command.start(actors.child, 'wrong' as never),
      ] } : undefined,
    });
    const runtime = createTestRuntime();
    runtime.start(creator);
    expect(() => runtime.flush()).toThrow();
    expect(runtime.inspect()).toMatchObject({ status: 'failed', error: { code: 'EXECUTION_FAILED' } });
    expect(runtime.inspect().actors).toHaveLength(1);
    expect(runtime.inspect().actors[0]).toMatchObject({ status: 'stopped', state: 0, queued: 0 });
  });

  it.each([
    ['an async handler', async () => { throw new Error('async rejection'); }],
    ['an explicitly rejected promise', () => Promise.reject(new Error('promise rejection'))],
  ])('rejects %s as a fatal contract violation without leaking an unhandled rejection', async (_label, on) => {
    const actor = defineActor({
      id: 'rejected-async-transition', message: s.null, state: s.number, initialState: 0,
      on: on as never,
    });
    const runtime = createTestRuntime();
    runtime.start(actor);
    expect(() => runtime.flush()).toThrow();
    expect(runtime.inspect()).toMatchObject({ status: 'failed', error: { code: 'EXECUTION_FAILED' } });
    expect(runtime.inspect().actors[0]).toMatchObject({ status: 'stopped', state: 0 });
    expect(runtime.inspect().actors[0]).not.toHaveProperty('error');
    // Reach the next host turn so Vitest observes any unhandled rejection.
    await new Promise(resolve => setTimeout(resolve, 0));
  });
});

describe('reserved lifecycle message admission', () => {
  const acceptsData = defineActor({
    id: 'accepts-data', message: s.data, state: s.data, initialState: null,
    on: ({ message }) => message.type === 'Next' ? { state: message.value } : undefined,
  });

  it('does not mistake arbitrary application values for lifecycle notifications', () => {
    const ref = createTestRuntime().start(acceptsData);
    for (const value of [
      null, 1, 'system:started', {}, { type: 'system:started' },
      { type: 'system:started', actor: 'child', ref: { id: 'fake' } },
      { type: 'system:stopped', actor: 'child', ref, error: { message: 1 } },
      { type: 'system:stopped', actor: 'child', ref, error: { name: 'Error', message: 'failure', stack: 1 } },
    ]) expect(isSystemMessage(value)).toBe(false);
  });

  it.each(['system:started', 'system:stopped', 'system:future'])('rejects external %s payloads without damaging the runtime', type => {
    const runtime = createTestRuntime();
    const recipient = runtime.start(acceptsData);
    expect(() => runtime.send(recipient, { type })).toThrow();
    expect(() => runtime.start(acceptsData, { message: { type } })).toThrow();
    expect(runtime.inspect().status).toBe('running');
    expect(runtime.inspect().actors).toHaveLength(1);
    runtime.send(recipient, { nested: { type } });
    runtime.flush();
    expect(runtime.inspect().actors[0]?.state).toEqual({ nested: { type } });
  });

  it.each(['send', 'start'] as const)('rejects a %s attempt to forge lifecycle messages before committing any state', kind => {
    const runtime = createTestRuntime();
    const target = runtime.start(acceptsData);
    const forged: Data = { type: 'system:stopped' };
    const actor = defineActor({
      id: 'forged-system-message', message: s.data, state: s.number, initialState: 0, actors: { child: acceptsData },
      on: ({ message, actors }) => {
        if (message.type !== 'Start') return;
        return { state: 99, commands: [
          Command.send(target, 'must not commit'),
          kind === 'send' ? Command.send(target, forged) : Command.start(actors.child, forged),
        ] };
      },
    });
    const parent = runtime.start(actor);
    expect(() => runtime.flush()).toThrow();
    expect(runtime.inspect().status).toBe('failed');
    expect(runtime.inspect().actors).toHaveLength(2);
    expect(runtime.inspect().actors.find(actor => actor.id === parent.id)?.state).toBe(0);
    expect(runtime.inspect().actors.find(actor => actor.id === target.id)?.state).toBe(null);
  });
});
