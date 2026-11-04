import { copyData, type Data, type Schema } from './schema';
import type { Actor, ActorContext, ActorMap, ActorTarget } from './definition';

/** Runtime schemas protect the erased callback boundary. */
export interface Definition {
  readonly id: string;
  readonly message: Schema;
  readonly state: Schema;
  readonly initialState: Data;
  readonly actors: ActorMap;
  readonly on: (context: Omit<ActorContext<Data, Data, ActorMap>, 'actors'>) => unknown;
}
const definitions = new WeakMap<object, Definition>();
const actorTargets = new WeakMap<object, { readonly name: string; readonly actor: Actor<any, any> }>();

/** An inert declaration token; only the runtime resolves it to a child blueprint. */
export function makeActorTarget<C, K extends string>(name: K, actor: Actor<C, any>): ActorTarget<C, K> {
  const target = Object.freeze({ name }) as ActorTarget<C, K>;
  actorTargets.set(target, { name, actor });
  return target;
}

export function getActorTarget(value: unknown): { readonly name: string; readonly actor: Actor<any, any> } {
  const target = typeof value === 'object' && value !== null ? actorTargets.get(value) : undefined;
  if (!target) throw new Error('Expected an actor target from the handler\'s declared actors.');
  return target;
}

/** Snapshot schemas and initial state; child blueprints retain their identity. */
export function makeActor<M, S>(definition: Definition): Actor<M, S> {
  const actor = Object.freeze({ id: definition.id }) as Actor<M, S>;
  definitions.set(actor, Object.freeze({
    id: definition.id,
    message: copyData(definition.message),
    state: copyData(definition.state),
    initialState: copyData(definition.initialState),
    actors: Object.freeze({ ...definition.actors }),
    on: definition.on,
  }));
  return actor;
}

export function getDefinition(actor: unknown): Definition {
  const definition = typeof actor === 'object' && actor !== null ? definitions.get(actor) : undefined;
  if (!definition) throw new Error('Expected an actor definition created by defineActor.');
  return definition;
}
