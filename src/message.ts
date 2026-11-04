import type { ActorMap, ActorMessage } from './definition';
import { getReference, type ActorRef } from './reference';

/** Serializable details from an actor's failed transition. */
export type ErrorData = {
  readonly name: string;
  readonly message: string;
  readonly stack?: string;
};

/** Runtime-authored notifications about the creator's declared children. */
export type SystemMessage<A extends ActorMap = never> = [A] extends [never] ? SystemMessageShape : {
  [K in keyof A & string]:
    | { readonly type: 'system:started'; readonly actor: K; readonly ref: ActorRef<ActorMessage<A[K]>> }
    | { readonly type: 'system:stopped'; readonly actor: K; readonly ref: ActorRef<ActorMessage<A[K]>>; readonly error?: ErrorData };
}[keyof A & string];

type SystemMessageShape =
  | { readonly type: 'system:started'; readonly actor: string; readonly ref: ActorRef<never> }
  | { readonly type: 'system:stopped'; readonly actor: string; readonly ref: ActorRef<never>; readonly error?: ErrorData };
type SystemMessageOf<T> = [Extract<T, SystemMessageShape>] extends [never]
  ? T & SystemMessageShape : Extract<T, SystemMessageShape>;

/** Distinguish lifecycle notifications without losing a child's declared message type. */
export function isSystemMessage<T>(value: T): value is SystemMessageOf<T> {
  if (typeof value !== 'object' || value === null || !('type' in value) ||
    (value.type !== 'system:started' && value.type !== 'system:stopped') ||
    !('actor' in value) || typeof value.actor !== 'string' ||
    !('ref' in value) || !getReference(value.ref)) return false;
  if (value.type === 'system:started' || !('error' in value) || value.error === undefined) return true;
  const error = value.error;
  return typeof error === 'object' && error !== null &&
    'name' in error && typeof error.name === 'string' &&
    'message' in error && typeof error.message === 'string' &&
    (!('stack' in error) || error.stack === undefined || typeof error.stack === 'string');
}

/** Lifecycle and application messages handled by the same pure transition. */
export type Message<M, A extends ActorMap = {}> =
  | { readonly type: 'Start'; readonly parentId: string | null }
  | { readonly type: 'Next'; readonly value: M | SystemMessage<A> }
  | { readonly type: 'Stop' };
