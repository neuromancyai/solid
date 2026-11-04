import type { Schema } from './schema';

// Protected retains the generic protocol in emitted declarations while preventing structural copies.
declare class ReferenceIdentity<M> {
  protected readonly referenceMessage: (message: M) => void;
}
/** An opaque address. A reference can only receive messages in its protocol. */
export interface ActorRef<M> extends ReferenceIdentity<M> {
  readonly id: string;
}

export interface ReferenceData {
  readonly id: string;
  readonly owner: object;
  readonly message: Schema;
}
const references = new WeakMap<object, ReferenceData>();

/** Internal: only the executor issues addresses. Copying an id does not copy a capability. */
export function createReference<M>(id: string, owner: object, message: Schema): ActorRef<M> {
  const reference = Object.freeze({ id }) as ActorRef<M>;
  references.set(reference, Object.freeze({ id, owner, message }));
  return reference;
}

export function getReference(value: unknown): ReferenceData | undefined {
  return typeof value === 'object' && value !== null ? references.get(value) : undefined;
}
