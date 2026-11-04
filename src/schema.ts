import { getReference, type ActorRef } from './reference';

/** Finite structured values, including opaque actor references, exchanged or stored by actors. */
export type Data = null | boolean | number | string | Data[] | { [key: string]: Data } | ActorRef<never>;

/** Runtime descriptions of Data values. Object schemas are structural. */
export type Schema =
  | { readonly kind: 'number' | 'string' | 'boolean' | 'null' | 'data' }
  | { readonly kind: 'literal'; readonly value: null | boolean | number | string }
  | { readonly kind: 'array'; readonly items: Schema }
  | { readonly kind: 'ref'; readonly message: Schema }
  | { readonly kind: 'object'; readonly properties: Readonly<Record<string, Schema>>; readonly optional?: readonly string[] }
  | { readonly kind: 'union'; readonly variants: readonly Schema[] };

export type Infer<T extends Schema> =
  Schema extends T ? Data :
  T extends { kind: 'number' } ? number :
  T extends { kind: 'string' } ? string :
  T extends { kind: 'boolean' } ? boolean :
  T extends { kind: 'null' } ? null :
  T extends { kind: 'literal'; value: infer V } ? V :
  T extends { kind: 'array'; items: infer I extends Schema } ? Infer<I>[] :
  T extends { kind: 'ref'; message: infer M extends Schema } ? ActorRef<Infer<M>> :
  T extends { kind: 'union'; variants: readonly (infer V extends Schema)[] } ? Infer<V> :
  T extends { kind: 'object'; properties: infer P extends Record<string, Schema> } ?
    { [K in keyof P as K extends (T extends { optional: readonly (infer O)[] } ? O : never) ? never : K]: Infer<P[K]> } &
    { [K in keyof P as K extends (T extends { optional: readonly (infer O)[] } ? O : never) ? K : never]?: Infer<P[K]> } : Data;

function object<const P extends Readonly<Record<string, Schema>>>(properties: P): { readonly kind: 'object'; readonly properties: P };
function object<const P extends Readonly<Record<string, Schema>>, const O extends readonly (keyof P & string)[]>(properties: P, optional: O): { readonly kind: 'object'; readonly properties: P; readonly optional: O };
function object(properties: Readonly<Record<string, Schema>>, optional?: readonly string[]): Schema {
  return optional === undefined ? { kind: 'object', properties } : { kind: 'object', properties, optional };
}

/** Runtime schemas also infer ordinary TypeScript value types. */
export const s = {
  number: { kind: 'number' } as const,
  string: { kind: 'string' } as const,
  boolean: { kind: 'boolean' } as const,
  null: { kind: 'null' } as const,
  data: { kind: 'data' } as const,
  literal: <const V extends string | number | boolean | null>(value: V) => ({ kind: 'literal', value } as const),
  object,
  array: <const I extends Schema>(items: I) => ({ kind: 'array', items } as const),
  ref: <const M extends Schema>(message: M) => ({ kind: 'ref', message } as const),
  union: <const V extends readonly [Schema, ...Schema[]]>(...variants: V) => ({ kind: 'union', variants } as const),
};

const forbiddenKeys = new Set(['__proto__', 'prototype', 'constructor']);
const own = (value: object, key: string) => Object.hasOwn(value, key);
export const isSafeKey = (key: string): boolean => !forbiddenKeys.has(key);
const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

/** Accept plain finite data and genuine actor references; reject executable or cyclic values. */
export function isData(value: unknown): value is Data {
  const ancestors = new Set<object>();
  function visit(item: unknown, depth: number): boolean {
    if (item === null || typeof item === 'string' || typeof item === 'boolean') return true;
    if (typeof item === 'number') return Number.isFinite(item);
    if (typeof item !== 'object' || depth > 128 || ancestors.has(item)) return false;
    if (getReference(item)) return true;
    const prototype = Object.getPrototypeOf(item);
    if (Array.isArray(item) ? prototype !== Array.prototype : prototype !== Object.prototype && prototype !== null) return false;
    if (Object.getOwnPropertySymbols(item).length) return false;
    ancestors.add(item);
    const descriptors = Object.getOwnPropertyDescriptors(item);
    let valid = true;
    if (Array.isArray(item)) {
      valid = Object.keys(descriptors).length === item.length + 1;
      for (let index = 0; valid && index < item.length; index++) {
        const descriptor = descriptors[String(index)];
        valid = !!descriptor && 'value' in descriptor && descriptor.enumerable === true && visit(descriptor.value, depth + 1);
      }
    } else {
      for (const [key, descriptor] of Object.entries(descriptors)) {
        if (!isSafeKey(key) || !('value' in descriptor) || !descriptor.enumerable || !visit(descriptor.value, depth + 1)) {
          valid = false;
          break;
        }
      }
    }
    ancestors.delete(item);
    return valid;
  }
  try { return visit(value, 0); } catch { return false; }
}

/** Validate schema data before interpreting it, including schemas loaded from JSON. */
export function validateSchema(value: unknown): string[] {
  if (!isData(value)) return ['$: expected a finite, safe JSON schema'];
  const errors: string[] = [];
  function visit(schema: unknown, path: string, depth: number): void {
    if (!record(schema) || typeof schema.kind !== 'string') {
      errors.push(`${path}: expected a schema object with a kind`);
      return;
    }
    if (depth > 64) {
      errors.push(`${path}: excessively nested schema`);
      return;
    }
    let allowed: readonly string[] = ['kind'];
    switch (schema.kind) {
      case 'number': case 'string': case 'boolean': case 'null': case 'data': break;
      case 'literal':
        allowed = ['kind', 'value'];
        if (!(schema.value === null || typeof schema.value === 'string' || typeof schema.value === 'boolean' ||
          (typeof schema.value === 'number' && Number.isFinite(schema.value)))) errors.push(`${path}.value: expected a finite primitive literal`);
        break;
      case 'ref':
        allowed = ['kind', 'message'];
        visit(schema.message, `${path}.message`, depth + 1);
        break;
      case 'array':
        allowed = ['kind', 'items'];
        visit(schema.items, `${path}.items`, depth + 1);
        break;
      case 'object': {
        allowed = ['kind', 'properties', 'optional'];
        if (!record(schema.properties)) errors.push(`${path}.properties: expected a property map`);
        else for (const [key, property] of Object.entries(schema.properties)) {
          if (!isSafeKey(key)) errors.push(`${path}.properties: unsafe property name ${key}`);
          visit(property, `${path}.properties.${key}`, depth + 1);
        }
        if (own(schema, 'optional')) {
          if (!Array.isArray(schema.optional) || schema.optional.some(key => typeof key !== 'string')) {
            errors.push(`${path}.optional: expected an array of property names`);
          } else {
            const seen = new Set<string>();
            for (const key of schema.optional as string[]) {
              if (seen.has(key)) errors.push(`${path}.optional: duplicate property ${key}`);
              if (!record(schema.properties) || !own(schema.properties, key)) errors.push(`${path}.optional: unknown property ${key}`);
              seen.add(key);
            }
          }
        }
        break;
      }
      case 'union':
        allowed = ['kind', 'variants'];
        if (!Array.isArray(schema.variants) || schema.variants.length === 0) errors.push(`${path}.variants: expected a nonempty array`);
        else schema.variants.forEach((variant, index) => visit(variant, `${path}.variants[${index}]`, depth + 1));
        break;
      default: errors.push(`${path}.kind: unknown schema kind ${schema.kind}`);
    }
    for (const key of Object.keys(schema)) if (!allowed.includes(key)) errors.push(`${path}.${key}: unknown schema field`);
  }
  try { visit(value, '$', 0); } catch { errors.push('$: schema could not be inspected'); }
  return errors;
}

/** Objects use structural typing; actor references additionally require runtime-issued identity. */
export function validateValue(schema: Schema, value: unknown): string[] {
  const schemaErrors = validateSchema(schema);
  if (schemaErrors.length) return schemaErrors.map(error => `Invalid schema: ${error}`);
  if (!isData(value)) return ['$: expected finite, safe data'];
  function visit(type: Schema, item: Data, path: string): string[] {
    switch (type.kind) {
      case 'data': return [];
      case 'null': return item === null ? [] : [`${path}: expected null`];
      case 'number': case 'string': case 'boolean':
        return typeof item === type.kind ? [] : [`${path}: expected ${type.kind}`];
      case 'literal': return item === type.value ? [] : [`${path}: expected ${JSON.stringify(type.value)}`];
      case 'ref': {
        const reference = getReference(item);
        return reference && isAssignable(type.message, reference.message) ? [] : [`${path}: expected a compatible actor reference`];
      }
      case 'array':
        return Array.isArray(item) ? item.flatMap((entry, index) => visit(type.items, entry, `${path}[${index}]`)) : [`${path}: expected array`];
      case 'object': {
        if (!record(item) || getReference(item)) return [`${path}: expected object`];
        const errors: string[] = [];
        const optional = new Set(type.optional ?? []);
        for (const [key, property] of Object.entries(type.properties)) {
          if (!own(item, key)) {
            if (!optional.has(key)) errors.push(`${path}.${key}: required property is missing`);
          } else errors.push(...visit(property, item[key] as Data, `${path}.${key}`));
        }
        return errors;
      }
      case 'union':
        return type.variants.some(variant => visit(variant, item, path).length === 0) ? [] : [`${path}: expected a matching union variant`];
    }
  }
  return visit(schema, value, '$');
}

/** Conservatively check that every value allowed by `from` is accepted by `to`. */
export function isAssignable(from: Schema, to: Schema): boolean {
  if (validateSchema(from).length || validateSchema(to).length) return false;
  function assign(source: Schema, target: Schema): boolean {
    if (target.kind === 'data') return true;
    if (source.kind === 'union') return source.variants.every(variant => assign(variant, target));
    if (target.kind === 'union') {
      if (source.kind === 'boolean') return [true, false].every(value => assign({ kind: 'literal', value }, target));
      return target.variants.some(variant => assign(source, variant));
    }
    if (source.kind === 'data') return false;
    if (source.kind === 'literal') return validateValue(target, source.value).length === 0;
    if (target.kind === 'literal') return source.kind === 'null' && target.value === null;
    if (source.kind !== target.kind) return false;
    if (source.kind === 'ref' && target.kind === 'ref') return assign(target.message, source.message);
    if (source.kind === 'array' && target.kind === 'array') return assign(source.items, target.items);
    if (source.kind === 'object' && target.kind === 'object') {
      const sourceOptional = new Set(source.optional ?? []);
      const targetOptional = new Set(target.optional ?? []);
      return Object.entries(target.properties).every(([key, property]) => {
        // Open objects may contain undeclared properties with arbitrary JSON values.
        if (!own(source.properties, key)) return targetOptional.has(key) && assign({ kind: 'data' }, property);
        if (sourceOptional.has(key) && !targetOptional.has(key)) return false;
        return assign(source.properties[key]!, property);
      });
    }
    return true;
  }
  return assign(from, to);
}

/** Copy and freeze data while retaining the identity of opaque reference capabilities. */
export function copyData<T>(value: T): T {
  if (!isData(value)) throw new Error('Expected finite, safe data.');
  function copy(item: Data): Data {
    if (item === null || typeof item !== 'object' || getReference(item)) return item;
    if (Array.isArray(item)) return Object.freeze(item.map(copy)) as unknown as Data[];
    const result: Record<string, Data> = Object.create(Object.getPrototypeOf(item));
    for (const [key, entry] of Object.entries(item)) result[key] = copy(entry);
    return Object.freeze(result);
  }
  return copy(value) as T;
}
