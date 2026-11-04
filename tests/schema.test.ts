import { describe, expect, expectTypeOf, it } from 'vitest';
import { isData, s, validateSchema, validateValue, type Infer } from '../src/schema';

describe('schemas', () => {
  it('infers and validates tagged messages, optional fields, and arrays', () => {
    const protocol = s.union(
      s.object({ type: s.literal('search'), query: s.string, limit: s.number }, ['limit']),
      s.object({ type: s.literal('cancel'), ids: s.array(s.string) }),
    );
    expect(validateSchema(protocol)).toEqual([]);
    expect(validateValue(protocol, { type: 'search', query: 'actors' })).toEqual([]);
    expect(validateValue(protocol, { type: 'search', query: 'actors', limit: 5 })).toEqual([]);
    expect(validateValue(protocol, { type: 'search', query: 'actors', limit: '5' })).not.toEqual([]);
    expect(validateValue(protocol, { type: 'search' })).not.toEqual([]);
    expect(validateValue(protocol, { type: 'cancel', ids: ['one'] })).toEqual([]);
    expect(validateValue(protocol, { type: 'cancel', ids: [1] })).not.toEqual([]);
    type Search = Extract<Infer<typeof protocol>, { type: 'search' }>;
    expectTypeOf<Search['query']>().toEqualTypeOf<string>();
    expectTypeOf<Search['limit']>().toEqualTypeOf<number | undefined>();
  });

  it('rejects unsafe or non-finite data without executing getters', () => {
    let reads = 0;
    const accessor = Object.defineProperty({}, 'value', { enumerable: true, get() { reads++; return 1; } });
    const cycle: unknown[] = []; cycle.push(cycle);
    for (const invalid of [undefined, NaN, Infinity, 1n, () => {}, accessor,
      { [Symbol('hidden')]: 1 }, new Array(1), cycle, new Date(), new Map(), JSON.parse('{"__proto__":{}}')]) {
      expect(isData(invalid)).toBe(false);
      expect(validateValue(s.data, invalid)).not.toEqual([]);
    }
    expect(reads).toBe(0);
    for (const valid of [null, false, 3, 'text', [], {}, { values: [1, true, null] }]) expect(isData(valid)).toBe(true);
  });

  it('rejects malformed schemas before interpreting values', () => {
    const recursive: { kind: 'array'; items?: unknown } = { kind: 'array' }; recursive.items = recursive;
    for (const invalid of [null, {}, { kind: 'unsupported' }, { kind: 'number', extra: true },
      { kind: 'literal', value: Infinity }, { kind: 'array' }, { kind: 'union', variants: [] },
      { kind: 'object', properties: { a: s.number }, optional: ['missing'] }, recursive]) {
      expect(validateSchema(invalid)).not.toEqual([]);
    }
  });
});
