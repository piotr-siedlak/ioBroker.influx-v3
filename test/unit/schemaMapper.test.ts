import { strict as assert } from 'node:assert';
import { DEFAULT_INSTANCE_CONFIG, type InstanceConfig } from '../../src/lib/config';
import {
    coerceValue,
    columnKindFromDataType,
    decideValueType,
    globToRegExp,
    resolveTarget,
    roundTo,
    SchemaRegistry,
    toFieldValue,
} from '../../src/lib/schemaMapper';

const inst = (over: Partial<InstanceConfig> = {}): InstanceConfig => ({ ...DEFAULT_INSTANCE_CONFIG, ...over });
const st = { table: '', storeAs: '' as const, field: '' };

describe('schemaMapper', () => {
    describe('resolveTarget', () => {
        it('grouped: default table per value type', () => {
            assert.deepEqual(resolveTarget(inst(), st, 'a.0.t', 'a.0.t', { type: 'number' }), {
                table: 'numeric_states',
                field: 'value',
                valueType: 'number',
                source: 'default',
            });
            assert.equal(resolveTarget(inst(), st, 'a.0.b', 'a.0.b', { type: 'boolean' }).table, 'boolean_states');
            assert.equal(resolveTarget(inst(), st, 'a.0.s', 'a.0.s', { type: 'string' }).table, 'string_states');
            assert.equal(resolveTarget(inst(), st, 'a.0.j', 'a.0.j', { type: 'json' }).valueType, 'string');
            assert.equal(resolveTarget(inst(), st, 'a.0.m', 'a.0.m', { type: 'mixed' }).valueType, 'string');
        });
        it('grouped: explicit table beats rules; rules match role or id with type filter', () => {
            const cfg = inst({
                tableRules: [
                    { match: 'role', pattern: 'value.temperature*', valueType: '', table: 'temperature' },
                    { match: 'id', pattern: 'heat.*', valueType: 'any', table: 'heating' },
                ],
            });
            assert.equal(
                resolveTarget(cfg, st, 'x.0.t', 'x.0.t', { type: 'number', role: 'value.temperature' }).table,
                'temperature',
            );
            assert.equal(
                resolveTarget(cfg, st, 'x.0.t', 'x.0.t', { type: 'number', role: 'VALUE.TEMPERATURE.outside' }).table,
                'temperature',
            );
            // rule with default type filter does not take booleans
            assert.equal(
                resolveTarget(cfg, st, 'x.0.b', 'x.0.b', { type: 'boolean', role: 'value.temperature' }).table,
                'boolean_states',
            );
            assert.equal(resolveTarget(cfg, st, 'heat.0.pump', 'heat.0.pump', { type: 'boolean' }).table, 'heating');
            const explicit = resolveTarget(cfg, { ...st, table: 'mine' }, 'heat.0.x', 'heat.0.x', { type: 'number' });
            assert.deepEqual([explicit.table, explicit.source], ['mine', 'explicit']);
        });
        it('per-state: table = alias or id, optional field', () => {
            const cfg = inst({ schemaMode: 'perState' });
            assert.equal(resolveTarget(cfg, st, 'a.0.t', 'alias.t', { type: 'number' }).table, 'alias.t');
            assert.equal(
                resolveTarget(cfg, { ...st, field: 'temp' }, 'a.0.t', 'a.0.t', { type: 'number' }).field,
                'temp',
            );
            assert.equal(
                resolveTarget(cfg, { ...st, table: 'x' }, 'a.0.t', 'a.0.t', { type: 'number' }).source,
                'explicit',
            );
        });
        it('storeAs overrides common.type', () => {
            assert.equal(decideValueType('number', 'boolean'), 'number');
            assert.equal(decideValueType('', undefined), 'string');
        });
        it('glob patterns are anchored and escape regex characters', () => {
            assert.ok(globToRegExp('value.temp*').test('value.temperature'));
            assert.ok(!globToRegExp('value.temp*').test('xvalue.temperature'));
            assert.ok(!globToRegExp('a.b').test('axb'));
            assert.ok(globToRegExp('a?c').test('abc'));
        });
    });

    describe('coerceValue', () => {
        const o = { maxStringLength: 20, objectPolicy: 'json' as const };
        it('numbers', () => {
            assert.deepEqual(coerceValue(1.5, 'number', o), { ok: true, value: 1.5 });
            assert.deepEqual(coerceValue(' 21.5 ', 'number', o), { ok: true, value: 21.5 });
            assert.deepEqual(coerceValue(true, 'number', o), { ok: true, value: 1 });
            assert.equal(coerceValue('abc', 'number', o).ok, false);
            assert.equal(coerceValue(NaN, 'number', o).ok, false);
            assert.equal(coerceValue(Infinity, 'number', o).ok, false);
            assert.equal(coerceValue('', 'number', o).ok, false);
            assert.equal(coerceValue({}, 'number', o).ok, false);
        });
        it('booleans', () => {
            assert.deepEqual(coerceValue(false, 'boolean', o), { ok: true, value: false });
            assert.deepEqual(coerceValue(1, 'boolean', o), { ok: true, value: true });
            assert.deepEqual(coerceValue('ON', 'boolean', o), { ok: true, value: true });
            assert.deepEqual(coerceValue('false', 'boolean', o), { ok: true, value: false });
            assert.equal(coerceValue(2, 'boolean', o).ok, false);
            assert.equal(coerceValue('maybe', 'boolean', o).ok, false);
        });
        it('strings, JSON and limits', () => {
            assert.deepEqual(coerceValue('x', 'string', o), { ok: true, value: 'x' });
            assert.deepEqual(coerceValue(5, 'string', o), { ok: true, value: '5' });
            assert.deepEqual(coerceValue({ a: 1 }, 'string', o), { ok: true, value: '{"a":1}' });
            assert.deepEqual(coerceValue([1, 2], 'string', o), { ok: true, value: '[1,2]' });
            assert.equal(coerceValue({ a: 1 }, 'string', { ...o, objectPolicy: 'reject' }).ok, false);
            assert.equal(coerceValue('x'.repeat(21), 'string', o).ok, false);
            assert.equal(coerceValue('ż'.repeat(11), 'string', o).ok, false, 'limit counts UTF-8 bytes');
            assert.equal(coerceValue(NaN, 'string', o).ok, false);
        });
        it('null and undefined are never stored', () => {
            for (const t of ['number', 'boolean', 'string'] as const) {
                assert.deepEqual(coerceValue(null, t, o), { ok: false, reason: 'null' });
                assert.deepEqual(coerceValue(undefined, t, o), { ok: false, reason: 'null' });
            }
        });
        it('rounding and field values', () => {
            assert.equal(roundTo(1.23456, 2), 1.23);
            assert.equal(roundTo(1.235, null), 1.235);
            assert.deepEqual(toFieldValue(5, 'number'), { type: 'float', value: 5 });
            assert.deepEqual(toFieldValue(true, 'boolean'), { type: 'boolean', value: true });
        });
    });

    describe('SchemaRegistry', () => {
        const claim = (
            stateId: string,
            table: string,
            kind: 'float' | 'boolean' | 'string',
            tagKeys: string[] = ['state_id'],
        ) => ({
            stateId,
            table,
            field: 'value',
            fieldKind: kind,
            extraFields: { ack: 'boolean' as const, q: 'integer' as const },
            tagKeys,
        });

        it('maps Arrow data types', () => {
            assert.equal(columnKindFromDataType('Dictionary(Int32, Utf8)'), 'tag');
            assert.equal(columnKindFromDataType('Float64'), 'float');
            assert.equal(columnKindFromDataType('Int64'), 'integer');
            assert.equal(columnKindFromDataType('Boolean'), 'boolean');
            assert.equal(columnKindFromDataType('Utf8'), 'string');
            assert.equal(columnKindFromDataType('Timestamp(ns)'), 'time');
        });
        it('enforces one value type per table (first claim wins)', () => {
            const r = new SchemaRegistry(2000, 500);
            assert.equal(r.claim(claim('a', 'heating', 'float')).ok, true);
            const c = r.claim(claim('b', 'heating', 'boolean'));
            assert.equal(c.ok, false);
            assert.ok(!c.ok && c.reason.includes('type conflict'));
            // after the only owner leaves, another type may take the table (not yet in the DB)
            r.release('a');
            assert.equal(r.claim(claim('b', 'heating', 'boolean')).ok, true);
        });
        it('uses the database schema as source of truth', () => {
            const r = new SchemaRegistry(2000, 500);
            r.seed([
                { table: 'heating', column: 'value', dataType: 'Float64' },
                { table: 'heating', column: 'unit', dataType: 'Utf8' },
                { table: 'heating', column: 'time', dataType: 'Timestamp(ns)' },
            ]);
            const bad = r.claim(claim('x', 'heating', 'string'));
            assert.equal(bad.ok, false);
            const ok = r.claim(claim('y', 'heating', 'float', ['state_id', 'unit', 'room']));
            assert.ok(ok.ok);
            assert.deepEqual(ok.ok && ok.droppedTags, ['unit'], 'a tag key that is a field in the DB is dropped');
            r.release('y');
            assert.equal(r.hasTable('heating'), true, 'DB tables stay known');
        });
        it('enforces the table limit and warns at 80 %', () => {
            const r = new SchemaRegistry(5, 500);
            for (let i = 0; i < 4; i++) {
                const res = r.claim(claim(`s${i}`, `t${i}`, 'float'));
                assert.ok(res.ok);
                if (i === 3) {
                    assert.ok(res.ok && res.warnings.some(w => w.includes('of 5 tables')));
                }
            }
            assert.ok(r.claim(claim('s4', 't4', 'float')).ok);
            const over = r.claim(claim('s5', 't5', 'float'));
            assert.equal(over.ok, false);
            assert.ok(!over.ok && over.reason.includes('table limit'));
            // existing table still usable
            assert.ok(r.claim(claim('s6', 't0', 'float')).ok);
        });
        it('enforces the column limit', () => {
            const r = new SchemaRegistry(10, 10);
            assert.ok(r.claim(claim('a', 't', 'float', ['state_id', 'k1', 'k2'])).ok); // time,value,ack,q,state_id,k1,k2 = 7
            const res = r.claim(claim('b', 't', 'float', ['state_id', 'k3', 'k4', 'k5', 'k6']));
            assert.equal(res.ok, false);
            assert.ok(!res.ok && res.reason.includes('column limit'));
        });
        it('detects metadata placement conflicts (tag vs field)', () => {
            const r = new SchemaRegistry(10, 100);
            assert.ok(r.claim({ ...claim('a', 't', 'float'), extraFields: {}, tagKeys: ['state_id', 'ack'] }).ok);
            const res = r.claim(claim('b', 't', 'float'));
            assert.equal(res.ok, false);
        });
        it('check() does not change the registry', () => {
            const r = new SchemaRegistry(10, 100);
            assert.ok(r.claim(claim('a', 't', 'float')).ok);
            const before = JSON.stringify(r.summary());
            assert.equal(r.check(claim('b', 't', 'string')).ok, false);
            assert.ok(r.check(claim('a', 'other', 'string')).ok);
            assert.equal(JSON.stringify(r.summary()), before);
            assert.ok(r.claim(claim('c', 't', 'float')).ok);
        });
    });
});
