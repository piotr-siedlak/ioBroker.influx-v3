import { strict as assert } from 'node:assert';
import {
    escapeKey,
    escapeStringField,
    escapeTable,
    formatFloat,
    lineBytes,
    LineProtocolError,
    sanitizeIdentifier,
    serialize,
} from '../../src/lib/lineProtocol';

describe('lineProtocol', () => {
    describe('escaping', () => {
        it('escapes table names (comma, space, backslash)', () => {
            assert.equal(escapeTable('my table,x'), 'my\\ table\\,x');
            assert.equal(escapeTable('a\\b'), 'a\\\\b');
            assert.equal(escapeTable('a=b'), 'a=b', '= is allowed in table names');
        });
        it('escapes tag keys/values and field keys (comma, equals, space, backslash)', () => {
            assert.equal(escapeKey('x,y=z w'), 'x\\,y\\=z\\ w');
            assert.equal(escapeKey('a\\b'), 'a\\\\b');
            assert.equal(escapeKey('Sub-Type'), 'Sub-Type');
        });
        it('escapes string fields (quote, backslash)', () => {
            assert.equal(escapeStringField('q"uo\\te'), 'q\\"uo\\\\te');
            assert.equal(escapeStringField('a,b c=d'), 'a,b c=d');
        });
        it('sanitizes identifiers: control chars, trailing backslashes, whitespace', () => {
            assert.equal(sanitizeIdentifier(' a\nb\tc '), 'a b c');
            assert.equal(sanitizeIdentifier('abc\\\\'), 'abc');
            assert.equal(sanitizeIdentifier('a\\b'), 'a\\b');
        });
    });

    describe('formatFloat', () => {
        it('formats finite numbers', () => {
            assert.equal(formatFloat(38.7), '38.7');
            assert.equal(formatFloat(5), '5');
            assert.equal(formatFloat(-0), '0');
            assert.equal(formatFloat(1e21), '1e+21');
            assert.equal(formatFloat(-1.5e-7), '-1.5e-7');
        });
        it('rejects NaN and Infinity', () => {
            assert.throws(() => formatFloat(NaN), LineProtocolError);
            assert.throws(() => formatFloat(Infinity), LineProtocolError);
            assert.throws(() => formatFloat(-Infinity), LineProtocolError);
        });
    });

    describe('serialize', () => {
        it('writes the documented example with sorted tags and ms timestamp', () => {
            const line = serialize({
                table: 'heating',
                tags: { subtype: 'HC1', location: 'KOTLOWNIA', device: 'PC', state_id: 'heat.0.hc1.temperature' },
                fields: { value: { type: 'float', value: 38.7 }, ack: { type: 'boolean', value: true } },
                ts: 1792418400000,
            });
            assert.equal(
                line,
                'heating,device=PC,location=KOTLOWNIA,state_id=heat.0.hc1.temperature,subtype=HC1 value=38.7,ack=true 1792418400000',
            );
        });
        it('serializes all field types', () => {
            const line = serialize({
                table: 't',
                tags: {},
                fields: {
                    f: { type: 'float', value: 1.25 },
                    i: { type: 'integer', value: -3 },
                    b: { type: 'boolean', value: false },
                    s: { type: 'string', value: 'he said "hi"\\' },
                },
                ts: 1,
            });
            assert.equal(line, 't f=1.25,i=-3i,b=false,s="he said \\"hi\\"\\\\" 1');
        });
        it('omits empty tag values', () => {
            const line = serialize({
                table: 't',
                tags: { a: '', b: 'x' },
                fields: { v: { type: 'float', value: 1 } },
                ts: 2,
            });
            assert.equal(line, 't,b=x v=1 2');
        });
        it('escapes special characters everywhere', () => {
            const line = serialize({
                table: 'my table',
                tags: { 'Sub-Type': 'HC 1', state_id: 'x,y=z w' },
                fields: { 'f x': { type: 'string', value: 'a\nb' } },
                ts: 3,
            });
            assert.equal(line, 'my\\ table,Sub-Type=HC\\ 1,state_id=x\\,y\\=z\\ w f\\ x="a\nb" 3');
        });
        it('keeps UTF-8 characters', () => {
            const line = serialize({
                table: 'temperatura',
                tags: { lokalizacja: 'Kotłownia' },
                fields: { value: { type: 'float', value: 1.5 } },
                ts: 7,
            });
            assert.equal(line, 'temperatura,lokalizacja=Kotłownia value=1.5 7');
            assert.equal(lineBytes(line), Buffer.byteLength(line) + 1);
        });
        it('rejects invalid records', () => {
            const base = { table: 't', tags: {}, fields: { v: { type: 'float' as const, value: 1 } }, ts: 1 };
            assert.throws(() => serialize({ ...base, table: '' }), /Empty table/);
            assert.throws(() => serialize({ ...base, fields: {} }), /At least one field/);
            assert.throws(() => serialize({ ...base, ts: 1.5 }), /Invalid timestamp/);
            assert.throws(() => serialize({ ...base, ts: NaN }), /Invalid timestamp/);
            assert.throws(() => serialize({ ...base, tags: { k: 'v\\' } }), /backslash/);
            assert.throws(() => serialize({ ...base, tags: { k: 'a\nb' } }), /line breaks/);
            assert.throws(
                () => serialize({ ...base, fields: { v: { type: 'float', value: NaN } } }),
                LineProtocolError,
            );
            assert.throws(
                () => serialize({ ...base, fields: { v: { type: 'integer', value: 1.5 } } }),
                /Invalid integer/,
            );
        });
        it('never converts the millisecond timestamp', () => {
            const ts = Date.UTC(2026, 9, 19, 14, 0, 0, 123);
            const line = serialize({ table: 't', tags: {}, fields: { v: { type: 'float', value: 1 } }, ts });
            assert.ok(line.endsWith(` ${ts}`));
            assert.equal(String(ts).length, 13);
        });
    });
});
