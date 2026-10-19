import { strict as assert } from 'node:assert';
import { DEFAULT_INSTANCE_CONFIG, type InstanceConfig, type StateConfig } from '../../src/lib/config';
import {
    CardinalityGuard,
    checkTagAgainstCatalog,
    evaluateTemplate,
    looksSensitive,
    normalizeTagKey,
    resolveDynamicTags,
    resolveTags,
    sanitizeTagValue,
    type TagContext,
    type TagSettings,
} from '../../src/lib/tagResolver';

const ctx = (over: Partial<TagContext> = {}): TagContext => ({
    id: 'heat.0.hc1.temperature',
    storeId: 'heat.0.hc1.temperature',
    common: {
        name: { en: 'Flow temperature', pl: 'Temperatura zasilania' },
        unit: '°C',
        role: 'value.temperature',
        type: 'number',
    },
    native: { serial: 'ABC123', secretKey: 'x' },
    rooms: ['Kotłownia'],
    functions: ['Heating'],
    parentDevice: 'Boiler',
    parentChannel: 'HC1',
    language: 'pl',
    ...over,
});

/** Builds settings; unless a test passes its own catalog, every per-state key is allowed with any value. */
function settings(inst: Partial<InstanceConfig> = {}, st: Partial<StateConfig> = {}): TagSettings {
    const permissive = (st.customTags ?? []).map(t => ({
        key: t.key,
        values: [],
        allowOther: true,
        defaultValue: '',
        description: '',
    }));
    return {
        instance: { ...DEFAULT_INSTANCE_CONFIG, tagDefinitions: permissive, ...inst },
        state: {
            customTags: [],
            excludeTags: [],
            useGlobalTags: true,
            useAutoTags: true,
            allowSensitiveTags: false,
            ...st,
        },
        reserved: new Set(['time', 'state_id', 'value', 'ack', 'q', 'from']),
    };
}

describe('tagResolver', () => {
    describe('normalizeTagKey', () => {
        it('snake style lowercases and replaces invalid characters', () => {
            assert.equal(normalizeTagKey('Sub-Type', 'snake'), 'sub_type');
            assert.equal(normalizeTagKey('Location', 'snake'), 'location');
            assert.equal(normalizeTagKey('  Łazienka Górna ', 'snake'), 'lazienka_gorna');
            assert.equal(normalizeTagKey('__a--b__', 'snake'), 'a_b');
        });
        it('preserve style keeps the key but sanitizes it', () => {
            assert.equal(normalizeTagKey('Sub-Type', 'preserve'), 'Sub-Type');
            assert.equal(normalizeTagKey('a\nb\\', 'preserve'), 'a b');
        });
    });

    describe('layer precedence', () => {
        it('global < automatic < per-state < exclude; state_id stays reserved', () => {
            const r = resolveTags(
                ctx(),
                settings(
                    {
                        globalTags: [
                            { key: 'room', value: 'GLOBAL' },
                            { key: 'site', value: 'home' },
                        ],
                        autoTagUnit: true,
                    },
                    {
                        customTags: [
                            { key: 'Room', value: 'Boiler room' },
                            { key: 'state_id', value: 'hack' },
                        ],
                        excludeTags: ['function'],
                    },
                ),
            );
            assert.deepEqual(r.tags, { room: 'Boiler room', site: 'home', unit: '°C' });
            assert.equal(r.origin.room, 'state');
            assert.equal(r.origin.site, 'global');
            assert.equal(r.origin.unit, 'auto');
            assert.ok(r.warnings.some(w => w.includes('reserved')));
            assert.ok(r.warnings.some(w => w.includes('overrides')));
        });
        it('automatic tags override global ones and can be disabled per state', () => {
            const s = settings({ globalTags: [{ key: 'room', value: 'G' }] });
            assert.equal(resolveTags(ctx(), s).tags.room, 'Kotłownia');
            const s2 = settings({ globalTags: [{ key: 'room', value: 'G' }] }, { useAutoTags: false });
            assert.equal(resolveTags(ctx(), s2).tags.room, 'G');
            const s3 = settings(
                { globalTags: [{ key: 'room', value: 'G' }] },
                { useAutoTags: false, useGlobalTags: false },
            );
            assert.deepEqual(resolveTags(ctx(), s3).tags, {});
        });
        it('detects duplicates inside one layer after normalisation (first wins)', () => {
            const r = resolveTags(
                ctx(),
                settings(
                    {},
                    {
                        customTags: [
                            { key: 'Sub-Type', value: 'HC1' },
                            { key: 'sub_type', value: 'HC2' },
                        ],
                    },
                ),
            );
            assert.equal(r.tags.sub_type, 'HC1');
            assert.ok(r.warnings.some(w => w.includes('defined twice')));
        });
        it('keeps original keys in preserve mode (Location/Sub-Type/Device)', () => {
            const r = resolveTags(
                ctx(),
                settings(
                    { tagKeyStyle: 'preserve', autoTagRoom: false, autoTagFunction: false },
                    {
                        customTags: [
                            { key: 'Location', value: 'KOTLOWNIA' },
                            { key: 'Sub-Type', value: 'HC1' },
                            { key: 'Device', value: 'PC' },
                        ],
                    },
                ),
            );
            assert.deepEqual(r.tags, { Location: 'KOTLOWNIA', 'Sub-Type': 'HC1', Device: 'PC' });
        });
        it('all automatic tags', () => {
            const r = resolveTags(
                ctx(),
                settings({
                    autoTagAdapter: true,
                    autoTagInstance: true,
                    autoTagDevice: true,
                    autoTagChannel: true,
                    autoTagUnit: true,
                    autoTagRole: true,
                    autoTagName: true,
                }),
            );
            assert.deepEqual(r.tags, {
                room: 'Kotłownia',
                function: 'Heating',
                adapter: 'heat',
                instance: 'heat.0',
                device: 'Boiler',
                channel: 'HC1',
                unit: '°C',
                role: 'value.temperature',
                name: 'Temperatura zasilania',
            });
            assert.deepEqual(r.variableKeys.sort(), ['channel', 'device', 'name']);
        });
    });

    describe('templates', () => {
        it('resolves id segments, common, enums and parents', () => {
            const w: string[] = [];
            const c = ctx();
            assert.equal(evaluateTemplate('{id.segment[2]}', c, [], w).text, 'hc1');
            assert.equal(evaluateTemplate('{id.segment[-1]}', c, [], w).text, 'temperature');
            assert.equal(evaluateTemplate('{id.adapter}/{id.instance}', c, [], w).text, 'heat/heat.0');
            assert.equal(evaluateTemplate('{common.unit}|{common.role}', c, [], w).text, '°C|value.temperature');
            assert.equal(evaluateTemplate('{enum.room}-{enum.function}', c, [], w).text, 'Kotłownia-Heating');
            assert.equal(evaluateTemplate('{parent.device}.{parent.channel}', c, [], w).text, 'Boiler.HC1');
            assert.equal(evaluateTemplate('plain', c, [], w).variable, false);
            assert.equal(w.length, 0);
        });
        it('allows native keys only from the allow-list', () => {
            const w: string[] = [];
            assert.equal(evaluateTemplate('{native.serial}', ctx(), ['serial'], w).text, 'ABC123');
            assert.equal(evaluateTemplate('{native.secretKey}', ctx(), ['serial'], w).text, '');
            assert.ok(w[0].includes('allow-list'));
        });
        it('unknown placeholders become empty with a warning; no code execution', () => {
            const w: string[] = [];
            assert.equal(evaluateTemplate('{process.exit()}x', ctx(), [], w).text, 'x');
            assert.equal(evaluateTemplate('{constructor}', ctx(), [], w).text, '');
            assert.equal(w.length, 2);
        });
        it('an empty template result omits the tag', () => {
            const r = resolveTags(
                ctx({ common: {} }),
                settings({}, { customTags: [{ key: 'unit', value: '{common.unit}' }] }),
            );
            assert.equal(r.tags.unit, undefined);
        });
    });

    describe('safety', () => {
        it('detects secrets and personal data', () => {
            assert.ok(looksSensitive('password', 'x'));
            assert.ok(looksSensitive('api_key', 'x'));
            assert.ok(looksSensitive('owner', 'jan.kowalski@example.com'));
            assert.ok(looksSensitive('x', 'apiv3_abcdefgh'));
            assert.ok(looksSensitive('x', 'eyJhbGciOiJIUzI1NiJ9'));
            assert.ok(looksSensitive('x', 'aB3dE5fG7hJ9kL1mN3pQ5rS7tU9vW1xY3'));
            assert.ok(looksSensitive('x', 'a'.repeat(40)));
            assert.ok(!looksSensitive('device', 'HM-LC-SW1-FM_LEQ1234567_CHANNEL_1'));
            assert.ok(!looksSensitive('location', 'KOTLOWNIA'));
        });
        it('drops sensitive tags unless allowed per state', () => {
            const s = settings(
                {},
                {
                    customTags: [
                        { key: 'token', value: 'abc' },
                        { key: 'mail', value: 'a@b.cd' },
                    ],
                },
            );
            assert.deepEqual(resolveTags(ctx({ rooms: [], functions: [] }), s).tags, {});
            const s2 = settings({}, { customTags: [{ key: 'token', value: 'abc' }], allowSensitiveTags: true });
            assert.equal(resolveTags(ctx(), s2).tags.token, 'abc');
            const s3 = settings({ blockSensitiveTags: false }, { customTags: [{ key: 'token', value: 'abc' }] });
            assert.equal(resolveTags(ctx(), s3).tags.token, 'abc');
        });
        it('sanitizes and truncates values', () => {
            assert.equal(sanitizeTagValue('  a\nb\\ ', 10), 'a b');
            assert.equal(sanitizeTagValue('abcdefghij\\k', 10), 'abcdefghij'.replace(/\\+$/, ''));
            assert.equal(sanitizeTagValue('abcdefghi\\jk', 10), 'abcdefghi');
        });
    });

    describe('tag catalog', () => {
        const catalog = [
            {
                key: 'Location',
                values: ['KOTLOWNIA', 'SALON'],
                allowOther: false,
                defaultValue: '',
                description: 'room',
            },
            { key: 'Sub-Type', values: ['HC1', 'HC2', 'HC3'], allowOther: false, defaultValue: '', description: '' },
            { key: 'device', values: ['PC'], allowOther: true, defaultValue: '', description: '' },
        ];
        const resolve = (customTags: { key: string; value: string }[]) =>
            resolveTags(ctx({ rooms: [], functions: [] }), settings({ tagDefinitions: catalog }, { customTags }));

        it('accepts catalog keys with allowed values (keys compared after normalisation)', () => {
            const r = resolve([
                { key: 'Location', value: 'KOTLOWNIA' },
                { key: 'sub_type', value: 'HC2' },
            ]);
            assert.deepEqual(r.tags, { location: 'KOTLOWNIA', sub_type: 'HC2' });
            assert.deepEqual(r.warnings, []);
        });
        it('ignores keys that are not in the catalog', () => {
            const r = resolve([{ key: 'floor', value: '1' }]);
            assert.deepEqual(r.tags, {});
            assert.ok(r.warnings[0].includes('not defined in the tag catalog'));
        });
        it('ignores values that are not allowed for the key', () => {
            const r = resolve([{ key: 'Location', value: 'GARAZ' }]);
            assert.deepEqual(r.tags, {});
            assert.ok(r.warnings[0].includes('not allowed for "Location"'));
            assert.ok(r.warnings[0].includes('KOTLOWNIA, SALON'));
        });
        it('allows other values and templates only for keys that allow them', () => {
            const ok = resolve([{ key: 'device', value: '{id.segment[2]}' }]);
            assert.deepEqual(ok.tags, { device: 'hc1' });
            const bad = resolve([{ key: 'Sub-Type', value: '{id.segment[2]}' }]);
            assert.deepEqual(bad.tags, {});
        });
        it('an empty catalog allows no per-state tags', () => {
            const r = resolveTags(
                ctx(),
                settings({ tagDefinitions: [] }, { customTags: [{ key: 'location', value: 'X' }] }),
            );
            assert.equal(r.tags.location, undefined);
        });
        it('checkTagAgainstCatalog reports the reason', () => {
            const r = checkTagAgainstCatalog({ key: 'Location', value: ' SALON ' }, catalog, 'snake');
            assert.ok(r.ok && r.definition.key === 'Location');
            const r2 = checkTagAgainstCatalog({ key: 'location', value: 'SALON' }, catalog, 'preserve');
            assert.equal(r2.ok, false, 'in preserve mode "location" and "Location" are different keys');
        });
    });

    describe('CardinalityGuard', () => {
        it('replaces new values above the limit with _other and warns once', () => {
            const warned: string[] = [];
            const g = new CardinalityGuard(2, k => warned.push(k));
            assert.equal(g.check('k', 'a'), 'a');
            assert.equal(g.check('k', 'b'), 'b');
            assert.equal(g.check('k', 'a'), 'a');
            assert.equal(g.check('k', 'c'), '_other');
            assert.equal(g.check('k', 'd'), '_other');
            assert.equal(g.check('other', 'x'), 'x');
            assert.deepEqual(warned, ['k']);
            assert.equal(g.count('k'), 2);
        });
    });

    describe('dynamic tags', () => {
        it('are disabled by default', () => {
            const r = resolveTags(
                ctx(),
                settings({}, { customTags: [{ key: 'mode', value: '{state:0_userdata.0.mode}' }] }),
            );
            assert.equal(r.dynamic.length, 0);
            assert.ok(r.warnings.some(w => w.includes('disabled')));
        });
        it('resolve with TTL, allowed pattern and template text', () => {
            const r = resolveTags(
                ctx(),
                settings(
                    { dynamicTagsEnabled: true },
                    { customTags: [{ key: 'Mode', value: 'm-{state:0_userdata.0.mode}' }] },
                ),
            );
            assert.deepEqual(r.dynamic, [
                { key: 'mode', template: 'm-{state:0_userdata.0.mode}', sources: ['0_userdata.0.mode'] },
            ]);
            const now = 1_000_000;
            const cache = new Map([['0_userdata.0.mode', { value: 'eco', ts: now - 1000 }]]);
            const allowed = /^[\w .:/-]{1,64}$/;
            assert.deepEqual(
                resolveDynamicTags(r.dynamic, id => cache.get(id), now, 3600, allowed, 256),
                { mode: 'm-eco' },
            );
            // stale
            assert.deepEqual(
                resolveDynamicTags(r.dynamic, id => cache.get(id), now + 3600_000, 3600, allowed, 256),
                {},
            );
            // TTL 0 = unlimited
            assert.deepEqual(
                resolveDynamicTags(r.dynamic, id => cache.get(id), now + 3600_000, 0, allowed, 256),
                { mode: 'm-eco' },
            );
            // missing source
            assert.deepEqual(
                resolveDynamicTags(r.dynamic, () => undefined, now, 0, allowed, 256),
                {},
            );
            // value not allowed
            cache.set('0_userdata.0.mode', { value: 'bad,value;', ts: now });
            assert.deepEqual(
                resolveDynamicTags(r.dynamic, id => cache.get(id), now, 0, allowed, 256),
                {},
            );
        });
        it('global tags cannot be dynamic', () => {
            const r = resolveTags(
                ctx(),
                settings({ dynamicTagsEnabled: true, globalTags: [{ key: 'x', value: '{state:a.b}' }] }),
            );
            assert.equal(r.tags.x, undefined);
            assert.equal(r.dynamic.length, 0);
        });
    });
});
