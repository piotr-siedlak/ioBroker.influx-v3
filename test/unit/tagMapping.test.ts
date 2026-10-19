import { strict as assert } from 'node:assert';
import type { TagDefinition } from '../../src/lib/config';
import { formatSettingsPreview, planSettingsMigration, preformattedHtml } from '../../src/lib/migration';
import { applyTagRules, canonicalTag, findTagRule, parseTagRules } from '../../src/lib/tagMapping';

const CATALOG: TagDefinition[] = [
    { key: 'Location', values: ['KOTLOWNIA', 'SALON'], allowOther: false, defaultValue: '', description: '' },
    { key: 'device', values: [], allowOther: true, defaultValue: '', description: '' },
    { key: 'kind', values: ['heating', 'cooling'], allowOther: false, defaultValue: '', description: '' },
];

describe('tag rules (migration)', () => {
    it('parses the rule table and warns about useless rows', () => {
        const warnings: string[] = [];
        const rules = parseTagRules(
            [
                { fromKey: ' room ', fromValue: '', toKey: 'Location', toValue: '' },
                { fromKey: '', toKey: 'x' },
                { fromKey: 'a' },
                null,
            ],
            warnings,
        );
        assert.deepEqual(rules, [
            { fromKey: 'room', fromValue: '', toKey: 'Location', toValue: '' },
            { fromKey: 'a', fromValue: '', toKey: '', toValue: '' },
        ]);
        assert.equal(warnings.length, 2);
        assert.deepEqual(parseTagRules({ 0: { fromKey: 'k', toKey: '-' } }).length, 1, 'index-keyed object');
    });

    it('a rule for the exact value wins over a rule for every value, otherwise table order', () => {
        const rules = parseTagRules([
            { fromKey: 'room', toKey: 'Location' },
            { fromKey: 'room', fromValue: 'Garage', toKey: '-' },
            { fromKey: 'ROOM', toKey: 'device' },
        ]);
        assert.equal(findTagRule({ key: 'Room', value: 'garage' }, rules), 1);
        assert.equal(findTagRule({ key: 'room', value: 'Kitchen' }, rules), 0);
        assert.equal(findTagRule({ key: 'other', value: 'x' }, rules), -1);
    });

    it('adapts key and value spelling to the catalog', () => {
        assert.deepEqual(canonicalTag({ key: 'location', value: 'kotlownia' }, CATALOG, 'preserve'), {
            key: 'Location',
            value: 'KOTLOWNIA',
        });
        assert.deepEqual(canonicalTag({ key: 'Device', value: 'Pump 1' }, CATALOG, 'snake'), {
            key: 'device',
            value: 'Pump 1',
        });
        assert.deepEqual(canonicalTag({ key: 'unknown', value: ' x ' }, CATALOG, 'snake'), {
            key: 'unknown',
            value: 'x',
        });
    });

    it('renames keys and values, removes tags, rejects what the catalog does not allow', () => {
        const rules = parseTagRules([
            { fromKey: 'room', fromValue: 'Kotłownia', toKey: 'Location', toValue: 'KOTLOWNIA' },
            { fromKey: 'room', fromValue: 'Salon', toKey: 'Location' },
            { fromKey: 'room', toKey: 'Location' },
            { fromKey: 'source', toKey: '-' },
            { fromKey: 'type', toKey: 'kind', toValue: 'heating' },
        ]);
        const r = applyTagRules(
            [
                { key: 'room', value: 'Kotłownia' },
                { key: 'source', value: 'zigbee' },
                { key: 'type', value: 'radiator' },
                { key: 'device', value: 'Pump 1' },
                { key: 'floor', value: '1' },
            ],
            rules,
            CATALOG,
            'snake',
        );
        assert.deepEqual(r.tags, [
            { key: 'Location', value: 'KOTLOWNIA' },
            { key: 'kind', value: 'heating' },
            { key: 'device', value: 'Pump 1' },
        ]);
        assert.deepEqual(
            r.outcomes.map(o => o.status),
            ['mapped', 'removed', 'mapped', 'kept', 'rejected'],
        );
        // value-only rename via a rule for every value + catalog spelling
        const salon = applyTagRules([{ key: 'room', value: 'salon' }], rules, CATALOG, 'snake');
        assert.deepEqual(salon.tags, [{ key: 'Location', value: 'SALON' }]);
        // a value the catalog does not know is reported, not written
        const garage = applyTagRules([{ key: 'room', value: 'Garage' }], rules, CATALOG, 'snake');
        assert.deepEqual(garage.tags, []);
        assert.equal(garage.outcomes[0].status, 'rejected');
        assert.match((garage.outcomes[0] as { reason: string }).reason, /not allowed for "Location"/);
    });

    it('two old tags producing the same key: the first one wins', () => {
        const rules = parseTagRules([
            { fromKey: 'room', toKey: 'Location' },
            { fromKey: 'raum', toKey: 'Location' },
        ]);
        const r = applyTagRules(
            [
                { key: 'room', value: 'SALON' },
                { key: 'raum', value: 'KOTLOWNIA' },
            ],
            rules,
            CATALOG,
            'snake',
        );
        assert.deepEqual(r.tags, [{ key: 'Location', value: 'SALON' }]);
        assert.equal(r.outcomes[1].status, 'conflict');
    });

    it('without rules and catalog entries every old tag is reported', () => {
        const r = applyTagRules([{ key: 'room', value: 'x' }], [], [], 'snake');
        assert.deepEqual(r.tags, []);
        assert.equal(r.outcomes[0].status, 'rejected');
    });
});

describe('settings migration plan', () => {
    const objects = [
        {
            id: 'a.0.temp',
            custom: {
                'influxdb.0': {
                    enabled: true,
                    changesOnly: true,
                    customTags: [
                        { name: 'room', value: 'Kotlownia' },
                        { name: 'floor', value: '1' },
                    ],
                },
            },
        },
        { id: 'a.0.plain', custom: { 'influxdb.0': { enabled: true, debounceTime: 500 } } },
        {
            id: 'a.0.done',
            custom: { 'influxdb.0': { enabled: true }, 'influx-v3.0': { enabled: true } },
        },
        { id: 'a.0.off', custom: { 'influxdb.0': { enabled: false } } },
    ];
    const rules = parseTagRules([{ fromKey: 'room', toKey: 'Location' }]);

    it('writes only accepted tags and collects the old tags that do not fit', () => {
        const plan = planSettingsMigration(objects, 'influxdb.0', 'influx-v3.0', {
            rules,
            catalog: CATALOG,
            style: 'snake',
            overwrite: false,
        });
        assert.equal(plan.total, 3);
        assert.equal(plan.skipped, 1);
        assert.deepEqual(
            plan.todo.map(p => p.id),
            ['a.0.plain', 'a.0.temp'],
        );
        const temp = plan.todo.find(p => p.id === 'a.0.temp')!;
        assert.deepEqual(temp.config.customTags, [{ key: 'Location', value: 'KOTLOWNIA' }]);
        assert.equal(temp.config.changesOnly, true);
        const plain = plan.todo.find(p => p.id === 'a.0.plain')!;
        assert.equal(plain.config.customTags, undefined);
        assert.deepEqual([...plan.unmatched.keys()], ['floor=1']);

        const text = formatSettingsPreview(plan);
        assert.match(text, /3 state\(s\) are logged by influxdb\.0: 2 will be configured/);
        assert.match(text, /room=Kotlownia → Location=KOTLOWNIA \(rule 1\)/);
        assert.match(text, /floor=1 - 1 state\(s\)/);
        // states with tag changes are listed first
        assert.ok(text.indexOf('a.0.temp') < text.indexOf('a.0.plain'));
    });

    it('overwrite and id selection', () => {
        const all = planSettingsMigration(objects, 'influxdb.0', 'influx-v3.0', {
            rules,
            catalog: CATALOG,
            style: 'snake',
            overwrite: true,
        });
        assert.equal(all.todo.length, 3);
        const one = planSettingsMigration(objects, 'influxdb.0', 'influx-v3.0', {
            rules,
            catalog: CATALOG,
            style: 'snake',
            overwrite: true,
            ids: new Set(['a.0.done']),
        });
        assert.deepEqual(
            one.todo.map(p => p.id),
            ['a.0.done'],
        );
    });

    it('the HTML preview escapes everything', () => {
        const html = preformattedHtml('<img src=x onerror=alert(1)> & "q"');
        assert.ok(!html.includes('<img'));
        assert.match(html, /&lt;img src=x onerror=alert\(1\)&gt; &amp; &quot;q&quot;/);
    });
});
