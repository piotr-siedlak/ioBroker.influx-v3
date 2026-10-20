import { strict as assert } from 'node:assert';
import type { TagDefinition } from '../../src/lib/config';
import { formatSettingsPreview, planSettingsMigration, preformattedHtml } from '../../src/lib/migration';
import {
    applyTagRules,
    buildOldTagInventory,
    canonicalTag,
    describeInventory,
    findTagRule,
    foldText,
    parseTagRules,
    ruleToRow,
    ruleValues,
    suggestTagRules,
} from '../../src/lib/tagMapping';

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
            { fromKey: 'room', fromValues: [], toKey: 'Location', toValue: '' },
            { fromKey: 'a', fromValues: [], toKey: '', toValue: '' },
        ]);
        assert.equal(warnings.length, 2);
        assert.deepEqual(parseTagRules({ 0: { fromKey: 'k', toKey: '-' } }).length, 1, 'index-keyed object');
    });

    it('old values are a list (multi-select) or one string from rules saved before', () => {
        assert.deepEqual(ruleValues(['Kotłownia', ' kotłownia ', '', 'Salon']), ['Kotłownia', 'Salon']);
        assert.deepEqual(ruleValues('Garage'), ['Garage']);
        assert.deepEqual(ruleValues(''), []);
        assert.deepEqual(ruleValues(undefined), []);
        const [r] = parseTagRules([{ fromKey: 'room', fromValue: ['A', 'B'], toKey: 'Location', toValue: 'SALON' }]);
        assert.deepEqual(r.fromValues, ['A', 'B']);
        assert.deepEqual(ruleToRow(r), { fromKey: 'room', fromValue: ['A', 'B'], toKey: 'Location', toValue: 'SALON' });
    });

    it('one rule maps several old values', () => {
        const rules = parseTagRules([
            {
                fromKey: 'room',
                fromValue: ['Kotłownia', 'boiler room', 'KOTL'],
                toKey: 'Location',
                toValue: 'KOTLOWNIA',
            },
            { fromKey: 'room', toKey: 'Location' },
        ]);
        for (const v of ['Kotłownia', 'Boiler Room', 'kotl']) {
            assert.deepEqual(applyTagRules([{ key: 'room', value: v }], rules, CATALOG, 'snake').tags, [
                { key: 'Location', value: 'KOTLOWNIA' },
            ]);
        }
        assert.deepEqual(applyTagRules([{ key: 'room', value: 'salon' }], rules, CATALOG, 'snake').tags, [
            { key: 'Location', value: 'SALON' },
        ]);
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
        assert.match(
            text,
            /Custom tags in the settings of influxdb\.0 \(value \(states\)\):\n {4}floor: 1 \(1\)\n {4}room: Kotlownia \(1\)/,
        );
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

describe('old tags of the influxdb settings', () => {
    const inv = buildOldTagInventory([
        [
            { key: 'room', value: 'Kotłownia' },
            { key: 'floor', value: '1' },
        ],
        [{ key: 'Room', value: 'Kotłownia' }],
        [{ key: 'room', value: 'Salon' }],
        [{ key: 'room', value: 'kotlownia' }],
        [{ key: 'room', value: 'Garaż' }],
        [
            { key: 'source', value: 'zigbee' },
            { key: 'floor', value: '2' },
        ],
        [{ key: 'device', value: 'Pump 1' }],
    ]);

    it('collects keys ignoring case with every value and the number of states', () => {
        const room = inv.keys.get('room')!;
        assert.equal(room.key, 'room');
        assert.equal(room.states, 5);
        assert.deepEqual(
            [...room.values.entries()],
            [
                ['Kotłownia', 2],
                ['Salon', 1],
                ['kotlownia', 1],
                ['Garaż', 1],
            ],
        );
        assert.deepEqual(describeInventory(inv), [
            'device: Pump 1 (1)',
            'floor: 1 (1), 2 (1)',
            'room: Kotłownia (2), Garaż (1), kotlownia (1), Salon (1)',
            'source: zigbee (1)',
        ]);
    });

    it('folds case, diacritics and punctuation only for suggestions', () => {
        assert.equal(foldText('Kotłownia'), 'kotlownia');
        assert.equal(foldText('KOTLOWNIA'), 'kotlownia');
        assert.equal(foldText('Garaż'), 'garaz');
        assert.equal(foldText('Pump-1'), 'pump1');
        // automatic mapping stays case-only: Kotłownia is not mapped without a rule
        const r = applyTagRules([{ key: 'Location', value: 'Kotłownia' }], [], CATALOG, 'snake');
        assert.equal(r.outcomes[0].status, 'rejected');
    });

    it('suggests the rows that are still needed, grouping old values per suggested catalog value', () => {
        // no rules yet: room and floor and source have no catalog key, device fits as is
        assert.deepEqual(suggestTagRules(inv, [], CATALOG, 'snake'), [
            { fromKey: 'floor', fromValues: [], toKey: '', toValue: '' },
            { fromKey: 'room', fromValues: [], toKey: '', toValue: '' },
            { fromKey: 'source', fromValues: [], toKey: '', toValue: '' },
        ]);
        // after choosing the new key for room (and removing source), the value rows follow
        const rules = parseTagRules([
            { fromKey: 'room', toKey: 'Location' },
            { fromKey: 'source', toKey: '-' },
        ]);
        assert.deepEqual(suggestTagRules(inv, rules, CATALOG, 'snake'), [
            { fromKey: 'floor', fromValues: [], toKey: '', toValue: '' },
            { fromKey: 'room', fromValues: ['Kotłownia'], toKey: 'Location', toValue: 'KOTLOWNIA' },
            { fromKey: 'room', fromValues: ['Garaż'], toKey: 'Location', toValue: '' },
        ]);
        // values with a rule of their own are left alone; nothing is suggested twice
        const done = parseTagRules([
            ...rules.map(ruleToRow),
            { fromKey: 'room', fromValue: ['Kotłownia', 'Garaż'], toKey: 'Location', toValue: 'KOTLOWNIA' },
            { fromKey: 'floor', toKey: '-' },
        ]);
        assert.deepEqual(suggestTagRules(inv, done, CATALOG, 'snake'), []);
    });

    it('several old values with the same suggestion share one row', () => {
        const inv2 = buildOldTagInventory([
            [{ key: 'Location', value: 'Kotłownia' }],
            [{ key: 'location', value: 'kotłownia!' }],
        ]);
        assert.deepEqual(suggestTagRules(inv2, [], CATALOG, 'snake'), [
            { fromKey: 'Location', fromValues: ['Kotłownia', 'kotłownia!'], toKey: 'Location', toValue: 'KOTLOWNIA' },
        ]);
    });
});
