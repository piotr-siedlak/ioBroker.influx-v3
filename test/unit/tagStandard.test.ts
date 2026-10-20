import { strict as assert } from 'node:assert';
import { parseTagDefinitions } from '../../src/lib/config';
import {
    addToInventory,
    applyTagRules,
    buildOldTagInventory,
    describeUse,
    mergeInventories,
    parseTagRules,
    ruleToRow,
    suggestTagRules,
} from '../../src/lib/tagMapping';
import { countTagUses } from '../../src/lib/legacySource';
import {
    keyGroup,
    proposeStandard,
    type ProposalInput,
    resolveValueStyle,
    sameKey,
    styleValue,
    valueGroup,
} from '../../src/lib/tagStandard';

const inv = buildOldTagInventory([
    [
        { key: 'room', value: 'Kotłownia' },
        { key: 'floor', value: '1' },
    ],
    [
        { key: 'room', value: 'Kotłownia' },
        { key: 'floor', value: '1' },
    ],
    [
        { key: 'room', value: 'kotlownia' },
        { key: 'floor', value: '-1' },
    ],
    [{ key: 'room', value: 'Heizungsraum' }],
    [
        { key: 'room', value: 'Salon' },
        { key: 'floor', value: '2' },
    ],
    [{ key: 'room', value: 'Garaż' }],
    [{ key: 'Raum', value: 'Küche' }],
    [{ key: 'source', value: 'zigbee' }],
]);

const ROOMS = ['Garaż', 'Kotłownia', 'Kuchnia', 'Salon', 'Sypialnia'];

function input(over: Partial<ProposalInput> = {}): ProposalInput {
    return {
        catalogRows: [],
        ruleRows: [],
        keyStyle: 'snake',
        valueField: 'value',
        valueStyle: 'auto',
        rooms: ROOMS,
        functions: [],
        ...over,
    };
}

/** every old tag ends up as an allowed catalog tag with the proposed rules */
function allFit(rows: { catalogRows: unknown; ruleRows: unknown }): string[] {
    const catalog = parseTagDefinitions(rows.catalogRows, 'snake', 'value', []);
    const rules = parseTagRules(rows.ruleRows);
    const out: string[] = [];
    for (const e of inv.keys.values()) {
        for (const v of e.values.keys()) {
            const r = applyTagRules([{ key: e.key, value: v }], rules, catalog, 'snake');
            out.push(r.tags.length ? `${e.key}=${v} → ${r.tags[0].key}=${r.tags[0].value}` : `${e.key}=${v} ✗`);
        }
    }
    return out;
}

describe('proposed standard for old tags (tagStandard)', () => {
    describe('helpers', () => {
        it('recognises keys and values across languages, singular and plural', () => {
            assert.equal(keyGroup('Raum'), 'location');
            assert.equal(keyGroup('Rooms'), 'location');
            assert.equal(keyGroup('Pomieszczenie'), 'location');
            assert.equal(keyGroup('Etage'), 'floor');
            assert.equal(keyGroup('serial'), undefined);
            assert.equal(valueGroup('Heizungsraum'), valueGroup('Kotłownia'));
            assert.equal(valueGroup('Küche'), valueGroup('kitchen'));
            assert.equal(valueGroup('Küche'), valueGroup('KUCHNIA'));
            assert.notEqual(valueGroup('Küche'), valueGroup('Salon'));
            assert.equal(sameKey('Rooms', 'room'), true);
            assert.equal(sameKey('Sub-Type', 'subtype'), true);
            assert.equal(sameKey('floor', 'flow'), false);
        });

        it('spells values in the chosen style and never breaks numbers or the comma list', () => {
            assert.equal(styleValue('Garaż', 'upper'), 'GARAZ');
            assert.equal(styleValue(' Boiler room ', 'upper'), 'BOILER_ROOM');
            assert.equal(styleValue('Straße', 'lower'), 'strasse');
            assert.equal(styleValue('Boiler room', 'asUsed'), 'Boiler room');
            assert.equal(styleValue('a, b', 'asUsed'), 'a b');
            assert.equal(styleValue('-1', 'upper'), '-1');
            assert.equal(styleValue('1,5', 'upper'), '1.5');
            assert.equal(styleValue('???', 'upper'), '???');
        });

        it('auto style follows the catalog values (empty catalog → upper)', () => {
            const cat = (values: string[]) => [
                { key: 'k', values, allowOther: false, defaultValue: '', description: '' },
            ];
            assert.equal(resolveValueStyle('auto', []), 'upper');
            assert.equal(resolveValueStyle('auto', cat(['KOTLOWNIA', 'HC1', '2'])), 'upper');
            assert.equal(resolveValueStyle('auto', cat(['early', 'late'])), 'lower');
            assert.equal(resolveValueStyle('auto', cat(['Kitchen', 'late'])), 'asUsed');
            assert.equal(resolveValueStyle('lower', cat(['KOTLOWNIA'])), 'lower');
        });
    });

    it('empty catalog: proposes keys, standard values (incl. ioBroker rooms) and the minimal rules', () => {
        const p = proposeStandard(inv, input());
        assert.deepEqual(p.newKeys, ['location', 'floor', 'source']);
        assert.deepEqual(p.catalogRows, [
            {
                key: 'location',
                values: 'KOTLOWNIA, GARAZ, KUCHNIA, SALON, SYPIALNIA',
                allowOther: false,
                defaultValue: '',
                description: 'proposed from room, Raum',
            },
            {
                key: 'floor',
                values: '1, -1, 2',
                allowOther: false,
                defaultValue: '',
                description: 'proposed from floor',
            },
            {
                key: 'source',
                values: 'ZIGBEE',
                allowOther: false,
                defaultValue: '',
                description: 'proposed from source',
            },
        ]);
        assert.deepEqual(p.ruleRows, [
            { fromKey: 'room', fromValue: [], toKey: 'location', toValue: '' },
            { fromKey: 'room', fromValue: ['Kotłownia', 'Heizungsraum'], toKey: 'location', toValue: 'KOTLOWNIA' },
            { fromKey: 'room', fromValue: ['Garaż'], toKey: 'location', toValue: 'GARAZ' },
            { fromKey: 'Raum', fromValue: [], toKey: 'location', toValue: '' },
            { fromKey: 'Raum', fromValue: ['Küche'], toKey: 'location', toValue: 'KUCHNIA' },
        ]);
        assert.equal(p.newRules, 5);
        assert.ok(
            p.summary.some(
                l =>
                    l ===
                    'room, Raum → location (new key): Kotłownia, kotlownia, Heizungsraum → KOTLOWNIA; Garaż → GARAZ; Küche → KUCHNIA; Salon → SALON',
            ),
            p.summary.join('\n'),
        );
        assert.ok(
            allFit(p).every(l => !l.endsWith('✗')),
            allFit(p).join('\n'),
        );
        assert.ok(allFit(p).includes('room=Heizungsraum → location=KOTLOWNIA'));
    });

    it('existing catalog: matches synonyms to its keys and values, appends only missing values', () => {
        const catalogRows = [
            {
                key: 'Location',
                values: 'KOTLOWNIA, SALON',
                allowOther: false,
                defaultValue: '',
                description: 'room of the sensor',
            },
            { key: 'Etage', values: '1, 2', allowOther: false, defaultValue: '', description: '' },
            { key: 'source', values: '', allowOther: true, defaultValue: '', description: '' },
        ];
        const p = proposeStandard(inv, input({ catalogRows, rooms: [] }));
        assert.deepEqual(p.newKeys, []);
        assert.deepEqual(p.catalogRows, [
            { ...catalogRows[0], values: 'KOTLOWNIA, SALON, GARAZ, KUCHE' },
            { ...catalogRows[1], values: '1, 2, -1' },
            { ...catalogRows[2], values: 'zigbee' }, // open list: used values are listed too
        ]);
        const rules = p.ruleRows as { fromKey: string; fromValue: string[]; toKey: string; toValue: string }[];
        assert.ok(rules.some(r => r.fromKey === 'room' && r.fromValue.length === 0 && r.toKey === 'Location'));
        assert.ok(rules.some(r => r.fromKey === 'floor' && r.fromValue.length === 0 && r.toKey === 'Etage'));
        assert.ok(!rules.some(r => r.fromKey === 'source'), 'source fits as it is (other values allowed)');
        assert.ok(
            allFit(p).every(l => !l.endsWith('✗')),
            allFit(p).join('\n'),
        );
        assert.ok(allFit(p).includes('source=zigbee → source=zigbee'));
    });

    it('user rules win: removed keys stay removed, values with own rules are left alone', () => {
        const ruleRows = [
            { fromKey: 'source', fromValue: [], toKey: '-', toValue: '' },
            { fromKey: 'room', fromValue: ['Garaż'], toKey: 'location', toValue: 'SALON' },
        ];
        const p = proposeStandard(inv, input({ ruleRows }));
        assert.deepEqual(p.newKeys, ['location', 'floor']);
        assert.deepEqual(p.ruleRows.slice(0, 2), ruleRows, 'existing rows unchanged and first');
        const rows = p.ruleRows.slice(2) as { fromKey: string; fromValue: string[] }[];
        assert.ok(!rows.some(r => r.fromValue.includes('Garaż')));
        assert.ok(!rows.some(r => r.fromKey === 'source'));
        assert.ok(allFit(p).includes('room=Garaż → location=SALON'));
    });

    it('a second run proposes nothing more', () => {
        const first = proposeStandard(inv, input());
        const second = proposeStandard(inv, input({ catalogRows: first.catalogRows, ruleRows: first.ruleRows }));
        assert.deepEqual([second.newKeys, second.newValues, second.newRules], [[], 0, 0]);
    });

    it('reserved key names get a suffix; many distinct values make an open list', () => {
        const many = buildOldTagInventory([
            [{ key: 'value', value: 'x' }],
            ...Array.from({ length: 30 }, (_, i) => [{ key: 'serial', value: `SN-${i}` }]),
        ]);
        const p = proposeStandard(many, input({ rooms: [] }));
        assert.deepEqual(
            p.catalogRows.map(r => [r.key, r.allowOther, r.values]),
            [
                [
                    'serial',
                    true,
                    Array.from({ length: 30 }, (_, i) => `SN-${i}`)
                        .sort()
                        .join(', '),
                ],
                ['value_tag', false, 'X'],
            ],
        );
        assert.ok(!p.ruleRows.some(r => (r as { fromKey: string }).fromKey === 'serial'), 'open list: values kept');
    });

    it('lower and as-used spelling', () => {
        const lower = proposeStandard(inv, input({ valueStyle: 'lower', rooms: [] }));
        assert.match(String(lower.catalogRows[0].values), /^kotlownia, garaz, heizungsraum|^kotlownia, garaz/);
        const used = proposeStandard(inv, input({ valueStyle: 'asUsed', rooms: [] }));
        assert.equal(String(used.catalogRows[0].values).split(', ')[0], 'Kotłownia');
    });

    describe('every used tag is covered (extension 3)', () => {
        it('rows left empty by "take over old tags" are completed in place, not skipped', () => {
            const catalogRows = [
                { key: 'Location', values: 'KOTLOWNIA, SALON', allowOther: false, defaultValue: '', description: '' },
            ];
            const catalog = parseTagDefinitions(catalogRows, 'snake', 'value', []);
            const taken = suggestTagRules(inv, [], catalog, 'snake').map(ruleToRow);
            assert.ok(
                taken.some(r => r.fromKey === 'floor' && r.toKey === ''),
                'placeholder present',
            );
            const p = proposeStandard(inv, input({ catalogRows, ruleRows: taken, rooms: [] }));
            const rows = p.ruleRows as { fromKey: string; fromValue: string[]; toKey: string; toValue: string }[];
            assert.ok(p.completedRows >= 2);
            assert.ok(!rows.some(r => r.toKey === '' && r.toValue === ''), JSON.stringify(rows));
            assert.equal(rows.find(r => r.fromKey === 'floor' && !r.fromValue.length)?.toKey, 'floor');
            assert.equal(p.coverage.uncovered.length, 0, p.coverage.uncovered.join('\n'));
            assert.equal(p.coverage.covered, p.coverage.total);
        });

        it('a placeholder with old values needing different standard values is split', () => {
            const catalogRows = [
                { key: 'Location', values: 'KOTLOWNIA, SALON', allowOther: false, defaultValue: '', description: '' },
            ];
            const ruleRows = [{ fromKey: 'room', fromValue: ['Kotłownia', 'Garaż'], toKey: 'Location', toValue: '' }];
            const p = proposeStandard(inv, input({ catalogRows, ruleRows, rooms: [] }));
            const rows = p.ruleRows as { fromKey: string; fromValue: string[]; toKey: string; toValue: string }[];
            assert.deepEqual(rows.slice(0, 2), [
                { fromKey: 'room', fromValue: ['Kotłownia'], toKey: 'Location', toValue: 'KOTLOWNIA' },
                { fromKey: 'room', fromValue: ['Garaż'], toKey: 'Location', toValue: 'GARAZ' },
            ]);
            assert.equal(p.coverage.uncovered.length, 0, p.coverage.uncovered.join('\n'));
        });

        it('open lists: spelling variants are merged and the used values are listed', () => {
            const devices = buildOldTagInventory([
                [{ key: 'device', value: 'Pump 1' }],
                [{ key: 'device', value: 'Pump 1' }],
                [{ key: 'Gerät', value: 'pump 1' }],
                [{ key: 'device', value: 'Boiler' }],
            ]);
            const catalogRows = [{ key: 'device', values: '', allowOther: true, defaultValue: '', description: '' }];
            const p = proposeStandard(devices, input({ catalogRows }));
            assert.equal(p.catalogRows[0].values, 'Pump 1, Boiler');
            // case-only variants need no rule of their own: the catalog spelling is applied automatically
            assert.deepEqual(p.ruleRows, [{ fromKey: 'Gerät', fromValue: [], toKey: 'device', toValue: '' }]);
            const cat = parseTagDefinitions(p.catalogRows, 'snake', 'value', []);
            assert.deepEqual(
                applyTagRules([{ key: 'Gerät', value: 'pump 1' }], parseTagRules(p.ruleRows), cat, 'snake').tags,
                [{ key: 'device', value: 'Pump 1' }],
            );
            assert.deepEqual(p.coverage, { total: 3, covered: 3, removed: 0, uncovered: [] });
        });

        it('coverage counts removed tags and reports what a user rule leaves invalid', () => {
            const catalogRows = [
                { key: 'Location', values: 'KOTLOWNIA, SALON', allowOther: false, defaultValue: '', description: '' },
            ];
            const ruleRows = [
                { fromKey: 'source', fromValue: [], toKey: '-', toValue: '' },
                { fromKey: 'room', fromValue: ['Garaż'], toKey: 'Location', toValue: 'NOPE' },
            ];
            const p = proposeStandard(inv, input({ catalogRows, ruleRows, rooms: [] }));
            assert.equal(p.coverage.removed, 1);
            assert.deepEqual(p.coverage.uncovered, [
                'room=Garaż: value "NOPE" is not allowed for "Location" (allowed: KOTLOWNIA, SALON, KUCHE)',
            ]);
            assert.equal(p.coverage.covered + p.coverage.removed + p.coverage.uncovered.length, p.coverage.total);
        });

        it('tags of old points and of this instance join the inventory with their origin', () => {
            const points = { keys: new Map() };
            for (const u of countTagUses([
                { key: 'room', value: 'Spiżarnia' },
                { key: 'room', value: 'Spiżarnia' },
                { key: 'ack', value: 'true' },
                { key: '_field', value: 'value' },
            ])) {
                addToInventory(points, u.key, u.value, u.measurements ?? 0, 'points');
            }
            const own = buildOldTagInventory([[{ key: 'Location', value: 'kotlownia' }]], 'instance');
            const all = mergeInventories(inv, points, own);
            const room = all.keys.get('room')!;
            assert.equal(room.values.get('Spiżarnia'), 0);
            assert.equal(room.points.get('Spiżarnia'), 2);
            assert.equal(describeUse(room, 'Spiżarnia'), '2 old measurement(s)');
            assert.equal(describeUse(room, 'Kotłownia'), '2 state(s)');
            assert.ok(!all.keys.has('ack') && !all.keys.has('_field'));
            assert.deepEqual([...all.keys.get('location')!.origins.get('kotlownia')!], ['instance']);
            const p = proposeStandard(all, input());
            assert.ok(
                p.summary.some(
                    l => /Spiżarnia → SPIZARNIA/.test(l) || /SPIZARNIA/.test(String(p.catalogRows[0].values)),
                ),
                p.summary.join('\n'),
            );
            assert.equal(p.coverage.uncovered.length, 0, p.coverage.uncovered.join('\n'));
        });
    });
});
