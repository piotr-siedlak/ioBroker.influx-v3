import { strict as assert } from 'node:assert';
import ioPackage from '../../io-package.json';
import {
    DEFAULT_INSTANCE_CONFIG,
    normalizeInstanceConfig,
    normalizeStateConfig,
    tableRows,
    validateTableName,
} from '../../src/lib/config';

describe('config', () => {
    it('io-package native defaults normalise without warnings except the missing token', () => {
        const { config, warnings } = normalizeInstanceConfig(ioPackage.native);
        assert.deepEqual(warnings, [
            'token: no API token configured - InfluxDB 3 requires authentication unless started with --without-auth',
        ]);
        for (const key of Object.keys(DEFAULT_INSTANCE_CONFIG) as (keyof typeof DEFAULT_INSTANCE_CONFIG)[]) {
            assert.deepEqual(
                config[key],
                DEFAULT_INSTANCE_CONFIG[key],
                `default of ${key} differs between io-package and code`,
            );
        }
    });

    it('every default config key exists in io-package native', () => {
        for (const key of Object.keys(DEFAULT_INSTANCE_CONFIG)) {
            assert.ok(key in ioPackage.native, `${key} missing in io-package.json native`);
        }
    });

    it('clamps numbers, rejects invalid values and keeps going', () => {
        const { config, warnings } = normalizeInstanceConfig({
            token: 'apiv3_x',
            batchMaxBytes: 50 * 1024 * 1024,
            flushIntervalMs: 'abc',
            maxConcurrentWrites: '3',
            url: 'ftp://host',
            database: 'bad name',
            edition: 'oss',
            valueField: 'time',
            tableNumber: '_internal',
            createDatabaseRetention: '30 days',
        });
        assert.equal(config.batchMaxBytes, 8 * 1024 * 1024);
        assert.equal(config.flushIntervalMs, DEFAULT_INSTANCE_CONFIG.flushIntervalMs);
        assert.equal(config.maxConcurrentWrites, 3);
        assert.equal(config.url, DEFAULT_INSTANCE_CONFIG.url);
        assert.equal(config.database, 'iobroker');
        assert.equal(config.edition, 'auto');
        assert.equal(config.valueField, 'value');
        assert.equal(config.tableNumber, 'numeric_states');
        assert.equal(config.createDatabaseRetention, '');
        assert.ok(warnings.length >= 7);
    });

    it('normalises the URL (no credentials, no trailing slash, keeps path prefix)', () => {
        const { config, warnings } = normalizeInstanceConfig({
            url: 'https://user:pw@influx.local:8443/proxy/',
            token: 't',
        });
        assert.equal(config.url, 'https://influx.local:8443/proxy');
        assert.ok(warnings.some(w => w.includes('credentials')));
    });

    it('requires three different default tables', () => {
        const { config } = normalizeInstanceConfig({ token: 't', tableNumber: 'x', tableBoolean: 'x' });
        assert.equal(config.tableBoolean, 'boolean_states');
    });

    it('parses tag lists in all accepted forms', () => {
        const a = normalizeInstanceConfig({
            token: 't',
            globalTags: [{ key: 'location', value: 'KOTLOWNIA' }, { key: 'x' }],
        });
        assert.deepEqual(a.config.globalTags, [{ key: 'location', value: 'KOTLOWNIA' }]);
        const b = normalizeInstanceConfig({ token: 't', globalTags: 'location=KOTLOWNIA, device=PC' });
        assert.deepEqual(b.config.globalTags, [
            { key: 'location', value: 'KOTLOWNIA' },
            { key: 'device', value: 'PC' },
        ]);
        const c = normalizeInstanceConfig({ token: 't', globalTags: { site: 'home' } });
        assert.deepEqual(c.config.globalTags, [{ key: 'site', value: 'home' }]);
    });

    describe('tag catalog', () => {
        it('accepts tables stored as index-keyed objects', () => {
            assert.deepEqual(tableRows({ 1: 'b', 0: 'a' }), ['a', 'b']);
            assert.deepEqual(tableRows({ x: 1 }), []);
            assert.deepEqual(tableRows(undefined), []);
            const { config } = normalizeInstanceConfig({
                token: 't',
                tagDefinitions: {
                    0: { key: 'Location', values: 'A, B', allowOther: false, defaultValue: 'A', description: '' },
                },
            });
            assert.deepEqual(
                config.tagDefinitions.map(t => t.key),
                ['Location'],
            );
            assert.deepEqual(config.globalTags, [{ key: 'Location', value: 'A' }]);
            const state = normalizeStateConfig(
                { enabled: true, customTags: { 0: { key: 'room', value: 'x' } } },
                config,
                'a.0.b',
            );
            assert.deepEqual(state.config.customTags, [{ key: 'room', value: 'x' }]);
        });

        it('parses keys, values, flags, defaults and derives the global tags', () => {
            const { config, warnings } = normalizeInstanceConfig({
                token: 't',
                tagDefinitions: [
                    {
                        key: 'Location',
                        values: 'KOTLOWNIA, SALON,  ,KOTLOWNIA',
                        allowOther: false,
                        defaultValue: 'KOTLOWNIA',
                        description: 'room',
                    },
                    { key: 'device', values: '', allowOther: true, defaultValue: '', description: '' },
                    { key: 'site', values: 'home', allowOther: false, defaultValue: 'office' },
                    { key: 'state_id', values: 'x' },
                    { key: 'location', values: 'x' },
                    { key: 'empty', values: '', allowOther: false },
                ],
            });
            assert.deepEqual(
                config.tagDefinitions.map(d => [d.key, d.values, d.allowOther, d.defaultValue]),
                [
                    ['Location', ['KOTLOWNIA', 'SALON'], false, 'KOTLOWNIA'],
                    ['device', [], true, ''],
                    ['site', ['home'], false, ''],
                    ['empty', [], false, ''],
                ],
            );
            assert.deepEqual(config.globalTags, [{ key: 'Location', value: 'KOTLOWNIA' }]);
            assert.ok(warnings.some(w => w.includes('"office"') && w.includes('not an allowed value')));
            assert.ok(warnings.some(w => w.includes('state_id') && w.includes('reserved')));
            assert.ok(warnings.some(w => w.includes('defined twice')));
            assert.ok(warnings.some(w => w.includes('"empty"') && w.includes('cannot be used')));
        });
        it('converts the former global tags into catalog keys with defaults', () => {
            const { config, warnings } = normalizeInstanceConfig({
                token: 't',
                globalTags: [{ key: 'site', value: 'home' }],
            });
            assert.deepEqual(config.tagDefinitions, [
                { key: 'site', values: [], allowOther: true, defaultValue: 'home', description: '' },
            ]);
            assert.deepEqual(config.globalTags, [{ key: 'site', value: 'home' }]);
            assert.ok(warnings.some(w => w.includes('converted into the tag catalog')));
            const both = normalizeInstanceConfig({
                token: 't',
                globalTags: [{ key: 'old', value: 'x' }],
                tagDefinitions: [{ key: 'new', allowOther: true }],
            });
            assert.deepEqual(
                both.config.tagDefinitions.map(d => d.key),
                ['new'],
                'an existing catalog wins',
            );
        });
    });

    it('validates table names', () => {
        assert.equal(validateTableName('heating'), null);
        assert.equal(validateTableName('hm-rpc.0.ABC.TEMPERATURE'), null);
        assert.ok(validateTableName('_x'));
        assert.ok(validateTableName(''));
        assert.ok(validateTableName('a\\'));
        assert.ok(validateTableName('a\nb'));
        assert.ok(validateTableName('x'.repeat(256)));
    });

    describe('state config', () => {
        const inst = normalizeInstanceConfig({ token: 't', debounceTime: 500, changesOnly: false, round: '2' }).config;

        it('takes defaults from the instance', () => {
            const { config } = normalizeStateConfig({ enabled: true }, inst, 'a.0.b');
            assert.equal(config.enabled, true);
            assert.equal(config.debounceTime, 500);
            assert.equal(config.changesOnly, false);
            assert.equal(config.round, 2);
            assert.equal(config.useGlobalTags, true);
            assert.equal(config.ackFilter, 'all');
        });

        it('accepts the option names of ioBroker.influxdb', () => {
            const { config } = normalizeStateConfig(
                {
                    enabled: true,
                    storageType: 'Number',
                    disableSkippedValueLogging: true,
                    customTags: [{ name: 'Location', value: 'KOTLOWNIA' }],
                    ignoreBelowNumber: '-5',
                    ignoreAboveNumber: '',
                    round: '',
                },
                inst,
                'a.0.b',
            );
            assert.equal(config.storeAs, 'number');
            assert.equal(config.logSkippedBeforeChange, false);
            assert.deepEqual(config.customTags, [{ key: 'Location', value: 'KOTLOWNIA' }]);
            assert.equal(config.ignoreBelowNumber, -5);
            assert.equal(config.ignoreAboveNumber, null);
            assert.equal(config.round, 2, 'empty round falls back to the instance default');
        });

        it('rejects invalid per-state values', () => {
            const { config, warnings } = normalizeStateConfig(
                {
                    enabled: true,
                    table: '_bad',
                    field: 'x',
                    aliasId: 'a.0.b',
                    ignoreBelowNumber: 10,
                    ignoreAboveNumber: 5,
                    round: 99,
                },
                inst,
                'a.0.b',
            );
            assert.equal(config.table, '');
            assert.equal(config.field, '', 'custom field only in per-state mode');
            assert.equal(config.aliasId, '');
            assert.equal(config.ignoreBelowNumber, null);
            assert.equal(config.round, null);
            assert.ok(warnings.length >= 5);
        });

        it('custom field allowed in per-state mode', () => {
            const ps = normalizeInstanceConfig({ token: 't', schemaMode: 'perState' }).config;
            assert.equal(normalizeStateConfig({ enabled: true, field: 'temp' }, ps, 'x').config.field, 'temp');
            assert.equal(normalizeStateConfig({ enabled: true, field: 'ack' }, ps, 'x').config.field, '');
        });
    });
});
