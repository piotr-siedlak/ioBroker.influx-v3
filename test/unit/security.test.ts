import { strict as assert } from 'node:assert';
import { buildMigrationPreview, mapInfluxdbConfig } from '../../src/lib/migration';
import { checkReadOnlyInfluxQL, checkReadOnlySql, senderAllowed, stripSql } from '../../src/lib/queryService';
import { clearSecrets, redact, registerSecret } from '../../src/lib/redact';

describe('security', () => {
    describe('read-only SQL guard', () => {
        const ok = [
            'SELECT * FROM numeric_states LIMIT 10',
            '  select 1;  ',
            "WITH t AS (SELECT * FROM x) SELECT * FROM t WHERE name = 'DROP TABLE x; DELETE'",
            'SHOW TABLES',
            'DESCRIBE numeric_states',
            'EXPLAIN SELECT 1',
            "SELECT replace(state_id, '.', '_') FROM t",
            '-- comment DELETE\nSELECT 1',
            'SELECT "insert" FROM t',
        ];
        for (const q of ok) {
            it(`accepts: ${q.replace(/\n/g, ' ')}`, () => assert.equal(checkReadOnlySql(q).ok, true));
        }
        const bad = [
            'DELETE FROM t',
            'INSERT INTO temperature VALUES (1)',
            'DROP TABLE t',
            'SELECT 1; DROP TABLE t',
            "COPY (SELECT 1) TO '/tmp/x.parquet'",
            "EXPLAIN ANALYZE COPY (SELECT 1) TO 'x'",
            "CREATE EXTERNAL TABLE x STORED AS CSV LOCATION '/etc/passwd'",
            'SET datafusion.execution.batch_size = 1',
            'WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x',
            '/* */ UPDATE t SET a = 1',
            '',
            42,
        ];
        for (const q of bad) {
            it(`rejects: ${String(q)}`, () => assert.equal(checkReadOnlySql(q).ok, false));
        }
        it('limits the query length', () => assert.equal(checkReadOnlySql(`SELECT ${'1,'.repeat(20000)}1`).ok, false));
        it('stripSql removes literals and comments', () => {
            assert.equal(stripSql('SELECT \'a;b\' -- x\n FROM "t;" /* ; */'), 'SELECT \'\'   FROM ""  ');
        });
    });

    describe('read-only InfluxQL guard', () => {
        it('accepts SELECT/SHOW, rejects writes', () => {
            assert.equal(checkReadOnlyInfluxQL('SELECT mean(value) FROM t GROUP BY time(1h)').ok, true);
            assert.equal(checkReadOnlyInfluxQL('SHOW MEASUREMENTS').ok, true);
            assert.equal(checkReadOnlyInfluxQL('SELECT * INTO x FROM t').ok, false);
            assert.equal(checkReadOnlyInfluxQL('DROP MEASUREMENT t').ok, false);
            assert.equal(checkReadOnlyInfluxQL('SELECT 1; DROP MEASUREMENT t').ok, false);
        });
    });

    describe('sender allow-list', () => {
        it('empty list allows everybody', () => assert.equal(senderAllowed('system.adapter.x.0', []), true));
        it('matches adapter or instance prefixes', () => {
            const list = ['admin', 'javascript.0'];
            assert.equal(senderAllowed('system.adapter.admin.0', list), true);
            assert.equal(senderAllowed('system.adapter.javascript.0', list), true);
            assert.equal(senderAllowed('system.adapter.javascript.1', list), false);
            assert.equal(senderAllowed('system.adapter.adminx.0', list), false);
            assert.equal(senderAllowed(undefined, list), false);
        });
    });

    describe('redact', () => {
        afterEach(() => clearSecrets());
        it('removes tokens in all known forms', () => {
            assert.equal(redact('Authorization: Bearer abcdefghijklmnop'), 'Authorization: ***');
            assert.equal(redact('token apiv3_AbCdEf-123_xyz failed'), '*** failed');
            assert.equal(redact('url?token=secret123&x=1'), 'url?token=***&x=1');
            assert.ok(!redact('jwt eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abcdefghijklmnop').includes('eyJ'));
        });
        it('removes registered secrets anywhere, also in objects and errors', () => {
            registerSecret('my-very-secret');
            assert.equal(redact('xx my-very-secret yy'), 'xx *** yy');
            assert.ok(!redact({ a: 'my-very-secret' }).includes('my-very-secret'));
            assert.ok(!redact(new Error('my-very-secret')).includes('my-very-secret'));
        });
    });

    describe('migration from ioBroker.influxdb', () => {
        it('maps classic settings', () => {
            assert.deepEqual(
                mapInfluxdbConfig({
                    enabled: true,
                    changesOnly: true,
                    debounceTime: 0,
                    debounce: 1000,
                    changesRelogInterval: 60,
                    changesMinDelta: 0.1,
                    storageType: 'Number',
                    disableSkippedValueLogging: true,
                    aliasId: '',
                    customTags: [
                        { name: 'location', value: 'KOTLOWNIA' },
                        { name: '', value: 'x' },
                    ],
                    retention: 31536000,
                }),
                {
                    enabled: true,
                    changesOnly: true,
                    debounceTime: 0,
                    blockTime: 1000,
                    changesRelogInterval: 60,
                    changesMinDelta: 0.1,
                    storeAs: 'number',
                    logSkippedBeforeChange: false,
                    customTags: [{ key: 'location', value: 'KOTLOWNIA' }],
                },
            );
        });
        it('preview lists only enabled states of the selected source and flags existing configs', () => {
            const items = buildMigrationPreview(
                [
                    { id: 'b.0.x', custom: { 'influxdb.0': { enabled: true }, 'influx_v3.0': { enabled: true } } },
                    { id: 'a.0.y', custom: { 'influxdb.0': { enabled: true } } },
                    { id: 'c.0.z', custom: { 'influxdb.0': { enabled: false } } },
                    { id: 'd.0.w', custom: { 'influxdb.1': { enabled: true } } },
                ],
                'influxdb.0',
                'influx_v3.0',
            );
            assert.deepEqual(
                items.map(i => [i.id, i.alreadyConfigured]),
                [
                    ['a.0.y', false],
                    ['b.0.x', true],
                ],
            );
        });
    });
});
