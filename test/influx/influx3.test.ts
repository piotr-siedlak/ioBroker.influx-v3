/**
 * Tests against a real InfluxDB 3 Core. Skipped unless INFLUX3_URL and INFLUX3_TOKEN are set:
 *
 *   eval "$(scripts/influx3-test-server.sh start)" && npm run test:influx
 */
import { strict as assert } from 'node:assert';
import { execFileSync, spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
    buildHistoryQueries,
    type HistoryTarget,
    mapHistoryRows,
    normalizeHistoryOptions,
} from '../../src/lib/historyService';
import { type ClientOptions, HttpError, InfluxV3Client } from '../../src/lib/influxClient';
import { serialize } from '../../src/lib/lineProtocol';
import { checkReadOnlySql } from '../../src/lib/queryService';
import { Spool } from '../../src/lib/spool';
import { type QueueOptions, WriteQueue } from '../../src/lib/writeQueue';

const URL = process.env.INFLUX3_URL;
const TOKEN = process.env.INFLUX3_TOKEN;
const CONTAINER = process.env.INFLUX3_CONTAINER;
const DB = 'iobtest';
const T0 = Date.UTC(2026, 9, 19, 10, 0, 0); // fixed timestamps → deterministic results

const options = (over: Partial<ClientOptions> = {}): ClientOptions => ({
    url: URL ?? '',
    token: TOKEN ?? '',
    database: DB,
    edition: 'auto',
    timeoutMs: 5000,
    rejectUnauthorized: true,
    gzip: true,
    noSync: false,
    maxSockets: 2,
    ...over,
});

const queueOptions = (over: Partial<QueueOptions> = {}): QueueOptions => ({
    writeMode: 'batched',
    batchMaxLines: 1000,
    batchMaxBytes: 1024 * 1024,
    flushIntervalMs: 100,
    maxConcurrentWrites: 1,
    retryBaseDelayMs: 200,
    retryMaxDelayMs: 1000,
    authRetryMs: 10000,
    bufferMode: 'memory',
    bufferMaxRecords: 1000000,
    bufferMaxBytes: 64 * 1024 * 1024,
    spoolMaxBytes: 64 * 1024 * 1024,
    bufferMaxAgeMs: 3600000,
    overflowPolicy: 'dropOldest',
    persistOnShutdown: true,
    ...over,
});

const noEvents = {
    written: () => undefined,
    rejected: () => undefined,
    dropped: () => undefined,
    connection: () => undefined,
    log: () => undefined,
};

async function waitFor(cond: () => boolean | Promise<boolean>, timeoutMs = 20000): Promise<void> {
    const start = Date.now();
    while (!(await cond())) {
        if (Date.now() - start > timeoutMs) {
            throw new Error('condition not reached in time');
        }
        await new Promise(r => setTimeout(r, 100));
    }
}

/** Grafana scenarios of docs/GRAFANA.md with the macros replaced by a fixed range */
export const GRAFANA_RANGE = "time >= TIMESTAMP '2026-10-19T09:00:00Z' AND time < TIMESTAMP '2026-10-19T12:00:00Z'";

(URL && TOKEN ? describe : describe.skip)('InfluxDB 3 Core (real server)', function () {
    this.timeout(60000);
    let client: InfluxV3Client;

    before(async () => {
        client = new InfluxV3Client(options());
    });
    after(() => client?.close());

    it('ping detects Core and its version', async () => {
        const p = await client.ping();
        assert.equal(p.ok, true, p.error);
        assert.equal(p.edition, 'core');
        assert.match(p.version ?? '', /^3\./);
    });

    it('wrong token: ping and write report authentication errors', async () => {
        const bad = new InfluxV3Client(options({ token: 'apiv3_wrong' }));
        const p = await bad.ping();
        assert.equal(p.ok, false);
        assert.equal(p.status, 401);
        const w = await bad.write(['x value=1 1']);
        assert.equal(w.kind, 'auth');
        bad.close();
    });

    it('invalid database name is rejected, not retried', async () => {
        const bad = new InfluxV3Client(options({ database: 'bad name' }));
        assert.equal((await bad.write(['x value=1 1'])).kind, 'invalid');
        bad.close();
    });

    it('closed port → transient', async () => {
        const bad = new InfluxV3Client(options({ url: 'http://127.0.0.1:1' }));
        assert.equal((await bad.write(['x value=1 1'])).kind, 'transient');
        bad.close();
    });

    it('writes numbers, booleans and strings with exact millisecond timestamps and special characters', async () => {
        const lines = [
            serialize({
                table: 'numeric_states',
                tags: { state_id: 'heat.0.hc1.temperature', room: 'Kotłownia', 'sub type': 'HC 1,a=b' },
                fields: {
                    value: { type: 'float', value: 38.7 },
                    ack: { type: 'boolean', value: true },
                    q: { type: 'integer', value: 0 },
                },
                ts: T0 + 123,
            }),
            serialize({
                table: 'boolean_states',
                tags: { state_id: 'heat.0.hc1.pump' },
                fields: {
                    value: { type: 'boolean', value: true },
                    ack: { type: 'boolean', value: false },
                    q: { type: 'integer', value: 0 },
                },
                ts: T0 + 1,
            }),
            serialize({
                table: 'string_states',
                tags: { state_id: 'info.0.text' },
                fields: {
                    value: { type: 'string', value: 'He said "hi"\\ – zażółć {"json":[1,2]}' },
                    ack: { type: 'boolean', value: true },
                    q: { type: 'integer', value: 0 },
                },
                ts: T0 + 2,
            }),
        ];
        assert.deepEqual(await client.write(lines), { kind: 'ok' });
        const num = await client.querySql<{
            ts: number;
            value: number;
            room: string;
            'sub type': string;
            ack: boolean;
            q: number;
        }>(
            'SELECT CAST(time AS BIGINT) / 1000000 AS ts, value, room, "sub type", ack, q FROM numeric_states WHERE state_id = $sid',
            { sid: 'heat.0.hc1.temperature' },
        );
        assert.deepEqual(num, [
            { ts: T0 + 123, value: 38.7, room: 'Kotłownia', 'sub type': 'HC 1,a=b', ack: true, q: 0 },
        ]);
        const str = await client.querySql<{ value: string }>(
            "SELECT value FROM string_states WHERE state_id = 'info.0.text'",
        );
        assert.equal(str[0].value, 'He said "hi"\\ – zażółć {"json":[1,2]}');
        const bool = await client.querySql<{ value: boolean }>(
            "SELECT value FROM boolean_states WHERE state_id = 'heat.0.hc1.pump'",
        );
        assert.equal(bool[0].value, true);
    });

    it('per-state table names with dots work with quoted identifiers', async () => {
        const line = serialize({
            table: 'hm-rpc.0.ABC.TEMPERATURE',
            tags: { state_id: 'hm-rpc.0.ABC.TEMPERATURE' },
            fields: { value: { type: 'float', value: 1 } },
            ts: T0,
        });
        assert.deepEqual(await client.write([line]), { kind: 'ok' });
        const rows = await client.querySql('SELECT value FROM "hm-rpc.0.ABC.TEMPERATURE"');
        assert.equal(rows.length, 1);
    });

    it('a type conflict is reported as partial write with the record number', async () => {
        const r = await client.write([
            `numeric_states,state_id=conflict.0.x value=1 ${T0 + 10}`,
            `numeric_states,state_id=conflict.0.x value="text" ${T0 + 11}`,
            `numeric_states,state_id=conflict.0.x value=3 ${T0 + 12}`,
        ]);
        assert.equal(r.kind, 'partial');
        assert.deepEqual(r.kind === 'partial' && r.rejected.map(x => x.line), [2]);
        const rows = await client.querySql('SELECT value FROM numeric_states WHERE state_id = $s ORDER BY time', {
            s: 'conflict.0.x',
        });
        assert.deepEqual(rows, [{ value: 1 }, { value: 3 }]);
    });

    it('writing the same point twice does not create a duplicate (idempotent replay)', async () => {
        const line = `numeric_states,state_id=dup.0.x value=5,ack=true ${T0 + 20}`;
        await client.write([line]);
        await client.write([line]);
        const rows = await client.querySql('SELECT count(*) AS n FROM numeric_states WHERE state_id = $s', {
            s: 'dup.0.x',
        });
        assert.equal(Number(rows[0].n), 1);
    });

    it('5000 records in one gzip batch', async () => {
        const lines = Array.from({ length: 5000 }, (_, i) => `bulk,state_id=bulk.${i % 50} value=${i} ${T0 + i}`);
        assert.deepEqual(await client.write(lines), { kind: 'ok' });
        const rows = await client.querySql('SELECT count(*) AS n FROM bulk');
        assert.equal(Number(rows[0].n), 5000);
    });

    describe('history queries on the server', () => {
        const id = 'hist.0.temp';
        const target = (): HistoryTarget => ({
            table: 'hist_numbers',
            field: 'value',
            valueType: 'number',
            storeId: id,
            columns: new Set(['time', 'value', 'state_id', 'ack', 'q']),
            metaColumns: { ack: 'ack', q: 'q' },
        });
        const run = async (opts: Record<string, unknown>, t = target()) => {
            const { options: o } = normalizeHistoryOptions(
                opts,
                { limit: 2000, hardLimit: 100000, aggregate: 'average' },
                T0,
            );
            const built = buildHistoryQueries(t, o);
            const results = await Promise.all(
                built.queries.map(async q => ({ kind: q.kind, rows: await client.querySql(q.sql, q.params) })),
            );
            return mapHistoryRows(results, o, built.step, t.storeId);
        };

        before(async () => {
            // 60 values, one per minute, value = minute index
            const lines = Array.from(
                { length: 60 },
                (_, i) => `hist_numbers,state_id=${id} value=${i},ack=${i % 2 === 0},q=0i ${T0 + i * 60000}`,
            );
            lines.push(`hist_numbers,state_id=other.0.x value=1000 ${T0}`);
            lines.push(
                ...Array.from({ length: 10 }, (_, i) => `hist_bool,state_id=hist.0.b value=${i < 3} ${T0 + i * 60000}`),
            );
            assert.deepEqual(await client.write(lines), { kind: 'ok' });
        });

        it('raw values with metadata and border values', async () => {
            const res = await run({
                start: T0 + 10 * 60000,
                end: T0 + 12 * 60000,
                aggregate: 'none',
                ack: true,
                q: true,
            });
            assert.deepEqual(
                res.map(r => [r.ts - T0, r.val, r.ack]),
                [
                    [9 * 60000, 9, false],
                    [10 * 60000, 10, true],
                    [11 * 60000, 11, false],
                    [12 * 60000, 12, true],
                    [13 * 60000, 13, false],
                ],
            );
            assert.equal(res[1].q, 0);
        });

        it('newest entries', async () => {
            const res = await run({ count: 3, aggregate: 'none' });
            assert.deepEqual(
                res.map(r => r.val),
                [57, 58, 59],
            );
        });

        it('average / min / max / total / count per 10 minutes (date_bin)', async () => {
            const range = { start: T0, end: T0 + 60 * 60000 - 1, step: 10 * 60000, removeBorderValues: true };
            const avg = await run({ ...range, aggregate: 'average' });
            assert.deepEqual(
                avg.map(r => r.val),
                [4.5, 14.5, 24.5, 34.5, 44.5, 54.5],
            );
            assert.equal(avg[0].ts, T0 + 5 * 60000, 'timestamp = middle of the bin');
            assert.deepEqual(
                (await run({ ...range, aggregate: 'min' })).map(r => r.val),
                [0, 10, 20, 30, 40, 50],
            );
            assert.deepEqual(
                (await run({ ...range, aggregate: 'max' })).map(r => r.val),
                [9, 19, 29, 39, 49, 59],
            );
            assert.deepEqual(
                (await run({ ...range, aggregate: 'total' })).map(r => r.val),
                [45, 145, 245, 345, 445, 545],
            );
            assert.deepEqual(
                (await run({ ...range, aggregate: 'count' })).map(r => Number(r.val)),
                [10, 10, 10, 10, 10, 10],
            );
            const p = await run({ ...range, aggregate: 'percentile', percentile: 50 });
            assert.equal(p.length, 6);
        });

        it('minmax returns the real times of the extremes', async () => {
            const res = await run({ start: T0, end: T0 + 20 * 60000 - 1, step: 10 * 60000, aggregate: 'minmax' });
            assert.deepEqual(
                res.map(r => [(r.ts - T0) / 60000, r.val]),
                [
                    [0, 0],
                    [9, 9],
                    [10, 10],
                    [19, 19],
                ],
            );
        });

        it('boolean average = share of true values', async () => {
            const res = await run(
                { start: T0, end: T0 + 10 * 60000 - 1, step: 10 * 60000, aggregate: 'average' },
                {
                    ...target(),
                    table: 'hist_bool',
                    valueType: 'boolean',
                    storeId: 'hist.0.b',
                    columns: new Set(['time', 'value', 'state_id']),
                    metaColumns: {},
                },
            );
            assert.deepEqual(
                res.map(r => r.val),
                [0.3],
            );
        });

        it('a missing table returns an HttpError (handled as "no data" by the adapter)', async () => {
            await assert.rejects(() => client.querySql('SELECT * FROM "does_not_exist"'), HttpError);
        });
    });

    it('user SQL guard blocks INSERT before it reaches the server; SELECT works', async () => {
        assert.equal(checkReadOnlySql('INSERT INTO numeric_states VALUES (1)').ok, false);
        const g = checkReadOnlySql('SELECT count(*) AS n FROM bulk');
        assert.ok(g.ok);
        assert.equal(Number((await client.querySql(g.ok ? g.statement : ''))[0].n), 5000);
    });

    it('InfluxQL endpoint works on Core', async () => {
        const rows = await client.queryInfluxQL('SELECT count(value) FROM bulk');
        assert.ok(rows.length >= 1);
    });

    it('schema: information_schema columns are read', async () => {
        const cols = await client.getSchemaColumns();
        const value = cols.find(c => c.table === 'numeric_states' && c.column === 'value');
        assert.equal(value?.dataType, 'Float64');
        assert.ok(
            cols.some(
                c => c.table === 'numeric_states' && c.column === 'state_id' && c.dataType.startsWith('Dictionary'),
            ),
        );
    });

    it('explicit database creation with retention; retention is readable', async () => {
        if (!(await client.listDatabases()).includes('iobtest_ret')) {
            await client.createDatabase('iobtest_ret', '30d');
        }
        await assert.rejects(
            () => client.createDatabase('iobtest_ret', '30d'),
            /409|exists/,
            'existing databases are never changed',
        );
        const ret = new InfluxV3Client(options({ database: 'iobtest_ret' }));
        assert.equal(await ret.getRetentionMs(), 30 * 86400000);
        assert.ok((await client.listDatabases()).includes('iobtest_ret'));
        assert.equal(await client.getRetentionMs(), undefined, 'database without retention = infinite');
        ret.close();
    });

    it('experimental cloud path (/api/v2/write) is accepted by the Core compatibility endpoint', async () => {
        const cloud = new InfluxV3Client(options({ edition: 'cloud' }));
        assert.deepEqual(await cloud.write([`v2path,state_id=v2.0.x value=1 ${T0}`]), { kind: 'ok' });
        cloud.close();
        const rows = await client.querySql('SELECT value FROM v2path');
        assert.deepEqual(rows, [{ value: 1 }]);
    });

    describe('reliability with the write queue', () => {
        it('outage (docker pause) → retries → replay without loss', async function () {
            if (!CONTAINER) {
                this.skip();
            }
            const c = new InfluxV3Client(options({ timeoutMs: 1000 }));
            let written = 0;
            const conn: boolean[] = [];
            const q = new WriteQueue(queueOptions(), c, {
                ...noEvents,
                written: n => (written += n),
                connection: ok => conn.push(ok),
            });
            await q.start();
            execFileSync('docker', ['pause', CONTAINER]);
            try {
                for (let i = 0; i < 300; i++) {
                    q.push(`outage,state_id=outage.0.x value=${i} ${T0 + i}`);
                }
                await waitFor(() => conn.includes(false), 15000);
            } finally {
                execFileSync('docker', ['unpause', CONTAINER]);
            }
            await waitFor(() => written === 300, 30000);
            const rows = await c.querySql('SELECT count(*) AS n FROM outage');
            assert.equal(Number(rows[0].n), 300);
            assert.equal(conn[conn.length - 1], true);
            await q.shutdown(1000);
            c.close();
        });

        it('kill -9 of a writer in disk mode → the next start replays the spool', async () => {
            const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'influx-v3-kill9-'));
            // one single node process (tsx as loader), so SIGKILL really kills the writer
            const child = spawn(
                process.execPath,
                ['--require', 'tsx/cjs', path.join(__dirname, 'fixtures/spoolWriter.ts'), dir, '1000'],
                {
                    stdio: ['ignore', 'pipe', 'inherit'],
                    cwd: path.join(__dirname, '../..'),
                },
            );
            await new Promise<void>((resolve, reject) => {
                const t = setTimeout(() => reject(new Error('writer did not spool in time')), 30000);
                child.stdout.on('data', (d: Buffer) => {
                    if (d.toString().includes('SPOOLED')) {
                        clearTimeout(t);
                        resolve();
                    }
                });
            });
            child.kill('SIGKILL');
            await new Promise(r => child.once('exit', r));
            const files = fs.readdirSync(dir).filter(f => f.endsWith('.json'));
            assert.ok(files.length >= 10, `${files.length} batch files left by the killed process`);

            let written = 0;
            const c = new InfluxV3Client(options());
            const q = new WriteQueue(
                queueOptions({ bufferMode: 'disk' }),
                c,
                { ...noEvents, written: n => (written += n) },
                new Spool(dir, true),
            );
            const recovered = await q.start();
            assert.equal(recovered, 1000);
            await waitFor(() => written === 1000);
            const rows = await c.querySql('SELECT count(*) AS n FROM kill9');
            assert.equal(Number(rows[0].n), 1000);
            await waitFor(() => fs.readdirSync(dir).filter(f => f.endsWith('.json')).length === 0);
            await q.shutdown(500);
            c.close();
            fs.rmSync(dir, { recursive: true, force: true });
        });
    });

    describe('Grafana scenarios (queries of docs/GRAFANA.md)', () => {
        before(async () => {
            const lines: string[] = [];
            const devices = [
                ['heat.0.hc1.temperature', 'KOTLOWNIA', 'PC', 'HC1', 41],
                ['heat.0.hc2.temperature', 'KOTLOWNIA', 'PC', 'HC2', 35],
                ['heat.0.hc3.temperature', 'KOTLOWNIA', 'PC', 'HC3', 30],
                ['heat.0.dhw.temperature', 'KOTLOWNIA', 'BOILER', 'DHW', 50],
                ['zigbee.0.salon.temperature', 'SALON', 'SENSOR', 'ROOM', 21],
            ] as const;
            for (let m = 0; m < 60; m++) {
                for (const [sid, loc, dev, sub, base] of devices) {
                    const ts = T0 + m * 60000;
                    const room = loc === 'KOTLOWNIA' ? 'Kotłownia' : 'Salon';
                    lines.push(
                        `temperature,state_id=${sid},location=${loc},device=${dev},subtype=${sub},room=${room},unit=°C,adapter=${sid.split('.')[0]} value=${base + (m % 5)} ${ts}`,
                    );
                    lines.push(
                        `temperature_tagged,state_id=${sid},Location=${loc},Device=${dev},Sub-Type=${sub} value=${base + (m % 5)} ${ts}`,
                    );
                }
                lines.push(`boolean_states,state_id=heat.0.pump value=${m % 3 === 0} ${T0 + m * 60000}`);
            }
            assert.deepEqual(await client.write(lines), { kind: 'ok' });
        });

        const q = async (sql: string) => client.querySql(sql.replace('$__timeFilter(time)', GRAFANA_RANGE));

        it('1. all temperatures for location = KOTLOWNIA', async () => {
            const rows = await q(
                "SELECT time, state_id, value FROM temperature WHERE location = 'KOTLOWNIA' AND $__timeFilter(time) ORDER BY time",
            );
            assert.equal(rows.length, 4 * 60);
        });
        it('2. device = PC and subtype IN (HC1, HC2, HC3)', async () => {
            const rows = await q(
                "SELECT time, subtype, value FROM temperature WHERE device = 'PC' AND subtype IN ('HC1', 'HC2', 'HC3') AND $__timeFilter(time) ORDER BY time",
            );
            assert.equal(rows.length, 3 * 60);
        });
        it('3. 15-minute averages with date_bin', async () => {
            const rows = await q(
                "SELECT date_bin(INTERVAL '15 minutes', time) AS time, subtype, avg(value) AS value FROM temperature WHERE device = 'PC' AND $__timeFilter(time) GROUP BY 1, subtype ORDER BY 1, subtype",
            );
            assert.equal(rows.length, 4 * 3);
            assert.equal(rows[0].value, 41 + 2);
        });
        it('4. history of one ioBroker state_id', async () => {
            const rows = await q(
                "SELECT time, value FROM temperature WHERE state_id = 'heat.0.hc1.temperature' AND $__timeFilter(time) ORDER BY time",
            );
            assert.equal(rows.length, 60);
        });
        it('5. counting boolean states in a time window', async () => {
            const rows = await q(
                'SELECT state_id, count(*) AS samples, sum(CASE WHEN value THEN 1 ELSE 0 END) AS true_count FROM boolean_states WHERE $__timeFilter(time) GROUP BY state_id',
            );
            const pump = rows.find(r => r.state_id === 'heat.0.pump');
            assert.equal(Number(pump?.samples), 60);
            assert.equal(Number(pump?.true_count), 20);
        });
        it('6. filter by room, device, unit, adapter', async () => {
            const rows = await q(
                "SELECT time, state_id, value FROM temperature WHERE room = 'Kotłownia' AND device = 'PC' AND unit = '°C' AND adapter = 'heat' AND $__timeFilter(time)",
            );
            assert.equal(rows.length, 3 * 60);
        });
        it('preserved tag keys Location / Sub-Type / Device need double quotes', async () => {
            const rows = await q(
                `SELECT time, "Sub-Type", value FROM temperature_tagged WHERE "Location" = 'KOTLOWNIA' AND "Device" = 'PC' AND "Sub-Type" IN ('HC1', 'HC2') AND $__timeFilter(time)`,
            );
            assert.equal(rows.length, 2 * 60);
            await assert.rejects(() => q("SELECT * FROM temperature_tagged WHERE Location = 'KOTLOWNIA'"), /location/i);
        });
    });
});
