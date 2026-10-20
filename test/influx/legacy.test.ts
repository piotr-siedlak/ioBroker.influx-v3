/**
 * Import of historical data against real servers: InfluxDB 1.8 and 2.7 seeded exactly like the classic
 * ioBroker.influxdb adapter writes, imported into a real InfluxDB 3 Core. Skipped unless the servers run:
 *
 *   eval "$(scripts/legacy-influx-test-server.sh start)"
 *   eval "$(scripts/influx3-test-server.sh start)"
 *   npm run test:influx
 */
import { strict as assert } from 'node:assert';
import http from 'node:http';
import {
    DataMigrationJob,
    type CheckpointStore,
    type Checkpoint,
    type ImportItem,
    type Progress,
    DAY,
} from '../../src/lib/dataMigration';
import { InfluxV3Client } from '../../src/lib/influxClient';
import {
    createLegacyReader,
    InfluxDB1Reader,
    type LegacyPoint,
    type LegacySourceConfig,
} from '../../src/lib/legacySource';
import { type TagDefinition } from '../../src/lib/config';
import { applyTagRules, parseTagRules } from '../../src/lib/tagMapping';

const env = process.env;
const HAS_V1 = !!(env.INFLUX1_URL && env.INFLUX1_READ_PASSWORD);
const HAS_V2 = !!(env.INFLUX2_URL && env.INFLUX2_TOKEN);
const HAS_V3 = !!(env.INFLUX3_URL && env.INFLUX3_TOKEN);
const RUN = `r${Date.now().toString(36)}`;
const T0 = Date.UTC(2024, 0, 1);
const HOUR = 3600_000;

/** Raw HTTP POST for seeding (with the admin credentials, never used by the code under test) */
function post(url: string, body: string, headers: Record<string, string>): Promise<number> {
    return new Promise((resolve, reject) => {
        const u = new URL(url);
        const req = http.request(
            { hostname: u.hostname, port: u.port, path: `${u.pathname}${u.search}`, method: 'POST', headers },
            res => {
                res.resume();
                res.on('end', () => resolve(res.statusCode ?? 0));
            },
        );
        req.on('error', reject);
        req.end(body);
    });
}

const esc = (s: string): string => s.replace(/([,= \\])/g, '\\$1');

/**
 * Line protocol in the format of the classic adapter: measurement = state id, field value,
 * ack/q/from as fields (or as tags with "use tags", 2.x only), custom tags as tags.
 */
function classicLines(
    measurement: string,
    points: { ts: number; value: number | string | boolean; from?: string }[],
    tags: Record<string, string>,
    metaAsTags = false,
): string {
    return points
        .map(p => {
            const tagPart = Object.entries({
                ...tags,
                ...(metaAsTags ? { ack: 'true', from: p.from ?? 'system.adapter.js.0', q: '0' } : {}),
            })
                .sort(([a], [b]) => a.localeCompare(b))
                .map(([k, v]) => `,${esc(k)}=${esc(v)}`)
                .join('');
            const v =
                typeof p.value === 'string'
                    ? `"${p.value.replace(/(["\\])/g, '\\$1')}"`
                    : typeof p.value === 'boolean'
                      ? String(p.value)
                      : String(p.value);
            const meta = metaAsTags ? '' : `,ack=true,from="${p.from ?? 'system.adapter.js.0'}",q=0`;
            return `${esc(measurement)}${tagPart} value=${v}${meta} ${p.ts}`;
        })
        .join('\n');
}

function hourly(count: number, start = T0): { ts: number; value: number }[] {
    return Array.from({ length: count }, (_, i) => ({
        ts: start + i * HOUR,
        value: Math.round((i / 10 + 0.5) * 10) / 10,
    }));
}

const CATALOG: TagDefinition[] = [
    { key: 'Location', values: ['KOTLOWNIA', 'SALON'], allowOther: false, defaultValue: '', description: '' },
];
const RULES = parseTagRules([{ fromKey: 'room', toKey: 'Location' }]);

function v1Config(over: Partial<LegacySourceConfig> = {}): LegacySourceConfig {
    return {
        version: '1.x',
        url: env.INFLUX1_URL ?? '',
        database: env.INFLUX1_DB ?? 'iobroker',
        retentionPolicy: '',
        user: env.INFLUX1_READ_USER ?? '',
        password: env.INFLUX1_READ_PASSWORD ?? '',
        org: '',
        bucket: '',
        token: '',
        rejectUnauthorized: true,
        timeoutMs: 30_000,
        maxBytes: 64 * 1024 * 1024,
        ...over,
    };
}

function v2Config(over: Partial<LegacySourceConfig> = {}): LegacySourceConfig {
    return {
        version: '2.x',
        url: env.INFLUX2_URL ?? '',
        database: '',
        retentionPolicy: '',
        user: '',
        password: '',
        org: env.INFLUX2_ORG ?? 'iob',
        bucket: env.INFLUX2_BUCKET ?? 'iobroker',
        token: env.INFLUX2_TOKEN ?? '',
        rejectUnauthorized: true,
        timeoutMs: 30_000,
        maxBytes: 64 * 1024 * 1024,
        ...over,
    };
}

class MemStore implements CheckpointStore {
    cp?: Checkpoint;
    load(): Promise<Checkpoint | undefined> {
        return Promise.resolve(this.cp ? (JSON.parse(JSON.stringify(this.cp)) as Checkpoint) : undefined);
    }
    save(cp: Checkpoint): Promise<void> {
        this.cp = JSON.parse(JSON.stringify(cp)) as Checkpoint;
        return Promise.resolve();
    }
}

describe('import of historical data with real InfluxDB 1.x / 2.x servers', function () {
    this.timeout(120_000);

    describe('InfluxDB 1.8 reader', function () {
        before(async function () {
            if (!HAS_V1) {
                this.skip();
            }
            const auth = Buffer.from(`${env.INFLUX1_ADMIN_USER}:${env.INFLUX1_ADMIN_PASSWORD}`).toString('base64');
            const body = [
                classicLines(`${RUN}.temp`, hourly(200), { room: 'kotlownia' }),
                classicLines(`${RUN}.text`, [{ ts: T0, value: 'a "quoted", text' }], {}),
                classicLines(`${RUN}.switch`, [{ ts: T0 + HOUR, value: true, from: 'admin.0' }], {}),
            ].join('\n');
            const status = await post(`${env.INFLUX1_URL}/write?db=${env.INFLUX1_DB}&precision=ms`, body, {
                Authorization: `Basic ${auth}`,
                'Content-Type': 'text/plain',
            });
            assert.equal(status, 204);
        });

        it('a read-only user can check, list, range and read', async () => {
            const r = createLegacyReader(v1Config());
            try {
                const check = await r.check();
                assert.equal(check.ok, true, check.error);
                assert.match(check.version ?? '', /^1\.8/);
                const ms = await r.listMeasurements(10_000);
                assert.ok(ms.includes(`${RUN}.temp`));
                assert.deepEqual(await r.timeRange(`${RUN}.temp`), { first: T0, last: T0 + 199 * HOUR });
                const pts = await r.read(`${RUN}.temp`, T0, T0 + 3 * HOUR);
                assert.deepEqual(
                    pts.map(p => [p.ts, p.value, p.ack, p.q, p.from, p.tags.room]),
                    [
                        [T0, 0.5, true, 0, 'system.adapter.js.0', 'kotlownia'],
                        [T0 + HOUR, 0.6, true, 0, 'system.adapter.js.0', 'kotlownia'],
                        [T0 + 2 * HOUR, 0.7, true, 0, 'system.adapter.js.0', 'kotlownia'],
                    ],
                );
                const text = await r.read(`${RUN}.text`, T0, T0 + 1);
                assert.equal(text[0].value, 'a "quoted", text');
                const sw = await r.read(`${RUN}.switch`, T0, T0 + DAY);
                assert.equal(sw[0].value, true);
                assert.equal(sw[0].from, 'admin.0');
                assert.deepEqual(await r.timeRange(`${RUN}.missing`), { first: undefined, last: undefined });
            } finally {
                r.close();
            }
        });

        it('lists the tags stored in the points with the number of measurements', async () => {
            const r = createLegacyReader(v1Config());
            try {
                const uses = await r.tagInventory();
                const room = uses.find(u => u.key === 'room' && u.value === 'kotlownia');
                assert.ok(room && (room.measurements ?? 0) >= 1, JSON.stringify(uses.slice(0, 20)));
                assert.ok(!uses.some(u => ['ack', 'q', 'from'].includes(u.key)));
            } finally {
                r.close();
            }
        });

        it('reports wrong credentials and a missing database', async () => {
            const bad = new InfluxDB1Reader(v1Config({ password: 'wrong' }));
            const res = await bad.check();
            bad.close();
            assert.equal(res.ok, false);
            assert.match(res.error ?? '', /authentication failed|authorization failed/);
            const missing = new InfluxDB1Reader(
                v1Config({ database: 'nope', user: env.INFLUX1_ADMIN_USER, password: env.INFLUX1_ADMIN_PASSWORD }),
            );
            const res2 = await missing.check();
            missing.close();
            assert.equal(res2.ok, false);
            assert.match(res2.error ?? '', /database not found/);
            const rp = new InfluxDB1Reader(v1Config({ retentionPolicy: 'global' }));
            const res3 = await rp.check();
            rp.close();
            assert.match(res3.error ?? '', /retention policy "global" not found \(existing: autogen\)/);
        });
    });

    describe('InfluxDB 2.7 reader', function () {
        before(async function () {
            if (!HAS_V2) {
                this.skip();
            }
            const body = [
                classicLines(`${RUN}.temp`, hourly(100), { room: 'Salon' }),
                // "use tags" variant of the classic adapter: ack/q/from as tags
                classicLines(`${RUN}.tagged`, hourly(5), {}, true),
                classicLines(`${RUN}.text`, [{ ts: T0, value: 'line\nbreak, comma "quote"' }], {}),
            ].join('\n');
            const status = await post(
                `${env.INFLUX2_URL}/api/v2/write?org=${env.INFLUX2_ORG}&bucket=${env.INFLUX2_BUCKET}&precision=ms`,
                body,
                { Authorization: `Token ${env.INFLUX2_TOKEN}`, 'Content-Type': 'text/plain' },
            );
            assert.equal(status, 204);
        });

        it('checks, lists, ranges and reads both metadata layouts', async () => {
            const r = createLegacyReader(v2Config());
            try {
                const check = await r.check();
                assert.equal(check.ok, true, check.error);
                assert.match(check.version ?? '', /2\.7/);
                assert.ok((await r.listMeasurements(10_000)).includes(`${RUN}.tagged`));
                assert.deepEqual(await r.timeRange(`${RUN}.temp`), { first: T0, last: T0 + 99 * HOUR });
                const pts = await r.read(`${RUN}.temp`, T0 + HOUR, T0 + 3 * HOUR);
                assert.deepEqual(
                    pts.map(p => [p.ts, p.value, p.ack, p.q, p.from, p.tags]),
                    [
                        [T0 + HOUR, 0.6, true, 0, 'system.adapter.js.0', { room: 'Salon' }],
                        [T0 + 2 * HOUR, 0.7, true, 0, 'system.adapter.js.0', { room: 'Salon' }],
                    ],
                );
                const tagged = await r.read(`${RUN}.tagged`, T0, T0 + DAY);
                assert.equal(tagged.length, 5);
                assert.deepEqual(
                    [tagged[0].ack, tagged[0].q, tagged[0].from, tagged[0].tags],
                    [true, 0, 'system.adapter.js.0', {}],
                );
                const text = await r.read(`${RUN}.text`, T0, T0 + 1);
                assert.equal(text[0].value, 'line\nbreak, comma "quote"');
            } finally {
                r.close();
            }
        });

        it('lists the tags stored in the points (ack/q/from tags of "use tags" excluded)', async () => {
            const r = createLegacyReader(v2Config());
            try {
                const uses = await r.tagInventory();
                assert.ok(
                    uses.some(u => u.key === 'room' && u.value === 'Salon'),
                    JSON.stringify(uses.slice(0, 20)),
                );
                assert.ok(!uses.some(u => ['ack', 'q', 'from'].includes(u.key) || u.key.startsWith('_')));
            } finally {
                r.close();
            }
        });

        it('reports a wrong token and a missing bucket', async () => {
            const bad = createLegacyReader(v2Config({ token: 'wrong' }));
            const res = await bad.check();
            bad.close();
            assert.equal(res.ok, false);
            assert.match(res.error ?? '', /authentication failed/);
            const missing = createLegacyReader(v2Config({ bucket: 'nope' }));
            const res2 = await missing.check();
            missing.close();
            assert.equal(res2.ok, false);
            assert.match(res2.error ?? '', /not found/);
        });
    });

    describe('full import into InfluxDB 3', function () {
        // one fixed database (InfluxDB 3 Core allows only 5), runs are separated by their state ids
        const DB = 'iobimport';
        let v3: InfluxV3Client;

        before(async function () {
            if (!HAS_V3 || !(HAS_V1 || HAS_V2)) {
                this.skip();
            }
            v3 = new InfluxV3Client({
                url: env.INFLUX3_URL ?? '',
                token: env.INFLUX3_TOKEN ?? '',
                database: DB,
                edition: 'core',
                timeoutMs: 30_000,
                rejectUnauthorized: true,
                gzip: true,
                noSync: false,
                maxSockets: 2,
            });
            if (!(await v3.listDatabases()).includes(DB)) {
                await v3.createDatabase(DB);
            }
        });

        after(() => v3?.close());

        /** simplified version of the adapter's conversion: grouped table, state_id + catalog tags, ack field */
        function item(id: string, measurement: string): ImportItem {
            return {
                id,
                measurement,
                table: 'numeric_states',
                convert: (pts: LegacyPoint[]) => {
                    const lines: string[] = [];
                    let skipped = 0;
                    for (const p of pts) {
                        if (typeof p.value !== 'number') {
                            skipped++;
                            continue;
                        }
                        const tags = applyTagRules(
                            Object.entries(p.tags).map(([key, value]) => ({ key, value })),
                            RULES,
                            CATALOG,
                            'snake',
                        ).tags;
                        const tagPart = tags.map(t => `,${t.key.toLowerCase()}=${esc(t.value)}`).join('');
                        lines.push(
                            `numeric_states,state_id=${esc(id)}${tagPart} value=${p.value},ack=${p.ack ?? false} ${p.ts}`,
                        );
                    }
                    return { lines, skipped };
                },
            };
        }

        async function runImport(
            cfg: LegacySourceConfig,
            items: ImportItem[],
            store: MemStore,
            stopAt?: number,
        ): Promise<string> {
            const source = createLegacyReader(cfg);
            const ctrl = new AbortController();
            const progress: Progress[] = [];
            const job = new DataMigrationJob(
                {
                    source,
                    target: {
                        firstTime: () => Promise.resolve(undefined),
                        write: lines => v3.write(lines),
                        busy: () => false,
                    },
                    store,
                    now: () => Date.now(),
                    sleep: ms => new Promise(r => setTimeout(r, Math.min(ms, 50))),
                    log: { info: () => undefined, warn: () => undefined, debug: () => undefined },
                    onProgress: p => {
                        progress.push(p);
                        if (stopAt !== undefined && p.points >= stopAt) {
                            ctrl.abort();
                        }
                    },
                },
                {
                    stateFilter: '',
                    untilFirstV3: true,
                    keepOldTags: true,
                    batchSize: 50,
                    pauseMs: 0,
                    influxdbInstance: '',
                },
            );
            try {
                return await job.run(items, 'k', ctrl.signal);
            } finally {
                source.close();
            }
        }

        async function count(id: string): Promise<{ n: number; loc: string | null }> {
            const rows = await v3.querySql<{ n: number; loc: string | null }>(
                'SELECT COUNT(*) AS n, MAX(location) AS loc FROM numeric_states WHERE state_id = $id',
                { id },
            );
            return { n: Number(rows[0]?.n ?? 0), loc: rows[0]?.loc ?? null };
        }

        it('InfluxDB 1.x → 3: all points, tags translated by the rules, resumable', async function () {
            if (!HAS_V1) {
                this.skip();
            }
            const store = new MemStore();
            const id = `iob.${RUN}.v1temp`;
            // stop in the middle, then resume
            assert.equal(await runImport(v1Config(), [item(id, `${RUN}.temp`)], store, 50), 'stopped');
            const half = await count(id);
            assert.ok(half.n > 0 && half.n < 200, `partial import (${half.n})`);
            assert.equal(await runImport(v1Config(), [item(id, `${RUN}.temp`)], store), 'finished');
            const all = await count(id);
            assert.equal(all.n, 200, 'every point exactly once');
            assert.equal(all.loc, 'KOTLOWNIA', 'room=kotlownia → Location=KOTLOWNIA');
        });

        it('InfluxDB 2.x → 3: both metadata layouts', async function () {
            if (!HAS_V2) {
                this.skip();
            }
            const store = new MemStore();
            const a = `iob.${RUN}.v2temp`;
            const b = `iob.${RUN}.v2tagged`;
            assert.equal(
                await runImport(v2Config(), [item(a, `${RUN}.temp`), item(b, `${RUN}.tagged`)], store),
                'finished',
            );
            assert.deepEqual(await count(a), { n: 100, loc: 'SALON' });
            assert.equal((await count(b)).n, 5);
            assert.equal(store.cp?.totals.points, 105);
            // a second run of the same import changes nothing (idempotent writes)
            store.cp = undefined;
            await runImport(v2Config(), [item(a, `${RUN}.temp`)], store);
            assert.equal((await count(a)).n, 100);
        });
    });
});
