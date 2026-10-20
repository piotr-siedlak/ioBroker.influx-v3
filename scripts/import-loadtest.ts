/**
 * Load test of the history import (ADR-0006, extension 7) against real servers.
 *
 *   eval "$(scripts/legacy-influx-test-server.sh start)"      # InfluxDB 2.7 as the old database
 *   docker run -d --name iob-influx3-load -p 127.0.0.1:18290:8181 influxdb:3-core \
 *       influxdb3 serve --node-id load --object-store memory --without-auth --wal-files-per-snapshot 10
 *   INFLUX3_LOAD_URL=http://127.0.0.1:18290 npx tsx scripts/import-loadtest.ts [states=548] [days=3] [stepSec=300] [modes=ts]
 *
 * `--wal-files-per-snapshot 10` makes InfluxDB 3 Core persist Parquet files every ~10 s, like a server that runs for
 * hours. The old database is seeded like the classic adapter writes (one measurement per state, ~10 % switches,
 * custom tags, ack/q/from fields), then imported time-sliced (`ts`: one pass over the time axis for all states) and/or
 * state after state (`sm`: like versions before extension 7 - slow, use on a fresh server). Reported: duration,
 * throughput, Parquet files per table and per hour, and whether queries over one hour / one day stay below Core's
 * default file limit (432).
 */
import { gzipSync } from 'node:zlib';
import http from 'node:http';
import {
    type Checkpoint,
    type CheckpointStore,
    DataMigrationJob,
    type ImportItem,
    type JobOutcome,
} from '../src/lib/dataMigration';
import { InfluxV3Client } from '../src/lib/influxClient';
import { createLegacyReader, type LegacyPoint, type LegacySourceConfig } from '../src/lib/legacySource';

const env = process.env;
const V3_URL = env.INFLUX3_LOAD_URL ?? '';
const states = Number(process.argv[2] ?? 548);
const days = Number(process.argv[3] ?? 3);
const stepSec = Number(process.argv[4] ?? 300);
const modes = (process.argv[5] ?? 'ts').split(',');
const DB = 'iobload';
const RUN = `lt${Date.now().toString(36)}`;
const END = Date.UTC(2025, 5, 1);
const START = END - days * 86_400_000;

if (!V3_URL || !env.INFLUX2_URL || !env.INFLUX2_TOKEN) {
    console.error('INFLUX3_LOAD_URL and the InfluxDB 2.7 test server (INFLUX2_*) are needed - see the header');
    process.exit(1);
}

const source: LegacySourceConfig = {
    version: '2.x',
    url: env.INFLUX2_URL,
    database: '',
    retentionPolicy: '',
    user: '',
    password: '',
    org: env.INFLUX2_ORG ?? 'iob',
    bucket: env.INFLUX2_BUCKET ?? 'iobroker',
    token: env.INFLUX2_TOKEN,
    rejectUnauthorized: true,
    timeoutMs: 120_000,
    maxBytes: 64 * 1024 * 1024,
};

function post(url: string, body: Buffer, headers: Record<string, string>): Promise<number> {
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

const isSwitch = (i: number): boolean => i % 10 === 9;
const ROOMS = ['KOTLOWNIA', 'SALON', 'PIWNICA', 'PODDASZE', 'GARAZ'];

async function seed(): Promise<number> {
    const perState = Math.floor((days * 86_400) / stepSec);
    let lines: string[] = [];
    let total = 0;
    const flush = async (): Promise<void> => {
        const status = await post(
            `${env.INFLUX2_URL}/api/v2/write?org=${source.org}&bucket=${source.bucket}&precision=ms`,
            gzipSync(lines.join('\n')),
            { Authorization: `Token ${env.INFLUX2_TOKEN}`, 'Content-Encoding': 'gzip', 'Content-Type': 'text/plain' },
        );
        if (status !== 204) {
            throw new Error(`seed: HTTP ${status}`);
        }
        total += lines.length;
        lines = [];
    };
    // time-major seeding (like the classic adapter logged it live)
    for (let k = 0; k < perState; k++) {
        for (let i = 0; i < states; i++) {
            const ts = START + k * stepSec * 1000 + i * 100;
            const value = isSwitch(i) ? String(k % 2 === 0) : (20 + Math.sin(k / 30 + i) * 5).toFixed(2);
            lines.push(
                `${RUN}.s${i},Location=${ROOMS[i % ROOMS.length]},Device=PC value=${value},ack=true,from="system.adapter.x.0",q=0 ${ts}`,
            );
            if (lines.length >= 50_000) {
                await flush();
            }
        }
    }
    if (lines.length) {
        await flush();
    }
    return total;
}

class MemStore implements CheckpointStore {
    cp?: Checkpoint;
    load(): Promise<Checkpoint | undefined> {
        return Promise.resolve(this.cp);
    }
    save(cp: Checkpoint): Promise<void> {
        this.cp = cp;
        return Promise.resolve();
    }
}

function items(prefix: string): ImportItem[] {
    return Array.from({ length: states }, (_, i) => {
        const table = isSwitch(i) ? `${prefix}_bool` : `${prefix}_num`;
        return {
            id: `${RUN}.s${i}`,
            measurement: `${RUN}.s${i}`,
            table,
            convert: (pts: LegacyPoint[]) => ({
                lines: pts.map(
                    p =>
                        `${table},state_id=${RUN}.s${i},location=${p.tags.Location ?? ''} value=${isSwitch(i) ? String(p.value) : Number(p.value)},ack=${p.ack ?? false} ${p.ts}`,
                ),
                skipped: 0,
            }),
        };
    });
}

async function importWith(list: ImportItem[][], v3: InfluxV3Client): Promise<{ ms: number; points: number }> {
    const began = Date.now();
    let points = 0;
    for (const group of list) {
        const reader = createLegacyReader(source);
        const store = new MemStore();
        const job = new DataMigrationJob(
            {
                source: reader,
                target: { write: lines => v3.write(lines, 60_000), busy: () => false },
                store,
                now: () => Date.now(),
                sleep: ms => new Promise(r => setTimeout(r, ms)),
                log: { info: () => undefined, warn: m => console.warn(m), debug: () => undefined },
                onProgress: () => undefined,
                liveStart: () => undefined,
            },
            {
                stateFilter: '',
                untilFirstV3: false,
                keepOldTags: false,
                batchSize: 50_000,
                parallelWrites: 2,
                pauseMs: 0,
                influxdbInstance: '',
                from: START,
                to: END,
            },
        );
        const outcome: JobOutcome = await job.run(group, 'load', new AbortController().signal);
        reader.close();
        if (outcome !== 'finished') {
            throw new Error(`import ${outcome}`);
        }
        points += store.cp?.totals.points ?? 0;
    }
    return { ms: Date.now() - began, points };
}

async function filesPerTable(v3: InfluxV3Client): Promise<Map<string, number>> {
    const rows = await v3.querySql<{ table_name: string; files: number }>(
        'SELECT table_name, COUNT(*) AS files FROM system.parquet_files GROUP BY table_name',
    );
    return new Map(rows.map(r => [r.table_name, Number(r.files)]));
}

async function rangeQuery(v3: InfluxV3Client, table: string, ms: number): Promise<string> {
    const from = START + 86_400_000;
    try {
        const rows = await v3.querySql<{ n: number }>(
            `SELECT COUNT(*) AS n FROM ${table} WHERE time >= to_timestamp_millis(${from}) AND time < to_timestamp_millis(${from + ms})`,
        );
        return `${rows[0]?.n} rows`;
    } catch (e) {
        return `ERROR ${(e instanceof Error ? e.message : String(e)).slice(0, 80)}`;
    }
}

async function filesInHour(v3: InfluxV3Client, table: string): Promise<number> {
    const from = (START + 86_400_000) * 1e6;
    const rows = await v3.querySql<{ n: number }>(
        `SELECT COUNT(*) AS n FROM system.parquet_files WHERE table_name = '${table}' AND max_time >= ${from} AND min_time < ${from + 3.6e12}`,
    );
    return Number(rows[0]?.n ?? 0);
}

async function main(): Promise<void> {
    const v3 = new InfluxV3Client({
        url: V3_URL,
        token: '',
        database: DB,
        edition: 'core',
        timeoutMs: 60_000,
        rejectUnauthorized: true,
        gzip: true,
        noSync: false,
        maxSockets: 2,
    });
    console.log(`seeding ${states} states × ${days} day(s), one point every ${stepSec} s …`);
    const t0 = Date.now();
    const seeded = await seed();
    console.log(`seeded ${seeded} points in ${((Date.now() - t0) / 1000).toFixed(1)} s`);

    const name = (m: string): string => (m === 'sm' ? 'state after state' : 'time-sliced      ');
    const mb = (n: number): number => Math.round(n / 1024 / 1024);
    for (const mode of modes) {
        global.gc?.();
        const base = process.memoryUsage().rss;
        let peak = base;
        const sampler = setInterval(() => (peak = Math.max(peak, process.memoryUsage().rss)), 200);
        const r = await importWith(mode === 'sm' ? items('sm').map(i => [i]) : [items('ts')], v3);
        clearInterval(sampler);
        console.log(
            `${name(mode)}: ${r.points} points in ${(r.ms / 1000).toFixed(1)} s (${Math.round(r.points / (r.ms / 1000))} points/s), RSS before ${mb(base)} MB, peak ${mb(peak)} MB`,
        );
    }
    console.log('waiting 40 s for InfluxDB 3 to persist …');
    await new Promise(r => setTimeout(r, 40_000));
    const files = await filesPerTable(v3);
    const blocks = Math.ceil((days * 86_400) / 600);
    for (const mode of modes) {
        const n = (files.get(`${mode}_num`) ?? 0) + (files.get(`${mode}_bool`) ?? 0);
        console.log(
            `${name(mode)}: ${n} Parquet files (2 tables × ${blocks} ten-minute blocks), ${await filesInHour(v3, `${mode}_num`)} file(s) per hour of ${mode}_num; query 1 h: ${await rangeQuery(v3, `${mode}_num`, 3_600_000)}; 1 day: ${await rangeQuery(v3, `${mode}_num`, 86_400_000)}`,
        );
    }
    v3.close();
}

main().catch(e => {
    console.error(e);
    process.exit(1);
});
