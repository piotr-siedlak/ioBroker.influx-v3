/**
 * Load test of the write path (FilterPipeline → tags → line protocol → WriteQueue → InfluxDB 3).
 *
 *   eval "$(scripts/influx3-test-server.sh start)"
 *   npx tsx scripts/loadtest.ts [states=100,1000,10000] [seconds=20]
 *
 * For every state count two scenarios run:
 *   constant – every state changes once every 10 s (evenly spread)
 *   burst    – all states change at the same moment, 5 bursts 2 s apart
 * Measured: written/rejected/dropped records, throughput, lag (state.ts → server acknowledgement)
 * p50/p95/p99, event-loop delay p99, RSS and CPU time. Finally the rebuild time of a disk spool.
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { monitorEventLoopDelay } from 'node:perf_hooks';
import { DEFAULT_INSTANCE_CONFIG } from '../src/lib/config';
import { FilterPipeline, type PreparedState } from '../src/lib/filterPipeline';
import { InfluxV3Client, type WriteResult } from '../src/lib/influxClient';
import { serialize } from '../src/lib/lineProtocol';
import { Spool } from '../src/lib/spool';
import { resolveTags } from '../src/lib/tagResolver';
import { type QueueOptions, WriteQueue } from '../src/lib/writeQueue';

const URL = process.env.INFLUX3_URL ?? '';
const TOKEN = process.env.INFLUX3_TOKEN ?? '';
const counts = (process.argv[2] ?? '100,1000,10000').split(',').map(Number);
const seconds = Number(process.argv[3] ?? 20);

if (!URL) {
    console.error('INFLUX3_URL not set - start the test server first (scripts/influx3-test-server.sh start)');
    process.exit(1);
}

const queueOpts: QueueOptions = {
    writeMode: 'batched',
    batchMaxLines: 5000,
    batchMaxBytes: 1024 * 1024,
    flushIntervalMs: 1000,
    maxConcurrentWrites: 1,
    retryBaseDelayMs: 1000,
    retryMaxDelayMs: 60000,
    authRetryMs: 300000,
    bufferMode: 'memory',
    bufferMaxRecords: 200000,
    bufferMaxBytes: 32 * 1024 * 1024,
    spoolMaxBytes: 256 * 1024 * 1024,
    bufferMaxAgeMs: 7 * 86400000,
    overflowPolicy: 'dropOldest',
    persistOnShutdown: false,
};

function percentile(sorted: number[], p: number): number {
    if (!sorted.length) {
        return 0;
    }
    return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
}

interface Result {
    scenario: string;
    states: number;
    generated: number;
    written: number;
    rejected: number;
    dropped: number;
    throughput: number;
    lagP50: number;
    lagP95: number;
    lagP99: number;
    loopP99: number;
    rssMb: number;
    cpuMs: number;
}

async function run(states: number, scenario: 'constant' | 'burst', db: string): Promise<Result> {
    const client = new InfluxV3Client({
        url: URL,
        token: TOKEN,
        database: db,
        edition: 'auto',
        timeoutMs: 30000,
        rejectUnauthorized: true,
        gzip: true,
        noSync: false,
        maxSockets: 3,
    });
    const lags: number[] = [];
    let written = 0;
    let rejected = 0;
    let dropped = 0;
    const target = {
        write: async (lines: string[]): Promise<WriteResult> => {
            const r = await client.write(lines);
            const now = Date.now();
            if (r.kind === 'ok') {
                for (const l of lines) {
                    lags.push(now - Number(l.slice(l.lastIndexOf(' ') + 1)));
                }
            }
            return r;
        },
    };
    const queue = new WriteQueue(queueOpts, target, {
        written: n => (written += n),
        rejected: items => (rejected += items.length),
        dropped: n => (dropped += n),
        connection: () => undefined,
        log: () => undefined,
    });
    await queue.start();

    const tagSettings = {
        instance: { ...DEFAULT_INSTANCE_CONFIG, globalTags: [{ key: 'site', value: 'loadtest' }] },
        state: {
            customTags: [{ key: 'device', value: '{id.segment[2]}' }],
            excludeTags: [],
            useGlobalTags: true,
            useAutoTags: true,
            allowSensitiveTags: false,
        },
        reserved: new Set(['time', 'state_id', 'value', 'ack', 'q', 'from']),
    };
    const pipelines: FilterPipeline[] = [];
    for (let i = 0; i < states; i++) {
        const id = `load.0.dev${i % 100}.state${i}`;
        const tags = resolveTags(
            {
                id,
                storeId: id,
                common: { type: 'number', unit: '°C' },
                native: {},
                rooms: [`room${i % 20}`],
                functions: ['Heating'],
                language: 'en',
            },
            tagSettings,
        ).tags;
        tags.state_id = id;
        pipelines.push(
            new FilterPipeline(
                {
                    ...DEFAULT_INSTANCE_CONFIG,
                    ignoreZero: false,
                    ignoreBelowNumber: null,
                    ignoreAboveNumber: null,
                    changesOnly: true,
                },
                (s: PreparedState) => {
                    queue.push(
                        serialize({
                            table: 'load_numbers',
                            tags,
                            fields: {
                                value: { type: 'float', value: s.val as number },
                                ack: { type: 'boolean', value: s.ack },
                                q: { type: 'integer', value: 0 },
                            },
                            ts: s.ts,
                        }),
                    );
                },
                'system.adapter.influx_v3.0',
            ),
        );
    }

    const loop = monitorEventLoopDelay({ resolution: 10 });
    loop.enable();
    const cpu0 = process.cpuUsage();
    let generated = 0;
    let counter = 0;
    const change = (i: number): void => {
        generated++;
        pipelines[i].process({ val: ++counter, ts: Date.now(), ack: true, q: 0, from: 'system.adapter.load.0' });
    };
    const start = Date.now();
    if (scenario === 'constant') {
        // every state once per 10 s → states/10 changes per second, spread over 100 ms ticks
        const perTick = Math.max(1, Math.round(states / 100));
        let next = 0;
        await new Promise<void>(resolve => {
            const t = setInterval(() => {
                for (let k = 0; k < perTick; k++) {
                    change(next);
                    next = (next + 1) % states;
                }
                if (Date.now() - start >= seconds * 1000) {
                    clearInterval(t);
                    resolve();
                }
            }, 100);
        });
    } else {
        for (let b = 0; b < 5; b++) {
            for (let i = 0; i < states; i++) {
                change(i);
            }
            await new Promise(r => setTimeout(r, 2000));
        }
    }
    await queue.flush(120000);
    const elapsed = (Date.now() - start) / 1000;
    const cpu = process.cpuUsage(cpu0);
    loop.disable();
    pipelines.forEach(p => p.stop());
    await queue.shutdown(1000);
    client.close();
    lags.sort((a, b) => a - b);
    return {
        scenario,
        states,
        generated,
        written,
        rejected,
        dropped,
        throughput: Math.round(written / elapsed),
        lagP50: percentile(lags, 50),
        lagP95: percentile(lags, 95),
        lagP99: percentile(lags, 99),
        loopP99: Math.round(loop.percentile(99) / 1e6),
        rssMb: Math.round(process.memoryUsage().rss / 1024 / 1024),
        cpuMs: Math.round((cpu.user + cpu.system) / 1000),
    };
}

async function spoolRebuild(
    records: number,
): Promise<{ records: number; files: number; startMs: number; drainMs: number }> {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'influx_v3-load-spool-'));
    const spool = new Spool(dir, false);
    await spool.init();
    let seq = 1;
    for (let i = 0; i < records; i += 5000) {
        const lines = Array.from(
            { length: Math.min(5000, records - i) },
            (_, k) => `load_spool,state_id=s${(i + k) % 1000} value=${i + k} ${1792418400000 + i + k}`,
        );
        await spool.write(seq++, lines, Date.now());
    }
    const client = new InfluxV3Client({
        url: URL,
        token: TOKEN,
        database: 'loadtest',
        edition: 'auto',
        timeoutMs: 30000,
        rejectUnauthorized: true,
        gzip: true,
        noSync: false,
        maxSockets: 2,
    });
    let written = 0;
    const q = new WriteQueue(
        { ...queueOpts, bufferMode: 'disk' },
        client,
        {
            written: n => (written += n),
            rejected: () => undefined,
            dropped: () => undefined,
            connection: () => undefined,
            log: () => undefined,
        },
        new Spool(dir, false),
    );
    const t0 = Date.now();
    await q.start();
    const startMs = Date.now() - t0;
    while (written < records) {
        await new Promise(r => setTimeout(r, 20));
    }
    const drainMs = Date.now() - t0;
    await q.shutdown(500);
    client.close();
    fs.rmSync(dir, { recursive: true, force: true });
    return { records, files: seq - 1, startMs, drainMs };
}

void (async () => {
    console.log(
        `Node ${process.version}, ${os.cpus()[0]?.model ?? 'cpu'} ×${os.cpus().length}, ${Math.round(os.totalmem() / 2 ** 30)} GiB, ${os.platform()} ${os.arch()}`,
    );
    const results: Result[] = [];
    for (const n of counts) {
        for (const scenario of ['constant', 'burst'] as const) {
            const r = await run(n, scenario, 'loadtest');
            results.push(r);
            console.log(JSON.stringify(r));
        }
    }
    console.log(
        '\n| scenario | states | generated | written | rejected | dropped | records/s | lag p50 / p95 / p99 (ms) | event loop p99 (ms) | RSS (MB) | CPU (ms) |',
    );
    console.log('| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |');
    for (const r of results) {
        console.log(
            `| ${r.scenario} | ${r.states} | ${r.generated} | ${r.written} | ${r.rejected} | ${r.dropped} | ${r.throughput} | ${r.lagP50} / ${r.lagP95} / ${r.lagP99} | ${r.loopP99} | ${r.rssMb} | ${r.cpuMs} |`,
        );
    }
    const s = await spoolRebuild(100000);
    console.log(
        `\nSpool rebuild: ${s.records} records in ${s.files} files → start() ${s.startMs} ms, all written after ${s.drainMs} ms`,
    );
})();
