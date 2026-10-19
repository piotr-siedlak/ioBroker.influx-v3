/**
 * Child process for the kill -9 test: writes records in disk buffer mode to an unreachable
 * server, reports "SPOOLED <n>" when all batches are on disk and then waits to be killed.
 *
 * usage: tsx spoolWriter.ts <spoolDir> <records>
 */
import fs from 'node:fs';
import { InfluxV3Client } from '../../../src/lib/influxClient';
import { Spool } from '../../../src/lib/spool';
import { WriteQueue } from '../../../src/lib/writeQueue';

const [dir, countArg] = process.argv.slice(2);
const count = Number(countArg);

const client = new InfluxV3Client({
    url: 'http://127.0.0.1:1', // nothing listens here
    token: '',
    database: 'unused',
    edition: 'core',
    timeoutMs: 1000,
    rejectUnauthorized: true,
    gzip: false,
    noSync: false,
    maxSockets: 1,
});
const queue = new WriteQueue(
    {
        writeMode: 'batched',
        batchMaxLines: 100,
        batchMaxBytes: 1024 * 1024,
        flushIntervalMs: 50,
        maxConcurrentWrites: 1,
        retryBaseDelayMs: 60000,
        retryMaxDelayMs: 60000,
        authRetryMs: 60000,
        bufferMode: 'disk',
        bufferMaxRecords: 1000000,
        bufferMaxBytes: 64 * 1024 * 1024,
        spoolMaxBytes: 64 * 1024 * 1024,
        bufferMaxAgeMs: 3600000,
        overflowPolicy: 'dropOldest',
        persistOnShutdown: true,
    },
    client,
    {
        written: () => undefined,
        rejected: () => undefined,
        dropped: () => undefined,
        connection: () => undefined,
        log: () => undefined,
    },
    new Spool(dir, true),
);

void (async () => {
    await queue.start();
    const base = 1792418400000;
    for (let i = 0; i < count; i++) {
        queue.push(`kill9,state_id=kill.0.s value=${i} ${base + i}`);
    }
    const files = (): number =>
        fs
            .readdirSync(dir)
            .filter(f => f.endsWith('.json'))
            .reduce((n, f) => n + Number(/-(\d+)\.json$/.exec(f)?.[1] ?? 0), 0);
    const timer = setInterval(() => {
        if (files() >= count) {
            clearInterval(timer);
            process.stdout.write(`SPOOLED ${files()}\n`);
            setInterval(() => undefined, 1000); // keep running until killed
        }
    }, 20);
})();
