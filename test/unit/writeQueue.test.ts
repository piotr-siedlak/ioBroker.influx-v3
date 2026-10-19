import { strict as assert } from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import type { WriteResult } from '../../src/lib/influxClient';
import { Spool } from '../../src/lib/spool';
import { backoffDelay, type QueueOptions, type Timers, WriteQueue } from '../../src/lib/writeQueue';

class FakeTimers implements Timers {
    t = 0;
    private seq = 0;
    private readonly timers = new Map<number, { at: number; fn: () => void }>();
    rnd = 0.5;
    now(): number {
        return this.t;
    }
    setTimeout(fn: () => void, ms: number): unknown {
        const id = ++this.seq;
        this.timers.set(id, { at: this.t + ms, fn });
        return id;
    }
    clearTimeout(h: unknown): void {
        this.timers.delete(h as number);
    }
    random(): number {
        return this.rnd;
    }
    async advance(ms: number): Promise<void> {
        const end = this.t + ms;
        for (;;) {
            let next: [number, { at: number; fn: () => void }] | undefined;
            for (const e of this.timers) {
                if (e[1].at <= end && (!next || e[1].at < next[1].at)) {
                    next = e;
                }
            }
            if (!next) {
                break;
            }
            this.timers.delete(next[0]);
            this.t = next[1].at;
            next[1].fn();
            await settle();
        }
        this.t = end;
        await settle();
    }
}

/** waits (real time) until a condition holds - needed where real file system I/O is involved */
async function waitFor(cond: () => boolean, timeoutMs = 2000): Promise<void> {
    const start = Date.now();
    while (!cond()) {
        if (Date.now() - start > timeoutMs) {
            throw new Error('condition not reached in time');
        }
        await new Promise(r => setTimeout(r, 5));
    }
}

async function settle(): Promise<void> {
    for (let i = 0; i < 20; i++) {
        await new Promise(r => setImmediate(r));
    }
}

class FakeTarget {
    calls: string[][] = [];
    results: WriteResult[] = [];
    gate?: Promise<void>;
    async write(lines: string[]): Promise<WriteResult> {
        this.calls.push(lines);
        if (this.gate) {
            await this.gate;
        }
        return this.results.shift() ?? { kind: 'ok' };
    }
}

const opts = (over: Partial<QueueOptions> = {}): QueueOptions => ({
    writeMode: 'batched',
    batchMaxLines: 3,
    batchMaxBytes: 1024 * 1024,
    flushIntervalMs: 1000,
    maxConcurrentWrites: 1,
    retryBaseDelayMs: 1000,
    retryMaxDelayMs: 8000,
    authRetryMs: 60000,
    bufferMode: 'memory',
    bufferMaxRecords: 1000,
    bufferMaxBytes: 1024 * 1024,
    spoolMaxBytes: 1024 * 1024,
    bufferMaxAgeMs: 3600_000,
    overflowPolicy: 'dropOldest',
    persistOnShutdown: true,
    ...over,
});

function make(over: Partial<QueueOptions> = {}, spool?: Spool) {
    const timers = new FakeTimers();
    const target = new FakeTarget();
    const ev = {
        written: 0,
        rejected: [] as { line: string; message: string }[],
        dropped: [] as [number, string][],
        conn: [] as boolean[],
        logs: [] as string[],
    };
    const q = new WriteQueue(
        opts(over),
        target,
        {
            written: n => (ev.written += n),
            rejected: items => ev.rejected.push(...items),
            dropped: (n, r) => ev.dropped.push([n, r]),
            connection: ok => ev.conn.push(ok),
            log: (l, m) => ev.logs.push(`${l}: ${m}`),
        },
        spool,
        timers,
    );
    return { q, timers, target, ev };
}

describe('WriteQueue', () => {
    it('backoffDelay grows exponentially with jitter and a cap', () => {
        assert.equal(
            backoffDelay(1, 1000, 60000, () => 0),
            1000,
        );
        assert.equal(
            backoffDelay(1, 1000, 60000, () => 0.999),
            1000,
        );
        assert.equal(
            backoffDelay(3, 1000, 60000, () => 1),
            4000,
        );
        assert.equal(
            backoffDelay(3, 1000, 60000, () => 0),
            1000,
        );
        assert.equal(
            backoffDelay(20, 1000, 60000, () => 1),
            60000,
        );
        const d = backoffDelay(4, 1000, 60000, () => 0.5);
        assert.ok(d >= 1000 && d <= 8000);
    });

    it('flushes by line count and by interval', async () => {
        const { q, timers, target, ev } = make();
        q.push('a');
        q.push('b');
        assert.equal(target.calls.length, 0);
        q.push('c');
        await settle();
        assert.deepEqual(target.calls, [['a', 'b', 'c']]);
        q.push('d');
        await timers.advance(999);
        assert.equal(target.calls.length, 1);
        await timers.advance(1);
        assert.deepEqual(target.calls[1], ['d']);
        assert.equal(ev.written, 4);
        assert.deepEqual(ev.conn, [true]);
    });

    it('flushes by byte size', async () => {
        const { q, target } = make({ batchMaxLines: 1000, batchMaxBytes: 4096 });
        q.push('x'.repeat(3000));
        await settle();
        assert.equal(target.calls.length, 0);
        q.push('y'.repeat(2000));
        await settle();
        assert.equal(target.calls.length, 1);
    });

    it('direct mode sends every record', async () => {
        const { q, target } = make({ writeMode: 'direct' });
        q.push('a');
        await settle();
        q.push('b');
        await settle();
        assert.deepEqual(target.calls, [['a'], ['b']]);
    });

    it('partial write: accepted lines count as written, rejected lines are reported, no retry', async () => {
        const { q, target, ev } = make();
        target.results.push({
            kind: 'partial',
            rejected: [{ line: 2, message: 'invalid column type' }],
            message: 'partial write',
        });
        q.push('a');
        q.push('b');
        q.push('c');
        await settle();
        assert.equal(ev.written, 2);
        assert.deepEqual(ev.rejected, [{ line: 'b', message: 'invalid column type' }]);
        assert.equal(target.calls.length, 1);
        assert.equal(q.stats().queuedRecords, 0);
    });

    it('invalid batch (4xx) is rejected, never retried', async () => {
        const { q, timers, target, ev } = make();
        target.results.push({ kind: 'invalid', message: 'bad' });
        ['a', 'b', 'c'].forEach(l => q.push(l));
        await settle();
        await timers.advance(100_000);
        assert.equal(target.calls.length, 1);
        assert.equal(ev.rejected.length, 3);
    });

    it('transient errors are retried with backoff; order is kept', async () => {
        const { q, timers, target, ev } = make();
        target.results.push(
            { kind: 'transient', status: 503, message: 'unavailable' },
            { kind: 'transient', message: 'ECONNREFUSED' },
        );
        ['a', 'b', 'c'].forEach(l => q.push(l));
        await settle();
        assert.equal(target.calls.length, 1);
        q.push('d');
        q.push('e');
        q.push('f'); // second batch waits behind the first
        await settle();
        assert.equal(target.calls.length, 1);
        assert.equal(q.stats().circuit, 'open');
        await timers.advance(1000); // first retry (jitter 0.5 → 1000 ms for failure 1)
        assert.equal(target.calls.length, 2);
        await timers.advance(2000); // second retry after the second failure
        assert.ok(target.calls.length >= 4);
        assert.deepEqual(target.calls[2], ['a', 'b', 'c']);
        assert.deepEqual(target.calls[3], ['d', 'e', 'f']);
        assert.equal(ev.written, 6);
        assert.deepEqual(ev.conn, [false, false, true]);
    });

    it('honours Retry-After (429)', async () => {
        const { q, timers, target } = make();
        target.results.push({ kind: 'transient', status: 429, message: 'slow down', retryAfterMs: 5000 });
        ['a', 'b', 'c'].forEach(l => q.push(l));
        await settle();
        await timers.advance(4999);
        assert.equal(target.calls.length, 1);
        await timers.advance(1);
        assert.equal(target.calls.length, 2);
    });

    it('authentication errors keep the data and wait authRetryMs', async () => {
        const { q, timers, target, ev } = make();
        target.results.push({ kind: 'auth', status: 401, message: 'not authenticated' });
        ['a', 'b', 'c'].forEach(l => q.push(l));
        await settle();
        await timers.advance(59_999);
        assert.equal(target.calls.length, 1);
        assert.equal(q.stats().queuedRecords, 3);
        await timers.advance(1);
        assert.equal(target.calls.length, 2);
        assert.equal(ev.written, 3);
        assert.ok(ev.logs.some(l => l.startsWith('error') && l.includes('401')));
    });

    it('413 splits the batch until it fits; a single oversized line is rejected', async () => {
        const { q, target, ev } = make({ batchMaxLines: 4 });
        target.results.push({ kind: 'tooLarge', message: '413' }, { kind: 'ok' }, { kind: 'tooLarge', message: '413' });
        ['a', 'b', 'c', 'd'].forEach(l => q.push(l));
        await settle();
        assert.deepEqual(target.calls, [['a', 'b', 'c', 'd'], ['a', 'b'], ['c', 'd'], ['c'], ['d']]);
        assert.equal(ev.written, 4);
        const r = make({ batchMaxLines: 1 });
        r.target.results.push({ kind: 'tooLarge', message: '413' });
        r.q.push('huge');
        await settle();
        assert.equal(r.ev.rejected.length, 1);
    });

    it('overflow dropOldest drops whole old batches and counts them', async () => {
        const { q, target, ev } = make({ bufferMaxRecords: 5, batchMaxLines: 2 });
        target.gate = new Promise(() => undefined); // server hangs: nothing completes
        for (let i = 0; i < 9; i++) {
            q.push(`r${i}`);
        }
        await settle();
        const dropped = ev.dropped.reduce((s, [n]) => s + n, 0);
        assert.ok(dropped >= 2, `dropped ${dropped}`);
        assert.ok(ev.dropped.every(([, r]) => r.includes('dropOldest')));
        assert.ok(q.stats().memoryRecords <= 5);
    });

    it('overflow dropNewest refuses new records', async () => {
        const { q, target, ev } = make({ bufferMaxRecords: 3, batchMaxLines: 10, overflowPolicy: 'dropNewest' });
        target.gate = new Promise(() => undefined);
        assert.equal(q.push('a'), true);
        assert.equal(q.push('b'), true);
        assert.equal(q.push('c'), true);
        assert.equal(q.push('d'), false);
        assert.deepEqual(ev.dropped, [[1, 'buffer full (dropNewest)']]);
    });

    it('drops batches older than the maximum age', async () => {
        const { q, timers, target, ev } = make({
            bufferMaxAgeMs: 10_000,
            retryMaxDelayMs: 1000,
            retryBaseDelayMs: 1000,
        });
        target.results = Array.from({ length: 50 }, () => ({ kind: 'transient' as const, message: 'down' }));
        ['a', 'b', 'c'].forEach(l => q.push(l));
        await settle();
        await timers.advance(15_000);
        assert.ok(ev.dropped.some(([n, r]) => n === 3 && r.includes('maximum buffer age')));
        assert.equal(q.stats().queuedRecords, 0);
    });

    it('flush() waits until everything is sent', async () => {
        const { q, target } = make({ batchMaxLines: 100, flushIntervalMs: 60_000 });
        q.push('a');
        const ok = await q.flush(1000);
        assert.equal(ok, true);
        assert.deepEqual(target.calls, [['a']]);
    });

    describe('with disk spool', () => {
        let dir: string;
        beforeEach(() => {
            dir = fs.mkdtempSync(path.join(os.tmpdir(), 'influx_v3-spool-'));
        });
        afterEach(() => {
            fs.rmSync(dir, { recursive: true, force: true });
        });

        it('memory mode persists unsent data on shutdown and replays it on the next start', async () => {
            const spool = new Spool(dir, true);
            const a = make({ authRetryMs: 3600_000 }, spool);
            await a.q.start();
            a.target.results.push({ kind: 'auth', status: 401, message: 'nope' });
            ['a', 'b', 'c', 'd'].forEach(l => a.q.push(l));
            await settle();
            const res = await a.q.shutdown(100);
            assert.deepEqual(res, { persisted: 4, lost: 0 });
            assert.equal(fs.readdirSync(dir).filter(f => f.endsWith('.json')).length, 2);

            const b = make({}, new Spool(dir, true));
            const recovered = await b.q.start();
            assert.equal(recovered, 4);
            const jsonFiles = (): number => fs.readdirSync(dir).filter(f => f.endsWith('.json')).length;
            await waitFor(() => b.ev.written === 4 && jsonFiles() === 0);
            assert.deepEqual(b.target.calls, [['a', 'b', 'c'], ['d']]);
            assert.equal(b.ev.written, 4);
            assert.equal(
                fs.readdirSync(dir).filter(f => f.endsWith('.json')).length,
                0,
                'sent batches are removed from disk',
            );
        });

        it('disk mode writes every batch before sending (crash safe) and deletes it after success', async () => {
            const spool = new Spool(dir, false);
            const a = make({ bufferMode: 'disk' }, spool);
            await a.q.start();
            let release: () => void = () => undefined;
            a.target.gate = new Promise<void>(r => (release = r));
            ['a', 'b', 'c'].forEach(l => a.q.push(l));
            await waitFor(() => a.target.calls.length === 1);
            const files = fs.readdirSync(dir).filter(f => f.endsWith('.json'));
            assert.equal(files.length, 1, 'batch is on disk while in flight');
            assert.match(files[0], /^batch-\d{12}-3\.json$/);
            // simulate a crash: a new queue on the same directory recovers the batch
            const b = make({ bufferMode: 'disk' }, new Spool(dir, false));
            assert.equal(await b.q.start(), 3);
            release();
            await waitFor(() => a.ev.written === 3);
            await waitFor(() => b.target.calls.length === 1);
        });

        it('quarantines corrupt files and ignores leftovers of interrupted writes', async () => {
            fs.writeFileSync(path.join(dir, 'batch-000000000001-2.json'), '{broken');
            fs.writeFileSync(path.join(dir, 'batch-000000000002-1.json.tmp'), 'partial');
            fs.writeFileSync(
                path.join(dir, 'batch-000000000003-1.json'),
                JSON.stringify({ v: 1, createdAt: 0, lines: ['ok'] }),
            );
            const b = make({ bufferMaxAgeMs: Number.MAX_SAFE_INTEGER }, new Spool(dir, false));
            assert.equal(await b.q.start(), 3);
            await waitFor(() => b.target.calls.length === 1 && b.ev.dropped.length === 1 && b.ev.written === 1);
            assert.deepEqual(b.target.calls, [['ok']]);
            assert.ok(b.ev.dropped.some(([n, r]) => n === 2 && r.includes('unreadable')));
            const names = fs.readdirSync(dir);
            assert.ok(names.some(n => n.startsWith('corrupt-')));
            assert.ok(!names.some(n => n.endsWith('.tmp')));
        });

        it('respects the disk limit', async () => {
            const a = make({ bufferMode: 'disk', spoolMaxBytes: 1024 * 1024, batchMaxLines: 1 }, new Spool(dir, false));
            // tiny limit after start to force eviction
            (a.q as unknown as { opts: QueueOptions }).opts.spoolMaxBytes = 150;
            await a.q.start();
            a.target.gate = new Promise(() => undefined);
            for (let i = 0; i < 5; i++) {
                a.q.push(`line-${i}-${'x'.repeat(40)}`);
            }
            await waitFor(() => a.ev.dropped.reduce((n, [c]) => n + c, 0) >= 3);
            assert.ok(a.ev.dropped.every(([, r]) => r === 'disk buffer full'));
            assert.ok(a.q.stats().spoolBytes <= 150 + 120);
            // let pending file operations finish before the directory is removed
            await (a.q as unknown as { persistChain: Promise<void> }).persistChain;
        });
    });
});
