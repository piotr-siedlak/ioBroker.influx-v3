import { strict as assert } from 'node:assert';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import type { WriteResult } from '../../src/lib/influxClient';
import {
    adaptWindow,
    type Checkpoint,
    type CheckpointStore,
    chunkLines,
    compileStateFilter,
    DataMigrationJob,
    DAY,
    explainUnselected,
    FileCheckpointStore,
    formatDuration,
    HOUR,
    type ImportItem,
    type ImportTarget,
    jobKey,
    MAX_WINDOW,
    PAUSE,
    type MeasurementOwner,
    MIN_WINDOW,
    newCheckpoint,
    parseDataMigrationConfig,
    parseDateSetting,
    planImport,
    previewImport,
    type Progress,
    validateSource,
} from '../../src/lib/dataMigration';
import { type LegacyPoint, LegacyReadError, type LegacyReader, type MeasurementSet } from '../../src/lib/legacySource';

const T0 = Date.UTC(2023, 0, 1);
const NOW = Date.UTC(2023, 2, 1);

class FakeSource implements LegacyReader {
    reads: { set: MeasurementSet; from: number; to: number }[] = [];
    firstCalls: MeasurementSet[] = [];
    /** consumed one per readMany call */
    failures: (LegacyReadError | undefined)[] = [];
    /** answers with more rows are "too big" */
    maxRows = Infinity;
    /** measurements that exist but are not imported */
    extra: string[] = [];
    constructor(public data: Record<string, LegacyPoint[]>) {}
    check(): Promise<{ ok: boolean }> {
        return Promise.resolve({ ok: true });
    }
    timeRange(m: string): Promise<{ first?: number; last?: number }> {
        const pts = this.data[m] ?? [];
        return Promise.resolve(pts.length ? { first: pts[0].ts, last: pts[pts.length - 1].ts } : {});
    }
    read(m: string, from: number, to: number): Promise<LegacyPoint[]> {
        return Promise.resolve((this.data[m] ?? []).filter(p => p.ts >= from && p.ts < to));
    }
    readMany(set: MeasurementSet, from: number, to: number): Promise<Map<string, LegacyPoint[]>> {
        this.reads.push({ set, from, to });
        const f = this.failures.shift();
        if (f) {
            return Promise.reject(f);
        }
        const out = new Map<string, LegacyPoint[]>();
        let rows = 0;
        for (const m of set === 'all' ? [...Object.keys(this.data), ...this.extra] : set) {
            const pts = (this.data[m] ?? []).filter(p => p.ts >= from && p.ts < to);
            if (pts.length) {
                out.set(m, pts);
                rows += pts.length;
            }
        }
        if (rows > this.maxRows) {
            return Promise.reject(new LegacyReadError('response too large', 'tooBig'));
        }
        return Promise.resolve(out);
    }
    /** undefined: counting fails */
    countFails = false;
    countPoints(set: MeasurementSet, from: number, to: number): Promise<number> {
        if (this.countFails) {
            return Promise.reject(new LegacyReadError('timeout after 120000 ms', 'timeout'));
        }
        let n = 0;
        for (const m of set === 'all' ? Object.keys(this.data) : set) {
            n += (this.data[m] ?? []).filter(p => p.ts >= from && p.ts < to).length;
        }
        return Promise.resolve(n);
    }
    firstTime(set: MeasurementSet): Promise<number | undefined> {
        this.firstCalls.push(set);
        const ts = (set === 'all' ? Object.keys(this.data) : set).flatMap(m => (this.data[m] ?? []).map(p => p.ts));
        return Promise.resolve(ts.length ? Math.min(...ts) : undefined);
    }
    listMeasurements(): Promise<string[]> {
        return Promise.resolve([...Object.keys(this.data), ...this.extra]);
    }
    tagInventory(): Promise<{ key: string; value: string }[]> {
        return Promise.resolve([]);
    }
    close(): void {}
}

class FakeTarget implements ImportTarget {
    written: string[] = [];
    chunks: number[] = [];
    results: WriteResult[] = [];
    busyCount = 0;
    write(lines: string[]): Promise<WriteResult> {
        this.chunks.push(lines.length);
        const r = this.results.shift() ?? { kind: 'ok' };
        if (r.kind === 'ok' || r.kind === 'partial') {
            this.written.push(...lines);
        }
        return Promise.resolve(r);
    }
    busy(): boolean {
        return this.busyCount-- > 0;
    }
}

class MemStore implements CheckpointStore {
    cp?: Checkpoint;
    saves = 0;
    load(): Promise<Checkpoint | undefined> {
        return Promise.resolve(this.cp ? (JSON.parse(JSON.stringify(this.cp)) as Checkpoint) : undefined);
    }
    save(cp: Checkpoint): Promise<void> {
        this.saves++;
        this.cp = JSON.parse(JSON.stringify(cp)) as Checkpoint;
        return Promise.resolve();
    }
}

function series(count: number, stepMs: number, start = T0): LegacyPoint[] {
    return Array.from({ length: count }, (_, i) => ({ ts: start + i * stepMs, value: i, tags: {} }));
}

function item(id: string, measurement = id, pending = false): ImportItem {
    return {
        id,
        measurement,
        table: 'numeric_states',
        pending,
        convert: (pts: LegacyPoint[]) => ({
            lines: pts
                .filter(p => typeof p.value === 'number')
                .map(p => `t,state_id=${id} value=${p.value as number} ${p.ts}`),
            skipped: pts.filter(p => typeof p.value !== 'number').length,
        }),
    };
}

const SETTINGS = {
    stateFilter: '',
    untilFirstV3: false,
    keepOldTags: true,
    batchSize: 1000,
    parallelWrites: 1,
    pauseMs: 0,
    influxdbInstance: '',
};

function makeJob(
    source: FakeSource,
    target: FakeTarget,
    store: MemStore,
    settings: Partial<typeof SETTINGS> & { from?: number; to?: number } = {},
    onProgress: (p: Progress) => void = () => undefined,
    live: Record<string, number> = {},
): { job: DataMigrationJob; logs: string[]; sleeps: number[] } {
    const logs: string[] = [];
    const sleeps: number[] = [];
    let now = NOW;
    const job = new DataMigrationJob(
        {
            source,
            target,
            store,
            now: () => (now += 10),
            sleep: ms => {
                sleeps.push(ms);
                now += ms;
                return Promise.resolve();
            },
            log: { info: m => logs.push(m), warn: m => logs.push(`WARN ${m}`), debug: () => undefined },
            onProgress,
            liveStart: id => live[id],
        },
        { ...SETTINGS, ...settings },
    );
    return { job, logs, sleeps };
}

const run = (j: { job: DataMigrationJob }, items: ImportItem[], key = 'k'): Promise<string> =>
    j.job.run(items, key, new AbortController().signal);

describe('data import (dataMigration)', () => {
    describe('helpers', () => {
        it('parses dates of the settings', () => {
            assert.equal(parseDateSetting(''), undefined);
            assert.equal(parseDateSetting('2024-03-01'), new Date(2024, 2, 1).getTime());
            assert.equal(parseDateSetting('2024-03-01 12:30'), new Date(2024, 2, 1, 12, 30).getTime());
            assert.equal(parseDateSetting('2024-03-01T12:30:00Z'), Date.UTC(2024, 2, 1, 12, 30));
            assert.equal(parseDateSetting('last week'), null);
        });

        it('reads the dm* settings with defaults and limits', () => {
            const warnings: string[] = [];
            const { source, job } = parseDataMigrationConfig(
                {
                    dmVersion: '1.x',
                    dmUrl: 'http://old:8086/',
                    dmDatabase: 'iobroker',
                    dmUser: 'u',
                    dmPassword: 'p',
                    dmFrom: '2024-02-01',
                    dmTo: 'x',
                    dmBatchLines: 1e9,
                    dmParallelWrites: 0,
                    dmInfluxdbInstance: 'evil.0',
                },
                warnings,
            );
            assert.equal(source.version, '1.x');
            assert.equal(source.url, 'http://old:8086');
            assert.equal(source.rejectUnauthorized, true);
            assert.equal(job.batchSize, 100_000);
            assert.equal(job.parallelWrites, 1);
            assert.equal(parseDataMigrationConfig({}).job.batchSize, 50_000);
            assert.equal(parseDataMigrationConfig({}).job.parallelWrites, 2);
            assert.equal(job.untilFirstV3, true);
            assert.equal(job.keepOldTags, true);
            assert.equal(job.influxdbInstance, '');
            assert.equal(job.to, undefined);
            assert.equal(warnings.length, 1);
            assert.deepEqual(validateSource(source), []);
            assert.equal(parseDataMigrationConfig({}).source.version, '2.x');
            assert.equal(validateSource(parseDataMigrationConfig({}).source).length, 2);
        });

        it('filters ids with globs', () => {
            const f = compileStateFilter('hm-rpc.0.*, javascript.0.heating.*');
            assert.equal(f('hm-rpc.0.ABC.1.TEMP'), true);
            assert.equal(f('javascript.0.heating.x'), true);
            assert.equal(f('javascript.0.other'), false);
            assert.equal(f('hm-rpcX0.y'), false, 'dot is literal');
            assert.equal(compileStateFilter('')('anything'), true);
        });

        it('job key changes with source, scope and tag handling', () => {
            const { source, job } = parseDataMigrationConfig({ dmUrl: 'http://a', dmOrg: 'o', dmBucket: 'b' });
            const k = jobKey(source, job, { rules: [] });
            assert.equal(k, jobKey(source, job, { rules: [] }));
            assert.notEqual(k, jobKey(source, job, { rules: [{ fromKey: 'x' }] }));
            assert.notEqual(k, jobKey({ ...source, bucket: 'c' }, job, { rules: [] }));
            assert.notEqual(k, jobKey(source, { ...job, from: 1 }, { rules: [] }));
        });

        it('chunks by line count and bytes, adapts the window', () => {
            assert.deepEqual(
                chunkLines(['a', 'b', 'c'], 2).map(c => c.length),
                [2, 1],
            );
            assert.deepEqual(
                chunkLines(['aaaa', 'bbbb', 'cccc'], 100, 10).map(c => c.length),
                [2, 1],
            );
            assert.equal(adaptWindow(DAY, 200_000), DAY / 2);
            assert.equal(adaptWindow(DAY, 50_000), DAY);
            assert.equal(adaptWindow(DAY, 10), 2 * DAY);
            assert.equal(adaptWindow(MAX_WINDOW, 0), MAX_WINDOW);
            assert.equal(adaptWindow(MIN_WINDOW, 1e6), MIN_WINDOW);
        });

        it('file checkpoint store round trip; a version 1 checkpoint only hands over its live-start times', async () => {
            const dir = mkdtempSync(path.join(os.tmpdir(), 'dm-'));
            try {
                const file = path.join(dir, 'sub', 'data-migration.json');
                const store = new FileCheckpointStore(file);
                assert.equal(await store.load(), undefined);
                const cp = newCheckpoint('k', 1);
                cp.ends.a = 5;
                cp.doneUntil = 3;
                await store.save(cp);
                assert.deepEqual(await store.load(), cp);
                writeFileSync(
                    file,
                    JSON.stringify({
                        version: 1,
                        jobKey: 'k',
                        finished: false,
                        states: { a: { points: 1, finished: false } },
                        liveStart: { a: 42 },
                        updatedAt: 7,
                    }),
                );
                const old = await store.load();
                assert.equal(old?.version, 2);
                assert.equal(old?.finished, true, 'never resumed');
                assert.deepEqual(old?.liveStart, { a: 42 });
            } finally {
                rmSync(dir, { recursive: true, force: true });
            }
        });

        it('formats durations', () => {
            assert.equal(formatDuration(10_000), '1 min');
            assert.equal(formatDuration(45 * 60_000), '45 min');
            assert.equal(formatDuration(125 * 60_000), '2 h 05 min');
            assert.equal(formatDuration(5 * DAY), '5 days');
        });
    });

    it('imports all states in one pass over the time axis, window by window', async () => {
        const source = new FakeSource({ a: series(500, HOUR), b: series(10, DAY), empty: [] });
        const target = new FakeTarget();
        const store = new MemStore();
        const progress: Progress[] = [];
        const outcome = await run(
            makeJob(source, target, store, {}, p => progress.push({ ...p })),
            [item('a'), item('b'), item('empty')],
        );
        assert.equal(outcome, 'finished');
        assert.equal(target.written.length, 510);
        assert.equal(new Set(target.written).size, 510, 'no duplicates');
        assert.equal(store.cp!.finished, true);
        assert.equal(store.cp!.totals.points, 510);
        assert.equal(store.cp!.start, T0);
        assert.deepEqual(source.firstCalls, ['all'], 'one query for the oldest point');
        assert.ok(
            source.reads.every(r => r.set === 'all'),
            'every measurement selected → whole database read',
        );
        for (let i = 1; i < source.reads.length; i++) {
            assert.equal(source.reads[i].from, source.reads[i - 1].to, 'windows follow each other');
        }
        assert.ok(source.reads.length < 30, `few windows (${source.reads.length})`);
        // the windows of b's data also carry a's points: one write per window holds both states
        assert.ok(
            target.written.findIndex(l => l.includes('state_id=b')) <
                target.written.findIndex(l => l.includes('value=499')),
        );
        assert.ok(progress.some(p => p.running && p.percent > 0 && p.percent < 100));
        assert.match(
            progress.find(p => /points\/s/.test(p.status))!.status,
            /^2023-01-\d\d \d\d:\d\d UTC · \d+ of about 510 point\(s\) · \d+ points\/s/,
        );
        assert.equal(progress[progress.length - 1].percent, 100);
    });

    it('reads a list of measurements when the selection is only a small part of the old database', async () => {
        const source = new FakeSource({ a: series(5, DAY), b: series(5, DAY) });
        source.extra = ['x1', 'x2', 'x3', 'x4'];
        const target = new FakeTarget();
        await run(makeJob(source, target, new MemStore()), [item('a')]);
        assert.deepEqual(source.firstCalls, [['a']]);
        assert.ok(source.reads.every(r => Array.isArray(r.set) && r.set.join() === 'a'));
        assert.equal(target.written.length, 5);
    });

    it('stops and resumes where it stopped without writing a window twice', async () => {
        const source = new FakeSource({ a: series(40, DAY / 2), b: series(40, DAY / 2) });
        source.maxRows = 10; // many small windows
        const target = new FakeTarget();
        const store = new MemStore();
        const ctrl = new AbortController();
        const first = makeJob(source, target, store, {}, p => {
            if (p.points >= 8) {
                ctrl.abort();
            }
        });
        assert.equal(await first.job.run([item('a'), item('b')], 'k', ctrl.signal), 'stopped');
        const afterStop = target.written.length;
        assert.ok(afterStop > 0 && afterStop < 80);
        assert.equal(store.cp!.finished, false);
        const second = makeJob(source, target, store);
        assert.equal(await run(second, [item('a'), item('b')]), 'finished');
        assert.equal(target.written.length, 80);
        assert.equal(new Set(target.written).size, 80);
        assert.ok(second.logs.some(l => /resuming at 2023-01/.test(l)));
    });

    it('a finished import starts over only with a new job', async () => {
        const source = new FakeSource({ a: series(5, DAY) });
        const target = new FakeTarget();
        const store = new MemStore();
        await run(makeJob(source, target, store), [item('a')]);
        assert.equal(target.written.length, 5);
        await run(makeJob(source, target, store), [item('a')], 'k2');
        assert.equal(target.written.length, 10, 'new key imports again');
    });

    it('each state ends at its first live value - from the adapter record, never from InfluxDB 3', async () => {
        const source = new FakeSource({ a: series(10, DAY), b: series(10, DAY), c: series(10, DAY) });
        const target = new FakeTarget();
        const store = new MemStore();
        // a live-start time found by an older version (checkpoint version 1) is still used
        store.cp = { ...newCheckpoint('v1', 1, { liveStart: { b: T0 + 3 * DAY } }), finished: true };
        await run(
            makeJob(source, target, store, { untilFirstV3: true }, undefined, { a: T0 + 6 * DAY, b: T0 + 8 * DAY }),
            [item('a'), item('b'), item('c')],
        );
        const count = (id: string): number => target.written.filter(l => l.includes(`state_id=${id} `)).length;
        assert.equal(count('a'), 6);
        assert.equal(count('b'), 3, 'the earlier of both times');
        assert.equal(count('c'), 10, 'no live value yet: up to the start of the job');
        const ends = store.cp.ends;
        assert.deepEqual({ ...ends, c: 0 }, { a: T0 + 6 * DAY, b: T0 + 3 * DAY, c: 0 });
        assert.ok(ends.c > NOW && ends.c < NOW + 1000, 'start of the job');
        assert.equal(store.cp.end, ends.c);
    });

    it('a state not configured yet is imported completely, a live record does not cut it', async () => {
        const source = new FakeSource({ BUFOR: series(10, DAY) });
        const target = new FakeTarget();
        await run(makeJob(source, target, new MemStore(), { untilFirstV3: true }, undefined, { 'kebahp.0.top': T0 }), [
            item('kebahp.0.top', 'BUFOR', true),
        ]);
        assert.equal(target.written.length, 10);
    });

    it('respects start and end dates', async () => {
        const source = new FakeSource({ a: series(10, DAY) });
        const target = new FakeTarget();
        await run(makeJob(source, target, new MemStore(), { from: T0 + 2 * DAY, to: T0 + 5 * DAY }), [item('a')]);
        assert.deepEqual(
            target.written.map(l => Number(l.split(' ')[2])),
            [T0 + 2 * DAY, T0 + 3 * DAY, T0 + 4 * DAY],
        );
        assert.equal(source.firstCalls.length, 0, 'no query for the oldest point with a start date');
    });

    it('nothing to do when the old database has no points for the selection', async () => {
        const source = new FakeSource({ a: [] });
        const store = new MemStore();
        const r = makeJob(source, new FakeTarget(), store);
        assert.equal(await run(r, [item('a')]), 'finished');
        assert.ok(r.logs.some(l => /has no points/.test(l)));
        assert.equal(source.reads.length, 0);
    });

    it('shrinks the window when an answer is too large or the old database times out', async () => {
        const source = new FakeSource({ a: series(100, HOUR), b: series(100, HOUR) });
        source.maxRows = 30;
        source.failures = [
            new LegacyReadError('timeout after 120000 ms', 'timeout'),
            undefined,
            new LegacyReadError('HTTP 500: out of memory', 'server'),
        ];
        const target = new FakeTarget();
        assert.equal(await run(makeJob(source, target, new MemStore()), [item('a'), item('b')]), 'finished');
        assert.equal(target.written.length, 200);
        assert.equal(new Set(target.written).size, 200);
    });

    it('waits for an unreachable old database as long as it takes; wrong settings stop at once', async () => {
        const source = new FakeSource({ a: series(3, DAY) });
        source.failures = [
            undefined, // first window
            new LegacyReadError('ECONNREFUSED', 'transport'),
            new LegacyReadError('ECONNRESET', 'transport'),
        ];
        const target = new FakeTarget();
        const r = makeJob(source, target, new MemStore());
        assert.equal(await run(r, [item('a')]), 'finished');
        assert.deepEqual(r.sleeps, [1000, 2000]);
        assert.equal(target.written.length, 3);

        // a night of maintenance: 300 failed attempts (about 5 hours) - then it continues
        const down = new FakeSource({ a: series(3, DAY) });
        down.failures = [
            undefined,
            ...Array.from({ length: 300 }, () => new LegacyReadError('ECONNREFUSED', 'transport')),
        ];
        const progress: Progress[] = [];
        const t2 = new FakeTarget();
        const r2 = makeJob(down, t2, new MemStore(), {}, p => progress.push({ ...p }));
        assert.equal(await run(r2, [item('a')]), 'finished');
        assert.equal(t2.written.length, 3);
        assert.equal(Math.max(...r2.sleeps), 60_000, 'at most one minute between attempts');
        assert.ok(
            progress.some(p =>
                /^waiting for the old database since 2023-03-01 \d\d:\d\d UTC \(ECONNREFUSED\) - continues by itself/.test(
                    p.status,
                ),
            ),
        );

        const bad = new FakeSource({ a: series(3, DAY) });
        bad.failures = [new LegacyReadError('authentication failed', 'auth')];
        const p2: Progress[] = [];
        const store = new MemStore();
        assert.equal(
            await run(
                makeJob(bad, new FakeTarget(), store, {}, p => p2.push(p)),
                [item('a')],
            ),
            'failed',
        );
        assert.match(p2[p2.length - 1].status, /failed: authentication failed/);
        assert.equal(store.cp!.active, false, 'not continued by itself');
    });

    it('a pause (adapter restart) keeps the job active, "Stop" does not', async () => {
        for (const [reason, active] of [
            [PAUSE, true],
            ['stop', false],
        ] as const) {
            const source = new FakeSource({ a: series(40, DAY / 2) });
            source.maxRows = 10;
            const store = new MemStore();
            const ctrl = new AbortController();
            const progress: Progress[] = [];
            const r = makeJob(source, new FakeTarget(), store, {}, p => {
                progress.push({ ...p });
                if (p.points >= 8) {
                    ctrl.abort(reason);
                }
            });
            assert.equal(await r.job.run([item('a')], 'k', ctrl.signal), 'stopped');
            assert.equal(store.cp!.active, active, reason);
            assert.match(
                progress[progress.length - 1].status,
                active ? /^paused - continues after the restart at 2023-01/ : /^stopped at 2023-01/,
            );
        }
        const store = new MemStore();
        await run(makeJob(new FakeSource({ a: series(3, DAY) }), new FakeTarget(), store), [item('a')]);
        assert.equal(store.cp!.active, false, 'finished');
    });

    it('progress follows the counted points; without a count it follows the time', async () => {
        const source = new FakeSource({ a: series(300, HOUR), b: series(100, HOUR, T0 + 200 * HOUR) });
        source.maxRows = 60;
        const progress: Progress[] = [];
        const store = new MemStore();
        await run(
            makeJob(source, new FakeTarget(), store, {}, p => progress.push({ ...p })),
            [item('a'), item('b')],
        );
        assert.equal(store.cp!.expected, 400);
        const mid = progress.filter(p => p.running && /of about 400 point\(s\)/.test(p.status));
        assert.ok(mid.length > 2);
        for (const p of mid) {
            assert.equal(p.percent, Math.min(99.9, Math.floor((p.points / 400) * 1000) / 10));
        }
        const noCount = new FakeSource({ a: series(3, DAY) });
        noCount.countFails = true;
        const s2 = new MemStore();
        assert.equal(await run(makeJob(noCount, new FakeTarget(), s2), [item('a')]), 'finished');
        assert.equal(s2.cp!.expected, undefined, 'an estimate only - never stops the import');
    });

    it('handles write results: partial, too large, unreachable, fatal', async () => {
        const source = new FakeSource({ a: series(4, 60_000) }); // one window
        const target = new FakeTarget();
        target.results = [
            { kind: 'transient', message: 'ECONNREFUSED' },
            { kind: 'tooLarge', message: '413' },
            { kind: 'ok' },
            { kind: 'partial', message: 'type conflict', rejected: [{ line: 1, message: 'x' }] },
        ];
        const store = new MemStore();
        const r = makeJob(source, target, store, { batchSize: 10 });
        assert.equal(await run(r, [item('a')]), 'finished');
        assert.equal(target.written.length, 4);
        assert.deepEqual(target.chunks, [4, 4, 2, 2]);
        assert.equal(store.cp!.totals.errors, 1);
        assert.ok(r.logs.some(l => /1 point\(s\) rejected/.test(l)));

        const t2 = new FakeTarget();
        t2.results = [{ kind: 'auth', status: 401, message: 'unauthorized' }];
        assert.equal(
            await run(makeJob(new FakeSource({ a: series(2, DAY) }), t2, new MemStore()), [item('a')]),
            'failed',
        );

        // InfluxDB 3 restarting for hours: the import waits and continues
        const t3 = new FakeTarget();
        t3.results = Array.from({ length: 200 }, () => ({ kind: 'transient' as const, message: 'ECONNREFUSED' }));
        const p3: Progress[] = [];
        assert.equal(
            await run(
                makeJob(new FakeSource({ a: series(2, 60_000) }), t3, new MemStore(), {}, p => p3.push({ ...p })),
                [item('a')],
            ),
            'finished',
        );
        assert.equal(t3.written.length, 2);
        assert.ok(
            p3.some(p => /^waiting for InfluxDB 3 since .* UTC \(ECONNREFUSED\) - continues by itself/.test(p.status)),
        );
    });

    it('writes the requests of a window in parallel', async () => {
        const source = new FakeSource({ a: series(3000, 1000) }); // one window
        let running = 0;
        let most = 0;
        const target = new FakeTarget();
        const write = target.write.bind(target);
        target.write = async lines => {
            running++;
            most = Math.max(most, running);
            await new Promise(r => setTimeout(r, 5));
            running--;
            return write(lines);
        };
        assert.equal(
            await run(makeJob(source, target, new MemStore(), { batchSize: 1000, parallelWrites: 2 }), [item('a')]),
            'finished',
        );
        assert.equal(target.written.length, 3000);
        assert.equal(new Set(target.written).size, 3000);
        assert.equal(most, 2);
    });

    it('a write timeout halves the batch (not below 500)', async () => {
        const source = new FakeSource({ a: series(3000, 1000) }); // one window
        const target = new FakeTarget();
        target.results = [
            { kind: 'transient', message: 'timeout after 60000 ms' },
            { kind: 'transient', message: 'timeout after 60000 ms' },
        ];
        const r = makeJob(source, target, new MemStore(), { batchSize: 2000 });
        assert.equal(await run(r, [item('a')]), 'finished');
        assert.equal(target.written.length, 3000);
        // 2000 times out → 1000; the next request (1000) times out → 500; the rest follows in requests of ≤ 1000
        assert.deepEqual(target.chunks, [2000, 1000, 1000, 1000, 500, 500]);
    });

    it('waits while live logging needs the connection', async () => {
        const source = new FakeSource({ a: series(2, HOUR) });
        const target = new FakeTarget();
        target.busyCount = 3;
        const r = makeJob(source, target, new MemStore());
        await run(r, [item('a')]);
        assert.deepEqual(r.sleeps, [1000, 1000, 1000]);
        assert.equal(target.written.length, 2);
    });

    it('counts points that cannot be converted', async () => {
        const pts = series(3, DAY);
        pts[1].value = 'n/a';
        const store = new MemStore();
        await run(makeJob(new FakeSource({ a: pts }), new FakeTarget(), store), [item('a')]);
        assert.equal(store.cp!.totals.skipped, 1);
        assert.equal(store.cp!.totals.points, 2);
    });

    it('planImport: period, ends and how the old database is read', async () => {
        const source = new FakeSource({ a: series(3, DAY), b: series(3, DAY, T0 + DAY) });
        const plan = await planImport(
            [item('a'), item('b', 'b', true)],
            source,
            { ...SETTINGS, untilFirstV3: true },
            NOW,
            id => (id === 'a' ? T0 + DAY : undefined),
        );
        assert.deepEqual(plan, {
            start: T0,
            end: NOW,
            ends: { a: T0 + DAY, b: NOW },
            readAll: true,
            knownLive: 1,
            expected: 6,
        });
    });

    it('preview: period, ends and the newest records, without writing or asking InfluxDB 3', async () => {
        const source = new FakeSource({ a: series(3, DAY), other: series(1, DAY) });
        source.extra = ['x1', 'x2'];
        const text = await previewImport([item('a')], {
            source,
            settings: { ...SETTINGS, untilFirstV3: true },
            key: 'k',
            now: T0 + 2 * DAY + 30 * 60_000,
            liveStart: id => (id === 'a' ? T0 + 2 * DAY + 10 * 60_000 : undefined),
            target: 'influx-v3.0',
        });
        assert.match(text, /^1 state\(s\) of influx-v3\.0 are selected for the import\./);
        assert.match(
            text,
            /Period: 2023-01-01 00:00 … 2023-01-03 00:10 UTC \(about 2 day\(s\)\), read in time slices for all 1 state\(s\) together\./,
        );
        assert.match(text, /End per state: 1 state\(s\) at their first value logged by influx-v3\.0\./);
        assert.match(text, /ends at its first live value: 2023-01-03 00:10 UTC/);
        assert.match(text, /t,state_id=a value=2 1672704000000/);
        assert.match(text, /3 measurement\(s\) of the old database are not selected/);
        assert.match(text, /3 - belong to no state of the influxdb settings .*: other, x1, x2/);
    });

    it('preview of states only the influxdb adapter logs so far', async () => {
        const source = new FakeSource({
            'BUFOR-TOP': series(3, HOUR, NOW - 3 * HOUR),
            a: series(3, HOUR, NOW - 3 * HOUR),
        });
        const text = await previewImport([item('a'), item('kebahp.0.Temperatures.Buffer_Top', 'BUFOR-TOP', true)], {
            source,
            settings: { ...SETTINGS, untilFirstV3: true },
            key: 'k',
            now: NOW,
            liveStart: () => undefined,
            target: 'influx-v3.0',
        });
        assert.match(
            text,
            /^2 state\(s\) are selected for the import: 1 configured for influx-v3\.0, 1 logged only by the influxdb adapter so far\./,
        );
        assert.match(
            text,
            /End per state: 0 state\(s\) at their first value logged by influx-v3\.0, 2 at the start of the import/,
        );
        assert.match(
            text,
            /• kebahp\.0\.Temperatures\.Buffer_Top {2}\(old measurement "BUFOR-TOP" → table "numeric_states"\) {2}- not configured for influx-v3\.0 yet\n {4}t,state_id=kebahp/,
        );
        const none = await previewImport([], {
            source,
            settings: SETTINGS,
            key: 'k',
            now: NOW,
            liveStart: () => undefined,
            target: 'influx-v3.0',
            owners: new Map([
                ['BUFOR-TOP', { id: 'x', instance: 'influxdb.0', reason: 'excluded by the state filter' }],
            ]),
        });
        assert.match(none, /^No state is selected for the import: neither influx-v3\.0 nor the influxdb adapter/);
        assert.match(none, /1 - excluded by the state filter/);
    });

    describe('unselected old measurements are explained', () => {
        const owners = new Map<string, MeasurementOwner>([
            [
                'BAWIALNIA-TEMP',
                { id: '0_userdata.0.bawialnia.temp', instance: 'influxdb.0', reason: 'excluded by the state filter' },
            ],
            ['BUFOR-TOP', { id: 'kebahp.0.Temperatures.Buffer_Top', instance: 'influxdb.0' }],
            [
                'javascript.0.pv',
                {
                    id: 'javascript.0.pv',
                    instance: 'influxdb.1',
                    reason: 'logged by influxdb.1, not by the selected influxdb.0',
                },
            ],
            [
                'TEXT',
                {
                    id: 'hm-rpc.0.x',
                    instance: 'influxdb.0',
                    reason: 'excluded by the state filter',
                },
            ],
        ]);

        it('groups them by reason, the largest group first', () => {
            const text = explainUnselected(
                ['BAWIALNIA-TEMP', 'BUFOR-TOP', 'javascript.0.pv', 'TEXT', 'HOL-ANIA-ZAWORY'],
                owners,
            ).join('\n');
            assert.match(text, /5 measurement\(s\) of the old database are not selected/);
            assert.match(
                text,
                /not selected \(not imported\):\n {2}2 - excluded by the state filter, e\.g\.: BAWIALNIA-TEMP \(0_userdata\.0\.bawialnia\.temp\), TEXT \(hm-rpc\.0\.x\)/,
            );
            assert.match(
                text,
                /1 - logged by influxdb\.1, not by the selected influxdb\.0, e\.g\.: javascript\.0\.pv$/m,
                'alias = id is shown once',
            );
            assert.match(text, /1 - not selected, e\.g\.: BUFOR-TOP \(kebahp\.0\.Temperatures\.Buffer_Top\)/);
            assert.match(
                text,
                /1 - belong to no state of the influxdb settings \(state deleted, logging disabled or alias changed\), e\.g\.: HOL-ANIA-ZAWORY/,
            );
            assert.deepEqual(explainUnselected([], owners), []);
        });
    });
});
