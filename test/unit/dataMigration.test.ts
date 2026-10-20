import { strict as assert } from 'node:assert';
import { mkdtempSync, rmSync } from 'node:fs';
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
    type MeasurementOwner,
    FileCheckpointStore,
    type ImportItem,
    type ImportTarget,
    jobKey,
    MAX_WINDOW,
    MIN_WINDOW,
    newCheckpoint,
    parseDataMigrationConfig,
    parseDateSetting,
    previewImport,
    type Progress,
    validateSource,
} from '../../src/lib/dataMigration';
import { type LegacyPoint, LegacyReadError, type LegacyReader } from '../../src/lib/legacySource';

const T0 = Date.UTC(2023, 0, 1);

class FakeSource implements LegacyReader {
    reads: { m: string; from: number; to: number }[] = [];
    failures: (LegacyReadError | undefined)[] = [];
    maxWindow = Infinity;
    constructor(public data: Record<string, LegacyPoint[]>) {}
    check(): Promise<{ ok: boolean }> {
        return Promise.resolve({ ok: true });
    }
    timeRange(m: string): Promise<{ first?: number; last?: number }> {
        const pts = this.data[m] ?? [];
        return Promise.resolve(pts.length ? { first: pts[0].ts, last: pts[pts.length - 1].ts } : {});
    }
    read(m: string, from: number, to: number): Promise<LegacyPoint[]> {
        this.reads.push({ m, from, to });
        const f = this.failures.shift();
        if (f) {
            return Promise.reject(f);
        }
        if (to - from > this.maxWindow) {
            return Promise.reject(new LegacyReadError('response too large', 'tooBig'));
        }
        return Promise.resolve((this.data[m] ?? []).filter(p => p.ts >= from && p.ts < to));
    }
    listMeasurements(): Promise<string[]> {
        return Promise.resolve(Object.keys(this.data));
    }
    tagInventory(): Promise<{ key: string; value: string }[]> {
        return Promise.resolve([]);
    }
    close(): void {}
}

class FakeTarget implements ImportTarget {
    written: string[] = [];
    results: WriteResult[] = [];
    first: Record<string, number> = {};
    firstCalls = 0;
    busyCount = 0;
    firstTime(item: ImportItem): Promise<number | undefined> {
        this.firstCalls++;
        return Promise.resolve(this.first[item.id]);
    }
    write(lines: string[]): Promise<WriteResult> {
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

function item(id: string, measurement = id): ImportItem {
    return {
        id,
        measurement,
        table: 'numeric_states',
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
    pauseMs: 0,
    influxdbInstance: '',
};

function makeJob(
    source: FakeSource,
    target: FakeTarget,
    store: MemStore,
    settings: Partial<typeof SETTINGS> & { from?: number; to?: number } = {},
    onProgress: (p: Progress) => void = () => undefined,
): { job: DataMigrationJob; logs: string[]; sleeps: number[] } {
    const logs: string[] = [];
    const sleeps: number[] = [];
    let now = Date.UTC(2026, 0, 1);
    const job = new DataMigrationJob(
        {
            source,
            target,
            store,
            now: () => (now += 10),
            sleep: ms => {
                sleeps.push(ms);
                return Promise.resolve();
            },
            log: { info: m => logs.push(m), warn: m => logs.push(`WARN ${m}`), debug: () => undefined },
            onProgress,
        },
        { ...SETTINGS, ...settings },
    );
    return { job, logs, sleeps };
}

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
                    dmBatchSize: 1e9,
                    dmInfluxdbInstance: 'evil.0',
                },
                warnings,
            );
            assert.equal(source.version, '1.x');
            assert.equal(source.url, 'http://old:8086');
            assert.equal(source.rejectUnauthorized, true);
            assert.equal(job.batchSize, 50_000);
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
            assert.equal(adaptWindow(DAY, 60_000), DAY / 2);
            assert.equal(adaptWindow(DAY, 20_000), DAY);
            assert.equal(adaptWindow(DAY, 10), 2 * DAY);
            assert.equal(adaptWindow(MAX_WINDOW, 0), MAX_WINDOW);
            assert.equal(adaptWindow(MIN_WINDOW, 1e6), MIN_WINDOW);
        });

        it('file checkpoint store round trip', async () => {
            const dir = mkdtempSync(path.join(os.tmpdir(), 'dm-'));
            try {
                const store = new FileCheckpointStore(path.join(dir, 'sub', 'data-migration.json'));
                assert.equal(await store.load(), undefined);
                const cp = newCheckpoint('k', 1);
                cp.states.a = { points: 3, finished: true };
                await store.save(cp);
                assert.deepEqual(await store.load(), cp);
            } finally {
                rmSync(dir, { recursive: true, force: true });
            }
        });
    });

    it('imports every point of every state in adaptive windows and reports progress', async () => {
        const source = new FakeSource({ a: series(500, 3600_000), b: series(10, DAY), empty: [] });
        const target = new FakeTarget();
        const store = new MemStore();
        const progress: Progress[] = [];
        const { job } = makeJob(source, target, store, {}, p => progress.push({ ...p }));
        const outcome = await job.run([item('a'), item('b'), item('empty')], 'k1', new AbortController().signal);
        assert.equal(outcome, 'finished');
        assert.equal(target.written.length, 510);
        assert.equal(new Set(target.written).size, 510, 'no duplicates');
        assert.equal(store.cp!.finished, true);
        assert.equal(store.cp!.totals.points, 510);
        assert.equal(store.cp!.states.empty.finished, true);
        const last = progress[progress.length - 1];
        assert.equal(last.running, false);
        assert.equal(last.percent, 100);
        assert.match(last.status, /finished: 510 point\(s\) imported for 3 state\(s\)/);
        // windows grow while they return few rows
        const aReads = source.reads.filter(r => r.m === 'a');
        assert.ok(aReads.length < 10, `few reads for 21 days of hourly data (${aReads.length})`);
        assert.ok(progress.some(p => p.running && p.percent > 0 && p.percent < 100));
    });

    it('stops and resumes where it stopped without writing a window twice', async () => {
        const source = new FakeSource({ a: series(40, DAY / 2) });
        const target = new FakeTarget();
        const store = new MemStore();
        const ctrl = new AbortController();
        const first = makeJob(source, target, store, {}, p => {
            if (p.points >= 4) {
                ctrl.abort();
            }
        });
        assert.equal(await first.job.run([item('a')], 'k', ctrl.signal), 'stopped');
        const afterStop = target.written.length;
        assert.ok(afterStop > 0 && afterStop < 40);
        assert.equal(store.cp!.finished, false);
        const second = makeJob(source, target, store);
        assert.equal(await second.job.run([item('a')], 'k', new AbortController().signal), 'finished');
        assert.equal(target.written.length, 40);
        assert.equal(new Set(target.written).size, 40);
        assert.ok(second.logs.some(l => /resuming/.test(l)));
    });

    it('a finished import is not repeated; other settings start a new import', async () => {
        const source = new FakeSource({ a: series(5, DAY) });
        const target = new FakeTarget();
        const store = new MemStore();
        await makeJob(source, target, store).job.run([item('a')], 'k', new AbortController().signal);
        assert.equal(target.written.length, 5);
        // same key: the checkpoint is finished -> a new job with the same key starts over (explicit reset is the UI's job)
        const again = makeJob(source, target, store);
        await again.job.run([item('a')], 'k2', new AbortController().signal);
        assert.equal(target.written.length, 10, 'new key imports again');
    });

    it('stops each state at the first value already in InfluxDB 3 and remembers it', async () => {
        const source = new FakeSource({ a: series(10, DAY) });
        const target = new FakeTarget();
        target.first.a = T0 + 6 * DAY;
        const store = new MemStore();
        await makeJob(source, target, store, { untilFirstV3: true }).job.run(
            [item('a')],
            'k',
            new AbortController().signal,
        );
        assert.equal(target.written.length, 6);
        assert.equal(store.cp!.liveStart.a, T0 + 6 * DAY);
        // a new import (e.g. after a reset) must not take the imported points as "first live value"
        target.first.a = T0;
        target.firstCalls = 0;
        await makeJob(source, target, store, { untilFirstV3: true }).job.run(
            [item('a')],
            'other',
            new AbortController().signal,
        );
        assert.equal(target.firstCalls, 0);
        assert.equal(target.written.length, 12);
    });

    it('respects start and end dates', async () => {
        const source = new FakeSource({ a: series(10, DAY) });
        const target = new FakeTarget();
        await makeJob(source, target, new MemStore(), { from: T0 + 2 * DAY, to: T0 + 5 * DAY }).job.run(
            [item('a')],
            'k',
            new AbortController().signal,
        );
        assert.deepEqual(
            target.written.map(l => Number(l.split(' ')[2])),
            [T0 + 2 * DAY, T0 + 3 * DAY, T0 + 4 * DAY],
        );
    });

    it('shrinks the window when an answer is too large', async () => {
        const source = new FakeSource({ a: series(100, 3600_000) });
        source.maxWindow = 6 * 3600_000;
        const target = new FakeTarget();
        assert.equal(
            await makeJob(source, target, new MemStore()).job.run([item('a')], 'k', new AbortController().signal),
            'finished',
        );
        assert.equal(target.written.length, 100);
    });

    it('retries an unreachable source, fails on other errors with the progress saved', async () => {
        const source = new FakeSource({ a: series(3, DAY) });
        source.failures = [
            new LegacyReadError('ECONNREFUSED', 'transport'),
            new LegacyReadError('ECONNRESET', 'transport'),
        ];
        const target = new FakeTarget();
        const store = new MemStore();
        const run = makeJob(source, target, store);
        assert.equal(await run.job.run([item('a')], 'k', new AbortController().signal), 'finished');
        assert.deepEqual(run.sleeps, [1000, 2000]);

        const bad = new FakeSource({ a: series(3, DAY) });
        bad.failures = [new LegacyReadError('authentication failed', 'auth')];
        const store2 = new MemStore();
        const progress: Progress[] = [];
        const r2 = makeJob(bad, new FakeTarget(), store2, {}, p => progress.push(p));
        assert.equal(await r2.job.run([item('a')], 'k', new AbortController().signal), 'failed');
        assert.match(progress[progress.length - 1].status, /failed: authentication failed/);
        assert.ok(store2.cp, 'checkpoint saved');
    });

    it('handles write results: partial, too large, transient, fatal', async () => {
        const source = new FakeSource({ a: series(4, 3600_000) });
        const target = new FakeTarget();
        target.results = [
            { kind: 'transient', message: 'HTTP 503' },
            { kind: 'tooLarge', message: '413' },
            { kind: 'ok' },
            { kind: 'partial', message: 'type conflict', rejected: [{ line: 1, message: 'x' }] },
        ];
        const store = new MemStore();
        const run = makeJob(source, target, store, { batchSize: 10 });
        // all 4 points end up in one window and one chunk
        assert.equal(await run.job.run([item('a')], 'k', new AbortController().signal), 'finished');
        assert.equal(target.written.length, 4);
        assert.equal(store.cp!.totals.errors, 1);
        assert.ok(run.logs.some(l => /1 point\(s\) rejected/.test(l)));

        const t2 = new FakeTarget();
        t2.results = [{ kind: 'auth', status: 401, message: 'unauthorized' }];
        assert.equal(
            await makeJob(new FakeSource({ a: series(2, DAY) }), t2, new MemStore()).job.run(
                [item('a')],
                'k',
                new AbortController().signal,
            ),
            'failed',
        );
    });

    it('waits while live logging needs the connection', async () => {
        const target = new FakeTarget();
        target.busyCount = 3;
        const run = makeJob(new FakeSource({ a: series(2, DAY) }), target, new MemStore());
        await run.job.run([item('a')], 'k', new AbortController().signal);
        assert.deepEqual(run.sleeps, [1000, 1000, 1000]);
        assert.equal(target.written.length, 2);
    });

    it('counts points that cannot be converted', async () => {
        const pts = series(3, DAY);
        pts[1].value = 'n/a';
        const store = new MemStore();
        const run = makeJob(new FakeSource({ a: pts }), new FakeTarget(), store);
        await run.job.run([item('a')], 'k', new AbortController().signal);
        assert.equal(store.cp!.totals.skipped, 1);
        assert.equal(store.cp!.totals.points, 2);
    });

    it('preview shows ranges, samples and unconfigured measurements without writing', async () => {
        const source = new FakeSource({ a: series(3, DAY), other: series(1, DAY), none: [] });
        const target = new FakeTarget();
        target.first.a = T0 + DAY;
        const text = await previewImport(
            [item('a'), item('none')],
            { source, target },
            { ...SETTINGS, untilFirstV3: true },
            undefined,
            'k',
        );
        assert.match(text, /2 state\(s\) of this instance are selected/);
        assert.match(text, /old data: 2023-01-01 00:00:00 … 2023-01-03 00:00:00/);
        assert.match(text, /stops at the first value already in InfluxDB 3/);
        assert.match(text, /t,state_id=a value=0 1672531200000/);
        assert.match(text, /1 state\(s\) have no data/);
        assert.match(text, /1 measurement\(s\) of the old database are not selected/);
        assert.match(text, /1 belong to no state of the influxdb settings .*: other/);
        assert.equal(target.written.length, 0);
    });

    describe('unselected old measurements are explained', () => {
        const owners = new Map<string, MeasurementOwner>([
            ['BAWIALNIA-TEMP', { id: '0_userdata.0.bawialnia.temp', instance: 'influxdb.0', configuredHere: false }],
            ['BUFOR-TOP', { id: 'modbus.0.bufor.top', instance: 'influxdb.0', configuredHere: false }],
            ['javascript.0.pv', { id: 'javascript.0.pv', instance: 'influxdb.1', configuredHere: false }],
            ['FILTERED', { id: 'hm-rpc.0.x', instance: 'influxdb.0', configuredHere: true }],
        ]);

        it('groups them by owner and tells what to do', () => {
            const text = explainUnselected(
                ['BAWIALNIA-TEMP', 'BUFOR-TOP', 'javascript.0.pv', 'FILTERED', 'OLD-GONE'],
                owners,
                'influx-v3.0',
            ).join('\n');
            assert.match(text, /5 measurement\(s\) of the old database are not selected/);
            assert.match(
                text,
                /2 belong to states logged by influxdb\.0 that are not configured for influx-v3\.0 yet, e\.g\.: BAWIALNIA-TEMP \(0_userdata\.0\.bawialnia\.temp\), BUFOR-TOP \(modbus\.0\.bufor\.top\)/,
            );
            assert.match(text, /choose influxdb\.0, check the preview and press "Apply"/);
            assert.match(
                text,
                /1 belong to states logged by influxdb\.1 .*javascript\.0\.pv$/m,
                'alias = id is shown once',
            );
            assert.match(
                text,
                /1 belong to states of this instance excluded by the state filter, e\.g\.: FILTERED \(hm-rpc\.0\.x\)/,
            );
            assert.match(
                text,
                /1 belong to no state of the influxdb settings \(state deleted, logging disabled or alias changed\), e\.g\.: OLD-GONE/,
            );
            assert.deepEqual(explainUnselected([], owners, 'influx-v3.0'), []);
        });

        it('the preview with no selected state says why and lists the owners', async () => {
            const source = new FakeSource({ 'BAWIALNIA-TEMP': series(2, DAY) });
            const text = await previewImport(
                [],
                { source, target: new FakeTarget() },
                SETTINGS,
                undefined,
                'k',
                undefined,
                owners,
                'influx-v3.0',
            );
            assert.match(text, /^No state of influx-v3\.0 is selected for the import/);
            assert.match(text, /1 belong to states logged by influxdb\.0 that are not configured for influx-v3\.0 yet/);
        });
    });
});
