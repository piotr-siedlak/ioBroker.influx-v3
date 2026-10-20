/**
 * Import of historical data from the database of the classic influxdb adapter (ADR-0006).
 *
 * The job walks the time axis once (extension 7): each adaptive time window is read for all selected states together
 * from the source (read-only), converted with the same mapping as live logging (done by the caller in
 * `ImportItem.convert`) and written directly into InfluxDB 3 - so each time block of InfluxDB 3 is written once, like
 * live data. InfluxDB 3 is never queried. Progress is saved after each window, so a stopped or interrupted job
 * resumes where it stopped; re-written windows are idempotent (same table, tags and time overwrite each other).
 *
 * The module has no timers or I/O of its own: source, target, checkpoint store and clock are injected.
 */
import { createHash } from 'node:crypto';
import { promises as fsp } from 'node:fs';
import path from 'node:path';
import type { WriteResult } from './influxClient';
import { type LegacyPoint, LegacyReadError, type LegacyReader, type LegacySourceConfig } from './legacySource';
import { redact } from './redact';

export const DAY = 24 * 3600 * 1000;
export const HOUR = 3600 * 1000;
/** smallest / largest / first time window (all states together) */
export const MIN_WINDOW = 60 * 1000;
export const MAX_WINDOW = 30 * DAY;
export const START_WINDOW = HOUR;
/** rows per window (all states together): above → halve the next window, below → double it */
export const HIGH_ROWS = 100_000;
export const LOW_ROWS = 40_000;
/** maximum bytes of one write request */
export const MAX_WRITE_BYTES = 8 * 1024 * 1024;
/** smallest write batch after timeouts */
export const MIN_BATCH = 500;
/** longest wait between two attempts while a server is not reachable */
export const MAX_RETRY_DELAY = 60_000;
/**
 * Abort reason that keeps the job active: it continues by itself after the adapter restarts. Any other reason
 * (e.g. "Stop" in Admin) ends it until "Start".
 */
export const PAUSE = 'pause';
/** longest wait for the point count when a job is planned (an estimate only) */
const COUNT_WITHIN_MS = 5 * 60 * 1000;
/** the whole old database is read when the selection covers at least this share of its measurements */
const READ_ALL_SHARE = 0.8;
/** measurements listed to decide that */
const LIST_LIMIT = 20_000;

// ------------------------------------------------------------------ settings

export interface DataMigrationSettings {
    /** id patterns (comma separated, `*` wildcard); empty = all states of this instance and of the influxdb adapter */
    stateFilter: string;
    /** start (ms) or undefined = from the first point */
    from?: number;
    /** end (ms, exclusive) or undefined = up to the last point */
    to?: number;
    /** stop each state at the first value influx-v3 already stored */
    untilFirstV3: boolean;
    /** take over tags of the old points (through the tag rules and the catalog) */
    keepOldTags: boolean;
    /** lines per write request (InfluxDB 3 answers a write after its WAL flush, ~1 s - big requests are fast) */
    batchSize: number;
    /** write requests in parallel */
    parallelWrites: number;
    /** pause between time windows (ms) */
    pauseMs: number;
    /** influxdb instance whose alias settings name the source measurement, e.g. influxdb.0 */
    influxdbInstance: string;
}

const str = (v: unknown): string => (typeof v === 'string' ? v.trim() : '');

function intIn(v: unknown, def: number, min: number, max: number): number {
    const n = typeof v === 'number' ? v : typeof v === 'string' && v.trim() !== '' ? Number(v) : NaN;
    return Number.isFinite(n) ? Math.min(max, Math.max(min, Math.round(n))) : def;
}

/**
 * Parses a date of the settings: `YYYY-MM-DD` (local midnight), `YYYY-MM-DD HH:mm` or ISO 8601.
 *
 * @param v setting value
 * @returns epoch ms or undefined for empty
 */
export function parseDateSetting(v: unknown): number | undefined | null {
    const s = str(v);
    if (!s) {
        return undefined;
    }
    let m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s);
    if (m) {
        return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3])).getTime();
    }
    m = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?$/.exec(s);
    if (m) {
        return new Date(
            Number(m[1]),
            Number(m[2]) - 1,
            Number(m[3]),
            Number(m[4]),
            Number(m[5]),
            Number(m[6] ?? 0),
        ).getTime();
    }
    const t = Date.parse(s);
    return Number.isFinite(t) ? t : null;
}

/**
 * Reads the source connection and job settings from `native` (dm* attributes).
 *
 * @param native instance settings (secrets already decrypted)
 * @param warnings collector
 */
export function parseDataMigrationConfig(
    native: Record<string, unknown>,
    warnings: string[] = [],
): { source: LegacySourceConfig; job: DataMigrationSettings } {
    const version = native.dmVersion === '1.x' ? '1.x' : '2.x';
    const source: LegacySourceConfig = {
        version,
        url: str(native.dmUrl).replace(/\/+$/, ''),
        database: str(native.dmDatabase),
        retentionPolicy: str(native.dmRetentionPolicy),
        user: str(native.dmUser),
        password: typeof native.dmPassword === 'string' ? native.dmPassword : '',
        org: str(native.dmOrg),
        bucket: str(native.dmBucket),
        token: str(native.dmToken),
        rejectUnauthorized: native.dmRejectUnauthorized !== false,
        timeoutMs: intIn(native.dmTimeoutSec, 120, 5, 3600) * 1000,
        maxBytes: 64 * 1024 * 1024,
    };
    const from = parseDateSetting(native.dmFrom);
    const to = parseDateSetting(native.dmTo);
    if (from === null) {
        warnings.push(`dmFrom: "${str(native.dmFrom)}" is not a date (YYYY-MM-DD) - ignored`);
    }
    if (to === null) {
        warnings.push(`dmTo: "${str(native.dmTo)}" is not a date (YYYY-MM-DD) - ignored`);
    }
    const job: DataMigrationSettings = {
        stateFilter: str(native.dmStates),
        from: from ?? undefined,
        to: to ?? undefined,
        untilFirstV3: native.dmUntilFirstV3 !== false,
        keepOldTags: native.dmKeepOldTags !== false,
        batchSize: intIn(native.dmBatchLines, 50_000, 1000, 100_000),
        parallelWrites: intIn(native.dmParallelWrites, 2, 1, 4),
        pauseMs: intIn(native.dmPauseMs, 0, 0, 60_000),
        influxdbInstance: /^influxdb\.\d+$/.test(str(native.dmInfluxdbInstance)) ? str(native.dmInfluxdbInstance) : '',
    };
    if (job.from !== undefined && job.to !== undefined && job.from >= job.to) {
        warnings.push('dmFrom is not before dmTo - nothing would be imported');
    }
    return { source, job };
}

/**
 * Problems that prevent a start (empty list = ok).
 *
 * @param source source settings
 */
export function validateSource(source: LegacySourceConfig): string[] {
    const errors: string[] = [];
    try {
        const u = new URL(source.url);
        if (u.protocol !== 'http:' && u.protocol !== 'https:') {
            errors.push('URL of the old database must start with http:// or https://');
        }
    } catch {
        errors.push('URL of the old database is missing or invalid');
    }
    if (source.version === '1.x' && !source.database) {
        errors.push('database name (InfluxDB 1.x) is missing');
    }
    if (source.version === '2.x' && (!source.org || !source.bucket)) {
        errors.push('organisation and bucket (InfluxDB 2.x) are required');
    }
    return errors;
}

/**
 * Compiles the id filter (comma/space separated globs with `*`).
 *
 * @param filter filter text
 */
export function compileStateFilter(filter: string): (id: string) => boolean {
    const parts = filter
        .split(/[,\s]+/)
        .map(p => p.trim())
        .filter(Boolean);
    if (!parts.length) {
        return () => true;
    }
    const res = parts.map(p => new RegExp(`^${p.replace(/[.+?^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*')}$`));
    return id => res.some(r => r.test(id));
}

/**
 * Identity of a job: progress is only resumed for the same source, scope and tag handling.
 *
 * @param source source settings
 * @param job job settings
 * @param tagConfig anything that changes the written tags (rules, catalog)
 */
export function jobKey(source: LegacySourceConfig, job: DataMigrationSettings, tagConfig: unknown): string {
    const ident = [
        'time-sliced',
        source.version,
        source.url,
        source.version === '1.x' ? `${source.database}/${source.retentionPolicy}` : `${source.org}/${source.bucket}`,
        job.stateFilter,
        job.from ?? '',
        job.to ?? '',
        job.keepOldTags,
        job.influxdbInstance,
        JSON.stringify(tagConfig ?? null),
    ];
    return createHash('sha256').update(JSON.stringify(ident)).digest('hex').slice(0, 16);
}

// ------------------------------------------------------------------ checkpoint

export interface Checkpoint {
    version: 2;
    jobKey: string;
    startedAt: number;
    updatedAt: number;
    finished: boolean;
    /** planned period of the job (undefined until it is planned) */
    start?: number;
    end?: number;
    /** everything before this time is imported for every state */
    doneUntil?: number;
    /** size of the last time window (ms) */
    windowMs?: number;
    /** the whole old database is read (most of its measurements are selected) instead of a list */
    readAll?: boolean;
    /** points expected in the period (counted when planned; undefined if the old database could not count them) */
    expected?: number;
    /** the job runs or was interrupted by a restart: it continues by itself when the adapter starts */
    active?: boolean;
    /** per state: end of its import (first live value of this instance, or the end of the job) */
    ends: Record<string, number>;
    /**
     * Per state: time of the first value influx-v3 had stored, found by older versions. Kept across jobs and resets
     * (newer versions use the live-start record of the adapter instead).
     */
    liveStart: Record<string, number>;
    totals: { points: number; errors: number; skipped: number };
}

export interface CheckpointStore {
    load(): Promise<Checkpoint | undefined>;
    save(cp: Checkpoint): Promise<void>;
}

/** Checkpoint as JSON file (written to a temporary file and renamed) */
export class FileCheckpointStore implements CheckpointStore {
    constructor(private readonly file: string) {}

    async load(): Promise<Checkpoint | undefined> {
        type Stored = Omit<Partial<Checkpoint>, 'version'> & { version?: number };
        let cp: Stored | undefined;
        try {
            cp = JSON.parse(await fsp.readFile(this.file, 'utf8')) as Stored;
        } catch {
            return undefined;
        }
        if (cp?.version === 2 && typeof cp.ends === 'object') {
            return cp as Checkpoint;
        }
        if (cp?.version === 1) {
            // per-state progress of older versions cannot be resumed - only their live-start times are kept
            return { ...newCheckpoint('v1', cp.updatedAt ?? 0, { liveStart: cp.liveStart ?? {} }), finished: true };
        }
        return undefined;
    }

    async save(cp: Checkpoint): Promise<void> {
        await fsp.mkdir(path.dirname(this.file), { recursive: true });
        const tmp = `${this.file}.tmp`;
        await fsp.writeFile(tmp, JSON.stringify(cp), 'utf8');
        await fsp.rename(tmp, this.file);
    }
}

/**
 * Empty checkpoint for a job (keeps the live-start times of an older checkpoint).
 *
 * @param key job key
 * @param now current time
 * @param previous older checkpoint
 */
export function newCheckpoint(key: string, now: number, previous?: Pick<Checkpoint, 'liveStart'>): Checkpoint {
    return {
        version: 2,
        jobKey: key,
        startedAt: now,
        updatedAt: now,
        finished: false,
        ends: {},
        liveStart: { ...(previous?.liveStart ?? {}) },
        totals: { points: 0, errors: 0, skipped: 0 },
    };
}

// ------------------------------------------------------------------ plan

/** One state to import */
export interface ImportItem {
    id: string;
    /** measurement in the old database */
    measurement: string;
    /** target table (for messages) */
    table: string;
    /**
     * not configured for this instance yet (only the old adapter logs it): converted with the mapping the settings
     * migration would give it; it has no live values to stop at
     */
    pending?: boolean;
    /** converts old points into line protocol */
    convert(points: LegacyPoint[]): { lines: string[]; skipped: number };
}

export interface ImportPlan {
    /** oldest point to import, undefined when the old database has none for the selection */
    start?: number;
    end: number;
    /** end per state */
    ends: Record<string, number>;
    /** the whole old database is read and filtered here */
    readAll: boolean;
    /** states that end at a known first live value */
    knownLive: number;
    /** points in the period (all selected states), undefined when the old database could not count them */
    expected?: number;
}

/**
 * Plans an import: the period, the end of each state and how the old database is read. Nothing is asked from
 * InfluxDB 3 - the first live value of a state comes from the adapter's own record.
 *
 * @param items states
 * @param source old database
 * @param settings job settings
 * @param now start of the job
 * @param liveStartOf first live value of a state recorded by the adapter
 * @param oldLiveStart first values found by older versions (checkpoint)
 * @param countWithin how long counting the points may take (ms); the count is only an estimate
 */
export async function planImport(
    items: readonly ImportItem[],
    source: LegacyReader,
    settings: DataMigrationSettings,
    now: number,
    liveStartOf: (id: string) => number | undefined,
    oldLiveStart: Readonly<Record<string, number>> = {},
    countWithin = Number.POSITIVE_INFINITY,
): Promise<ImportPlan> {
    const wanted = [...new Set(items.map(i => i.measurement))];
    const all = await source.listMeasurements(LIST_LIMIT).catch(() => [] as string[]);
    const wantedSet = new Set(wanted);
    const covered = all.filter(m => wantedSet.has(m)).length;
    const readAll = all.length > 0 && all.length < LIST_LIMIT && covered >= READ_ALL_SHARE * all.length;
    const ends: Record<string, number> = {};
    let knownLive = 0;
    let end = Number.NEGATIVE_INFINITY;
    for (const item of items) {
        let e = settings.to ?? now;
        if (settings.untilFirstV3 && !item.pending) {
            const candidates = [oldLiveStart[item.id], liveStartOf(item.id)].filter(
                (t): t is number => typeof t === 'number' && Number.isFinite(t),
            );
            if (candidates.length) {
                knownLive++;
                e = Math.min(e, ...candidates);
            }
        }
        ends[item.id] = Math.floor(e);
        end = Math.max(end, ends[item.id]);
    }
    let start = settings.from;
    if (start === undefined && items.length) {
        start = await source.firstTime(readAll ? 'all' : wanted);
    }
    const planEnd = Number.isFinite(end) ? end : Math.floor(now);
    let expected: number | undefined;
    if (start !== undefined && start < planEnd) {
        // an estimate only: a failing or slow count never stops the import
        const counting = source.countPoints(readAll ? 'all' : wanted, start, planEnd).catch(() => undefined);
        expected = Number.isFinite(countWithin)
            ? await Promise.race([
                  counting,
                  new Promise<undefined>(r => setTimeout(() => r(undefined), countWithin).unref()),
              ])
            : await counting;
    }
    return {
        start: start === undefined ? undefined : Math.floor(start),
        end: planEnd,
        ends,
        readAll,
        knownLive,
        expected,
    };
}

// ------------------------------------------------------------------ job

export interface ImportTarget {
    write(lines: string[]): Promise<WriteResult>;
    /** true while live logging needs the connection (queue filling up) */
    busy(): boolean;
}

export interface Progress {
    running: boolean;
    percent: number;
    status: string;
    points: number;
    errors: number;
    current: string;
}

export interface JobDeps {
    source: LegacyReader;
    target: ImportTarget;
    store: CheckpointStore;
    now: () => number;
    /** resolves after ms or when the signal aborts */
    sleep: (ms: number, signal: AbortSignal) => Promise<void>;
    log: { info(m: string): void; warn(m: string): void; debug(m: string): void };
    onProgress: (p: Progress) => void;
    /** first live value this instance queued for a state (adapter record) */
    liveStart: (id: string) => number | undefined;
}

export type JobOutcome = 'finished' | 'stopped' | 'failed';

export class FatalImportError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'FatalImportError';
    }
}

/**
 * Splits lines into write requests by count and size.
 *
 * @param lines line protocol
 * @param maxLines lines per request
 * @param maxBytes bytes per request
 */
export function chunkLines(lines: string[], maxLines: number, maxBytes = MAX_WRITE_BYTES): string[][] {
    const out: string[][] = [];
    for (let i = 0; i < lines.length;) {
        const end = chunkEnd(lines, i, maxLines, maxBytes);
        out.push(lines.slice(i, end));
        i = end;
    }
    return out;
}

/**
 * End (exclusive) of the next write request starting at `start`.
 *
 * @param lines line protocol
 * @param start first line
 * @param maxLines lines per request
 * @param maxBytes bytes per request
 * @param limit end of the lines to split (exclusive)
 */
function chunkEnd(lines: string[], start: number, maxLines: number, maxBytes: number, limit = lines.length): number {
    let bytes = 0;
    let i = start;
    while (i < limit && i - start < maxLines) {
        const b = Buffer.byteLength(lines[i]) + 1;
        if (i > start && bytes + b > maxBytes) {
            break;
        }
        bytes += b;
        i++;
    }
    return i;
}

/**
 * Next window size after a window of `current` ms returned `rows` rows (all states together).
 *
 * @param current size of the window just read (ms)
 * @param rows rows of that window
 */
export function adaptWindow(current: number, rows: number): number {
    if (rows > HIGH_ROWS) {
        return Math.max(MIN_WINDOW, Math.floor(current / 2));
    }
    if (rows < LOW_ROWS) {
        return Math.min(MAX_WINDOW, current * 2);
    }
    return Math.min(MAX_WINDOW, Math.max(MIN_WINDOW, current));
}

/**
 * Short duration text, e.g. "2 h 05 min".
 *
 * @param ms duration
 */
export function formatDuration(ms: number): string {
    const min = Math.max(1, Math.round(ms / 60_000));
    if (min < 60) {
        return `${min} min`;
    }
    const h = Math.floor(min / 60);
    return h < 48 ? `${h} h ${String(min % 60).padStart(2, '0')} min` : `${Math.round(h / 24)} days`;
}

const minuteText = (t: number): string => new Date(t).toISOString().replace('T', ' ').slice(0, 16);

type ReadResult = { from: number; to: number; points?: Map<string, LegacyPoint[]>; error?: unknown };

export class DataMigrationJob {
    private cp!: Checkpoint;
    private progress: Progress = { running: false, percent: 0, status: '', points: 0, errors: 0, current: '' };
    private lastSave = 0;
    private batch: number;
    private okWrites = 0;

    constructor(
        private readonly deps: JobDeps,
        private readonly settings: DataMigrationSettings,
    ) {
        this.batch = settings.batchSize;
    }

    private report(p: Partial<Progress>): void {
        this.progress = { ...this.progress, ...p };
        this.deps.onProgress(this.progress);
    }

    private async save(force = false): Promise<void> {
        const now = this.deps.now();
        if (!force && now - this.lastSave < 1000) {
            return;
        }
        this.lastSave = now;
        this.cp.updatedAt = now;
        await this.deps.store.save(this.cp);
    }

    /**
     * Runs (or resumes) the import.
     *
     * @param items states to import
     * @param key job key ({@link jobKey})
     * @param signal stops the job (progress is kept)
     */
    async run(items: ImportItem[], key: string, signal: AbortSignal): Promise<JobOutcome> {
        const previous = await this.deps.store.load();
        if (previous && previous.jobKey === key && !previous.finished && previous.start !== undefined) {
            this.cp = previous;
            this.deps.log.info(
                `Data import: resuming at ${minuteText(previous.doneUntil ?? previous.start)} (${previous.totals.points} point(s) imported before)`,
            );
        } else {
            if (previous && previous.jobKey !== key && !previous.finished) {
                this.deps.log.info('Data import: settings changed since the last run - starting a new import');
            }
            this.cp = newCheckpoint(key, this.deps.now(), previous);
        }
        this.cp.active = true;
        this.report({
            running: true,
            percent: 0,
            status: 'starting',
            points: this.cp.totals.points,
            errors: this.cp.totals.errors,
        });
        try {
            if (this.cp.start === undefined) {
                this.report({ status: 'planning: looking for the oldest point in the old database' });
                const plan = await this.patiently(
                    () =>
                        planImport(
                            items,
                            this.deps.source,
                            this.settings,
                            this.deps.now(),
                            this.deps.liveStart,
                            this.cp.liveStart,
                            COUNT_WITHIN_MS,
                        ),
                    signal,
                    'the old database',
                );
                if (signal.aborted) {
                    return this.stopped(signal);
                }
                this.cp.start = plan.start ?? plan.end;
                this.cp.end = plan.end;
                this.cp.ends = plan.ends;
                this.cp.readAll = plan.readAll;
                this.cp.expected = plan.expected;
                this.cp.doneUntil = this.cp.start;
                await this.save(true);
                this.deps.log.info(
                    plan.start === undefined
                        ? 'Data import: the old database has no points for the selected states'
                        : `Data import: ${items.length} state(s), ${minuteText(plan.start)} … ${minuteText(plan.end)} UTC${plan.expected !== undefined ? `, about ${plan.expected} point(s)` : ''}, read in time slices${plan.readAll ? ' (whole old database)' : ''}; ${plan.knownLive} state(s) end at their first live value`,
                );
            }
            await this.walk(items, signal);
        } catch (e) {
            this.cp.active = false;
            await this.save(true);
            const message = e instanceof Error ? e.message : redact(e);
            this.report({ running: false, status: `failed: ${message}` });
            this.deps.log.warn(`Data import failed: ${message} - the progress is saved, "Start" continues`);
            return 'failed';
        }
        if (signal.aborted) {
            return this.stopped(signal);
        }
        this.cp.finished = true;
        this.cp.active = false;
        await this.save(true);
        const t = this.cp.totals;
        const text = `finished: ${t.points} point(s) imported for ${items.length} state(s)${t.errors ? `, ${t.errors} rejected` : ''}${t.skipped ? `, ${t.skipped} not convertible` : ''}`;
        this.report({ running: false, percent: 100, status: text, current: '' });
        this.deps.log.info(`Data import ${text}`);
        return 'finished';
    }

    private async stopped(signal: AbortSignal): Promise<JobOutcome> {
        const pause = signal.reason === PAUSE;
        this.cp.active = pause;
        await this.save(true);
        const at = this.cp.doneUntil ?? this.cp.start;
        this.report({
            running: false,
            status: `${pause ? 'paused - continues after the restart' : 'stopped'}${at !== undefined ? ` at ${minuteText(at)} UTC` : ''}`,
        });
        return 'stopped';
    }

    /** Walks the time axis: one window of all states at a time; the next window is read while one is written. */
    private async walk(items: ImportItem[], signal: AbortSignal): Promise<void> {
        const start = this.cp.start ?? 0;
        const end = this.cp.end ?? start;
        const byMeasurement = new Map<string, ImportItem[]>();
        for (const item of items) {
            byMeasurement.set(item.measurement, [...(byMeasurement.get(item.measurement) ?? []), item]);
        }
        const endOf = (item: ImportItem): number => this.cp.ends[item.id] ?? end;
        const readAll = !!this.cp.readAll;
        const startRead = (from: number, to: number): Promise<ReadResult> => {
            const set = readAll ? 'all' : [...new Set(items.filter(i => endOf(i) > from).map(i => i.measurement))];
            if (set !== 'all' && !set.length) {
                return Promise.resolve({ from, to, points: new Map() });
            }
            return this.deps.source.readMany(set, from, to).then(
                points => ({ from, to, points }),
                (error: unknown) => ({ from, to, error }),
            );
        };
        const t = this.cp.doneUntil ?? start;
        let windowMs = this.cp.windowMs ?? START_WINDOW;
        const began = { at: this.deps.now(), t, points: this.cp.totals.points };
        let outageSince: number | undefined;
        let retryDelay = 1000;
        let rate = 0;
        let pending: Promise<ReadResult> | undefined = t < end ? startRead(t, Math.min(end, t + windowMs)) : undefined;
        while (pending) {
            const r = await pending;
            pending = undefined;
            if (signal.aborted) {
                return;
            }
            if (r.error !== undefined) {
                const kind = r.error instanceof LegacyReadError ? r.error.kind : undefined;
                const message = r.error instanceof Error ? r.error.message : redact(r.error);
                if ((kind === 'tooBig' || kind === 'timeout' || kind === 'server') && r.to - r.from > MIN_WINDOW) {
                    windowMs = Math.max(MIN_WINDOW, Math.floor((r.to - r.from) / 4));
                    this.deps.log.debug(`Data import: ${message} - smaller time window (${windowMs} ms)`);
                    pending = startRead(r.from, Math.min(end, r.from + windowMs));
                    continue;
                }
                if (kind === 'transport' || kind === 'timeout' || kind === 'server') {
                    // waits as long as it takes (a restart, a night of maintenance) - "Stop" ends it
                    outageSince ??= this.deps.now();
                    this.report({
                        status: `waiting for the old database since ${minuteText(outageSince)} UTC (${message}) - continues by itself`,
                    });
                    await this.deps.sleep(retryDelay, signal);
                    retryDelay = Math.min(MAX_RETRY_DELAY, retryDelay * 2);
                    pending = startRead(r.from, r.to);
                    continue;
                }
                throw r.error instanceof Error ? r.error : new Error(message);
            }
            outageSince = undefined;
            retryDelay = 1000;
            const points = r.points ?? new Map<string, LegacyPoint[]>();
            let rows = 0;
            for (const list of points.values()) {
                rows += list.length;
            }
            windowMs = adaptWindow(r.to - r.from, rows);
            if (r.to < end) {
                pending = startRead(r.to, Math.min(end, r.to + windowMs)); // read ahead while this window is written
            }
            const lines: string[] = [];
            for (const [measurement, pts] of points) {
                for (const item of byMeasurement.get(measurement) ?? []) {
                    const itemEnd = endOf(item);
                    const own = pts.filter(p => p.ts >= r.from && p.ts < itemEnd);
                    if (!own.length) {
                        continue;
                    }
                    const conv = item.convert(own);
                    this.cp.totals.skipped += conv.skipped;
                    for (const l of conv.lines) {
                        lines.push(l);
                    }
                }
            }
            if (lines.length) {
                await this.writeLines(lines, signal);
            }
            if (signal.aborted) {
                return; // the window is read and written again on resume (idempotent)
            }
            this.cp.totals.points += lines.length;
            this.cp.doneUntil = r.to;
            this.cp.windowMs = windowMs;
            await this.save();
            const elapsed = this.deps.now() - began.at;
            const done = this.cp.totals.points - began.points;
            rate = elapsed > 0 ? Math.round((done * 1000) / elapsed) : rate;
            // with a point count the progress follows the points (early years often have fewer states), else the time
            const expected = this.cp.expected ?? 0;
            const byPoints = expected > 0;
            const fraction = byPoints
                ? Math.min(0.999, this.cp.totals.points / expected)
                : end > start
                  ? (r.to - start) / (end - start)
                  : 1;
            let left: number | undefined;
            if (elapsed >= 5000 && done > 0) {
                if (byPoints) {
                    left = (Math.max(0, expected - this.cp.totals.points) * elapsed) / done;
                } else if (r.to > began.t) {
                    left = ((end - r.to) * elapsed) / (r.to - began.t);
                }
            }
            this.report({
                points: this.cp.totals.points,
                errors: this.cp.totals.errors,
                percent: Math.min(100, Math.floor(fraction * 1000) / 10),
                current: minuteText(r.to),
                status: `${minuteText(r.to)} UTC · ${this.cp.totals.points}${byPoints ? ` of about ${expected}` : ''} point(s) · ${rate} points/s${left !== undefined ? ` · about ${formatDuration(left)} left` : ''}`,
            });
            if (this.settings.pauseMs) {
                await this.deps.sleep(this.settings.pauseMs, signal);
            }
        }
    }

    /**
     * Runs a step against a server and waits patiently while it is not reachable (transport, timeout, HTTP 5xx).
     *
     * @param fn step
     * @param signal stop
     * @param what server name for messages
     */
    private async patiently<T>(fn: () => Promise<T>, signal: AbortSignal, what: string): Promise<T> {
        const since = this.deps.now();
        let delay = 1000;
        for (;;) {
            try {
                return await fn();
            } catch (e) {
                const transient =
                    e instanceof LegacyReadError &&
                    (e.kind === 'transport' || e.kind === 'timeout' || e.kind === 'server');
                if (!transient || signal.aborted) {
                    throw e;
                }
                this.report({
                    status: `waiting for ${what} since ${minuteText(since)} UTC (${e.message}) - continues by itself`,
                });
                await this.deps.sleep(delay, signal);
                delay = Math.min(MAX_RETRY_DELAY, delay * 2);
            }
        }
    }

    /** Writes the lines of a window with up to `parallelWrites` requests at a time (their order does not matter). */
    private async writeLines(lines: string[], signal: AbortSignal): Promise<void> {
        const queue: [number, number][] = [];
        const split = (from: number, to: number): void => {
            for (let i = from; i < to;) {
                const e = chunkEnd(lines, i, this.batch, MAX_WRITE_BYTES, to);
                queue.push([i, e]);
                i = e;
            }
        };
        split(0, lines.length);
        const worker = async (): Promise<void> => {
            for (let r = queue.shift(); r && !signal.aborted; r = queue.shift()) {
                if (!(await this.writeChunk(lines.slice(r[0], r[1]), signal))) {
                    split(r[0], r[1]); // smaller requests (the batch size was reduced)
                }
            }
        };
        await Promise.all(Array.from({ length: Math.max(1, this.settings.parallelWrites) }, () => worker()));
    }

    /** @returns false when the chunk must be sent again in smaller parts (the batch size was reduced) */
    private async writeChunk(chunk: string[], signal: AbortSignal): Promise<boolean> {
        let delay = 1000;
        let outageSince: number | undefined;
        for (;;) {
            // live logging has priority: wait while its queue fills up
            while (this.deps.target.busy() && !signal.aborted) {
                await this.deps.sleep(1000, signal);
            }
            if (signal.aborted) {
                return true;
            }
            const res = await this.deps.target.write(chunk);
            switch (res.kind) {
                case 'ok':
                    if (++this.okWrites >= 20 && this.batch < this.settings.batchSize) {
                        this.batch = Math.min(this.settings.batchSize, this.batch * 2);
                        this.okWrites = 0;
                    }
                    return true;
                case 'partial':
                    this.cp.totals.errors += res.rejected.length;
                    this.deps.log.warn(
                        `Data import: ${res.rejected.length} point(s) rejected by InfluxDB 3 (${redact(res.message).slice(0, 300)})`,
                    );
                    return true;
                case 'invalid':
                    this.cp.totals.errors += chunk.length;
                    this.deps.log.warn(
                        `Data import: ${chunk.length} point(s) rejected by InfluxDB 3 (${redact(res.message).slice(0, 300)})`,
                    );
                    return true;
                case 'tooLarge':
                    if (chunk.length === 1) {
                        this.cp.totals.errors++;
                        return true;
                    }
                    this.batch = Math.max(1, Math.floor(chunk.length / 2));
                    return false;
                case 'auth':
                case 'config':
                    throw new FatalImportError(`InfluxDB 3 refused the write: ${res.message}`);
                case 'transient':
                    if (/timeout|timed out/i.test(res.message) && chunk.length > MIN_BATCH) {
                        // a slow server: smaller requests (points written twice overwrite each other)
                        this.batch = Math.max(MIN_BATCH, Math.floor(chunk.length / 2));
                        this.okWrites = 0;
                        this.deps.log.debug(`Data import: ${res.message} - batch size ${this.batch}`);
                        return false;
                    }
                    // waits as long as it takes (a restart of InfluxDB 3) - "Stop" ends it
                    outageSince ??= this.deps.now();
                    this.report({
                        status: `waiting for InfluxDB 3 since ${minuteText(outageSince)} UTC (${res.message}) - continues by itself`,
                    });
                    await this.deps.sleep(Math.max(delay, res.retryAfterMs ?? 0), signal);
                    if (signal.aborted) {
                        return true;
                    }
                    delay = Math.min(MAX_RETRY_DELAY, delay * 2);
            }
        }
    }
}

// ------------------------------------------------------------------ preview

/** Owner of an old measurement: the ioBroker state whose influxdb settings wrote it (alias, else the id) */
export interface MeasurementOwner {
    id: string;
    /** influxdb instance, e.g. influxdb.0 */
    instance: string;
    /** why the state is not imported (state filter, other influxdb instance, mapping conflict, …) */
    reason?: string;
}

/**
 * Explains the old measurements that are not selected for the import, grouped by the reason (state filter, other
 * influxdb instance, mapping conflict, …), and those that belong to no known state.
 *
 * @param others measurements of the old database that are not selected
 * @param owners measurement → owner (from the influxdb settings)
 */
export function explainUnselected(others: readonly string[], owners: ReadonlyMap<string, MeasurementOwner>): string[] {
    if (!others.length) {
        return [];
    }
    const example = (list: string[], show: (m: string) => string): string =>
        `${list.slice(0, 8).map(show).join(', ')}${list.length > 8 ? ', …' : ''}`;
    const byReason = new Map<string, string[]>(); // reason → measurements
    const unknown: string[] = [];
    for (const m of others) {
        const o = owners.get(m);
        if (!o) {
            unknown.push(m);
        } else {
            const reason = o.reason || 'not selected';
            byReason.set(reason, [...(byReason.get(reason) ?? []), m]);
        }
    }
    const show = (m: string): string => {
        const o = owners.get(m);
        return o && o.id !== m ? `${m} (${o.id})` : m;
    };
    const lines = ['', `${others.length} measurement(s) of the old database are not selected (not imported):`];
    for (const [reason, list] of [...byReason.entries()].sort((a, b) => b[1].length - a[1].length)) {
        lines.push(`  ${list.length} - ${reason}, e.g.: ${example(list, show)}`);
    }
    if (unknown.length) {
        lines.push(
            `  ${unknown.length} - belong to no state of the influxdb settings (state deleted, logging disabled or alias changed), e.g.: ${example(unknown, m => m)}`,
        );
    }
    return lines;
}

export interface PreviewContext {
    source: LegacyReader;
    settings: DataMigrationSettings;
    /** saved progress (for the resume hint and old live-start times) */
    checkpoint?: Checkpoint;
    key: string;
    now: number;
    /** first live value this instance queued for a state */
    liveStart: (id: string) => number | undefined;
    /** old measurement → state of the influxdb settings, with the reason it is not imported */
    owners?: ReadonlyMap<string, MeasurementOwner>;
    /** this instance, e.g. influx-v3.0 */
    target?: string;
    /** states shown with sample records */
    detailed?: number;
    /** sample records per state */
    samples?: number;
    /** how long the preview waits for the point count (ms) */
    countWithin?: number;
}

/**
 * Dry run: what the job would do. Nothing is written and InfluxDB 3 is not queried.
 *
 * @param items states
 * @param ctx source, settings and limits
 */
export async function previewImport(items: ImportItem[], ctx: PreviewContext): Promise<string> {
    const target = ctx.target ?? 'this instance';
    const detailed = ctx.detailed ?? 15;
    const samples = ctx.samples ?? 2;
    const lines: string[] = [];
    const pending = items.filter(i => i.pending).length;
    if (!items.length) {
        lines.push(
            `No state is selected for the import: neither ${target} nor the influxdb adapter logs a state that matches the selection.`,
        );
    } else if (!pending) {
        lines.push(`${items.length} state(s) of ${target} are selected for the import.`);
    } else {
        lines.push(
            `${items.length} state(s) are selected for the import: ${items.length - pending} configured for ${target}, ${pending} logged only by the influxdb adapter so far.`,
            `The ${pending} state(s) not configured for ${target} yet are imported with the table and tags that "Apply" in tab "Migration of settings" would give them (same tag rules and catalog).`,
            'Tip: apply the settings migration before the import - then each state is imported exactly up to its first live value.',
        );
    }
    const cp = ctx.checkpoint;
    if (cp && cp.jobKey === ctx.key && !cp.finished && cp.doneUntil !== undefined) {
        lines.push(
            `A started import with these settings exists (up to ${minuteText(cp.doneUntil)} UTC, ${cp.totals.points} point(s)) - "Start" continues it.`,
        );
    } else if (cp && cp.jobKey === ctx.key && cp.finished) {
        lines.push(
            'An import with these settings is already finished - "Start" does nothing until you reset the progress.',
        );
    } else if (cp && !cp.finished && cp.start !== undefined) {
        lines.push('A started import with OTHER settings exists - "Start" begins a new import from the beginning.');
    }
    if (items.length) {
        const plan = await planImport(
            items,
            ctx.source,
            ctx.settings,
            ctx.now,
            ctx.liveStart,
            cp?.liveStart,
            ctx.countWithin ?? 30_000,
        );
        if (plan.start === undefined || plan.start >= plan.end) {
            lines.push('', 'The old database has no points for the selected states in the selected period.');
        } else {
            const days = Math.max(1, Math.round((plan.end - plan.start) / DAY));
            lines.push(
                '',
                `Period: ${minuteText(plan.start)} … ${minuteText(plan.end)} UTC (about ${days} day(s)), read in time slices for all ${items.length} state(s) together${plan.readAll ? ' (the whole old database is read, other measurements are skipped)' : ''}.`,
                plan.expected !== undefined
                    ? `About ${plan.expected} point(s) to import - the progress shows the speed and the time left.`
                    : 'The points were not counted in time - the progress shows the time left once the import runs.',
            );
            if (ctx.settings.untilFirstV3) {
                const unknown = items.length - plan.knownLive;
                lines.push(
                    `End per state: ${plan.knownLive} state(s) at their first value logged by ${target}${unknown ? `, ${unknown} at the start of the import (points logged by both adapters have the same time and overwrite each other)` : ''}.`,
                );
            }
            // sample records from the newest hour of the period
            const shown = items.slice(0, detailed);
            const from = Math.max(plan.start, plan.end - HOUR);
            const sample = await ctx.source.readMany([...new Set(shown.map(i => i.measurement))], from, plan.end);
            let empty = 0;
            for (const item of shown) {
                const pts = (sample.get(item.measurement) ?? []).filter(p => p.ts < (plan.ends[item.id] ?? plan.end));
                lines.push(
                    '',
                    `• ${item.id}  (old measurement "${item.measurement}" → table "${item.table}")${item.pending ? `  - not configured for ${target} yet` : ''}`,
                );
                if (ctx.settings.untilFirstV3 && !item.pending && plan.ends[item.id] < (ctx.settings.to ?? ctx.now)) {
                    lines.push(`    ends at its first live value: ${minuteText(plan.ends[item.id])} UTC`);
                }
                if (!pts.length) {
                    empty++;
                    lines.push('    no point in the newest hour of the period');
                    continue;
                }
                const conv = item.convert(pts.slice(-samples));
                for (const l of conv.lines) {
                    lines.push(`    ${l.length > 220 ? `${l.slice(0, 220)}…` : l}`);
                }
                if (conv.skipped) {
                    lines.push(
                        `    ${conv.skipped} of the last ${samples} point(s) cannot be converted to the target type`,
                    );
                }
            }
            if (items.length > shown.length) {
                lines.push('', `… ${items.length - shown.length} more state(s) not shown.`);
            }
            if (empty === shown.length && shown.length) {
                lines.push('', 'None of the shown states has points in the newest hour - check the measurement names.');
            }
        }
    }
    try {
        const all = await ctx.source.listMeasurements(LIST_LIMIT);
        const known = new Set(items.map(i => i.measurement));
        lines.push(
            ...explainUnselected(
                all.filter(m => !known.has(m)),
                ctx.owners ?? new Map(),
            ),
        );
    } catch (e) {
        lines.push('', `List of old measurements not available: ${e instanceof Error ? e.message : redact(e)}`);
    }
    lines.push('', 'Nothing has been written. The old database is only read.');
    return lines.join('\n');
}
