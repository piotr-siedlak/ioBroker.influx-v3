"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DataMigrationJob = exports.FatalImportError = exports.FileCheckpointStore = exports.MAX_WRITE_BYTES = exports.LOW_ROWS = exports.HIGH_ROWS = exports.START_WINDOW = exports.MAX_WINDOW = exports.MIN_WINDOW = exports.DAY = void 0;
exports.parseDateSetting = parseDateSetting;
exports.parseDataMigrationConfig = parseDataMigrationConfig;
exports.validateSource = validateSource;
exports.compileStateFilter = compileStateFilter;
exports.jobKey = jobKey;
exports.newCheckpoint = newCheckpoint;
exports.chunkLines = chunkLines;
exports.adaptWindow = adaptWindow;
exports.explainUnselected = explainUnselected;
exports.previewImport = previewImport;
/**
 * Import of historical data from the database of the classic influxdb adapter (ADR-0006).
 *
 * The job reads each state in adaptive time windows from the source (read-only), converts the points with
 * the same mapping as live logging (done by the caller in `ImportItem.convert`) and writes them directly
 * into InfluxDB 3. Progress is saved after each window, so a stopped or interrupted job resumes where it
 * stopped; re-written windows are idempotent (same table, tags and time overwrite each other).
 *
 * The module has no timers or I/O of its own: source, target, checkpoint store and clock are injected.
 */
const node_crypto_1 = require("node:crypto");
const node_fs_1 = require("node:fs");
const node_path_1 = __importDefault(require("node:path"));
const legacySource_1 = require("./legacySource");
const redact_1 = require("./redact");
exports.DAY = 24 * 3600 * 1000;
/** smallest / largest / first time window */
exports.MIN_WINDOW = 60 * 1000;
exports.MAX_WINDOW = 90 * exports.DAY;
exports.START_WINDOW = exports.DAY;
/** rows per window: above → halve the next window, below → double it */
exports.HIGH_ROWS = 50_000;
exports.LOW_ROWS = 10_000;
/** maximum bytes of one write request */
exports.MAX_WRITE_BYTES = 8 * 1024 * 1024;
const MAX_RETRIES = 10;
const str = (v) => (typeof v === 'string' ? v.trim() : '');
function intIn(v, def, min, max) {
    const n = typeof v === 'number' ? v : typeof v === 'string' && v.trim() !== '' ? Number(v) : NaN;
    return Number.isFinite(n) ? Math.min(max, Math.max(min, Math.round(n))) : def;
}
/**
 * Parses a date of the settings: `YYYY-MM-DD` (local midnight), `YYYY-MM-DD HH:mm` or ISO 8601.
 *
 * @param v setting value
 * @returns epoch ms or undefined for empty
 */
function parseDateSetting(v) {
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
        return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]), Number(m[4]), Number(m[5]), Number(m[6] ?? 0)).getTime();
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
function parseDataMigrationConfig(native, warnings = []) {
    const version = native.dmVersion === '1.x' ? '1.x' : '2.x';
    const source = {
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
    const job = {
        stateFilter: str(native.dmStates),
        from: from ?? undefined,
        to: to ?? undefined,
        untilFirstV3: native.dmUntilFirstV3 !== false,
        keepOldTags: native.dmKeepOldTags !== false,
        batchSize: intIn(native.dmBatchSize, 5000, 100, 50_000),
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
function validateSource(source) {
    const errors = [];
    try {
        const u = new URL(source.url);
        if (u.protocol !== 'http:' && u.protocol !== 'https:') {
            errors.push('URL of the old database must start with http:// or https://');
        }
    }
    catch {
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
function compileStateFilter(filter) {
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
function jobKey(source, job, tagConfig) {
    const ident = [
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
    return (0, node_crypto_1.createHash)('sha256').update(JSON.stringify(ident)).digest('hex').slice(0, 16);
}
/** Checkpoint as JSON file (written to a temporary file and renamed) */
class FileCheckpointStore {
    file;
    constructor(file) {
        this.file = file;
    }
    async load() {
        try {
            const cp = JSON.parse(await node_fs_1.promises.readFile(this.file, 'utf8'));
            return cp && cp.version === 1 && typeof cp.states === 'object' ? cp : undefined;
        }
        catch {
            return undefined;
        }
    }
    async save(cp) {
        await node_fs_1.promises.mkdir(node_path_1.default.dirname(this.file), { recursive: true });
        const tmp = `${this.file}.tmp`;
        await node_fs_1.promises.writeFile(tmp, JSON.stringify(cp), 'utf8');
        await node_fs_1.promises.rename(tmp, this.file);
    }
}
exports.FileCheckpointStore = FileCheckpointStore;
/**
 * Empty checkpoint for a job (keeps the live-start times of an older checkpoint).
 *
 * @param key job key
 * @param now current time
 * @param previous older checkpoint
 */
function newCheckpoint(key, now, previous) {
    return {
        version: 1,
        jobKey: key,
        startedAt: now,
        updatedAt: now,
        finished: false,
        states: {},
        liveStart: { ...(previous?.liveStart ?? {}) },
        totals: { points: 0, errors: 0, skipped: 0 },
    };
}
class FatalImportError extends Error {
    constructor(message) {
        super(message);
        this.name = 'FatalImportError';
    }
}
exports.FatalImportError = FatalImportError;
/**
 * Splits lines into write requests by count and size.
 *
 * @param lines line protocol
 * @param maxLines lines per request
 * @param maxBytes bytes per request
 */
function chunkLines(lines, maxLines, maxBytes = exports.MAX_WRITE_BYTES) {
    const out = [];
    let cur = [];
    let bytes = 0;
    for (const l of lines) {
        const b = Buffer.byteLength(l) + 1;
        if (cur.length && (cur.length >= maxLines || bytes + b > maxBytes)) {
            out.push(cur);
            cur = [];
            bytes = 0;
        }
        cur.push(l);
        bytes += b;
    }
    if (cur.length) {
        out.push(cur);
    }
    return out;
}
/**
 * Next window size after a window returned `rows` rows.
 *
 * @param current current size (ms)
 * @param rows rows of the last window
 */
function adaptWindow(current, rows) {
    if (rows > exports.HIGH_ROWS) {
        return Math.max(exports.MIN_WINDOW, Math.floor(current / 2));
    }
    if (rows < exports.LOW_ROWS) {
        return Math.min(exports.MAX_WINDOW, current * 2);
    }
    return current;
}
class DataMigrationJob {
    deps;
    settings;
    cp;
    progress = { running: false, percent: 0, status: '', points: 0, errors: 0, current: '' };
    lastSave = 0;
    constructor(deps, settings) {
        this.deps = deps;
        this.settings = settings;
    }
    report(p) {
        this.progress = { ...this.progress, ...p };
        this.deps.onProgress(this.progress);
    }
    async save(force = false) {
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
    async run(items, key, signal) {
        const previous = await this.deps.store.load();
        if (previous && previous.jobKey === key && !previous.finished) {
            this.cp = previous;
            this.deps.log.info(`Data import: resuming (${Object.keys(previous.states).length} state(s) started before)`);
        }
        else {
            if (previous && previous.jobKey !== key && !previous.finished) {
                this.deps.log.info('Data import: settings changed since the last run - starting a new import');
            }
            this.cp = newCheckpoint(key, this.deps.now(), previous);
        }
        this.report({
            running: true,
            percent: 0,
            status: 'starting',
            points: this.cp.totals.points,
            errors: this.cp.totals.errors,
        });
        let index = 0;
        try {
            for (const item of items) {
                if (signal.aborted) {
                    break;
                }
                await this.importItem(item, index, items.length, signal);
                index++;
            }
        }
        catch (e) {
            await this.save(true);
            const message = e instanceof Error ? e.message : (0, redact_1.redact)(e);
            this.report({ running: false, status: `failed: ${message}` });
            this.deps.log.warn(`Data import failed: ${message} - the progress is saved, "Start" continues`);
            return 'failed';
        }
        if (signal.aborted) {
            await this.save(true);
            this.report({ running: false, status: `stopped (${index}/${items.length} state(s) done)` });
            return 'stopped';
        }
        this.cp.finished = true;
        await this.save(true);
        const t = this.cp.totals;
        const text = `finished: ${t.points} point(s) imported for ${items.length} state(s)${t.errors ? `, ${t.errors} rejected` : ''}${t.skipped ? `, ${t.skipped} not convertible` : ''}`;
        this.report({ running: false, percent: 100, status: text, current: '' });
        this.deps.log.info(`Data import ${text}`);
        return 'finished';
    }
    overall(index, total, fraction) {
        return total
            ? Math.min(100, Math.floor(((index + Math.min(1, Math.max(0, fraction))) / total) * 1000) / 10)
            : 100;
    }
    async importItem(item, index, total, signal) {
        const st = (this.cp.states[item.id] ??= { points: 0, finished: false });
        if (st.finished) {
            return;
        }
        this.report({
            current: item.id,
            status: `${item.id}: planning`,
            percent: this.overall(index, total, 0),
        });
        if (st.start === undefined || st.end === undefined) {
            const range = await this.withRetry(() => this.deps.source.timeRange(item.measurement), signal);
            if (range.first === undefined || range.last === undefined) {
                st.finished = true;
                await this.save();
                this.deps.log.debug(`Data import: ${item.id} has no data in the old database (${item.measurement})`);
                return;
            }
            let start = Math.max(range.first, this.settings.from ?? range.first);
            let end = Math.min(range.last + 1, this.settings.to ?? Number.MAX_SAFE_INTEGER);
            if (this.settings.untilFirstV3) {
                let live = this.cp.liveStart[item.id];
                if (live === undefined) {
                    // no value in InfluxDB 3 yet: live values can only come from now on
                    live = (await this.deps.target.firstTime(item)) ?? this.deps.now();
                    this.cp.liveStart[item.id] = live;
                }
                end = Math.min(end, live);
            }
            start = Math.floor(start);
            end = Math.floor(end);
            st.start = start;
            st.end = end;
            await this.save(true);
        }
        const start = st.start;
        const end = st.end;
        let t = Math.max(start, st.doneUntil ?? start);
        let windowMs = exports.START_WINDOW;
        while (t < end) {
            if (signal.aborted) {
                return;
            }
            const wEnd = Math.min(end, t + windowMs);
            let points;
            try {
                points = await this.withRetry(() => this.deps.source.read(item.measurement, t, wEnd), signal);
            }
            catch (e) {
                if (e instanceof legacySource_1.LegacyReadError && e.kind === 'tooBig' && windowMs > exports.MIN_WINDOW) {
                    windowMs = Math.max(exports.MIN_WINDOW, Math.floor(windowMs / 4));
                    continue;
                }
                throw e;
            }
            if (signal.aborted) {
                return;
            }
            const { lines, skipped } = item.convert(points);
            this.cp.totals.skipped += skipped;
            if (lines.length) {
                await this.writeLines(item, lines, signal);
            }
            if (signal.aborted) {
                return; // the window is written again on resume (idempotent)
            }
            st.points += lines.length;
            this.cp.totals.points += lines.length;
            st.doneUntil = wEnd;
            t = wEnd;
            windowMs = adaptWindow(windowMs, points.length);
            await this.save();
            this.report({
                points: this.cp.totals.points,
                errors: this.cp.totals.errors,
                percent: this.overall(index, total, end > start ? (t - start) / (end - start) : 1),
                status: `${item.id}: ${new Date(t).toISOString().slice(0, 10)} (${st.points} point(s))`,
            });
            if (this.settings.pauseMs) {
                await this.deps.sleep(this.settings.pauseMs, signal);
            }
        }
        st.finished = true;
        await this.save(true);
        this.deps.log.debug(`Data import: ${item.id} done (${st.points} point(s) → ${item.table})`);
    }
    /** Retries transport problems of the source with backoff; other errors are thrown at once. */
    async withRetry(fn, signal) {
        let delay = 1000;
        for (let attempt = 1;; attempt++) {
            try {
                return await fn();
            }
            catch (e) {
                const transient = e instanceof legacySource_1.LegacyReadError && e.kind === 'transport';
                if (!transient || attempt >= MAX_RETRIES || signal.aborted) {
                    throw e;
                }
                this.deps.log.debug(`Data import: old database not reachable (${e.message}) - retry in ${delay} ms`);
                await this.deps.sleep(delay, signal);
                delay = Math.min(60_000, delay * 2);
            }
        }
    }
    async writeLines(item, lines, signal) {
        for (const chunk of chunkLines(lines, this.settings.batchSize)) {
            await this.writeChunk(item, chunk, signal);
            if (signal.aborted) {
                return;
            }
        }
    }
    async writeChunk(item, chunk, signal) {
        let delay = 1000;
        for (let attempt = 1;; attempt++) {
            // live logging has priority: wait while its queue fills up
            while (this.deps.target.busy() && !signal.aborted) {
                await this.deps.sleep(1000, signal);
            }
            if (signal.aborted) {
                return;
            }
            const res = await this.deps.target.write(chunk);
            switch (res.kind) {
                case 'ok':
                    return;
                case 'partial':
                    this.cp.totals.errors += res.rejected.length;
                    this.deps.log.warn(`Data import ${item.id}: ${res.rejected.length} point(s) rejected by InfluxDB 3 (${(0, redact_1.redact)(res.message).slice(0, 300)})`);
                    return;
                case 'invalid':
                    this.cp.totals.errors += chunk.length;
                    this.deps.log.warn(`Data import ${item.id}: ${chunk.length} point(s) rejected by InfluxDB 3 (${(0, redact_1.redact)(res.message).slice(0, 300)})`);
                    return;
                case 'tooLarge':
                    if (chunk.length === 1) {
                        this.cp.totals.errors++;
                        return;
                    }
                    await this.writeChunk(item, chunk.slice(0, Math.ceil(chunk.length / 2)), signal);
                    await this.writeChunk(item, chunk.slice(Math.ceil(chunk.length / 2)), signal);
                    return;
                case 'auth':
                case 'config':
                    throw new FatalImportError(`InfluxDB 3 refused the write: ${res.message}`);
                case 'transient':
                    if (attempt >= MAX_RETRIES) {
                        throw new FatalImportError(`InfluxDB 3 not reachable: ${res.message}`);
                    }
                    await this.deps.sleep(Math.max(delay, res.retryAfterMs ?? 0), signal);
                    if (signal.aborted) {
                        return;
                    }
                    delay = Math.min(60_000, delay * 2);
            }
        }
    }
}
exports.DataMigrationJob = DataMigrationJob;
/**
 * Explains the old measurements that are not selected for the import: which belong to states that only the
 * influxdb adapter logs (settings migration not applied yet), which are excluded by the state filter and which
 * belong to no known state.
 *
 * @param others measurements of the old database that are not selected
 * @param owners measurement → owner (from the influxdb settings)
 * @param target this instance, e.g. influx-v3.0
 */
function explainUnselected(others, owners, target) {
    if (!others.length) {
        return [];
    }
    const example = (list, show) => `${list.slice(0, 8).map(show).join(', ')}${list.length > 8 ? ', …' : ''}`;
    const notHere = new Map(); // influxdb instance → measurements
    const filtered = [];
    const unknown = [];
    for (const m of others) {
        const o = owners.get(m);
        if (!o) {
            unknown.push(m);
        }
        else if (o.configuredHere) {
            filtered.push(m);
        }
        else {
            notHere.set(o.instance, [...(notHere.get(o.instance) ?? []), m]);
        }
    }
    const show = (m) => {
        const o = owners.get(m);
        return o && o.id !== m ? `${m} (${o.id})` : m;
    };
    const lines = ['', `${others.length} measurement(s) of the old database are not selected (not imported):`];
    for (const [instance, list] of [...notHere.entries()].sort()) {
        lines.push(`  ${list.length} belong to states logged by ${instance} that are not configured for ${target} yet, e.g.: ${example(list, show)}`, `      → tab "Migration of settings": choose ${instance}, check the preview and press "Apply" (copies the settings incl. the aliases), then import.`);
    }
    if (filtered.length) {
        lines.push(`  ${filtered.length} belong to states of this instance excluded by the state filter, e.g.: ${example(filtered, show)}`);
    }
    if (unknown.length) {
        lines.push(`  ${unknown.length} belong to no state of the influxdb settings (state deleted, logging disabled or alias changed), e.g.: ${example(unknown, m => m)}`);
    }
    return lines;
}
/**
 * Dry run: what the job would do, nothing is written.
 *
 * @param items states
 * @param deps source and target
 * @param deps.source old database
 * @param deps.target InfluxDB 3
 * @param settings job settings
 * @param checkpoint saved progress (for the resume hint)
 * @param key job key
 * @param opts limits
 */
async function previewImport(items, deps, settings, checkpoint, key, opts = { detailed: 15, samples: 2 }, owners = new Map(), target = 'this instance') {
    const lines = [];
    const day = (t) => new Date(t).toISOString().replace('T', ' ').slice(0, 19);
    lines.push(items.length
        ? `${items.length} state(s) of this instance are selected for the import.`
        : `No state of ${target} is selected for the import - only states configured for this instance can be imported (their table, type and tags come from these settings).`);
    if (checkpoint && checkpoint.jobKey === key && !checkpoint.finished) {
        const started = Object.values(checkpoint.states).filter(s => s.finished).length;
        lines.push(`A started import with these settings exists (${started} state(s) done, ${checkpoint.totals.points} point(s)) - "Start" continues it.`);
    }
    else if (checkpoint && checkpoint.jobKey === key && checkpoint.finished) {
        lines.push('An import with these settings is already finished - "Start" does nothing until you reset the progress.');
    }
    else if (checkpoint && !checkpoint.finished) {
        lines.push('A started import with OTHER settings exists - "Start" begins a new import from the beginning.');
    }
    let shown = 0;
    let noData = 0;
    for (const item of items) {
        if (shown >= opts.detailed) {
            break;
        }
        const range = await deps.source.timeRange(item.measurement);
        if (range.first === undefined || range.last === undefined) {
            noData++;
            continue;
        }
        shown++;
        const start = Math.max(range.first, settings.from ?? range.first);
        let end = Math.min(range.last + 1, settings.to ?? Number.MAX_SAFE_INTEGER);
        let limit = '';
        if (settings.untilFirstV3) {
            const live = checkpoint?.liveStart[item.id] ?? (await deps.target.firstTime(item));
            if (live !== undefined && live < end) {
                end = live;
                limit = ' (stops at the first value already in InfluxDB 3)';
            }
        }
        lines.push('', `• ${item.id}  (old measurement "${item.measurement}" → table "${item.table}")`);
        lines.push(`    old data: ${day(range.first)} … ${day(range.last)}`);
        if (end <= start) {
            lines.push('    nothing to import in the selected period');
            continue;
        }
        lines.push(`    import:   ${day(start)} … ${day(end - 1)}${limit}`);
        const sample = await deps.source.read(item.measurement, start, Math.min(end, start + exports.DAY));
        const conv = item.convert(sample.slice(0, opts.samples));
        for (const l of conv.lines) {
            lines.push(`    ${l.length > 220 ? `${l.slice(0, 220)}…` : l}`);
        }
        if (conv.skipped) {
            lines.push(`    ${conv.skipped} of the first ${opts.samples} point(s) cannot be converted to the target type`);
        }
    }
    if (items.length > shown + noData) {
        lines.push('', `… ${items.length - shown - noData} more state(s) not checked in this preview.`);
    }
    if (noData) {
        lines.push('', `${noData} state(s) have no data in the old database.`);
    }
    try {
        const all = await deps.source.listMeasurements(20_000);
        const known = new Set(items.map(i => i.measurement));
        lines.push(...explainUnselected(all.filter(m => !known.has(m)), owners, target));
    }
    catch (e) {
        lines.push('', `List of old measurements not available: ${e instanceof Error ? e.message : (0, redact_1.redact)(e)}`);
    }
    lines.push('', 'Nothing has been written. The old database is only read.');
    return lines.join('\n');
}
//# sourceMappingURL=dataMigration.js.map