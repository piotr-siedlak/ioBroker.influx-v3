/**
 * Read-only access to the database of the classic ioBroker.influxdb adapter (InfluxDB 1.x or 2.x) for the
 * import of historical data (ADR-0006). Only SELECT / Flux `from()` queries are sent - nothing is written
 * or deleted on the source.
 *
 * Layout written by the classic adapter: one measurement per state (alias or id), field `value`, metadata
 * `ack`/`q`/`from` as fields (InfluxDB 2.x with "use tags": as tags), custom tags as tags.
 */
import { InfluxV3Client, TransportError } from './influxClient';
import { redact } from './redact';

export type LegacyVersion = '1.x' | '2.x';

export interface LegacySourceConfig {
    version: LegacyVersion;
    /** e.g. http://192.168.1.10:8086 */
    url: string;
    /** 1.x database */
    database: string;
    /** 1.x retention policy (empty = default) */
    retentionPolicy: string;
    /** 1.x user (empty = no authentication) */
    user: string;
    password: string;
    /** 2.x organisation */
    org: string;
    /** 2.x bucket */
    bucket: string;
    /** 2.x token */
    token: string;
    rejectUnauthorized: boolean;
    timeoutMs: number;
    /** maximum response size of one query */
    maxBytes: number;
}

/** One point of a state as the classic adapter stored it */
export interface LegacyPoint {
    ts: number;
    value: unknown;
    ack?: boolean;
    q?: number;
    from?: string;
    /** custom tags (without ack/q/from) */
    tags: Record<string, string>;
}

/** One tag value stored in the old points */
export interface LegacyTagUse {
    key: string;
    value: string;
    /** number of measurements (states) containing it; undefined when the server cannot tell cheaply (2.x) */
    measurements?: number;
}

/** maximum number of values read per tag key */
export const MAX_TAG_VALUES = 5000;

export type LegacyErrorKind = 'tooBig' | 'auth' | 'notFound' | 'query' | 'transport';

export class LegacyReadError extends Error {
    constructor(
        message: string,
        readonly kind: LegacyErrorKind,
    ) {
        super(message);
        this.name = 'LegacyReadError';
    }
}

/** Interface of the source used by the import job (implemented for 1.x and 2.x, faked in tests) */
export interface LegacyReader {
    /** reachability, authentication and access to database/bucket */
    check(): Promise<{ ok: boolean; version?: string; error?: string }>;
    /** time of the first and last point of a measurement (undefined if it has no points) */
    timeRange(measurement: string): Promise<{ first?: number; last?: number }>;
    /** points with from <= ts < to, sorted by time */
    read(measurement: string, from: number, to: number): Promise<LegacyPoint[]>;
    /** names of the measurements (up to `limit`) */
    listMeasurements(limit: number): Promise<string[]>;
    /** all custom tag keys and values stored in the points (without ack/q/from) */
    tagInventory(): Promise<LegacyTagUse[]>;
    close(): void;
}

const META = new Set(['ack', 'q', 'from']);
const FAR_PAST = '1970-01-01T00:00:00Z';
const FAR_FUTURE = '2200-01-01T00:00:00Z';

// ------------------------------------------------------------------ query text

/**
 * Quotes an InfluxQL identifier.
 *
 * @param name identifier
 */
export function quoteInfluxQLIdentifier(name: string): string {
    return `"${name.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n')}"`;
}

/**
 * Quotes a Flux string literal (incl. `${` interpolation).
 *
 * @param value text
 */
export function quoteFluxString(value: string): string {
    return `"${value
        .replace(/\\/g, '\\\\')
        .replace(/"/g, '\\"')
        .replace(/\$\{/g, '\\${')
        .replace(/\n/g, '\\n')
        .replace(/\r/g, '\\r')}"`;
}

/**
 * FROM clause of InfluxQL for a measurement in an optional retention policy.
 *
 * @param measurement measurement
 * @param retentionPolicy retention policy or ''
 */
export function influxQLSource(measurement: string, retentionPolicy: string): string {
    const m = quoteInfluxQLIdentifier(measurement);
    return retentionPolicy ? `${quoteInfluxQLIdentifier(retentionPolicy)}.${m}` : m;
}

/**
 * InfluxQL query of one time window; `GROUP BY *` returns the tags separately from the fields.
 *
 * @param measurement measurement
 * @param retentionPolicy retention policy or ''
 * @param from start (ms, inclusive)
 * @param to end (ms, exclusive)
 */
export function buildInfluxQLRead(measurement: string, retentionPolicy: string, from: number, to: number): string {
    return `SELECT * FROM ${influxQLSource(measurement, retentionPolicy)} WHERE time >= ${Math.floor(from)}ms AND time < ${Math.floor(to)}ms GROUP BY *`;
}

/**
 * Flux query of one time window, fields pivoted into columns.
 *
 * @param bucket bucket
 * @param measurement measurement
 * @param from start (ms, inclusive)
 * @param to end (ms, exclusive)
 */
export function buildFluxRead(bucket: string, measurement: string, from: number, to: number): string {
    return [
        `from(bucket: ${quoteFluxString(bucket)})`,
        `  |> range(start: time(v: ${quoteFluxString(new Date(from).toISOString())}), stop: time(v: ${quoteFluxString(new Date(to).toISOString())}))`,
        `  |> filter(fn: (r) => r._measurement == ${quoteFluxString(measurement)})`,
        '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")',
    ].join('\n');
}

/**
 * Flux query of the first and last time of a measurement.
 *
 * @param bucket bucket
 * @param measurement measurement
 */
export function buildFluxTimeRange(bucket: string, measurement: string): string {
    return [
        `data = from(bucket: ${quoteFluxString(bucket)})`,
        `  |> range(start: time(v: "${FAR_PAST}"), stop: time(v: "${FAR_FUTURE}"))`,
        `  |> filter(fn: (r) => r._measurement == ${quoteFluxString(measurement)} and r._field == "value")`,
        'data |> first() |> keep(columns: ["_time"]) |> group() |> sort(columns: ["_time"]) |> limit(n: 1) |> yield(name: "first")',
        'data |> last() |> keep(columns: ["_time"]) |> group() |> sort(columns: ["_time"], desc: true) |> limit(n: 1) |> yield(name: "last")',
    ].join('\n');
}

// ------------------------------------------------------------------ parsing

/**
 * Parses an RFC 3339 time (any number of fraction digits) to epoch milliseconds.
 *
 * @param text e.g. 2024-01-01T10:00:00.123456789Z
 */
export function parseRfc3339(text: string): number {
    const m = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})$/.exec(text.trim());
    if (!m) {
        return NaN;
    }
    const base = Date.parse(`${m[1]}${m[3]}`);
    const frac = m[2] ? Number(`0.${m[2]}`) * 1000 : 0;
    return base + Math.floor(frac + 1e-6);
}

function toBool(v: unknown): boolean | undefined {
    if (typeof v === 'boolean') {
        return v;
    }
    if (typeof v === 'number') {
        return v !== 0;
    }
    if (typeof v === 'string') {
        const s = v.trim().toLowerCase();
        return s === 'true' ? true : s === 'false' ? false : undefined;
    }
    return undefined;
}

function toNum(v: unknown): number | undefined {
    const n = typeof v === 'number' ? v : typeof v === 'string' && v.trim() !== '' ? Number(v) : NaN;
    return Number.isFinite(n) ? n : undefined;
}

/**
 * Builds a point from a row of fields and tags; ack/q/from are metadata wherever they are stored.
 *
 * @param ts time
 * @param fields field columns
 * @param tags tag columns
 */
export function toLegacyPoint(
    ts: number,
    fields: Record<string, unknown>,
    tags: Record<string, string>,
): LegacyPoint | undefined {
    if (!Number.isFinite(ts) || !('value' in fields)) {
        return undefined;
    }
    const value = fields.value;
    if (value === null || value === undefined) {
        return undefined;
    }
    const p: LegacyPoint = { ts, value, tags: {} };
    const ack = toBool(fields.ack ?? tags.ack);
    if (ack !== undefined) {
        p.ack = ack;
    }
    const q = toNum(fields.q ?? tags.q);
    if (q !== undefined) {
        p.q = q;
    }
    const from = fields.from ?? tags.from;
    if (typeof from === 'string' && from) {
        p.from = from;
    }
    for (const [k, v] of Object.entries(tags)) {
        if (!META.has(k) && typeof v === 'string' && v !== '') {
            p.tags[k] = v;
        }
    }
    return p;
}

interface InfluxQLSeries {
    name?: string;
    tags?: Record<string, string>;
    columns?: string[];
    values?: unknown[][];
}

interface InfluxQLResponse {
    error?: string;
    results?: { statement_id?: number; error?: string; series?: InfluxQLSeries[] }[];
}

/**
 * Parses the JSON answer of InfluxDB 1.x `/query` (with `epoch=ms`).
 *
 * @param body response body
 * @returns series of every statement
 */
export function parseInfluxQLResponse(body: string): InfluxQLSeries[][] {
    let json: InfluxQLResponse;
    try {
        json = JSON.parse(body) as InfluxQLResponse;
    } catch {
        throw new LegacyReadError(`invalid answer of InfluxDB 1.x: ${redact(body.slice(0, 200))}`, 'query');
    }
    if (json.error) {
        throw new LegacyReadError(redact(json.error), 'query');
    }
    return (json.results ?? []).map(r => {
        if (r.error) {
            throw new LegacyReadError(redact(r.error), /not found/i.test(r.error) ? 'notFound' : 'query');
        }
        return r.series ?? [];
    });
}

/**
 * Points of an InfluxDB 1.x answer to {@link buildInfluxQLRead}.
 *
 * @param series series of the statement
 */
export function influxQLSeriesToPoints(series: InfluxQLSeries[]): LegacyPoint[] {
    const out: LegacyPoint[] = [];
    for (const s of series) {
        const cols = s.columns ?? [];
        const timeIdx = cols.indexOf('time');
        for (const row of s.values ?? []) {
            const fields: Record<string, unknown> = {};
            cols.forEach((c, i) => {
                if (i !== timeIdx && row[i] !== null && row[i] !== undefined) {
                    fields[c] = row[i];
                }
            });
            const ts = typeof row[timeIdx] === 'number' ? row[timeIdx] : Date.parse(String(row[timeIdx]));
            const p = toLegacyPoint(ts, fields, s.tags ?? {});
            if (p) {
                out.push(p);
            }
        }
    }
    return out.sort((a, b) => a.ts - b.ts);
}

/** One table block of an annotated CSV answer */
export interface CsvTable {
    columns: string[];
    types: string[];
    group: boolean[];
    defaults: string[];
    rows: string[][];
}

/**
 * Splits CSV text into records (RFC 4180: quoted cells may contain commas, quotes and line breaks).
 *
 * @param text CSV text
 */
export function parseCsvRecords(text: string): string[][] {
    const records: string[][] = [];
    let record: string[] = [];
    let cell = '';
    let quoted = false;
    let i = 0;
    const n = text.length;
    while (i < n) {
        const ch = text[i];
        if (quoted) {
            if (ch === '\r' && text[i + 1] === '\n') {
                cell += '\n'; // InfluxDB writes line breaks inside cells as CRLF
                i += 2;
                continue;
            }
            if (ch === '"') {
                if (text[i + 1] === '"') {
                    cell += '"';
                    i += 2;
                    continue;
                }
                quoted = false;
                i++;
                continue;
            }
            cell += ch;
            i++;
            continue;
        }
        if (ch === '"' && cell === '') {
            quoted = true;
            i++;
        } else if (ch === ',') {
            record.push(cell);
            cell = '';
            i++;
        } else if (ch === '\r' || ch === '\n') {
            record.push(cell);
            records.push(record);
            record = [];
            cell = '';
            i += ch === '\r' && text[i + 1] === '\n' ? 2 : 1;
        } else {
            cell += ch;
            i++;
        }
    }
    if (cell !== '' || record.length) {
        record.push(cell);
        records.push(record);
    }
    return records;
}

/**
 * Parses an annotated CSV answer of InfluxDB 2.x into table blocks.
 *
 * @param text response body
 */
export function parseAnnotatedCsv(text: string): CsvTable[] {
    const tables: CsvTable[] = [];
    let ann: { types?: string[]; group?: string[]; defaults?: string[] } = {};
    let current: CsvTable | undefined;
    for (const rec of parseCsvRecords(text)) {
        if (rec.length === 1 && rec[0] === '') {
            current = undefined; // blank line: next block
            ann = {};
            continue;
        }
        const first = rec[0];
        if (first.startsWith('#')) {
            current = undefined;
            if (first === '#datatype') {
                ann.types = rec;
            } else if (first === '#group') {
                ann.group = rec;
            } else if (first === '#default') {
                ann.defaults = rec;
            }
            continue;
        }
        if (!current) {
            // header row
            current = {
                columns: rec,
                types: ann.types ?? rec.map(() => 'string'),
                group: (ann.group ?? rec.map(() => 'false')).map(g => g === 'true'),
                defaults: ann.defaults ?? rec.map(() => ''),
                rows: [],
            };
            tables.push(current);
            if (current.columns.includes('error') && current.columns.includes('reference')) {
                current.rows = []; // error table, rows follow
            }
            continue;
        }
        current.rows.push(rec);
    }
    for (const t of tables) {
        if (t.columns.includes('error') && t.columns.includes('reference') && t.rows.length) {
            const idx = t.columns.indexOf('error');
            throw new LegacyReadError(redact(t.rows[0][idx] || 'query failed'), 'query');
        }
    }
    return tables;
}

function convertCell(raw: string, type: string, def: string): unknown {
    const v = raw === '' ? def : raw;
    if (v === '') {
        return type === 'string' ? '' : null;
    }
    switch (type) {
        case 'boolean':
            return v === 'true';
        case 'long':
        case 'unsignedLong':
        case 'double': {
            const n = Number(v);
            return Number.isFinite(n) ? n : null;
        }
        default:
            return type.startsWith('dateTime') ? parseRfc3339(v) : v;
    }
}

const NOT_DATA = new Set(['', 'result', 'table', '_start', '_stop', '_measurement', '_field', '_time']);

/**
 * Points of an InfluxDB 2.x answer to {@link buildFluxRead}.
 *
 * @param tables parsed CSV blocks
 */
export function fluxTablesToPoints(tables: CsvTable[]): LegacyPoint[] {
    const out: LegacyPoint[] = [];
    for (const t of tables) {
        const timeIdx = t.columns.indexOf('_time');
        if (timeIdx < 0) {
            continue;
        }
        for (const row of t.rows) {
            const fields: Record<string, unknown> = {};
            const tags: Record<string, string> = {};
            t.columns.forEach((c, i) => {
                if (NOT_DATA.has(c)) {
                    return;
                }
                const val = convertCell(row[i] ?? '', t.types[i] ?? 'string', t.defaults[i] ?? '');
                if (t.group[i]) {
                    if (typeof val === 'string' && val !== '') {
                        tags[c] = val;
                    }
                } else if (val !== null && val !== undefined) {
                    fields[c] = val;
                }
            });
            const ts = convertCell(row[timeIdx] ?? '', t.types[timeIdx] ?? 'dateTime:RFC3339', '') as number;
            const p = toLegacyPoint(ts, fields, tags);
            if (p) {
                out.push(p);
            }
        }
    }
    return out.sort((a, b) => a.ts - b.ts);
}

// ------------------------------------------------------------------ readers

/** Shared HTTP part of both readers */
abstract class HttpReader implements LegacyReader {
    protected readonly http: InfluxV3Client;
    protected readonly basePath: string;

    constructor(protected readonly cfg: LegacySourceConfig) {
        const url = new URL(cfg.url);
        if (url.protocol !== 'http:' && url.protocol !== 'https:') {
            throw new LegacyReadError(`unsupported protocol ${url.protocol}`, 'transport');
        }
        this.basePath = url.pathname.replace(/\/+$/, '');
        // the v3 client is only used as keep-alive HTTP transport; authentication headers are set per request
        this.http = new InfluxV3Client({
            url: `${url.protocol}//${url.host}`,
            token: '',
            database: '',
            edition: 'core',
            timeoutMs: cfg.timeoutMs,
            rejectUnauthorized: cfg.rejectUnauthorized,
            gzip: false,
            noSync: false,
            maxSockets: 2,
        });
    }

    protected abstract authHeaders(): Record<string, string>;

    protected async call(
        method: 'GET' | 'POST',
        path: string,
        body?: string,
        headers: Record<string, string> = {},
    ): Promise<string> {
        let res;
        try {
            res = await this.http.request(
                method,
                `${this.basePath}${path}`,
                body,
                { ...this.authHeaders(), ...headers },
                this.cfg.timeoutMs,
                this.cfg.maxBytes,
            );
        } catch (e) {
            if (e instanceof TransportError && e.code === 'ETOOBIG') {
                throw new LegacyReadError(e.message, 'tooBig');
            }
            throw new LegacyReadError(redact(e), 'transport');
        }
        if (res.status === 401 || res.status === 403) {
            let detail = '';
            try {
                const j = JSON.parse(res.body) as { message?: string; error?: string };
                detail = j.message ?? j.error ?? '';
            } catch {
                // plain text
            }
            throw new LegacyReadError(
                `${res.status === 401 ? 'authentication failed' : 'access denied'} (HTTP ${res.status})${detail ? `: ${redact(detail).slice(0, 200)}` : ''}`,
                'auth',
            );
        }
        if (res.status === 404) {
            throw new LegacyReadError(`not found (HTTP 404): ${redact(res.body.slice(0, 200))}`, 'notFound');
        }
        if (res.status >= 300) {
            let message = res.body.slice(0, 300);
            try {
                const j = JSON.parse(res.body) as { message?: string; error?: string };
                message = j.message ?? j.error ?? message;
            } catch {
                // plain text
            }
            throw new LegacyReadError(`HTTP ${res.status}: ${redact(message)}`, 'query');
        }
        return res.body;
    }

    protected async version(): Promise<string | undefined> {
        const res = await this.http
            .request('GET', `${this.basePath}/ping`, undefined, {}, this.cfg.timeoutMs, 64 * 1024)
            .catch((e: unknown) => {
                throw new LegacyReadError(redact(e), 'transport');
            });
        if (res.status >= 300) {
            throw new LegacyReadError(`ping: HTTP ${res.status}`, 'transport');
        }
        const v = res.headers['x-influxdb-version'];
        return typeof v === 'string' ? v : undefined;
    }

    async check(): Promise<{ ok: boolean; version?: string; error?: string }> {
        try {
            const version = await this.version();
            await this.listMeasurements(1);
            return { ok: true, version };
        } catch (e) {
            return { ok: false, error: e instanceof Error ? e.message : redact(e) };
        }
    }

    abstract timeRange(measurement: string): Promise<{ first?: number; last?: number }>;
    abstract read(measurement: string, from: number, to: number): Promise<LegacyPoint[]>;
    abstract listMeasurements(limit: number): Promise<string[]>;
    abstract tagInventory(): Promise<LegacyTagUse[]>;

    close(): void {
        this.http.close();
    }
}

/** InfluxDB 1.x (InfluxQL over `/query`) */
export class InfluxDB1Reader extends HttpReader {
    protected authHeaders(): Record<string, string> {
        if (!this.cfg.user) {
            return {};
        }
        const b64 = Buffer.from(`${this.cfg.user}:${this.cfg.password}`, 'utf8').toString('base64');
        return { Authorization: `Basic ${b64}` };
    }

    /** SHOW MEASUREMENTS does not fail for a missing database - the retention policies do (read access suffices) */
    override async check(): Promise<{ ok: boolean; version?: string; error?: string }> {
        try {
            const version = await this.version();
            const [series] = await this.query(
                `SHOW RETENTION POLICIES ON ${quoteInfluxQLIdentifier(this.cfg.database)}`,
            );
            const policies = (series?.[0]?.values ?? []).map(v => String(v[0]));
            if (this.cfg.retentionPolicy && !policies.includes(this.cfg.retentionPolicy)) {
                return {
                    ok: false,
                    version,
                    error: `retention policy "${this.cfg.retentionPolicy}" not found (existing: ${policies.join(', ') || '-'})`,
                };
            }
            return { ok: true, version };
        } catch (e) {
            return { ok: false, error: e instanceof Error ? e.message : redact(e) };
        }
    }

    private async query(q: string): Promise<InfluxQLSeries[][]> {
        const form = new URLSearchParams({ db: this.cfg.database, epoch: 'ms', q });
        const body = await this.call('POST', '/query', form.toString(), {
            'Content-Type': 'application/x-www-form-urlencoded',
            Accept: 'application/json',
        });
        return parseInfluxQLResponse(body);
    }

    async timeRange(measurement: string): Promise<{ first?: number; last?: number }> {
        const src = influxQLSource(measurement, this.cfg.retentionPolicy);
        const [first, last] = await this.query(`SELECT FIRST("value") FROM ${src}; SELECT LAST("value") FROM ${src}`);
        const t = (s: InfluxQLSeries[] | undefined): number | undefined => {
            const v = s?.[0]?.values?.[0]?.[0];
            return typeof v === 'number' ? v : undefined;
        };
        return { first: t(first), last: t(last) };
    }

    async read(measurement: string, from: number, to: number): Promise<LegacyPoint[]> {
        const [series] = await this.query(buildInfluxQLRead(measurement, this.cfg.retentionPolicy, from, to));
        return influxQLSeriesToPoints(series ?? []);
    }

    async listMeasurements(limit: number): Promise<string[]> {
        const [series] = await this.query(`SHOW MEASUREMENTS LIMIT ${Math.max(1, Math.floor(limit))}`);
        return (series?.[0]?.values ?? []).map(v => String(v[0]));
    }

    async tagInventory(): Promise<LegacyTagUse[]> {
        // one series per measurement with rows [key, value]
        const [series] = await this.query('SHOW TAG VALUES WITH KEY =~ /.+/');
        return countTagUses(
            (series ?? []).flatMap(s => (s.values ?? []).map(v => ({ key: String(v[0]), value: String(v[1]) }))),
        );
    }
}

/**
 * Counts tag uses (one entry per measurement and value), drops metadata keys and caps the values per key.
 *
 * @param rows key/value per measurement
 */
export function countTagUses(rows: { key: string; value: string }[]): LegacyTagUse[] {
    const counts = new Map<string, LegacyTagUse>();
    const perKey = new Map<string, number>();
    for (const r of rows) {
        if (!r.key || r.key.startsWith('_') || META.has(r.key) || r.value === '') {
            continue;
        }
        const id = `${r.key}\u0000${r.value}`;
        const e = counts.get(id);
        if (e) {
            e.measurements = (e.measurements ?? 0) + 1;
            continue;
        }
        const n = perKey.get(r.key) ?? 0;
        if (n >= MAX_TAG_VALUES) {
            continue;
        }
        perKey.set(r.key, n + 1);
        counts.set(id, { key: r.key, value: r.value, measurements: 1 });
    }
    return [...counts.values()];
}

/** InfluxDB 2.x (Flux over `/api/v2/query`, annotated CSV) */
export class InfluxDB2Reader extends HttpReader {
    protected authHeaders(): Record<string, string> {
        return this.cfg.token ? { Authorization: `Token ${this.cfg.token}` } : {};
    }

    private async flux(query: string): Promise<CsvTable[]> {
        const body = JSON.stringify({
            query,
            type: 'flux',
            dialect: { header: true, delimiter: ',', annotations: ['datatype', 'group', 'default'] },
        });
        const qs = new URLSearchParams({ org: this.cfg.org }).toString();
        const text = await this.call('POST', `/api/v2/query?${qs}`, body, {
            'Content-Type': 'application/json',
            Accept: 'application/csv',
        });
        return parseAnnotatedCsv(text);
    }

    async timeRange(measurement: string): Promise<{ first?: number; last?: number }> {
        const tables = await this.flux(buildFluxTimeRange(this.cfg.bucket, measurement));
        const out: { first?: number; last?: number } = {};
        for (const t of tables) {
            const r = t.columns.indexOf('result');
            const ti = t.columns.indexOf('_time');
            for (const row of t.rows) {
                const name = row[r] || t.defaults[r];
                const ts = parseRfc3339(row[ti] ?? '');
                if (Number.isFinite(ts) && (name === 'first' || name === 'last')) {
                    out[name] = ts;
                }
            }
        }
        return out;
    }

    async read(measurement: string, from: number, to: number): Promise<LegacyPoint[]> {
        return fluxTablesToPoints(await this.flux(buildFluxRead(this.cfg.bucket, measurement, from, to)));
    }

    async tagInventory(): Promise<LegacyTagUse[]> {
        const range = `start: time(v: "${FAR_PAST}"), stop: time(v: "${FAR_FUTURE}")`;
        const schemaImport = `import ${quoteFluxString('influxdata/influxdb/schema')}`;
        const keys: string[] = [];
        for (const t of await this.flux(
            `${schemaImport}\nschema.tagKeys(bucket: ${quoteFluxString(this.cfg.bucket)}, ${range})`,
        )) {
            const idx = t.columns.indexOf('_value');
            for (const row of t.rows) {
                const k = row[idx];
                if (k && !k.startsWith('_') && !META.has(k) && !keys.includes(k)) {
                    keys.push(k);
                }
            }
        }
        const out: LegacyTagUse[] = [];
        for (const key of keys) {
            const q = `${schemaImport}\nschema.tagValues(bucket: ${quoteFluxString(this.cfg.bucket)}, tag: ${quoteFluxString(key)}, ${range})\n  |> limit(n: ${MAX_TAG_VALUES})`;
            for (const t of await this.flux(q)) {
                const idx = t.columns.indexOf('_value');
                for (const row of t.rows) {
                    if (row[idx]) {
                        out.push({ key, value: row[idx] });
                    }
                }
            }
        }
        return out;
    }

    async listMeasurements(limit: number): Promise<string[]> {
        const q = [
            // Flux package import (built from parts so code scanners do not take it for a JavaScript import)
            `import ${quoteFluxString('influxdata/influxdb/schema')}`,
            `schema.measurements(bucket: ${quoteFluxString(this.cfg.bucket)}, start: time(v: "${FAR_PAST}"), stop: time(v: "${FAR_FUTURE}"))`,
            `  |> limit(n: ${Math.max(1, Math.floor(limit))})`,
        ].join('\n');
        const out: string[] = [];
        for (const t of await this.flux(q)) {
            const idx = t.columns.indexOf('_value');
            for (const row of t.rows) {
                if (row[idx]) {
                    out.push(row[idx]);
                }
            }
        }
        return out;
    }
}

/**
 * Creates the reader for the configured version.
 *
 * @param cfg source settings
 */
export function createLegacyReader(cfg: LegacySourceConfig): LegacyReader {
    return cfg.version === '2.x' ? new InfluxDB2Reader(cfg) : new InfluxDB1Reader(cfg);
}
