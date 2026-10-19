"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.InfluxDB2Reader = exports.InfluxDB1Reader = exports.LegacyReadError = void 0;
exports.quoteInfluxQLIdentifier = quoteInfluxQLIdentifier;
exports.quoteFluxString = quoteFluxString;
exports.influxQLSource = influxQLSource;
exports.buildInfluxQLRead = buildInfluxQLRead;
exports.buildFluxRead = buildFluxRead;
exports.buildFluxTimeRange = buildFluxTimeRange;
exports.parseRfc3339 = parseRfc3339;
exports.toLegacyPoint = toLegacyPoint;
exports.parseInfluxQLResponse = parseInfluxQLResponse;
exports.influxQLSeriesToPoints = influxQLSeriesToPoints;
exports.parseCsvRecords = parseCsvRecords;
exports.parseAnnotatedCsv = parseAnnotatedCsv;
exports.fluxTablesToPoints = fluxTablesToPoints;
exports.createLegacyReader = createLegacyReader;
/**
 * Read-only access to the database of the classic ioBroker.influxdb adapter (InfluxDB 1.x or 2.x) for the
 * import of historical data (ADR-0006). Only SELECT / Flux `from()` queries are sent - nothing is written
 * or deleted on the source.
 *
 * Layout written by the classic adapter: one measurement per state (alias or id), field `value`, metadata
 * `ack`/`q`/`from` as fields (InfluxDB 2.x with "use tags": as tags), custom tags as tags.
 */
const influxClient_1 = require("./influxClient");
const redact_1 = require("./redact");
class LegacyReadError extends Error {
    kind;
    constructor(message, kind) {
        super(message);
        this.kind = kind;
        this.name = 'LegacyReadError';
    }
}
exports.LegacyReadError = LegacyReadError;
const META = new Set(['ack', 'q', 'from']);
const FAR_PAST = '1970-01-01T00:00:00Z';
const FAR_FUTURE = '2200-01-01T00:00:00Z';
// ------------------------------------------------------------------ query text
/**
 * Quotes an InfluxQL identifier.
 *
 * @param name identifier
 */
function quoteInfluxQLIdentifier(name) {
    return `"${name.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n')}"`;
}
/**
 * Quotes a Flux string literal (incl. `${` interpolation).
 *
 * @param value text
 */
function quoteFluxString(value) {
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
function influxQLSource(measurement, retentionPolicy) {
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
function buildInfluxQLRead(measurement, retentionPolicy, from, to) {
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
function buildFluxRead(bucket, measurement, from, to) {
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
function buildFluxTimeRange(bucket, measurement) {
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
function parseRfc3339(text) {
    const m = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})$/.exec(text.trim());
    if (!m) {
        return NaN;
    }
    const base = Date.parse(`${m[1]}${m[3]}`);
    const frac = m[2] ? Number(`0.${m[2]}`) * 1000 : 0;
    return base + Math.floor(frac + 1e-6);
}
function toBool(v) {
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
function toNum(v) {
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
function toLegacyPoint(ts, fields, tags) {
    if (!Number.isFinite(ts) || !('value' in fields)) {
        return undefined;
    }
    const value = fields.value;
    if (value === null || value === undefined) {
        return undefined;
    }
    const p = { ts, value, tags: {} };
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
/**
 * Parses the JSON answer of InfluxDB 1.x `/query` (with `epoch=ms`).
 *
 * @param body response body
 * @returns series of every statement
 */
function parseInfluxQLResponse(body) {
    let json;
    try {
        json = JSON.parse(body);
    }
    catch {
        throw new LegacyReadError(`invalid answer of InfluxDB 1.x: ${(0, redact_1.redact)(body.slice(0, 200))}`, 'query');
    }
    if (json.error) {
        throw new LegacyReadError((0, redact_1.redact)(json.error), 'query');
    }
    return (json.results ?? []).map(r => {
        if (r.error) {
            throw new LegacyReadError((0, redact_1.redact)(r.error), /not found/i.test(r.error) ? 'notFound' : 'query');
        }
        return r.series ?? [];
    });
}
/**
 * Points of an InfluxDB 1.x answer to {@link buildInfluxQLRead}.
 *
 * @param series series of the statement
 */
function influxQLSeriesToPoints(series) {
    const out = [];
    for (const s of series) {
        const cols = s.columns ?? [];
        const timeIdx = cols.indexOf('time');
        for (const row of s.values ?? []) {
            const fields = {};
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
/**
 * Splits CSV text into records (RFC 4180: quoted cells may contain commas, quotes and line breaks).
 *
 * @param text CSV text
 */
function parseCsvRecords(text) {
    const records = [];
    let record = [];
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
        }
        else if (ch === ',') {
            record.push(cell);
            cell = '';
            i++;
        }
        else if (ch === '\r' || ch === '\n') {
            record.push(cell);
            records.push(record);
            record = [];
            cell = '';
            i += ch === '\r' && text[i + 1] === '\n' ? 2 : 1;
        }
        else {
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
function parseAnnotatedCsv(text) {
    const tables = [];
    let ann = {};
    let current;
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
            }
            else if (first === '#group') {
                ann.group = rec;
            }
            else if (first === '#default') {
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
            throw new LegacyReadError((0, redact_1.redact)(t.rows[0][idx] || 'query failed'), 'query');
        }
    }
    return tables;
}
function convertCell(raw, type, def) {
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
function fluxTablesToPoints(tables) {
    const out = [];
    for (const t of tables) {
        const timeIdx = t.columns.indexOf('_time');
        if (timeIdx < 0) {
            continue;
        }
        for (const row of t.rows) {
            const fields = {};
            const tags = {};
            t.columns.forEach((c, i) => {
                if (NOT_DATA.has(c)) {
                    return;
                }
                const val = convertCell(row[i] ?? '', t.types[i] ?? 'string', t.defaults[i] ?? '');
                if (t.group[i]) {
                    if (typeof val === 'string' && val !== '') {
                        tags[c] = val;
                    }
                }
                else if (val !== null && val !== undefined) {
                    fields[c] = val;
                }
            });
            const ts = convertCell(row[timeIdx] ?? '', t.types[timeIdx] ?? 'dateTime:RFC3339', '');
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
class HttpReader {
    cfg;
    http;
    basePath;
    constructor(cfg) {
        this.cfg = cfg;
        const url = new URL(cfg.url);
        if (url.protocol !== 'http:' && url.protocol !== 'https:') {
            throw new LegacyReadError(`unsupported protocol ${url.protocol}`, 'transport');
        }
        this.basePath = url.pathname.replace(/\/+$/, '');
        // the v3 client is only used as keep-alive HTTP transport; authentication headers are set per request
        this.http = new influxClient_1.InfluxV3Client({
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
    async call(method, path, body, headers = {}) {
        let res;
        try {
            res = await this.http.request(method, `${this.basePath}${path}`, body, { ...this.authHeaders(), ...headers }, this.cfg.timeoutMs, this.cfg.maxBytes);
        }
        catch (e) {
            if (e instanceof influxClient_1.TransportError && e.code === 'ETOOBIG') {
                throw new LegacyReadError(e.message, 'tooBig');
            }
            throw new LegacyReadError((0, redact_1.redact)(e), 'transport');
        }
        if (res.status === 401 || res.status === 403) {
            let detail = '';
            try {
                const j = JSON.parse(res.body);
                detail = j.message ?? j.error ?? '';
            }
            catch {
                // plain text
            }
            throw new LegacyReadError(`${res.status === 401 ? 'authentication failed' : 'access denied'} (HTTP ${res.status})${detail ? `: ${(0, redact_1.redact)(detail).slice(0, 200)}` : ''}`, 'auth');
        }
        if (res.status === 404) {
            throw new LegacyReadError(`not found (HTTP 404): ${(0, redact_1.redact)(res.body.slice(0, 200))}`, 'notFound');
        }
        if (res.status >= 300) {
            let message = res.body.slice(0, 300);
            try {
                const j = JSON.parse(res.body);
                message = j.message ?? j.error ?? message;
            }
            catch {
                // plain text
            }
            throw new LegacyReadError(`HTTP ${res.status}: ${(0, redact_1.redact)(message)}`, 'query');
        }
        return res.body;
    }
    async version() {
        const res = await this.http
            .request('GET', `${this.basePath}/ping`, undefined, {}, this.cfg.timeoutMs, 64 * 1024)
            .catch((e) => {
            throw new LegacyReadError((0, redact_1.redact)(e), 'transport');
        });
        if (res.status >= 300) {
            throw new LegacyReadError(`ping: HTTP ${res.status}`, 'transport');
        }
        const v = res.headers['x-influxdb-version'];
        return typeof v === 'string' ? v : undefined;
    }
    async check() {
        try {
            const version = await this.version();
            await this.listMeasurements(1);
            return { ok: true, version };
        }
        catch (e) {
            return { ok: false, error: e instanceof Error ? e.message : (0, redact_1.redact)(e) };
        }
    }
    close() {
        this.http.close();
    }
}
/** InfluxDB 1.x (InfluxQL over `/query`) */
class InfluxDB1Reader extends HttpReader {
    authHeaders() {
        if (!this.cfg.user) {
            return {};
        }
        const b64 = Buffer.from(`${this.cfg.user}:${this.cfg.password}`, 'utf8').toString('base64');
        return { Authorization: `Basic ${b64}` };
    }
    /** SHOW MEASUREMENTS does not fail for a missing database - the retention policies do (read access suffices) */
    async check() {
        try {
            const version = await this.version();
            const [series] = await this.query(`SHOW RETENTION POLICIES ON ${quoteInfluxQLIdentifier(this.cfg.database)}`);
            const policies = (series?.[0]?.values ?? []).map(v => String(v[0]));
            if (this.cfg.retentionPolicy && !policies.includes(this.cfg.retentionPolicy)) {
                return {
                    ok: false,
                    version,
                    error: `retention policy "${this.cfg.retentionPolicy}" not found (existing: ${policies.join(', ') || '-'})`,
                };
            }
            return { ok: true, version };
        }
        catch (e) {
            return { ok: false, error: e instanceof Error ? e.message : (0, redact_1.redact)(e) };
        }
    }
    async query(q) {
        const form = new URLSearchParams({ db: this.cfg.database, epoch: 'ms', q });
        const body = await this.call('POST', '/query', form.toString(), {
            'Content-Type': 'application/x-www-form-urlencoded',
            Accept: 'application/json',
        });
        return parseInfluxQLResponse(body);
    }
    async timeRange(measurement) {
        const src = influxQLSource(measurement, this.cfg.retentionPolicy);
        const [first, last] = await this.query(`SELECT FIRST("value") FROM ${src}; SELECT LAST("value") FROM ${src}`);
        const t = (s) => {
            const v = s?.[0]?.values?.[0]?.[0];
            return typeof v === 'number' ? v : undefined;
        };
        return { first: t(first), last: t(last) };
    }
    async read(measurement, from, to) {
        const [series] = await this.query(buildInfluxQLRead(measurement, this.cfg.retentionPolicy, from, to));
        return influxQLSeriesToPoints(series ?? []);
    }
    async listMeasurements(limit) {
        const [series] = await this.query(`SHOW MEASUREMENTS LIMIT ${Math.max(1, Math.floor(limit))}`);
        return (series?.[0]?.values ?? []).map(v => String(v[0]));
    }
}
exports.InfluxDB1Reader = InfluxDB1Reader;
/** InfluxDB 2.x (Flux over `/api/v2/query`, annotated CSV) */
class InfluxDB2Reader extends HttpReader {
    authHeaders() {
        return this.cfg.token ? { Authorization: `Token ${this.cfg.token}` } : {};
    }
    async flux(query) {
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
    async timeRange(measurement) {
        const tables = await this.flux(buildFluxTimeRange(this.cfg.bucket, measurement));
        const out = {};
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
    async read(measurement, from, to) {
        return fluxTablesToPoints(await this.flux(buildFluxRead(this.cfg.bucket, measurement, from, to)));
    }
    async listMeasurements(limit) {
        const q = [
            // Flux package import (built from parts so code scanners do not take it for a JavaScript import)
            `import ${quoteFluxString('influxdata/influxdb/schema')}`,
            `schema.measurements(bucket: ${quoteFluxString(this.cfg.bucket)}, start: time(v: "${FAR_PAST}"), stop: time(v: "${FAR_FUTURE}"))`,
            `  |> limit(n: ${Math.max(1, Math.floor(limit))})`,
        ].join('\n');
        const out = [];
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
exports.InfluxDB2Reader = InfluxDB2Reader;
/**
 * Creates the reader for the configured version.
 *
 * @param cfg source settings
 */
function createLegacyReader(cfg) {
    return cfg.version === '2.x' ? new InfluxDB2Reader(cfg) : new InfluxDB1Reader(cfg);
}
//# sourceMappingURL=legacySource.js.map