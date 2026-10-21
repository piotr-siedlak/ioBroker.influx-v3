"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.InfluxV3Client = exports.TransportError = exports.HttpError = void 0;
exports.parseRetryAfter = parseRetryAfter;
exports.classifyWrite = classifyWrite;
/**
 * InfluxV3Client: minimal HTTP client for the InfluxDB 3 Core/Enterprise HTTP API.
 * See docs/adr/0003-http-client-instead-of-flight.md.
 */
const node_http_1 = __importDefault(require("node:http"));
const node_https_1 = __importDefault(require("node:https"));
const node_zlib_1 = require("node:zlib");
const node_util_1 = require("node:util");
const redact_1 = require("./redact");
const gzipAsync = (0, node_util_1.promisify)(node_zlib_1.gzip);
class HttpError extends Error {
    status;
    body;
    retryAfterMs;
    constructor(message, status, body, retryAfterMs) {
        super(message);
        this.status = status;
        this.body = body;
        this.retryAfterMs = retryAfterMs;
        this.name = 'HttpError';
    }
}
exports.HttpError = HttpError;
class TransportError extends Error {
    code;
    constructor(message, code) {
        super(message);
        this.code = code;
        this.name = 'TransportError';
    }
}
exports.TransportError = TransportError;
/**
 * Parses a Retry-After header (seconds or HTTP date).
 *
 * @param value header value
 * @param now current time
 */
function parseRetryAfter(value, now = Date.now()) {
    const v = Array.isArray(value) ? value[0] : value;
    if (!v) {
        return undefined;
    }
    const secs = Number(v);
    if (Number.isFinite(secs) && secs >= 0) {
        return Math.min(secs * 1000, 3600000);
    }
    const date = Date.parse(v);
    if (Number.isFinite(date)) {
        return Math.max(0, Math.min(date - now, 3600000));
    }
    return undefined;
}
/**
 * Classifies a write response. Exported for unit tests.
 *
 * @param res response or transport error
 */
function classifyWrite(res) {
    if (res instanceof TransportError) {
        return { kind: 'transient', message: res.message };
    }
    const { status, body } = res;
    if (status >= 200 && status < 300) {
        return { kind: 'ok' };
    }
    let parsed;
    try {
        parsed = body ? JSON.parse(body) : undefined;
    }
    catch {
        parsed = undefined;
    }
    const message = (0, redact_1.redact)(extractMessage(parsed) ?? (body.slice(0, 500) || `HTTP ${status}`));
    if (status === 400 || status === 422) {
        const data = parsed?.data;
        const list = Array.isArray(data) ? data : data && typeof data === 'object' ? [data] : [];
        const rejected = [];
        for (const item of list) {
            const rec = item;
            if (typeof rec.line_number === 'number') {
                rejected.push({
                    line: rec.line_number,
                    message: (0, redact_1.redact)(typeof rec.error_message === 'string' ? rec.error_message : ''),
                });
            }
        }
        if (rejected.length && /partial write/i.test(message)) {
            return { kind: 'partial', rejected, message };
        }
        if (rejected.length) {
            // accept_partial=false or a parser error for a single line: whole batch rejected
            return {
                kind: 'invalid',
                message: `${message}: ${rejected.map(r => `#${r.line} ${r.message}`).join('; ')}`,
            };
        }
        if (status === 422 && /limit/i.test(message)) {
            return { kind: 'invalid', message };
        }
        return { kind: 'invalid', message };
    }
    if (status === 413) {
        return { kind: 'tooLarge', message };
    }
    if (status === 401 || status === 403) {
        return { kind: 'auth', status, message };
    }
    if (status === 404) {
        return { kind: 'config', status, message };
    }
    if (status === 408 || status === 429 || status >= 500) {
        return { kind: 'transient', status, message, retryAfterMs: parseRetryAfter(res.headers['retry-after']) };
    }
    return { kind: 'invalid', message };
}
function extractMessage(parsed) {
    if (parsed && typeof parsed === 'object') {
        const rec = parsed;
        if (typeof rec.error === 'string') {
            return rec.error;
        }
        if (typeof rec.message === 'string') {
            return rec.message;
        }
    }
    return undefined;
}
/**
 * Maps the answer of a permission probe.
 *
 * @param res HTTP answer
 * @param allowed the answer means "allowed"
 * @param missing the answer means "the database does not exist"
 */
function probeResult(res, allowed, missing) {
    if (allowed) {
        return { status: 'ok', httpStatus: res.status };
    }
    if (res.status === 401 || res.status === 403) {
        return { status: 'denied', httpStatus: res.status, message: `HTTP ${res.status}` };
    }
    if (missing) {
        return { status: 'missing', httpStatus: res.status };
    }
    return { status: 'error', httpStatus: res.status, message: (0, redact_1.redact)(`HTTP ${res.status} ${res.body.slice(0, 200)}`) };
}
class InfluxV3Client {
    opts;
    agent;
    base;
    isHttps;
    closed = false;
    active = new Set();
    constructor(opts) {
        this.opts = opts;
        this.base = new URL(opts.url);
        this.isHttps = this.base.protocol === 'https:';
        const agentOpts = {
            keepAlive: true,
            keepAliveMsecs: 10000,
            maxSockets: Math.max(1, opts.maxSockets),
            maxFreeSockets: 2,
        };
        if (this.isHttps) {
            agentOpts.rejectUnauthorized = opts.rejectUnauthorized;
            if (opts.ca?.trim()) {
                agentOpts.ca = opts.ca;
            }
            this.agent = new node_https_1.default.Agent(agentOpts);
        }
        else {
            this.agent = new node_http_1.default.Agent(agentOpts);
        }
    }
    get database() {
        return this.opts.database;
    }
    get edition() {
        return this.opts.edition;
    }
    path(p, query) {
        const basePath = this.base.pathname.replace(/\/+$/, '');
        const qs = query ? `?${new URLSearchParams(query).toString()}` : '';
        return `${basePath}${p}${qs}`;
    }
    authHeader() {
        if (!this.opts.token) {
            return {};
        }
        // InfluxDB 3 Core/Enterprise accept "Bearer"; the v2 API of the cloud products documents "Token"
        const scheme = this.opts.edition === 'cloud' ? 'Token' : 'Bearer';
        return { Authorization: `${scheme} ${this.opts.token}` };
    }
    /**
     * Executes one HTTP request. Never throws HttpError — HTTP status codes are returned.
     * Throws TransportError on network problems / timeouts.
     *
     * @param method HTTP method
     * @param path path incl. query string
     * @param body request body
     * @param headers additional headers
     * @param timeoutMs request timeout
     * @param maxBytes maximum response size
     */
    request(method, path, body, headers = {}, timeoutMs = this.opts.timeoutMs, maxBytes = 64 * 1024 * 1024) {
        if (this.closed) {
            return Promise.reject(new TransportError('client closed', 'ECLOSED'));
        }
        const mod = this.isHttps ? node_https_1.default : node_http_1.default;
        return new Promise((resolvePromise, rejectPromise) => {
            // exactly one outcome per request, whatever order socket/response events arrive in
            let settled = false;
            const holder = {};
            const resolve = (r) => {
                if (!settled) {
                    settled = true;
                    if (holder.req) {
                        this.active.delete(holder.req);
                    }
                    resolvePromise(r);
                }
            };
            const reject = (e) => {
                if (!settled) {
                    settled = true;
                    if (holder.req) {
                        this.active.delete(holder.req);
                    }
                    rejectPromise(e);
                }
            };
            const req = mod.request({
                protocol: this.base.protocol,
                hostname: this.base.hostname.replace(/^\[|\]$/g, ''),
                port: this.base.port || (this.isHttps ? 443 : 80),
                method,
                path,
                agent: this.agent,
                headers: {
                    Accept: 'application/json',
                    'User-Agent': 'iobroker.influx-v3',
                    ...this.authHeader(),
                    ...headers,
                    ...(body !== undefined ? { 'Content-Length': String(Buffer.byteLength(body)) } : {}),
                },
            }, res => {
                const chunks = [];
                let size = 0;
                res.on('data', (chunk) => {
                    if (settled) {
                        return;
                    }
                    size += chunk.length;
                    if (size > maxBytes) {
                        reject(new TransportError(`response larger than ${maxBytes} bytes`, 'ETOOBIG'));
                        res.destroy();
                        req.destroy();
                        return;
                    }
                    chunks.push(chunk);
                });
                res.on('end', () => {
                    resolve({
                        status: res.statusCode ?? 0,
                        headers: res.headers,
                        body: Buffer.concat(chunks).toString('utf8'),
                    });
                });
                res.on('error', err => {
                    reject(new TransportError((0, redact_1.redact)(err.message), err.code ?? 'ERESPONSE'));
                });
                res.on('close', () => {
                    if (!res.complete) {
                        reject(new TransportError('connection closed before the response was complete', 'ECONNRESET'));
                    }
                });
            });
            holder.req = req;
            this.active.add(req);
            req.setTimeout(timeoutMs, () => {
                reject(new TransportError(`timeout after ${timeoutMs} ms`, 'ETIMEDOUT'));
                req.destroy();
            });
            req.on('error', err => {
                if (err instanceof TransportError) {
                    reject(err);
                }
                else {
                    const code = err.code ?? 'EREQUEST';
                    reject(new TransportError(`${code}: ${(0, redact_1.redact)(err.message)}`, code));
                }
            });
            if (body !== undefined) {
                req.write(body);
            }
            req.end();
        });
    }
    /** Checks reachability, authentication, edition and version. */
    async ping() {
        const start = Date.now();
        try {
            const res = await this.request('GET', this.path('/ping'));
            const latencyMs = Date.now() - start;
            if (res.status === 200 || res.status === 204) {
                const build = String(res.headers['x-influxdb-build'] ?? '').toLowerCase();
                let version = String(res.headers['x-influxdb-version'] ?? '');
                let product = '';
                try {
                    const parsed = JSON.parse(res.body);
                    version ||= parsed.version ?? '';
                    product = parsed.product_name ?? '';
                }
                catch {
                    // body is optional
                }
                const text = `${build} ${product}`.toLowerCase();
                const edition = text.includes('enterprise') ? 'enterprise' : text.includes('core') ? 'core' : 'unknown';
                return { ok: true, status: res.status, edition, version, latencyMs };
            }
            return {
                ok: false,
                status: res.status,
                latencyMs,
                error: res.status === 401 || res.status === 403
                    ? 'authentication failed (check the API token)'
                    : (0, redact_1.redact)(`HTTP ${res.status} ${res.body.slice(0, 200)}`),
            };
        }
        catch (e) {
            return { ok: false, status: 0, latencyMs: Date.now() - start, error: (0, redact_1.redact)(e) };
        }
    }
    /**
     * Read permission on the configured database: `SELECT 1` (creates nothing; HTTP 404 = the database does not
     * exist yet).
     */
    async probeRead() {
        try {
            const res = await this.request('POST', this.path('/api/v3/query_sql'), JSON.stringify({ db: this.opts.database, q: 'SELECT 1', format: 'json' }), { 'Content-Type': 'application/json' });
            return probeResult(res, res.status === 200, /database not found/i.test(res.body));
        }
        catch (e) {
            return { status: 'error', httpStatus: 0, message: (0, redact_1.redact)(e) };
        }
    }
    /**
     * Write permission without writing data: InfluxDB 3 refuses an empty write with HTTP 400 "incoming write was
     * empty" after authentication and authorisation. Caution (measured on Core 3.12.1): even an empty write creates a
     * missing database - only probe a database that exists or that the adapter writes to anyway.
     */
    async probeWrite() {
        try {
            const res = await this.request('POST', this.path('/api/v3/write_lp', { db: this.opts.database, precision: 'millisecond' }), '', { 'Content-Type': 'text/plain; charset=utf-8' });
            const accepted = res.status === 204 || res.status === 200 || (res.status === 400 && /empty/i.test(res.body));
            return probeResult(res, accepted, res.status === 404 && /database/i.test(res.body));
        }
        catch (e) {
            return { status: 'error', httpStatus: 0, message: (0, redact_1.redact)(e) };
        }
    }
    /**
     * Writes line protocol records.
     *
     * @param lines serialised lines
     * @param timeoutMs request timeout (default: the client's)
     */
    async write(lines, timeoutMs) {
        const text = lines.join('\n');
        let body = Buffer.from(text, 'utf8');
        const headers = { 'Content-Type': 'text/plain; charset=utf-8' };
        if (this.opts.gzip && body.length >= (this.opts.gzipThreshold ?? 1024)) {
            body = await gzipAsync(body);
            headers['Content-Encoding'] = 'gzip';
        }
        let path;
        if (this.opts.edition === 'cloud') {
            path = this.path('/api/v2/write', { bucket: this.opts.database, precision: 'ms' });
        }
        else {
            const q = { db: this.opts.database, precision: 'millisecond' };
            if (this.opts.noSync) {
                q.no_sync = 'true';
            }
            path = this.path('/api/v3/write_lp', q);
        }
        try {
            const res = await this.request('POST', path, body, headers, timeoutMs);
            return classifyWrite(res);
        }
        catch (e) {
            if (e instanceof TransportError) {
                return classifyWrite(e);
            }
            return { kind: 'transient', message: (0, redact_1.redact)(e) };
        }
    }
    ensureQueryable() {
        if (this.opts.edition === 'cloud') {
            throw new HttpError('Queries are not supported for the experimental "cloud" edition (HTTP query API is Core/Enterprise only)', 501, '');
        }
    }
    /**
     * Runs an SQL query and returns parsed JSON rows.
     *
     * @param sql query text
     * @param params `$name` parameters
     * @param options timeout, size limit, database
     */
    async querySql(sql, params, options = {}) {
        this.ensureQueryable();
        const payload = { db: options.database ?? this.opts.database, q: sql, format: 'json' };
        if (params && Object.keys(params).length) {
            payload.params = params;
        }
        return this.queryJson('/api/v3/query_sql', payload, options);
    }
    /**
     * Runs an InfluxQL query (Core/Enterprise).
     *
     * @param q query text
     * @param options timeout, size limit
     */
    async queryInfluxQL(q, options = {}) {
        this.ensureQueryable();
        return this.queryJson('/api/v3/query_influxql', { db: options.database ?? this.opts.database, q, format: 'json' }, options);
    }
    async queryJson(endpoint, payload, options) {
        const res = await this.request('POST', this.path(endpoint), JSON.stringify(payload), { 'Content-Type': 'application/json' }, options.timeoutMs ?? this.opts.timeoutMs, options.maxBytes);
        if (res.status !== 200) {
            let msg = res.body.slice(0, 1000);
            try {
                msg = extractMessage(JSON.parse(res.body)) ?? msg;
            }
            catch {
                // plain text error
            }
            throw new HttpError((0, redact_1.redact)(msg.split('\n')[0] || `HTTP ${res.status}`), res.status, '', parseRetryAfter(res.headers['retry-after']));
        }
        if (!res.body.trim()) {
            return [];
        }
        const parsed = JSON.parse(res.body);
        return Array.isArray(parsed) ? parsed : [];
    }
    /** Lists the databases (requires a token with the right permission). */
    async listDatabases() {
        this.ensureQueryable();
        const res = await this.request('GET', this.path('/api/v3/configure/database', { format: 'json' }));
        if (res.status !== 200) {
            throw new HttpError((0, redact_1.redact)(`HTTP ${res.status} ${res.body.slice(0, 200)}`), res.status, '');
        }
        const rows = JSON.parse(res.body);
        return rows
            .map(r => r['iox::database'] ?? r.database ?? r.name)
            .filter((n) => typeof n === 'string' && !!n);
    }
    /**
     * Creates a database. Only called on explicit user request.
     *
     * @param name database name
     * @param retention retention period (e.g. `30d`), empty = infinite
     */
    async createDatabase(name, retention) {
        this.ensureQueryable();
        const payload = { db: name };
        if (retention && retention !== 'none') {
            payload.retention_period = retention;
        }
        const res = await this.request('POST', this.path('/api/v3/configure/database'), JSON.stringify(payload), {
            'Content-Type': 'application/json',
        });
        if (res.status !== 200 && res.status !== 201 && res.status !== 204) {
            throw new HttpError((0, redact_1.redact)(`HTTP ${res.status} ${res.body.slice(0, 300)}`), res.status, '');
        }
    }
    /** Columns of all user tables of the configured database. */
    async getSchemaColumns() {
        try {
            const rows = await this.querySql("SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'iox'");
            return rows.map(r => ({ table: r.table_name, column: r.column_name, dataType: r.data_type }));
        }
        catch (e) {
            if (e instanceof HttpError && e.status === 404) {
                return []; // database does not exist yet - it is created by the first write
            }
            throw e;
        }
    }
    /** Retention of the configured database in ms (undefined = infinite or not readable). */
    async getRetentionMs() {
        try {
            const rows = await this.querySql('SELECT retention_period_ns FROM system.databases WHERE database_name = $db', { db: this.opts.database }, { database: '_internal' });
            if (!rows.length) {
                return null;
            }
            const ns = rows[0].retention_period_ns;
            return typeof ns === 'number' ? Math.round(ns / 1e6) : undefined;
        }
        catch {
            return null; // not available (permissions, edition)
        }
    }
    /** Aborts running requests and closes keep-alive sockets. */
    close() {
        this.closed = true;
        for (const req of this.active) {
            req.destroy(new TransportError('client closed', 'ECLOSED'));
        }
        this.active.clear();
        this.agent.destroy();
    }
}
exports.InfluxV3Client = InfluxV3Client;
//# sourceMappingURL=influxClient.js.map