/**
 * InfluxV3Client: minimal HTTP client for the InfluxDB 3 Core/Enterprise HTTP API.
 * See docs/adr/0003-http-client-instead-of-flight.md.
 */
import http from 'node:http';
import https from 'node:https';
import { gzip as gzipCb } from 'node:zlib';
import { promisify } from 'node:util';
import type { Edition } from './config';
import { redact } from './redact';

const gzipAsync = promisify(gzipCb);

export interface ClientOptions {
    url: string;
    token: string;
    database: string;
    edition: Edition;
    timeoutMs: number;
    rejectUnauthorized: boolean;
    ca?: string;
    gzip: boolean;
    noSync: boolean;
    maxSockets: number;
    /** minimum body size for gzip */
    gzipThreshold?: number;
}

export interface HttpResponse {
    status: number;
    headers: http.IncomingHttpHeaders;
    body: string;
}

export class HttpError extends Error {
    constructor(
        message: string,
        readonly status: number,
        readonly body: string,
        readonly retryAfterMs?: number,
    ) {
        super(message);
        this.name = 'HttpError';
    }
}

export class TransportError extends Error {
    constructor(
        message: string,
        readonly code: string,
    ) {
        super(message);
        this.name = 'TransportError';
    }
}

export interface RejectedLine {
    /** 1-based record number inside the batch */
    line: number;
    message: string;
}

export type WriteResult =
    | { kind: 'ok' }
    | { kind: 'partial'; rejected: RejectedLine[]; message: string }
    | { kind: 'invalid'; message: string }
    | { kind: 'tooLarge'; message: string }
    | { kind: 'auth'; status: number; message: string }
    | { kind: 'config'; status: number; message: string }
    | { kind: 'transient'; status?: number; message: string; retryAfterMs?: number };

export interface PingResult {
    ok: boolean;
    status: number;
    edition?: 'core' | 'enterprise' | 'unknown';
    version?: string;
    latencyMs: number;
    error?: string;
}

export interface QueryOptions {
    timeoutMs?: number;
    maxBytes?: number;
    database?: string;
}

/**
 * Parses a Retry-After header (seconds or HTTP date).
 *
 * @param value header value
 * @param now current time
 */
export function parseRetryAfter(value: string | string[] | undefined, now = Date.now()): number | undefined {
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
export function classifyWrite(res: HttpResponse | TransportError): WriteResult {
    if (res instanceof TransportError) {
        return { kind: 'transient', message: res.message };
    }
    const { status, body } = res;
    if (status >= 200 && status < 300) {
        return { kind: 'ok' };
    }
    let parsed: unknown;
    try {
        parsed = body ? JSON.parse(body) : undefined;
    } catch {
        parsed = undefined;
    }
    const message = redact(extractMessage(parsed) ?? (body.slice(0, 500) || `HTTP ${status}`));
    if (status === 400 || status === 422) {
        const data = (parsed as { data?: unknown } | undefined)?.data;
        const list = Array.isArray(data) ? data : data && typeof data === 'object' ? [data] : [];
        const rejected: RejectedLine[] = [];
        for (const item of list) {
            const rec = item as { line_number?: unknown; error_message?: unknown };
            if (typeof rec.line_number === 'number') {
                rejected.push({
                    line: rec.line_number,
                    message: redact(typeof rec.error_message === 'string' ? rec.error_message : ''),
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

function extractMessage(parsed: unknown): string | undefined {
    if (parsed && typeof parsed === 'object') {
        const rec = parsed as Record<string, unknown>;
        if (typeof rec.error === 'string') {
            return rec.error;
        }
        if (typeof rec.message === 'string') {
            return rec.message;
        }
    }
    return undefined;
}

export class InfluxV3Client {
    private readonly agent: http.Agent;
    private readonly base: URL;
    private readonly isHttps: boolean;
    private closed = false;
    private readonly active = new Set<http.ClientRequest>();

    constructor(private readonly opts: ClientOptions) {
        this.base = new URL(opts.url);
        this.isHttps = this.base.protocol === 'https:';
        const agentOpts: https.AgentOptions = {
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
            this.agent = new https.Agent(agentOpts);
        } else {
            this.agent = new http.Agent(agentOpts);
        }
    }

    get database(): string {
        return this.opts.database;
    }

    get edition(): Edition {
        return this.opts.edition;
    }

    private path(p: string, query?: Record<string, string>): string {
        const basePath = this.base.pathname.replace(/\/+$/, '');
        const qs = query ? `?${new URLSearchParams(query).toString()}` : '';
        return `${basePath}${p}${qs}`;
    }

    private authHeader(): Record<string, string> {
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
    request(
        method: string,
        path: string,
        body?: Buffer | string,
        headers: Record<string, string> = {},
        timeoutMs = this.opts.timeoutMs,
        maxBytes = 64 * 1024 * 1024,
    ): Promise<HttpResponse> {
        if (this.closed) {
            return Promise.reject(new TransportError('client closed', 'ECLOSED'));
        }
        const mod = this.isHttps ? https : http;
        return new Promise<HttpResponse>((resolve, reject) => {
            const req = mod.request(
                {
                    protocol: this.base.protocol,
                    hostname: this.base.hostname.replace(/^\[|\]$/g, ''),
                    port: this.base.port || (this.isHttps ? 443 : 80),
                    method,
                    path,
                    agent: this.agent,
                    headers: {
                        Accept: 'application/json',
                        'User-Agent': 'iobroker.influx_v3',
                        ...this.authHeader(),
                        ...headers,
                        ...(body !== undefined ? { 'Content-Length': String(Buffer.byteLength(body)) } : {}),
                    },
                },
                res => {
                    const chunks: Buffer[] = [];
                    let size = 0;
                    res.on('data', (chunk: Buffer) => {
                        size += chunk.length;
                        if (size > maxBytes) {
                            req.destroy(new TransportError(`response larger than ${maxBytes} bytes`, 'ETOOBIG'));
                            return;
                        }
                        chunks.push(chunk);
                    });
                    res.on('end', () => {
                        this.active.delete(req);
                        resolve({
                            status: res.statusCode ?? 0,
                            headers: res.headers,
                            body: Buffer.concat(chunks).toString('utf8'),
                        });
                    });
                    res.on('error', err => {
                        this.active.delete(req);
                        reject(
                            new TransportError(redact(err.message), (err as NodeJS.ErrnoException).code ?? 'ERESPONSE'),
                        );
                    });
                },
            );
            this.active.add(req);
            req.setTimeout(timeoutMs, () => {
                req.destroy(new TransportError(`timeout after ${timeoutMs} ms`, 'ETIMEDOUT'));
            });
            req.on('error', err => {
                this.active.delete(req);
                if (err instanceof TransportError) {
                    reject(err);
                } else {
                    const code = (err as NodeJS.ErrnoException).code ?? 'EREQUEST';
                    reject(new TransportError(`${code}: ${redact(err.message)}`, code));
                }
            });
            if (body !== undefined) {
                req.write(body);
            }
            req.end();
        });
    }

    /** Checks reachability, authentication, edition and version. */
    async ping(): Promise<PingResult> {
        const start = Date.now();
        try {
            const res = await this.request('GET', this.path('/ping'));
            const latencyMs = Date.now() - start;
            if (res.status === 200 || res.status === 204) {
                const build = String(res.headers['x-influxdb-build'] ?? '').toLowerCase();
                let version = String(res.headers['x-influxdb-version'] ?? '');
                let product = '';
                try {
                    const parsed = JSON.parse(res.body) as { version?: string; product_name?: string };
                    version ||= parsed.version ?? '';
                    product = parsed.product_name ?? '';
                } catch {
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
                error:
                    res.status === 401 || res.status === 403
                        ? 'authentication failed (check the API token)'
                        : redact(`HTTP ${res.status} ${res.body.slice(0, 200)}`),
            };
        } catch (e) {
            return { ok: false, status: 0, latencyMs: Date.now() - start, error: redact(e) };
        }
    }

    /**
     * Writes line protocol records.
     *
     * @param lines serialised lines
     */
    async write(lines: string[]): Promise<WriteResult> {
        const text = lines.join('\n');
        let body: Buffer = Buffer.from(text, 'utf8');
        const headers: Record<string, string> = { 'Content-Type': 'text/plain; charset=utf-8' };
        if (this.opts.gzip && body.length >= (this.opts.gzipThreshold ?? 1024)) {
            body = await gzipAsync(body);
            headers['Content-Encoding'] = 'gzip';
        }
        let path: string;
        if (this.opts.edition === 'cloud') {
            path = this.path('/api/v2/write', { bucket: this.opts.database, precision: 'ms' });
        } else {
            const q: Record<string, string> = { db: this.opts.database, precision: 'millisecond' };
            if (this.opts.noSync) {
                q.no_sync = 'true';
            }
            path = this.path('/api/v3/write_lp', q);
        }
        try {
            const res = await this.request('POST', path, body, headers);
            return classifyWrite(res);
        } catch (e) {
            if (e instanceof TransportError) {
                return classifyWrite(e);
            }
            return { kind: 'transient', message: redact(e) };
        }
    }

    private ensureQueryable(): void {
        if (this.opts.edition === 'cloud') {
            throw new HttpError(
                'Queries are not supported for the experimental "cloud" edition (HTTP query API is Core/Enterprise only)',
                501,
                '',
            );
        }
    }

    /**
     * Runs an SQL query and returns parsed JSON rows.
     *
     * @param sql query text
     * @param params `$name` parameters
     * @param options timeout, size limit, database
     */
    async querySql<T = Record<string, unknown>>(
        sql: string,
        params?: Record<string, string | number | boolean>,
        options: QueryOptions = {},
    ): Promise<T[]> {
        this.ensureQueryable();
        const payload: Record<string, unknown> = { db: options.database ?? this.opts.database, q: sql, format: 'json' };
        if (params && Object.keys(params).length) {
            payload.params = params;
        }
        return this.queryJson<T>('/api/v3/query_sql', payload, options);
    }

    /**
     * Runs an InfluxQL query (Core/Enterprise).
     *
     * @param q query text
     * @param options timeout, size limit
     */
    async queryInfluxQL<T = Record<string, unknown>>(q: string, options: QueryOptions = {}): Promise<T[]> {
        this.ensureQueryable();
        return this.queryJson<T>(
            '/api/v3/query_influxql',
            { db: options.database ?? this.opts.database, q, format: 'json' },
            options,
        );
    }

    private async queryJson<T>(
        endpoint: string,
        payload: Record<string, unknown>,
        options: QueryOptions,
    ): Promise<T[]> {
        const res = await this.request(
            'POST',
            this.path(endpoint),
            JSON.stringify(payload),
            { 'Content-Type': 'application/json' },
            options.timeoutMs ?? this.opts.timeoutMs,
            options.maxBytes,
        );
        if (res.status !== 200) {
            let msg = res.body.slice(0, 1000);
            try {
                msg = extractMessage(JSON.parse(res.body)) ?? msg;
            } catch {
                // plain text error
            }
            throw new HttpError(
                redact(msg.split('\n')[0] || `HTTP ${res.status}`),
                res.status,
                '',
                parseRetryAfter(res.headers['retry-after']),
            );
        }
        if (!res.body.trim()) {
            return [];
        }
        const parsed = JSON.parse(res.body) as unknown;
        return Array.isArray(parsed) ? (parsed as T[]) : [];
    }

    /** Lists the databases (requires a token with the right permission). */
    async listDatabases(): Promise<string[]> {
        this.ensureQueryable();
        const res = await this.request('GET', this.path('/api/v3/configure/database', { format: 'json' }));
        if (res.status !== 200) {
            throw new HttpError(redact(`HTTP ${res.status} ${res.body.slice(0, 200)}`), res.status, '');
        }
        const rows = JSON.parse(res.body) as Record<string, unknown>[];
        return rows
            .map(r => r['iox::database'] ?? r.database ?? r.name)
            .filter((n): n is string => typeof n === 'string' && !!n);
    }

    /**
     * Creates a database. Only called on explicit user request.
     *
     * @param name database name
     * @param retention retention period (e.g. `30d`), empty = infinite
     */
    async createDatabase(name: string, retention?: string): Promise<void> {
        this.ensureQueryable();
        const payload: Record<string, string> = { db: name };
        if (retention && retention !== 'none') {
            payload.retention_period = retention;
        }
        const res = await this.request('POST', this.path('/api/v3/configure/database'), JSON.stringify(payload), {
            'Content-Type': 'application/json',
        });
        if (res.status !== 200 && res.status !== 201 && res.status !== 204) {
            throw new HttpError(redact(`HTTP ${res.status} ${res.body.slice(0, 300)}`), res.status, '');
        }
    }

    /** Columns of all user tables of the configured database. */
    async getSchemaColumns(): Promise<{ table: string; column: string; dataType: string }[]> {
        try {
            const rows = await this.querySql<{ table_name: string; column_name: string; data_type: string }>(
                "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'iox'",
            );
            return rows.map(r => ({ table: r.table_name, column: r.column_name, dataType: r.data_type }));
        } catch (e) {
            if (e instanceof HttpError && e.status === 404) {
                return []; // database does not exist yet - it is created by the first write
            }
            throw e;
        }
    }

    /** Retention of the configured database in ms (undefined = infinite or not readable). */
    async getRetentionMs(): Promise<number | undefined | null> {
        try {
            const rows = await this.querySql<{ retention_period_ns?: number | null }>(
                'SELECT retention_period_ns FROM system.databases WHERE database_name = $db',
                { db: this.opts.database },
                { database: '_internal' },
            );
            if (!rows.length) {
                return null;
            }
            const ns = rows[0].retention_period_ns;
            return typeof ns === 'number' ? Math.round(ns / 1e6) : undefined;
        } catch {
            return null; // not available (permissions, edition)
        }
    }

    /** Aborts running requests and closes keep-alive sockets. */
    close(): void {
        this.closed = true;
        for (const req of this.active) {
            req.destroy(new TransportError('client closed', 'ECLOSED'));
        }
        this.active.clear();
        this.agent.destroy();
    }
}
