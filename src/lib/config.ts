/**
 * ConfigurationService: normalisation and validation of the instance configuration (`native`)
 * and of the per-state configuration (`common.custom["influx-v3.N"]`).
 *
 * Every value is clamped into a documented range; invalid values fall back to the default and
 * produce a warning instead of crashing the adapter.
 */

export type Edition = 'auto' | 'core' | 'enterprise' | 'cloud';
export type SchemaMode = 'grouped' | 'perState';
export type MetaPlacement = 'field' | 'tag' | 'none';
export type TagKeyStyle = 'snake' | 'preserve';
export type StoreAs = '' | 'number' | 'boolean' | 'string';
export type AckFilter = 'all' | 'ackOnly' | 'notAckOnly';
export type QFilter = 'all' | 'goodOnly';
export type ValueType = 'number' | 'boolean' | 'string';

export interface TagPair {
    key: string;
    value: string;
}

export interface TableRule {
    /** what the glob pattern is compared with */
    match: 'role' | 'id';
    pattern: string;
    /** value types the rule applies to; '' = number only (default, avoids type conflicts) */
    valueType: '' | ValueType | 'any';
    table: string;
}

/** Instance configuration after normalisation */
export interface InstanceConfig {
    edition: Edition;
    url: string;
    database: string;
    token: string;
    tlsRejectUnauthorized: boolean;
    tlsCa: string;
    requestTimeoutMs: number;
    pingIntervalSec: number;
    createDatabaseRetention: string;

    writeMode: 'batched' | 'direct';
    batchMaxLines: number;
    batchMaxBytes: number;
    flushIntervalMs: number;
    gzip: boolean;
    noSync: boolean;
    maxConcurrentWrites: number;
    retryBaseDelayMs: number;
    retryMaxDelayMs: number;
    authRetryMs: number;

    bufferMode: 'memory' | 'disk';
    bufferMaxRecords: number;
    bufferMaxBytes: number;
    spoolMaxBytes: number;
    bufferMaxAgeHours: number;
    overflowPolicy: 'dropOldest' | 'dropNewest';
    persistOnShutdown: boolean;
    spoolFsync: boolean;
    shutdownFlushTimeoutMs: number;

    schemaMode: SchemaMode;
    tableNumber: string;
    tableBoolean: string;
    tableString: string;
    tableRules: TableRule[];
    valueField: string;
    maxTables: number;
    maxColumns: number;
    maxStringLength: number;
    objectPolicy: 'json' | 'reject';
    metaAck: MetaPlacement;
    metaQ: MetaPlacement;
    metaFrom: MetaPlacement;

    globalTags: TagPair[];
    tagKeyStyle: TagKeyStyle;
    autoTagRoom: boolean;
    autoTagFunction: boolean;
    autoTagAdapter: boolean;
    autoTagInstance: boolean;
    autoTagDevice: boolean;
    autoTagChannel: boolean;
    autoTagUnit: boolean;
    autoTagRole: boolean;
    autoTagName: boolean;
    nativeAllowlist: string[];
    maxTagValuesPerKey: number;
    maxTagValueLength: number;
    blockSensitiveTags: boolean;
    dynamicTagsEnabled: boolean;
    dynamicTagTtlSec: number;
    dynamicTagMaxValues: number;

    // defaults for states
    changesOnly: boolean;
    debounceTime: number;
    blockTime: number;
    changesRelogInterval: number;
    changesMinDelta: number;
    storeOnStart: boolean;
    logSkippedBeforeChange: boolean;
    ackFilter: AckFilter;
    qFilter: QFilter;
    round: number | null;
    storeAck: boolean;
    storeQ: boolean;
    storeFrom: boolean;

    historyMaxRows: number;
    historyHardLimit: number;
    queryTimeoutMs: number;
    queryMaxRows: number;
    queryMaxBytes: number;
    defaultAggregate: string;
    querySenders: string[];
    allowUserQueries: boolean;

    subscribeAllThreshold: number;
    logRejectedLimitPerMin: number;
    statsIntervalSec: number;
    writeStatsToInflux: boolean;
    debugStates: boolean;
}

/** Per-state configuration after normalisation */
export interface StateConfig {
    enabled: boolean;
    table: string;
    aliasId: string;
    storeAs: StoreAs;
    field: string;
    customTags: TagPair[];
    excludeTags: string[];
    useGlobalTags: boolean;
    useAutoTags: boolean;
    allowSensitiveTags: boolean;
    changesOnly: boolean;
    debounceTime: number;
    blockTime: number;
    changesRelogInterval: number;
    changesMinDelta: number;
    storeOnStart: boolean;
    logSkippedBeforeChange: boolean;
    ignoreZero: boolean;
    ignoreBelowNumber: number | null;
    ignoreAboveNumber: number | null;
    ackFilter: AckFilter;
    qFilter: QFilter;
    round: number | null;
    storeAck: boolean;
    storeQ: boolean;
    storeFrom: boolean;
    enableDebugLogs: boolean;
}

export interface Normalized<T> {
    config: T;
    warnings: string[];
}

type Raw = Record<string, unknown>;

/** Hard limits of the server measured on Core 3.12.1, used as upper bounds */
export const SERVER_MAX_REQUEST_BYTES = 10 * 1024 * 1024;
export const SERVER_MAX_STRING_BYTES = 1024 * 1024;
export const MAX_RELOG_SECONDS = 2147483; // setTimeout limit (2^31-1 ms)

export const DEFAULT_INSTANCE_CONFIG: Readonly<InstanceConfig> = Object.freeze({
    edition: 'auto',
    url: 'http://localhost:8181',
    database: 'iobroker',
    token: '',
    tlsRejectUnauthorized: true,
    tlsCa: '',
    requestTimeoutMs: 10000,
    pingIntervalSec: 60,
    createDatabaseRetention: '',
    writeMode: 'batched',
    batchMaxLines: 5000,
    batchMaxBytes: 1048576,
    flushIntervalMs: 2000,
    gzip: true,
    noSync: false,
    maxConcurrentWrites: 1,
    retryBaseDelayMs: 1000,
    retryMaxDelayMs: 60000,
    authRetryMs: 300000,
    bufferMode: 'memory',
    bufferMaxRecords: 200000,
    bufferMaxBytes: 33554432,
    spoolMaxBytes: 268435456,
    bufferMaxAgeHours: 168,
    overflowPolicy: 'dropOldest',
    persistOnShutdown: true,
    spoolFsync: true,
    shutdownFlushTimeoutMs: 5000,
    schemaMode: 'grouped',
    tableNumber: 'numeric_states',
    tableBoolean: 'boolean_states',
    tableString: 'string_states',
    tableRules: [],
    valueField: 'value',
    maxTables: 2000,
    maxColumns: 500,
    maxStringLength: 65536,
    objectPolicy: 'json',
    metaAck: 'field',
    metaQ: 'field',
    metaFrom: 'none',
    globalTags: [],
    tagKeyStyle: 'snake',
    autoTagRoom: true,
    autoTagFunction: true,
    autoTagAdapter: false,
    autoTagInstance: false,
    autoTagDevice: false,
    autoTagChannel: false,
    autoTagUnit: false,
    autoTagRole: false,
    autoTagName: false,
    nativeAllowlist: [],
    maxTagValuesPerKey: 1000,
    maxTagValueLength: 256,
    blockSensitiveTags: true,
    dynamicTagsEnabled: false,
    dynamicTagTtlSec: 3600,
    dynamicTagMaxValues: 100,
    changesOnly: true,
    debounceTime: 0,
    blockTime: 0,
    changesRelogInterval: 0,
    changesMinDelta: 0,
    storeOnStart: true,
    logSkippedBeforeChange: true,
    ackFilter: 'all',
    qFilter: 'all',
    round: null,
    storeAck: true,
    storeQ: true,
    storeFrom: false,
    historyMaxRows: 2000,
    historyHardLimit: 100000,
    queryTimeoutMs: 30000,
    queryMaxRows: 10000,
    queryMaxBytes: 10485760,
    defaultAggregate: 'average',
    querySenders: [],
    allowUserQueries: true,
    subscribeAllThreshold: 20,
    logRejectedLimitPerMin: 10,
    statsIntervalSec: 10,
    writeStatsToInflux: false,
    debugStates: false,
});

/** Database names allowed by InfluxDB 3 (measured error text): ASCII letters, digits, `_` and `-` */
export const DATABASE_NAME_RE = /^[A-Za-z0-9_-]{1,64}$/;
/** Retention durations accepted by InfluxDB 3: combinations of h/d/w/mo/y, or `none` */
export const RETENTION_RE = /^(none|(\d+(h|d|w|mo|y))+)$/;

class Reader {
    readonly warnings: string[] = [];

    constructor(
        private readonly raw: Raw,
        private readonly prefix: string,
    ) {}

    has(key: string): boolean {
        const v = this.raw[key];
        return v !== undefined && v !== null && v !== '';
    }

    bool(key: string, def: boolean): boolean {
        const v = this.raw[key];
        if (v === undefined || v === null || v === '') {
            return def;
        }
        if (typeof v === 'boolean') {
            return v;
        }
        if (v === 'true' || v === 1 || v === '1') {
            return true;
        }
        if (v === 'false' || v === 0 || v === '0') {
            return false;
        }
        this.warnings.push(`${this.prefix}${key}: invalid boolean ${JSON.stringify(v)}, using ${def}`);
        return def;
    }

    num(key: string, def: number, min: number, max: number, integer = true): number {
        const v = this.raw[key];
        if (v === undefined || v === null || v === '') {
            return def;
        }
        let n = typeof v === 'number' ? v : typeof v === 'string' ? Number(v.trim()) : NaN;
        if (!Number.isFinite(n)) {
            this.warnings.push(`${this.prefix}${key}: invalid number ${JSON.stringify(v)}, using ${def}`);
            return def;
        }
        if (integer) {
            n = Math.round(n);
        }
        if (n < min) {
            this.warnings.push(`${this.prefix}${key}: ${n} is below ${min}, using ${min}`);
            return min;
        }
        if (n > max) {
            this.warnings.push(`${this.prefix}${key}: ${n} is above ${max}, using ${max}`);
            return max;
        }
        return n;
    }

    optNum(key: string): number | null {
        const v = this.raw[key];
        if (v === undefined || v === null || (typeof v === 'string' && v.trim() === '')) {
            return null;
        }
        const n = typeof v === 'number' ? v : typeof v === 'string' ? Number(v.trim()) : NaN;
        if (!Number.isFinite(n)) {
            this.warnings.push(`${this.prefix}${key}: invalid number ${JSON.stringify(v)}, ignored`);
            return null;
        }
        return n;
    }

    str(key: string, def: string, maxLen = 1024): string {
        const v = this.raw[key];
        if (v === undefined || v === null) {
            return def;
        }
        if (typeof v !== 'string' && typeof v !== 'number') {
            this.warnings.push(`${this.prefix}${key}: invalid text, using default`);
            return def;
        }
        const s = String(v).trim();
        if (s.length > maxLen) {
            this.warnings.push(`${this.prefix}${key}: longer than ${maxLen} characters, truncated`);
            return s.slice(0, maxLen);
        }
        return s;
    }

    oneOf<T extends string>(key: string, def: T, allowed: readonly T[]): T {
        const v = this.raw[key];
        if (v === undefined || v === null || v === '') {
            return def;
        }
        if (typeof v === 'string' && (allowed as readonly string[]).includes(v)) {
            return v as T;
        }
        this.warnings.push(
            `${this.prefix}${key}: ${JSON.stringify(v)} is not one of ${allowed.join(', ')}, using ${def}`,
        );
        return def;
    }

    tags(key: string): TagPair[] {
        const v = this.raw[key];
        if (v === undefined || v === null || v === '') {
            return [];
        }
        let list: unknown = v;
        if (typeof v === 'string') {
            // "a=b, c=d" text form (e.g. from enableHistory scripts)
            list = v
                .split(/[,;\n]/)
                .map(p => p.trim())
                .filter(Boolean)
                .map(p => {
                    const i = p.indexOf('=');
                    return i > 0 ? { key: p.slice(0, i), value: p.slice(i + 1) } : { key: p, value: '' };
                });
        } else if (!Array.isArray(v) && typeof v === 'object') {
            list = Object.entries(v as Raw).map(([k, val]) => ({ key: k, value: val }));
        }
        if (!Array.isArray(list)) {
            this.warnings.push(`${this.prefix}${key}: invalid tag list, ignored`);
            return [];
        }
        const out: TagPair[] = [];
        for (const item of list) {
            if (!item || typeof item !== 'object') {
                continue;
            }
            const rec = item as Raw;
            // accept {key,value} (this adapter) and {name,value} (ioBroker.influxdb)
            const k = rec.key ?? rec.name;
            const val = rec.value;
            if (typeof k !== 'string' || !k.trim()) {
                continue;
            }
            if (
                val === undefined ||
                val === null ||
                (typeof val !== 'string' && typeof val !== 'number' && typeof val !== 'boolean')
            ) {
                this.warnings.push(`${this.prefix}${key}: tag "${k}" has no value, ignored`);
                continue;
            }
            const value = String(val).trim();
            if (!value) {
                this.warnings.push(`${this.prefix}${key}: tag "${k}" has no value, ignored`);
                continue;
            }
            out.push({ key: k.trim(), value });
        }
        return out;
    }

    list(key: string): string[] {
        const v = this.raw[key];
        if (Array.isArray(v)) {
            return v
                .filter((x): x is string => typeof x === 'string')
                .map(s => s.trim())
                .filter(Boolean);
        }
        if (typeof v === 'string') {
            return v
                .split(/[,;\s]+/)
                .map(s => s.trim())
                .filter(Boolean);
        }
        return [];
    }
}

/**
 * Validates a table name chosen by the user (explicit or default).
 * Returns an error text or null.
 *
 * @param name the table name
 */
export function validateTableName(name: string): string | null {
    if (!name) {
        return 'table name is empty';
    }
    if (name.length > 255) {
        return 'table name is longer than 255 characters';
    }
    // eslint-disable-next-line no-control-regex
    if (/[\u0000-\u001f\u007f]/.test(name)) {
        return 'table name contains control characters';
    }
    if (name.startsWith('_')) {
        return 'table names starting with "_" are reserved';
    }
    if (name.endsWith('\\')) {
        return 'table name must not end with a backslash';
    }
    if (name.startsWith('iox::') || name.toLowerCase() === 'time') {
        return `table name "${name}" is reserved`;
    }
    return null;
}

/** Field (column) names usable for the value field */
export const FIELD_NAME_RE = /^[A-Za-z][A-Za-z0-9_]{0,63}$/;

function normalizeUrl(url: string, warnings: string[]): string {
    let u: URL;
    try {
        u = new URL(url);
    } catch {
        warnings.push(`url: "${url}" is not a valid URL, using ${DEFAULT_INSTANCE_CONFIG.url}`);
        return DEFAULT_INSTANCE_CONFIG.url;
    }
    if (u.protocol !== 'http:' && u.protocol !== 'https:') {
        warnings.push(
            `url: protocol ${u.protocol} not supported (http/https only), using ${DEFAULT_INSTANCE_CONFIG.url}`,
        );
        return DEFAULT_INSTANCE_CONFIG.url;
    }
    if (u.username || u.password) {
        warnings.push('url: credentials inside the URL are ignored - use the token field');
        u.username = '';
        u.password = '';
    }
    u.search = '';
    u.hash = '';
    return u.toString().replace(/\/+$/, '');
}

function parseRules(raw: unknown, warnings: string[]): TableRule[] {
    if (!Array.isArray(raw)) {
        return [];
    }
    const rules: TableRule[] = [];
    raw.forEach((r, i) => {
        if (!r || typeof r !== 'object') {
            return;
        }
        const rec = r as Raw;
        const pattern = typeof rec.pattern === 'string' ? rec.pattern.trim() : '';
        const table = typeof rec.table === 'string' ? rec.table.trim() : '';
        if (!pattern || !table) {
            return;
        }
        const err = validateTableName(table);
        if (err) {
            warnings.push(`tableRules[${i}]: ${err}, rule ignored`);
            return;
        }
        const match = rec.match === 'id' ? 'id' : 'role';
        const vt = rec.valueType;
        const valueType: TableRule['valueType'] =
            vt === 'number' || vt === 'boolean' || vt === 'string' || vt === 'any' ? vt : '';
        rules.push({ match, pattern, valueType, table });
    });
    return rules;
}

/**
 * Normalises the instance configuration.
 *
 * @param raw `adapter.config` as received from js-controller (token already decrypted)
 */
export function normalizeInstanceConfig(raw: Raw): Normalized<InstanceConfig> {
    const r = new Reader(raw ?? {}, '');
    const d = DEFAULT_INSTANCE_CONFIG;
    const warnings = r.warnings;

    const database = r.str('database', d.database, 64);
    let db = database;
    if (!DATABASE_NAME_RE.test(database)) {
        warnings.push(`database: "${database}" is invalid (letters, digits, "_" and "-" only), using "${d.database}"`);
        db = d.database;
    }
    const retention = r.str('createDatabaseRetention', '', 32);
    const config: InstanceConfig = {
        edition: r.oneOf('edition', d.edition, ['auto', 'core', 'enterprise', 'cloud'] as const),
        url: normalizeUrl(r.str('url', d.url, 2048), warnings),
        database: db,
        token: typeof raw?.token === 'string' ? raw.token.trim() : '',
        tlsRejectUnauthorized: r.bool('tlsRejectUnauthorized', d.tlsRejectUnauthorized),
        tlsCa: r.str('tlsCa', '', 65536),
        requestTimeoutMs: r.num('requestTimeoutMs', d.requestTimeoutMs, 1000, 300000),
        pingIntervalSec: r.num('pingIntervalSec', d.pingIntervalSec, 10, 3600),
        createDatabaseRetention: retention && RETENTION_RE.test(retention) ? retention : '',

        writeMode: r.oneOf('writeMode', d.writeMode, ['batched', 'direct'] as const),
        batchMaxLines: r.num('batchMaxLines', d.batchMaxLines, 1, 100000),
        batchMaxBytes: r.num('batchMaxBytes', d.batchMaxBytes, 4096, 8 * 1024 * 1024),
        flushIntervalMs: r.num('flushIntervalMs', d.flushIntervalMs, 100, 600000),
        gzip: r.bool('gzip', d.gzip),
        noSync: r.bool('noSync', d.noSync),
        maxConcurrentWrites: r.num('maxConcurrentWrites', d.maxConcurrentWrites, 1, 8),
        retryBaseDelayMs: r.num('retryBaseDelayMs', d.retryBaseDelayMs, 100, 60000),
        retryMaxDelayMs: r.num('retryMaxDelayMs', d.retryMaxDelayMs, 1000, 3600000),
        authRetryMs: r.num('authRetryMs', d.authRetryMs, 10000, 86400000),

        bufferMode: r.oneOf('bufferMode', d.bufferMode, ['memory', 'disk'] as const),
        bufferMaxRecords: r.num('bufferMaxRecords', d.bufferMaxRecords, 1000, 10000000),
        bufferMaxBytes: r.num('bufferMaxBytes', d.bufferMaxBytes, 1024 * 1024, 1024 * 1024 * 1024),
        spoolMaxBytes: r.num('spoolMaxBytes', d.spoolMaxBytes, 1024 * 1024, 50 * 1024 * 1024 * 1024),
        bufferMaxAgeHours: r.num('bufferMaxAgeHours', d.bufferMaxAgeHours, 1, 24 * 365),
        overflowPolicy: r.oneOf('overflowPolicy', d.overflowPolicy, ['dropOldest', 'dropNewest'] as const),
        persistOnShutdown: r.bool('persistOnShutdown', d.persistOnShutdown),
        spoolFsync: r.bool('spoolFsync', d.spoolFsync),
        shutdownFlushTimeoutMs: r.num('shutdownFlushTimeoutMs', d.shutdownFlushTimeoutMs, 500, 8000),

        schemaMode: r.oneOf('schemaMode', d.schemaMode, ['grouped', 'perState'] as const),
        tableNumber: r.str('tableNumber', d.tableNumber, 255) || d.tableNumber,
        tableBoolean: r.str('tableBoolean', d.tableBoolean, 255) || d.tableBoolean,
        tableString: r.str('tableString', d.tableString, 255) || d.tableString,
        tableRules: parseRules(raw?.tableRules, warnings),
        valueField: r.str('valueField', d.valueField, 64) || d.valueField,
        maxTables: r.num('maxTables', d.maxTables, 1, 1000000),
        maxColumns: r.num('maxColumns', d.maxColumns, 10, 100000),
        maxStringLength: r.num('maxStringLength', d.maxStringLength, 16, SERVER_MAX_STRING_BYTES - 16),
        objectPolicy: r.oneOf('objectPolicy', d.objectPolicy, ['json', 'reject'] as const),
        metaAck: r.oneOf('metaAck', d.metaAck, ['field', 'tag', 'none'] as const),
        metaQ: r.oneOf('metaQ', d.metaQ, ['field', 'tag', 'none'] as const),
        metaFrom: r.oneOf('metaFrom', d.metaFrom, ['field', 'tag', 'none'] as const),

        globalTags: r.tags('globalTags'),
        tagKeyStyle: r.oneOf('tagKeyStyle', d.tagKeyStyle, ['snake', 'preserve'] as const),
        autoTagRoom: r.bool('autoTagRoom', d.autoTagRoom),
        autoTagFunction: r.bool('autoTagFunction', d.autoTagFunction),
        autoTagAdapter: r.bool('autoTagAdapter', d.autoTagAdapter),
        autoTagInstance: r.bool('autoTagInstance', d.autoTagInstance),
        autoTagDevice: r.bool('autoTagDevice', d.autoTagDevice),
        autoTagChannel: r.bool('autoTagChannel', d.autoTagChannel),
        autoTagUnit: r.bool('autoTagUnit', d.autoTagUnit),
        autoTagRole: r.bool('autoTagRole', d.autoTagRole),
        autoTagName: r.bool('autoTagName', d.autoTagName),
        nativeAllowlist: r.list('nativeAllowlist').filter(k => /^[A-Za-z0-9_.-]+$/.test(k)),
        maxTagValuesPerKey: r.num('maxTagValuesPerKey', d.maxTagValuesPerKey, 1, 1000000),
        maxTagValueLength: r.num('maxTagValueLength', d.maxTagValueLength, 8, 4096),
        blockSensitiveTags: r.bool('blockSensitiveTags', d.blockSensitiveTags),
        dynamicTagsEnabled: r.bool('dynamicTagsEnabled', d.dynamicTagsEnabled),
        dynamicTagTtlSec: r.num('dynamicTagTtlSec', d.dynamicTagTtlSec, 0, 31536000),
        dynamicTagMaxValues: r.num('dynamicTagMaxValues', d.dynamicTagMaxValues, 1, 100000),

        changesOnly: r.bool('changesOnly', d.changesOnly),
        debounceTime: r.num('debounceTime', d.debounceTime, 0, 86400000),
        blockTime: r.num('blockTime', d.blockTime, 0, 86400000),
        changesRelogInterval: r.num('changesRelogInterval', d.changesRelogInterval, 0, MAX_RELOG_SECONDS),
        changesMinDelta: r.num('changesMinDelta', d.changesMinDelta, 0, Number.MAX_SAFE_INTEGER, false),
        storeOnStart: r.bool('storeOnStart', d.storeOnStart),
        logSkippedBeforeChange: r.bool('logSkippedBeforeChange', d.logSkippedBeforeChange),
        ackFilter: r.oneOf('ackFilter', d.ackFilter, ['all', 'ackOnly', 'notAckOnly'] as const),
        qFilter: r.oneOf('qFilter', d.qFilter, ['all', 'goodOnly'] as const),
        round: normalizeRound(r.optNum('round'), warnings, 'round'),
        storeAck: r.bool('storeAck', d.storeAck),
        storeQ: r.bool('storeQ', d.storeQ),
        storeFrom: r.bool('storeFrom', d.storeFrom),

        historyMaxRows: r.num('historyMaxRows', d.historyMaxRows, 1, 1000000),
        historyHardLimit: r.num('historyHardLimit', d.historyHardLimit, 100, 1000000),
        queryTimeoutMs: r.num('queryTimeoutMs', d.queryTimeoutMs, 1000, 600000),
        queryMaxRows: r.num('queryMaxRows', d.queryMaxRows, 1, 1000000),
        queryMaxBytes: r.num('queryMaxBytes', d.queryMaxBytes, 65536, 512 * 1024 * 1024),
        defaultAggregate: r.oneOf('defaultAggregate', 'average', [
            'none',
            'onchange',
            'average',
            'min',
            'max',
            'minmax',
            'total',
            'count',
        ] as const),
        querySenders: r.list('querySenders'),
        allowUserQueries: r.bool('allowUserQueries', d.allowUserQueries),

        subscribeAllThreshold: r.num('subscribeAllThreshold', d.subscribeAllThreshold, 1, 100000),
        logRejectedLimitPerMin: r.num('logRejectedLimitPerMin', d.logRejectedLimitPerMin, 0, 10000),
        statsIntervalSec: r.num('statsIntervalSec', d.statsIntervalSec, 2, 3600),
        writeStatsToInflux: r.bool('writeStatsToInflux', d.writeStatsToInflux),
        debugStates: r.bool('debugStates', d.debugStates),
    };

    if (
        !FIELD_NAME_RE.test(config.valueField) ||
        ['time', 'ack', 'q', 'from', 'state_id'].includes(config.valueField)
    ) {
        warnings.push(`valueField: "${config.valueField}" is invalid or reserved, using "value"`);
        config.valueField = 'value';
    }
    for (const key of ['tableNumber', 'tableBoolean', 'tableString'] as const) {
        const err = validateTableName(config[key]);
        if (err) {
            warnings.push(`${key}: ${err}, using "${d[key]}"`);
            config[key] = d[key];
        }
    }
    if (new Set([config.tableNumber, config.tableBoolean, config.tableString]).size !== 3) {
        warnings.push('tableNumber/tableBoolean/tableString must differ (one value type per table) - defaults used');
        config.tableNumber = d.tableNumber;
        config.tableBoolean = d.tableBoolean;
        config.tableString = d.tableString;
    }
    if (config.retryMaxDelayMs < config.retryBaseDelayMs) {
        config.retryMaxDelayMs = config.retryBaseDelayMs;
    }
    if (config.edition === 'cloud') {
        warnings.push('edition "cloud" is experimental and write-only (history and SQL are not available)');
    }
    if (!config.token) {
        warnings.push(
            'token: no API token configured - InfluxDB 3 requires authentication unless started with --without-auth',
        );
    }
    return { config, warnings };
}

function normalizeRound(v: number | null, warnings: string[], key: string): number | null {
    if (v === null) {
        return null;
    }
    if (!Number.isInteger(v) || v < 0 || v > 15) {
        warnings.push(`${key}: must be an integer 0..15 (decimal places), rounding disabled`);
        return null;
    }
    return v;
}

/**
 * Normalises the per-state configuration. Missing values are taken from the instance defaults.
 *
 * @param raw `common.custom[namespace]`
 * @param inst normalised instance configuration (provides defaults)
 * @param id state id (for warning texts)
 */
export function normalizeStateConfig(
    raw: Raw | undefined | null,
    inst: InstanceConfig,
    id: string,
): Normalized<StateConfig> {
    const r = new Reader(raw ?? {}, `${id}: `);
    const warnings = r.warnings;
    let table = r.str('table', '', 255);
    if (table) {
        const err = validateTableName(table);
        if (err) {
            warnings.push(`${id}: ${err} - automatic table used`);
            table = '';
        }
    }
    let field = r.str('field', '', 64);
    if (field && (!FIELD_NAME_RE.test(field) || ['time', 'ack', 'q', 'from', 'state_id'].includes(field))) {
        warnings.push(`${id}: field name "${field}" is invalid or reserved - "${inst.valueField}" used`);
        field = '';
    }
    if (field && inst.schemaMode !== 'perState') {
        warnings.push(`${id}: a custom field name is only allowed in per-state mode - "${inst.valueField}" used`);
        field = '';
    }
    let aliasId = r.str('aliasId', '', 255);
    if (aliasId === id) {
        warnings.push(`${id}: alias id is identical to the id - ignored`);
        aliasId = '';
    }
    // eslint-disable-next-line no-control-regex
    if (aliasId && /[\u0000-\u001f\u007f]/.test(aliasId)) {
        warnings.push(`${id}: alias id contains control characters - ignored`);
        aliasId = '';
    }

    const storeAsRaw =
        typeof raw?.storeAs === 'string' ? raw.storeAs : typeof raw?.storageType === 'string' ? raw.storageType : '';
    const storeAsLower = storeAsRaw.toLowerCase();
    const storeAs: StoreAs =
        storeAsLower === 'number' || storeAsLower === 'boolean' || storeAsLower === 'string' ? storeAsLower : '';

    const config: StateConfig = {
        enabled: r.bool('enabled', false),
        table,
        aliasId,
        storeAs,
        field,
        customTags: r.tags('customTags'),
        excludeTags: r.list('excludeTags'),
        useGlobalTags: r.bool('useGlobalTags', true),
        useAutoTags: r.bool('useAutoTags', true),
        allowSensitiveTags: r.bool('allowSensitiveTags', false),
        changesOnly: r.bool('changesOnly', inst.changesOnly),
        debounceTime: r.num('debounceTime', inst.debounceTime, 0, 86400000),
        blockTime: r.num('blockTime', inst.blockTime, 0, 86400000),
        changesRelogInterval: r.num('changesRelogInterval', inst.changesRelogInterval, 0, MAX_RELOG_SECONDS),
        changesMinDelta: r.num('changesMinDelta', inst.changesMinDelta, 0, Number.MAX_SAFE_INTEGER, false),
        storeOnStart: r.bool('storeOnStart', inst.storeOnStart),
        logSkippedBeforeChange: r.has('disableSkippedValueLogging')
            ? !r.bool('disableSkippedValueLogging', false)
            : r.bool('logSkippedBeforeChange', inst.logSkippedBeforeChange),
        ignoreZero: r.bool('ignoreZero', false),
        ignoreBelowNumber: r.optNum('ignoreBelowNumber'),
        ignoreAboveNumber: r.optNum('ignoreAboveNumber'),
        ackFilter: r.oneOf('ackFilter', inst.ackFilter, ['all', 'ackOnly', 'notAckOnly'] as const),
        qFilter: r.oneOf('qFilter', inst.qFilter, ['all', 'goodOnly'] as const),
        round: r.has('round') ? normalizeRound(r.optNum('round'), warnings, `${id}: round`) : inst.round,
        storeAck: r.bool('storeAck', inst.storeAck),
        storeQ: r.bool('storeQ', inst.storeQ),
        storeFrom: r.bool('storeFrom', inst.storeFrom),
        enableDebugLogs: r.bool('enableDebugLogs', false),
    };
    if (
        config.ignoreBelowNumber !== null &&
        config.ignoreAboveNumber !== null &&
        config.ignoreBelowNumber > config.ignoreAboveNumber
    ) {
        warnings.push(`${id}: ignoreBelowNumber > ignoreAboveNumber - every number would be ignored, both disabled`);
        config.ignoreBelowNumber = null;
        config.ignoreAboveNumber = null;
    }
    return { config, warnings };
}

/**
 * Stable hash-like string of a state configuration, used to detect "object changed but our
 * configuration did not" (objectChange fires for any change of the object).
 *
 * @param cfg normalised state config
 */
export function stateConfigKey(cfg: StateConfig): string {
    return JSON.stringify(cfg);
}
