"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.InfluxV3Adapter = void 0;
/**
 * ioBroker adapter "influx-v3" — logs selected states into InfluxDB 3 (Core / Enterprise).
 *
 * AdapterRuntime: lifecycle, subscriptions, per-state registry and sendTo commands.
 * The logic lives in src/lib/*; see docs/ARCHITECTURE.md.
 */
const utils = __importStar(require("@iobroker/adapter-core"));
const node_path_1 = __importDefault(require("node:path"));
const promises_1 = require("node:timers/promises");
const config_1 = require("./lib/config");
const dataMigration_1 = require("./lib/dataMigration");
const filterPipeline_1 = require("./lib/filterPipeline");
const historyService_1 = require("./lib/historyService");
const influxClient_1 = require("./lib/influxClient");
const legacySource_1 = require("./lib/legacySource");
const lineProtocol_1 = require("./lib/lineProtocol");
const migration_1 = require("./lib/migration");
const tagMapping_1 = require("./lib/tagMapping");
const observability_1 = require("./lib/observability");
const queryService_1 = require("./lib/queryService");
const redact_1 = require("./lib/redact");
const schemaMapper_1 = require("./lib/schemaMapper");
const spool_1 = require("./lib/spool");
const tagResolver_1 = require("./lib/tagResolver");
const sharedCache_1 = require("./lib/sharedCache");
const tagStandard_1 = require("./lib/tagStandard");
const writeQueue_1 = require("./lib/writeQueue");
const DYNAMIC_ALLOWED = /^[\p{L}\p{N} _.:/-]{1,128}$/u;
class InfluxV3Adapter extends utils.Adapter {
    cfg;
    client;
    queue;
    schema;
    cardinality;
    dynamicGuard;
    stats = new observability_1.Observability();
    registry = new Map();
    storeIdToReal = new Map();
    dynamicCache = new Map();
    dynamicSubs = new Map();
    roomsByMember = new Map();
    functionsByMember = new Map();
    parentCache = new Map();
    objectChains = new Map();
    warnedOnce = new Set();
    sysLanguage = 'en';
    subscribedAll = false;
    ready = false;
    stopping = false;
    pingTimer;
    statsTimer;
    enumReloadTimer;
    influxConnected = false;
    /** running import of historical data */
    dmRun;
    dmLastProgress = 0;
    /**
     * Used tags of the settings and of the old database. Every cell of the rule table asks for them (two requests per
     * row), so concurrent requests share one computation (ADR-0006, extension 5).
     */
    settingsTags = new sharedCache_1.SharedCache(() => Date.now(), 30_000);
    pointTags = new sharedCache_1.SharedCache(() => Date.now(), 600_000, 60_000);
    constructor(options = {}) {
        super({ ...options, name: 'influx-v3' });
        this.on('ready', this.onReady.bind(this));
        this.on('stateChange', this.onStateChange.bind(this));
        this.on('objectChange', this.onObjectChange.bind(this));
        this.on('message', this.onMessage.bind(this));
        this.on('unload', this.onUnload.bind(this));
    }
    // ------------------------------------------------------------------ lifecycle
    async onReady() {
        if (await this.migrateGlobalTagsToCatalog()) {
            return; // js-controller restarts the instance with the converted settings
        }
        const { config, warnings } = (0, config_1.normalizeInstanceConfig)(this.config);
        this.cfg = config;
        (0, redact_1.registerSecret)(config.token);
        for (const k of ['dmPassword', 'dmToken']) {
            const secret = this.config[k];
            if (typeof secret === 'string' && secret) {
                (0, redact_1.registerSecret)(secret);
            }
        }
        for (const w of warnings) {
            this.log.warn(`Configuration: ${w}`);
        }
        await this.setStateAsync('info.connection', false, true);
        await this.setStateAsync('info.dataMigration.running', false, true);
        this.schema = new schemaMapper_1.SchemaRegistry(config.maxTables, config.maxColumns);
        this.cardinality = new tagResolver_1.CardinalityGuard(config.maxTagValuesPerKey, (key, limit) => this.log.warn(`Tag "${key}" reached ${limit} distinct values - further values are written as "_other" (cardinality protection)`));
        this.dynamicGuard = new tagResolver_1.CardinalityGuard(config.dynamicTagMaxValues, (key, limit) => this.log.warn(`Dynamic tag "${key}" reached ${limit} distinct values - further values are written as "_other"`));
        this.client = this.createClient(config);
        const spool = new spool_1.Spool(node_path_1.default.join(utils.getAbsoluteInstanceDataDir(this), 'spool'), config.spoolFsync);
        this.queue = new writeQueue_1.WriteQueue({
            writeMode: config.writeMode,
            batchMaxLines: config.batchMaxLines,
            batchMaxBytes: config.batchMaxBytes,
            flushIntervalMs: config.flushIntervalMs,
            maxConcurrentWrites: config.maxConcurrentWrites,
            retryBaseDelayMs: config.retryBaseDelayMs,
            retryMaxDelayMs: config.retryMaxDelayMs,
            authRetryMs: config.authRetryMs,
            bufferMode: config.bufferMode,
            bufferMaxRecords: config.bufferMaxRecords,
            bufferMaxBytes: config.bufferMaxBytes,
            spoolMaxBytes: config.spoolMaxBytes,
            bufferMaxAgeMs: config.bufferMaxAgeHours * 3600000,
            overflowPolicy: config.overflowPolicy,
            persistOnShutdown: config.persistOnShutdown,
        }, this.client, {
            written: n => {
                this.stats.counters.written += n;
                this.stats.lastWrite = Date.now();
            },
            rejected: items => this.onRejected(items),
            dropped: (n, reason) => {
                this.stats.drop(n, reason);
                this.log.warn(`${n} record(s) dropped: ${reason}`);
            },
            connection: (ok, error) => this.setConnected(ok, error),
            log: (level, message) => this.log[level]((0, redact_1.redact)(message)),
        }, this.timers(), spool);
        try {
            await this.queue.start();
        }
        catch (e) {
            this.log.error(`Disk buffer not usable: ${(0, redact_1.redact)(e)}`);
        }
        await this.loadSystemLanguage();
        await this.loadEnums();
        await this.connect();
        // all objects: needed to see custom settings and enum changes of any object
        await this.subscribeForeignObjectsAsync('*');
        await this.loadCustomConfigs();
        this.pingTimer = this.setInterval(() => void this.periodicPing(), config.pingIntervalSec * 1000);
        this.statsTimer = this.setInterval(() => void this.publishStats(), config.statsIntervalSec * 1000);
        this.ready = true;
        await this.publishStats();
        this.log.info(`InfluxDB 3 adapter ready: ${this.registry.size} state(s) logged into database "${config.database}" (${config.schemaMode} mode, ${config.bufferMode} buffer)`);
    }
    /** Managed adapter timers (cleared by js-controller on unload) for the queue and the filters */
    timers() {
        return {
            now: () => Date.now(),
            after: (fn, ms) => this.setTimeout(fn, ms),
            cancel: h => this.clearTimeout(h),
            random: () => Math.random(),
        };
    }
    /**
     * One-time conversion of the former "global tags" table into the tag catalog, saved in the
     * instance settings so that Admin shows it. Returns true when the settings were changed.
     */
    async migrateGlobalTagsToCatalog() {
        const native = this.config;
        const legacy = (0, config_1.tableRows)(native.globalTags);
        const catalog = (0, config_1.tableRows)(native.tagDefinitions);
        if (!legacy.length || catalog.length) {
            return false;
        }
        const tagDefinitions = legacy
            .filter(t => typeof t?.key === 'string' && t.key.trim())
            .map(t => ({
            key: String(t.key).trim(),
            values: '',
            allowOther: true,
            defaultValue: typeof t.value === 'string' ? t.value.trim() : '',
            description: '',
        }));
        try {
            // read-modify-write: extendObject would merge the arrays element by element
            const obj = await this.getForeignObjectAsync(`system.adapter.${this.namespace}`);
            if (!obj) {
                return false;
            }
            obj.native = { ...obj.native, tagDefinitions, globalTags: [] };
            await this.setForeignObjectAsync(obj._id, obj);
            this.log.info(`${tagDefinitions.length} global tag(s) moved into the tag catalog - restarting with the new settings`);
            return true;
        }
        catch (e) {
            this.log.warn(`Cannot save the tag catalog (${(0, redact_1.redact)(e)}) - global tags are converted in memory only`);
            return false;
        }
    }
    async onUnload(callback) {
        this.stopping = true;
        // js-controller must get the callback at the latest shortly before common.stopTimeout (10 s).
        // Adapter timers are refused while stopping, so the limit is an abortable promise delay.
        const abort = new AbortController();
        const work = this.shutdown().catch(e => this.log.error(`Error during shutdown: ${(0, redact_1.redact)(e)}`));
        await Promise.race([work, (0, promises_1.setTimeout)(9000, undefined, { signal: abort.signal }).catch(() => undefined)]);
        abort.abort();
        callback();
    }
    async shutdown() {
        this.clearInterval(this.pingTimer);
        this.clearInterval(this.statsTimer);
        this.clearTimeout(this.enumReloadTimer);
        if (this.dmRun) {
            // the import saves its progress and continues with "Start" after the restart
            this.dmRun.ctrl.abort();
            await Promise.race([this.dmRun.done, (0, promises_1.setTimeout)(3000)]);
        }
        this.queue?.beginShutdown();
        for (const reg of this.registry.values()) {
            reg.pipeline.shutdown(); // writes values waiting in debounce/block windows, starts no new timers
        }
        if (this.queue) {
            const { persisted, lost } = await this.queue.shutdown(this.cfg?.shutdownFlushTimeoutMs ?? 5000);
            if (persisted) {
                this.log.info(`${persisted} record(s) kept in the disk buffer for the next start`);
            }
            if (lost) {
                this.log.warn(`${lost} record(s) could not be written before shutdown`);
            }
        }
        this.client?.close();
        await this.setStateAsync('info.connection', false, true).catch(() => undefined);
    }
    createClient(config) {
        return new influxClient_1.InfluxV3Client({
            url: config.url,
            token: config.token,
            database: config.database,
            edition: config.edition,
            timeoutMs: config.requestTimeoutMs,
            rejectUnauthorized: config.tlsRejectUnauthorized,
            ca: config.tlsCa,
            gzip: config.gzip,
            noSync: config.noSync,
            maxSockets: config.maxConcurrentWrites + 2,
        });
    }
    setConnected(ok, error) {
        if (error) {
            this.stats.error((0, redact_1.redact)(error));
        }
        this.stats.connected = ok;
        if (this.influxConnected !== ok) {
            this.influxConnected = ok;
            void this.setStateAsync('info.connection', ok, true);
            if (ok) {
                this.log.info('Connection to InfluxDB 3 established');
            }
            else if (error) {
                this.log.warn(`Connection to InfluxDB 3 lost: ${(0, redact_1.redact)(error)}`);
            }
        }
    }
    async connect() {
        if (!this.client) {
            return;
        }
        const ping = await this.client.ping();
        if (!ping.ok) {
            this.setConnected(false, ping.error ?? `HTTP ${ping.status}`);
            this.log.warn(`InfluxDB 3 not reachable at ${this.cfg.url}: ${ping.error ?? ping.status} - data is buffered`);
            return;
        }
        const edition = ping.edition ?? 'unknown';
        this.stats.serverEdition = edition;
        this.stats.serverVersion = ping.version ?? '';
        await this.setStateChangedAsync('info.serverVersion', `${edition} ${ping.version ?? ''}`.trim(), true);
        if (this.cfg.edition === 'auto' && edition === 'unknown') {
            this.log.warn('Could not detect the InfluxDB edition from /ping - assuming the Core/Enterprise API');
        }
        else if (this.cfg.edition !== 'auto' &&
            this.cfg.edition !== 'cloud' &&
            edition !== 'unknown' &&
            edition !== this.cfg.edition) {
            this.log.warn(`Configured edition "${this.cfg.edition}" but the server reports "${edition}"`);
        }
        this.setConnected(true);
        await this.refreshSchema();
        if (this.cfg.edition !== 'cloud') {
            const retention = await this.client.getRetentionMs();
            if (retention === undefined) {
                this.log.info(`Database "${this.cfg.database}": retention infinite`);
            }
            else if (typeof retention === 'number') {
                this.log.info(`Database "${this.cfg.database}": retention ${Math.round(retention / 86400000)} days`);
            }
        }
    }
    async refreshSchema() {
        if (!this.client || this.cfg.edition === 'cloud') {
            return;
        }
        try {
            const columns = await this.client.getSchemaColumns();
            this.schema.seed(columns);
            this.stats.lastRead = Date.now();
        }
        catch (e) {
            this.log.warn(`Cannot read the database schema: ${(0, redact_1.redact)(e)} - type checks use the adapter's own knowledge only`);
        }
    }
    async periodicPing() {
        if (!this.client || this.stopping) {
            return;
        }
        const ping = await this.client.ping();
        if (ping.ok) {
            const wasConnected = this.influxConnected;
            this.setConnected(true);
            if (!wasConnected) {
                await this.refreshSchema();
            }
        }
        else {
            this.setConnected(false, ping.error);
        }
    }
    async publishStats() {
        if (!this.queue) {
            return;
        }
        const q = this.queue.stats();
        const c = this.stats.counters;
        const conflicts = [...this.registry.values()]
            .filter(r => r.conflict)
            .map(r => ({ id: r.id, reason: r.conflict }));
        await Promise.all([
            this.setStateChangedAsync('info.queueRecords', q.queuedRecords, true),
            this.setStateChangedAsync('info.spoolBytes', q.spoolBytes, true),
            this.setStateChangedAsync('info.writtenRecords', c.written, true),
            this.setStateChangedAsync('info.rejectedRecords', c.rejected, true),
            this.setStateChangedAsync('info.droppedRecords', c.dropped, true),
            this.setStateChangedAsync('info.enabledStates', this.registry.size, true),
            this.setStateChangedAsync('info.lastWrite', this.stats.lastWrite, true),
            this.setStateChangedAsync('info.lastRead', this.stats.lastRead, true),
            this.setStateChangedAsync('info.lastError', this.stats.lastError, true),
            this.setStateChangedAsync('info.conflicts', JSON.stringify(conflicts), true),
        ]).catch(e => this.log.debug(`Cannot publish statistics: ${(0, redact_1.redact)(e)}`));
        if (this.cfg.writeStatsToInflux && this.ready && !this.stopping) {
            try {
                const line = (0, lineProtocol_1.serialize)({
                    table: 'iobroker_adapter_stats',
                    tags: { instance: this.namespace },
                    fields: {
                        written: { type: 'integer', value: c.written },
                        rejected: { type: 'integer', value: c.rejected },
                        dropped: { type: 'integer', value: c.dropped },
                        queued: { type: 'integer', value: q.queuedRecords },
                    },
                    ts: Date.now(),
                });
                this.queue.push(line);
            }
            catch {
                // statistics are best effort
            }
        }
    }
    // ------------------------------------------------------------------ objects & enums
    async loadSystemLanguage() {
        try {
            const sys = await this.getForeignObjectAsync('system.config');
            const lang = sys?.common?.language;
            if (lang) {
                this.sysLanguage = lang;
            }
        }
        catch {
            // default "en"
        }
    }
    nameOf(name, fallback) {
        if (typeof name === 'string' && name) {
            return name;
        }
        if (name && typeof name === 'object') {
            const rec = name;
            const v = rec[this.sysLanguage] ?? rec.en ?? Object.values(rec)[0];
            if (typeof v === 'string' && v) {
                return v;
            }
        }
        return fallback;
    }
    async loadEnums() {
        this.roomsByMember.clear();
        this.functionsByMember.clear();
        for (const [prefix, target] of [
            ['enum.rooms.', this.roomsByMember],
            ['enum.functions.', this.functionsByMember],
        ]) {
            try {
                const objs = await this.getForeignObjectsAsync(`${prefix}*`, 'enum');
                for (const [id, obj] of Object.entries(objs)) {
                    const members = obj?.common?.members;
                    if (!Array.isArray(members)) {
                        continue;
                    }
                    const name = this.nameOf(obj.common?.name, id.split('.').pop() ?? id);
                    for (const m of members) {
                        if (typeof m === 'string') {
                            const list = target.get(m) ?? [];
                            list.push(name);
                            list.sort();
                            target.set(m, list);
                        }
                    }
                }
            }
            catch (e) {
                this.log.debug(`Cannot read ${prefix}*: ${(0, redact_1.redact)(e)}`);
            }
        }
    }
    /** enum names for an id or its nearest parent that is a member */
    enumFor(map, id) {
        const parts = id.split('.');
        for (let i = parts.length; i > 0; i--) {
            const list = map.get(parts.slice(0, i).join('.'));
            if (list?.length) {
                return list;
            }
        }
        return [];
    }
    needsParents(cfg) {
        const templates = [...this.cfg.globalTags, ...cfg.customTags].map(t => t.value).join(' ');
        return ((cfg.useAutoTags && (this.cfg.autoTagDevice || this.cfg.autoTagChannel)) || templates.includes('{parent.'));
    }
    async parentInfo(id) {
        const parts = id.split('.');
        const out = {};
        for (let i = parts.length - 1; i >= 2; i--) {
            const pid = parts.slice(0, i).join('.');
            let info = this.parentCache.get(pid);
            if (info === undefined) {
                try {
                    const obj = await this.getForeignObjectAsync(pid);
                    info = obj ? { type: obj.type, name: this.nameOf(obj.common?.name, parts[i - 1]) } : null;
                }
                catch {
                    info = null;
                }
                this.parentCache.set(pid, info);
            }
            if (info?.type === 'channel' && !out.channel) {
                out.channel = info.name;
            }
            if (info?.type === 'device' && !out.device) {
                out.device = info.name;
                break;
            }
        }
        return out;
    }
    async loadCustomConfigs() {
        let rows = [];
        try {
            const view = await this.getObjectViewAsync('system', 'custom', {});
            rows = view?.rows ?? [];
        }
        catch (e) {
            this.log.error(`Cannot read custom settings: ${(0, redact_1.redact)(e)}`);
        }
        const ids = rows
            .filter(r => {
            const c = r.value?.[this.namespace];
            return !!c && typeof c === 'object' && !!c.enabled;
        })
            .map(r => r.id)
            .sort();
        // deterministic order → deterministic schema claims (first claim of a table defines its type)
        for (let i = 0; i < ids.length; i += 50) {
            const chunk = ids.slice(i, i + 50);
            const objs = await Promise.all(chunk.map(id => this.getForeignObjectAsync(id).catch(() => null)));
            for (let j = 0; j < chunk.length; j++) {
                const obj = objs[j];
                if (obj) {
                    await this.registerState(chunk[j], obj, false);
                }
            }
        }
        await this.updateSubscriptions();
        // initial values after subscriptions are active
        await Promise.all([...this.registry.values()].map(reg => this.initialValue(reg)));
    }
    customOf(obj) {
        const custom = obj?.common?.custom;
        const c = custom?.[this.namespace];
        return c && typeof c === 'object' ? c : undefined;
    }
    onObjectChange(id, obj) {
        if (!this.ready || this.stopping) {
            return;
        }
        if (id.startsWith('enum.rooms.') || id.startsWith('enum.functions.')) {
            this.scheduleEnumReload();
            return;
        }
        if (obj?.common?.custom || this.registry.has(id)) {
            this.settingsTags.invalidate(); // used tags of the migration
        }
        // system.* states (e.g. system.adapter.x.0.memRss) can be logged too, so no prefix filter here
        const custom = this.customOf(obj);
        if (!this.registry.has(id) && !custom?.enabled) {
            return; // not ours - O(1) exit for the '*' subscription
        }
        const enabledAt = Date.now();
        // serialise changes per id
        const prev = this.objectChains.get(id) ?? Promise.resolve();
        const next = prev
            .then(async () => {
            if (obj && custom?.enabled) {
                const isNew = !this.registry.has(id);
                await this.registerState(id, obj, true);
                if (isNew && this.registry.has(id)) {
                    await this.updateSubscriptions();
                    const reg = this.registry.get(id);
                    if (reg) {
                        await this.initialValue(reg, enabledAt);
                    }
                }
            }
            else if (this.registry.has(id)) {
                this.unregister(id, 'disabled');
                await this.updateSubscriptions();
            }
        })
            .catch(e => this.log.error(`Cannot apply configuration of ${id}: ${(0, redact_1.redact)(e)}`))
            .finally(() => {
            if (this.objectChains.get(id) === next) {
                this.objectChains.delete(id);
            }
        });
        this.objectChains.set(id, next);
    }
    scheduleEnumReload() {
        this.clearTimeout(this.enumReloadTimer);
        this.enumReloadTimer = this.setTimeout(() => {
            void (async () => {
                await this.loadEnums();
                for (const reg of [...this.registry.values()]) {
                    const obj = await this.getForeignObjectAsync(reg.id).catch(() => null);
                    if (obj) {
                        await this.registerState(reg.id, obj, true, true);
                    }
                }
            })().catch(e => this.log.warn(`Cannot apply enum change: ${(0, redact_1.redact)(e)}`));
        }, 2000);
    }
    async buildContext(id, storeId, obj, cfg) {
        const parents = this.needsParents(cfg) ? await this.parentInfo(id) : {};
        return {
            id,
            storeId,
            common: obj.common,
            native: obj.native,
            rooms: this.enumFor(this.roomsByMember, id),
            functions: this.enumFor(this.functionsByMember, id),
            parentDevice: parents.device,
            parentChannel: parents.channel,
            language: this.sysLanguage,
        };
    }
    reservedKeys(field) {
        return new Set(['time', 'state_id', field, 'ack', 'q', 'from'].map(k => k.toLowerCase()));
    }
    /**
     * Computes the complete mapping of a state (shared by registration and preview).
     *
     * @param id state id
     * @param obj object
     * @param customRaw custom config of this instance
     */
    async computeMapping(id, obj, customRaw) {
        const { config: cfg, warnings } = (0, config_1.normalizeStateConfig)(customRaw, this.cfg, id);
        const info = {
            common: obj.common ?? {},
            native: (obj.native ?? {}),
            type: obj.type,
        };
        if (obj.type !== 'state') {
            warnings.push(`${id} is a ${obj.type}, not a state - nothing will be logged`);
        }
        const storeId = cfg.aliasId || id;
        const target = (0, schemaMapper_1.resolveTarget)(this.cfg, cfg, id, storeId, info.common);
        const ctx = await this.buildContext(id, storeId, info, cfg);
        const tags = (0, tagResolver_1.resolveTags)(ctx, {
            instance: this.cfg,
            state: cfg,
            reserved: this.reservedKeys(target.field),
        });
        const metaFields = {
            ack: cfg.storeAck && this.cfg.metaAck === 'field',
            q: cfg.storeQ && this.cfg.metaQ === 'field',
            from: cfg.storeFrom && this.cfg.metaFrom === 'field',
        };
        const metaTags = {
            ack: cfg.storeAck && this.cfg.metaAck === 'tag',
            q: cfg.storeQ && this.cfg.metaQ === 'tag',
            from: cfg.storeFrom && this.cfg.metaFrom === 'tag',
        };
        if (metaTags.from) {
            warnings.push('"from" stored as tag creates one series per writing instance/user (high cardinality)');
        }
        const extraFields = {};
        if (metaFields.ack) {
            extraFields.ack = 'boolean';
        }
        if (metaFields.q) {
            extraFields.q = 'integer';
        }
        if (metaFields.from) {
            extraFields.from = 'string';
        }
        const tagKeys = [
            ...Object.keys(tags.tags),
            ...tags.dynamic.map(d => d.key),
            'state_id',
            ...['ack', 'q', 'from'].filter(k => metaTags[k]),
        ];
        const claim = {
            stateId: id,
            table: target.table,
            field: target.field,
            fieldKind: (0, schemaMapper_1.kindOfValueType)(target.valueType),
            extraFields,
            tagKeys,
        };
        const key = JSON.stringify([(0, config_1.stateConfigKey)(cfg), target, tags.tags, tags.dynamic, metaFields, metaTags]);
        return {
            cfg,
            storeId,
            target,
            tags,
            claim,
            metaFields,
            metaTags,
            warnings: [...warnings, ...tags.warnings],
            key,
        };
    }
    /**
     * Registers (or re-configures) a state.
     *
     * @param id state id
     * @param obj object
     * @param live called at runtime (not during start)
     * @param force re-apply even if unchanged (enum changes)
     */
    async registerState(id, obj, live, force = false) {
        const customRaw = this.customOf(obj);
        if (!customRaw?.enabled) {
            this.unregister(id, 'disabled');
            return;
        }
        const m = await this.computeMapping(id, obj, customRaw);
        const existing = this.registry.get(id);
        if (existing && existing.key === m.key && !force) {
            return; // object changed, but not our part
        }
        if (obj.type !== 'state') {
            this.warnOnce(`type:${id}`, `${id} is not a state (type ${obj.type}) - logging ignored`);
            this.unregister(id, 'not a state');
            return;
        }
        for (const w of m.warnings) {
            this.warnOnce(`${id}:${w}`, `${id}: ${w}`);
        }
        let conflict;
        const staticTags = {};
        const result = this.schema.claim(m.claim);
        if (!result.ok) {
            conflict = result.reason;
            this.stats.counters.conflicts++;
            this.log.warn(`${id}: not logged - ${result.reason}`);
        }
        else {
            for (const w of result.warnings) {
                this.warnOnce(`${id}:${w}`, `${id}: ${w}`);
            }
            for (const [k, v] of Object.entries(m.tags.tags)) {
                if (result.droppedTags.includes(k)) {
                    continue;
                }
                staticTags[k] = m.tags.variableKeys.includes(k) ? this.cardinality.check(k, v) : v;
            }
        }
        staticTags.state_id = m.storeId;
        if (existing && existing.storeId !== m.storeId) {
            this.storeIdToReal.delete(existing.storeId);
        }
        const pipeline = existing?.pipeline ??
            new filterPipeline_1.FilterPipeline(m.cfg, (s, reason) => this.write(id, s, reason), `system.adapter.${this.namespace}`, this.timers());
        if (existing) {
            pipeline.update(m.cfg);
        }
        const reg = {
            id,
            storeId: m.storeId,
            cfg: m.cfg,
            key: m.key,
            target: m.target,
            tags: m.tags,
            staticTags,
            metaFields: m.metaFields,
            metaTags: m.metaTags,
            conflict,
            warnings: m.warnings,
            pipeline,
            lastSeenTs: existing?.lastSeenTs,
        };
        this.registry.set(id, reg);
        this.storeIdToReal.set(m.storeId, id);
        this.updateDynamicSources(id, m.tags);
        const tagText = Object.entries(staticTags)
            .map(([k, v]) => `${k}=${v}`)
            .join(',');
        if (live || this.log.level === 'debug') {
            this.log.info(`${existing ? 'updated' : 'enabled'} logging of ${id} → table "${m.target.table}" (${m.target.valueType}${conflict ? ', CONFLICT' : ''}) tags: ${tagText}`);
        }
    }
    unregister(id, reason) {
        const reg = this.registry.get(id);
        if (!reg) {
            return;
        }
        reg.pipeline.stop(); // immediate stop: values waiting in debounce/block windows are discarded
        this.schema.release(id);
        this.registry.delete(id);
        if (this.storeIdToReal.get(reg.storeId) === id) {
            this.storeIdToReal.delete(reg.storeId);
        }
        this.updateDynamicSources(id, undefined);
        this.log.info(`disabled logging of ${id} (${reason})`);
    }
    warnOnce(key, text) {
        if (this.warnedOnce.has(key)) {
            return;
        }
        this.warnedOnce.add(key);
        this.log.warn(text);
    }
    updateDynamicSources(id, tags) {
        for (const [src, users] of this.dynamicSubs) {
            users.delete(id);
            if (!users.size) {
                this.dynamicSubs.delete(src);
                this.dynamicCache.delete(src);
                if (!this.subscribedAll && !this.registry.has(src)) {
                    void this.unsubscribeForeignStatesAsync(src);
                }
            }
        }
        if (!tags) {
            return;
        }
        for (const d of tags.dynamic) {
            for (const src of d.sources) {
                let users = this.dynamicSubs.get(src);
                if (!users) {
                    users = new Set();
                    this.dynamicSubs.set(src, users);
                    void this.primeDynamic(src);
                }
                users.add(id);
            }
        }
    }
    async primeDynamic(src) {
        try {
            if (!this.subscribedAll) {
                await this.subscribeForeignStatesAsync(src);
            }
            const st = await this.getForeignStateAsync(src);
            if (st && st.val !== null && st.val !== undefined) {
                this.dynamicCache.set(src, { value: String(st.val), ts: st.ts });
            }
        }
        catch (e) {
            this.log.debug(`Dynamic tag source ${src}: ${(0, redact_1.redact)(e)}`);
        }
    }
    async updateSubscriptions() {
        const shouldAll = this.registry.size >= this.cfg.subscribeAllThreshold;
        if (shouldAll && !this.subscribedAll) {
            await this.subscribeForeignStatesAsync('*');
            this.subscribedAll = true;
            for (const id of this.subscribedIds) {
                await this.unsubscribeForeignStatesAsync(id);
            }
            this.subscribedIds.clear();
            this.log.info(`${this.registry.size} logged states - subscribed to all state changes`);
            return;
        }
        if (this.subscribedAll) {
            if (this.registry.size < this.cfg.subscribeAllThreshold / 2) {
                // hysteresis: go back to individual subscriptions
                for (const id of [...this.registry.keys(), ...this.dynamicSubs.keys()]) {
                    await this.subscribeForeignStatesAsync(id);
                    this.subscribedIds.add(id);
                }
                await this.unsubscribeForeignStatesAsync('*');
                this.subscribedAll = false;
            }
            return;
        }
        const wanted = new Set([...this.registry.keys(), ...this.dynamicSubs.keys()]);
        for (const id of wanted) {
            if (!this.subscribedIds.has(id)) {
                await this.subscribeForeignStatesAsync(id);
                this.subscribedIds.add(id);
            }
        }
        for (const id of [...this.subscribedIds]) {
            if (!wanted.has(id)) {
                await this.unsubscribeForeignStatesAsync(id);
                this.subscribedIds.delete(id);
            }
        }
    }
    subscribedIds = new Set();
    /**
     * Reads the current value of a newly registered state.
     *
     * @param reg registration
     * @param enabledAt when logging was enabled while running: a value changed after that moment may have been
     *     set before the subscription was active, so it is logged like a state change instead of becoming the
     *     baseline of the filters
     */
    async initialValue(reg, enabledAt) {
        try {
            const st = await this.getForeignStateAsync(reg.id);
            if (!st || this.registry.get(reg.id) !== reg) {
                return;
            }
            if (reg.lastSeenTs !== undefined && reg.lastSeenTs >= st.ts) {
                return; // the subscription already delivered this (or a newer) value
            }
            if (enabledAt !== undefined && st.ts >= enabledAt) {
                this.onStateChange(reg.id, st);
                return;
            }
            const prepared = this.prepare(reg, st);
            if (prepared) {
                reg.pipeline.initialize(prepared, reg.cfg.storeOnStart && !reg.conflict);
            }
        }
        catch (e) {
            this.log.debug(`Cannot read initial value of ${reg.id}: ${(0, redact_1.redact)(e)}`);
        }
    }
    // ------------------------------------------------------------------ state changes → records
    onStateChange(id, state) {
        if (state && this.dynamicSubs.has(id) && state.val !== null && state.val !== undefined) {
            this.dynamicCache.set(id, { value: String(state.val), ts: state.ts });
        }
        const reg = this.registry.get(id);
        if (!reg || !state || this.stopping) {
            return;
        }
        reg.lastSeenTs = Math.max(reg.lastSeenTs ?? 0, state.ts);
        this.stats.counters.received++;
        if (reg.conflict) {
            this.stats.counters.filtered++;
            return;
        }
        const prepared = this.prepare(reg, state);
        if (!prepared) {
            return;
        }
        const decision = reg.pipeline.process(prepared);
        if (decision !== 'emitted' && !decision.startsWith('debounce')) {
            this.stats.counters.filtered++;
        }
        if (reg.cfg.enableDebugLogs) {
            this.log.debug(`${id}: val=${JSON.stringify(state.val)} ts=${state.ts} → ${decision}`);
        }
    }
    prepare(reg, state) {
        const res = (0, schemaMapper_1.coerceValue)(state.val, reg.target.valueType, {
            maxStringLength: this.cfg.maxStringLength,
            objectPolicy: this.cfg.objectPolicy,
        });
        if (!res.ok) {
            if (res.reason === 'null') {
                this.stats.counters.filtered++;
            }
            else {
                this.stats.counters.conversionErrors++;
                this.warnOnce(`conv:${reg.id}:${reg.target.valueType}`, `${reg.id}: value not stored (${res.reason}) - further conversion errors of this state are only counted`);
            }
            return undefined;
        }
        let val = res.value;
        if (typeof val === 'number') {
            val = (0, schemaMapper_1.roundTo)(val, reg.cfg.round);
        }
        const ts = Number.isFinite(state.ts) && state.ts > 0 ? Math.round(state.ts) : Date.now();
        return { val, ts, ack: !!state.ack, q: typeof state.q === 'number' ? state.q : 0, from: state.from ?? '' };
    }
    /**
     * Builds the record of a state value (exported logic used by write and preview).
     *
     * @param reg registration
     * @param s value
     */
    toRecord(reg, s, withDynamicTags = true) {
        const fields = { [reg.target.field]: (0, schemaMapper_1.toFieldValue)(s.val, reg.target.valueType) };
        if (reg.metaFields.ack) {
            fields.ack = { type: 'boolean', value: s.ack };
        }
        if (reg.metaFields.q) {
            fields.q = { type: 'integer', value: s.q };
        }
        if (reg.metaFields.from && s.from) {
            fields.from = { type: 'string', value: s.from };
        }
        const tags = { ...reg.staticTags };
        if (withDynamicTags && reg.tags.dynamic.length) {
            const dyn = (0, tagResolver_1.resolveDynamicTags)(reg.tags.dynamic, src => this.dynamicCache.get(src), Date.now(), this.cfg.dynamicTagTtlSec, DYNAMIC_ALLOWED, this.cfg.maxTagValueLength);
            for (const [k, v] of Object.entries(dyn)) {
                if (this.cfg.blockSensitiveTags && !reg.cfg.allowSensitiveTags && (0, tagResolver_1.looksSensitive)(k, v)) {
                    continue;
                }
                tags[k] = this.dynamicGuard.check(k, v);
            }
        }
        if (reg.metaTags.ack) {
            tags.ack = String(s.ack);
        }
        if (reg.metaTags.q) {
            tags.q = String(s.q);
        }
        if (reg.metaTags.from && s.from) {
            tags.from = s.from;
        }
        return { table: reg.target.table, tags, fields, ts: s.ts };
    }
    write(id, s, reason) {
        const reg = this.registry.get(id);
        if (!reg || reg.conflict || !this.queue) {
            return;
        }
        try {
            const line = (0, lineProtocol_1.serialize)(this.toRecord(reg, s));
            if (this.queue.push(line)) {
                this.stats.counters.queued++;
            }
            if (reg.cfg.enableDebugLogs) {
                this.log.debug(`${id}: queued (${reason}) ${line}`);
            }
        }
        catch (e) {
            this.stats.counters.rejected++;
            this.warnOnce(`lp:${id}`, `${id}: record cannot be serialised: ${e instanceof lineProtocol_1.LineProtocolError ? e.message : (0, redact_1.redact)(e)}`);
        }
    }
    onRejected(items) {
        this.stats.counters.rejected += items.length;
        for (const item of items) {
            if (!this.stats.mayLogReject(this.cfg.logRejectedLimitPerMin)) {
                this.log.debug(`Rejected by InfluxDB: ${(0, redact_1.redact)(item.message)}`);
                continue;
            }
            const line = item.line.length > 300 ? `${item.line.slice(0, 300)}…` : item.line;
            this.log.warn(`InfluxDB rejected a record (${(0, redact_1.redact)(item.message)}): ${(0, redact_1.redact)(line)}`);
        }
        if (items.length) {
            this.stats.error((0, redact_1.redact)(items[items.length - 1].message));
            // the schema may have changed (e.g. a column type defined by another client) - refresh soon
            void this.refreshSchema();
        }
    }
    // ------------------------------------------------------------------ messages
    reply(msg, payload) {
        if (msg.callback) {
            this.sendTo(msg.from, msg.command, payload, msg.callback);
        }
    }
    async onMessage(msg) {
        if (!msg || typeof msg !== 'object' || !msg.command) {
            return;
        }
        try {
            switch (msg.command) {
                case 'features':
                    return this.reply(msg, { supportedFeatures: ['storeState'] });
                case 'getHistory':
                    return await this.handleGetHistory(msg);
                case 'storeState':
                    return this.handleStoreState(msg);
                case 'enableHistory':
                    return await this.handleEnableHistory(msg);
                case 'disableHistory':
                    return await this.handleDisableHistory(msg);
                case 'getEnabledDPs':
                    return this.reply(msg, this.enabledDPs());
                case 'query':
                    return await this.handleQuery(msg, 'sql');
                case 'queryInfluxQL':
                    return await this.handleQuery(msg, 'influxql');
                case 'flushBuffer': {
                    const ok = await (this.queue?.flush(10000) ?? Promise.resolve(true));
                    return this.reply(msg, ok
                        ? { success: true }
                        : { error: 'buffer could not be flushed completely (server not reachable?)' });
                }
                case 'getSchema':
                    return await this.handleGetSchema(msg);
                case 'testConnection':
                    return await this.handleTestConnection(msg);
                case 'getAdapterStats':
                    return this.reply(msg, this.adapterStats());
                case 'diagnostics': {
                    // `text` for textSendTo, `result` for the sendTo button
                    const text = this.diagnosticsText();
                    return this.reply(msg, { text, result: text });
                }
                case 'previewMapping':
                    return await this.handlePreview(msg);
                case 'createDatabase':
                    return await this.handleCreateDatabase(msg);
                case 'listInfluxdbInstances':
                    return await this.handleListInfluxdbInstances(msg);
                case 'getTagKeys':
                    return this.reply(msg, this.tagKeyOptions());
                case 'getOldTagKeys':
                    return this.reply(msg, await this.oldTagKeyOptions(msg.message));
                case 'getOldTagValues':
                    return this.reply(msg, await this.oldTagValueOptions(msg.message));
                case 'migrationSuggestRules':
                    return await this.handleSuggestRules(msg);
                case 'migrationProposeStandard':
                    return await this.handleProposeStandard(msg);
                case 'getTagKeysForRules':
                    return this.reply(msg, [
                        ...this.cfg.tagDefinitions.map(d => ({ value: d.key, label: d.key })),
                        { value: '-', label: '- (remove the tag)' },
                    ]);
                case 'getTagValues':
                    return this.reply(msg, this.tagValueOptions(msg.message?.key));
                case 'migrationPreview':
                    return await this.handleMigration(msg, 'preview');
                case 'migrationPreviewHtml':
                    return await this.handleMigration(msg, 'html');
                case 'migrationApply':
                    return await this.handleMigration(msg, 'apply');
                case 'dmFillFromInfluxdb':
                    return await this.handleDmFill(msg);
                case 'dmTestSource':
                    return await this.handleDmTest(msg);
                case 'dmPreview':
                    return await this.handleDmPreview(msg);
                case 'dmStart':
                    return await this.handleDmStart(msg);
                case 'dmStop':
                    return this.handleDmStop(msg);
                case 'dmReset':
                    return await this.handleDmReset(msg);
                default:
                    this.log.debug(`Unknown command ${msg.command} from ${msg.from}`);
                    return this.reply(msg, { error: `Unknown command ${msg.command}` });
            }
        }
        catch (e) {
            this.log.error(`Cannot process ${msg.command}: ${(0, redact_1.redact)(e)}`);
            this.reply(msg, { error: (0, redact_1.redact)(e) });
        }
    }
    historyTarget(reg) {
        const columns = new Set(['time', reg.target.field, 'state_id']);
        const placement = { ack: this.cfg.metaAck, q: this.cfg.metaQ, from: this.cfg.metaFrom };
        const meta = {};
        for (const k of ['ack', 'q', 'from']) {
            if (placement[k] !== 'none' && this.schema.columnKind(reg.target.table, k)) {
                columns.add(k);
                meta[k] = k;
            }
        }
        if (this.schema.hasTable(reg.target.table) && !this.schema.columnKind(reg.target.table, 'state_id')) {
            columns.delete('state_id');
        }
        return {
            table: reg.target.table,
            field: reg.target.field,
            valueType: reg.target.valueType,
            storeId: reg.storeId,
            columns,
            metaColumns: meta,
        };
    }
    async handleGetHistory(msg) {
        const started = Date.now();
        const message = (msg.message ?? {});
        const { options, error } = (0, historyService_1.normalizeHistoryOptions)(message.options, {
            limit: this.cfg.historyMaxRows,
            hardLimit: this.cfg.historyHardLimit,
            aggregate: this.cfg.defaultAggregate,
        });
        const sessionId = message.options?.sessionId;
        if (error) {
            return this.reply(msg, { error, sessionId });
        }
        if (!this.client || this.cfg.edition === 'cloud') {
            return this.reply(msg, { error: 'getHistory is not available for this edition', sessionId });
        }
        const reqId = typeof message.id === 'string' ? message.id : '';
        const realId = this.registry.has(reqId) ? reqId : this.storeIdToReal.get(reqId);
        const reg = realId ? this.registry.get(realId) : undefined;
        if (!reg) {
            return this.reply(msg, {
                result: [],
                step: 0,
                error: `${reqId} is not logged by ${this.namespace}`,
                sessionId,
            });
        }
        // make recently logged values visible
        await this.queue?.flush(1000);
        const target = this.historyTarget(reg);
        const built = (0, historyService_1.buildHistoryQueries)(target, options);
        try {
            const results = await Promise.all(built.queries.map(async (q) => ({
                kind: q.kind,
                rows: await this.client.querySql(q.sql, q.params, {
                    timeoutMs: this.cfg.queryTimeoutMs,
                    maxBytes: this.cfg.queryMaxBytes,
                }),
            })));
            const result = (0, historyService_1.mapHistoryRows)(results, options, built.step, reg.storeId);
            this.stats.lastRead = Date.now();
            this.stats.counters.historyQueries++;
            if (reg.cfg.enableDebugLogs) {
                this.log.debug(`getHistory ${reg.id}: ${result.length} values in ${Date.now() - started} ms (${built.aggregate})`);
            }
            this.reply(msg, { result, step: built.step, error: null, sessionId });
        }
        catch (e) {
            if (e instanceof influxClient_1.HttpError && /not found/i.test(e.message)) {
                // nothing written yet
                return this.reply(msg, { result: [], step: built.step, error: null, sessionId });
            }
            this.reply(msg, { result: [], step: 0, error: (0, redact_1.redact)(e), sessionId });
        }
    }
    handleStoreState(msg) {
        const m = msg.message;
        const items = [];
        const add = (id, state) => {
            if (typeof id !== 'string') {
                return;
            }
            if (Array.isArray(state)) {
                state.forEach(s => add(id, s));
            }
            else if (state && typeof state === 'object' && 'val' in state) {
                items.push({ id, state: state });
            }
        };
        if (Array.isArray(m)) {
            m.forEach(e => add(e?.id, e?.state));
        }
        else if (m && typeof m === 'object') {
            add(m.id, m.state);
        }
        if (!items.length) {
            return this.reply(msg, { error: 'Invalid call: expected {id, state} or [{id, state}]' });
        }
        let stored = 0;
        const errors = [];
        for (const { id, state } of items) {
            const reg = this.registry.get(id) ?? this.registry.get(this.storeIdToReal.get(id) ?? '');
            if (!reg) {
                errors.push(`${id}: not logged by ${this.namespace}`);
                continue;
            }
            if (reg.conflict) {
                errors.push(`${id}: ${reg.conflict}`);
                continue;
            }
            const prepared = this.prepare(reg, {
                ...state,
                ack: state.ack ?? false,
                q: state.q ?? 0,
                from: state.from ?? msg.from,
                ts: state.ts ?? Date.now(),
            });
            if (!prepared) {
                errors.push(`${id}: value cannot be stored`);
                continue;
            }
            this.write(reg.id, prepared, 'change');
            stored++;
        }
        this.reply(msg, {
            success: errors.length === 0,
            stored,
            errors: errors.length ? errors.slice(0, 20) : undefined,
            connected: this.influxConnected,
        });
    }
    async writeCustomConfig(id, options) {
        const obj = await this.getForeignObjectAsync(id);
        if (!obj) {
            throw new Error(`object ${id} not found`);
        }
        obj.common ||= {};
        const common = obj.common;
        common.custom ||= {};
        const prev = common.custom[this.namespace] ?? {};
        common.custom[this.namespace] = { ...prev, ...options };
        await this.setForeignObjectAsync(id, obj);
    }
    async handleEnableHistory(msg) {
        const m = msg.message;
        if (!m || typeof m.id !== 'string' || !m.id) {
            return this.reply(msg, { error: 'Invalid call: id missing' });
        }
        const options = { ...(m.options ?? {}), enabled: true };
        // validate with the same rules the runtime uses
        const { config: stateCfg, warnings } = (0, config_1.normalizeStateConfig)(options, this.cfg, m.id);
        warnings.push(...this.catalogWarnings(stateCfg.customTags));
        await this.writeCustomConfig(m.id, options);
        this.log.info(`enableHistory ${m.id} by ${msg.from}${warnings.length ? ` (warnings: ${warnings.join('; ')})` : ''}`);
        this.reply(msg, { success: true, warnings: warnings.length ? warnings : undefined });
    }
    async handleDisableHistory(msg) {
        const m = msg.message;
        if (!m || typeof m.id !== 'string' || !m.id) {
            return this.reply(msg, { error: 'Invalid call: id missing' });
        }
        await this.writeCustomConfig(m.id, { enabled: false });
        this.log.info(`disableHistory ${m.id} by ${msg.from}`);
        this.reply(msg, { success: true });
    }
    enabledDPs() {
        const out = {};
        for (const reg of this.registry.values()) {
            out[reg.id] = { ...reg.cfg, table: reg.target.table, conflict: reg.conflict };
        }
        return out;
    }
    async handleQuery(msg, lang) {
        if (!this.cfg.allowUserQueries) {
            return this.reply(msg, { error: 'User queries are disabled in the instance settings' });
        }
        if (!(0, queryService_1.senderAllowed)(msg.from, this.cfg.querySenders)) {
            this.log.warn(`Query from ${msg.from} refused (not in the sender allow-list)`);
            return this.reply(msg, { error: `Sender ${msg.from} is not allowed to run queries` });
        }
        if (!this.client) {
            return this.reply(msg, { error: 'not connected' });
        }
        const m = msg.message;
        const text = typeof m === 'string' ? m : m?.query;
        const params = typeof m === 'object' && m ? m.params : undefined;
        const guard = lang === 'sql' ? (0, queryService_1.checkReadOnlySql)(text) : (0, queryService_1.checkReadOnlyInfluxQL)(text);
        if (!guard.ok) {
            return this.reply(msg, { error: guard.reason });
        }
        const cleanParams = {};
        if (params && typeof params === 'object') {
            for (const [k, v] of Object.entries(params)) {
                if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(k) &&
                    (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean')) {
                    cleanParams[k] = v;
                }
            }
        }
        const opts = { timeoutMs: this.cfg.queryTimeoutMs, maxBytes: this.cfg.queryMaxBytes };
        try {
            const rows = lang === 'sql'
                ? await this.client.querySql(guard.statement, cleanParams, opts)
                : await this.client.queryInfluxQL(guard.statement, opts);
            this.stats.counters.userQueries++;
            this.stats.lastRead = Date.now();
            const truncated = rows.length > this.cfg.queryMaxRows;
            this.reply(msg, {
                result: truncated ? rows.slice(0, this.cfg.queryMaxRows) : rows,
                rows: Math.min(rows.length, this.cfg.queryMaxRows),
                truncated,
                error: null,
            });
        }
        catch (e) {
            this.reply(msg, { result: [], error: (0, redact_1.redact)(e) });
        }
    }
    async handleGetSchema(msg) {
        await this.refreshSchema();
        const tables = this.schema.summary();
        const retention = this.client && this.cfg.edition !== 'cloud' ? await this.client.getRetentionMs() : null;
        this.reply(msg, {
            database: this.cfg.database,
            retentionMs: retention === null ? 'unknown' : (retention ?? 'infinite'),
            tables,
            tableCount: this.schema.tableCount(),
            maxTables: this.cfg.maxTables,
            maxColumns: this.cfg.maxColumns,
        });
    }
    adapterStats() {
        return {
            ...this.stats.snapshot(),
            queue: this.queue?.stats(),
            enabledStates: this.registry.size,
            subscribedAll: this.subscribedAll,
            tables: this.schema?.tableCount(),
            conflicts: [...this.registry.values()].filter(r => r.conflict).map(r => ({ id: r.id, reason: r.conflict })),
        };
    }
    diagnosticsText() {
        const st = this.stats;
        const c = st.counters;
        const q = this.queue?.stats();
        const fmt = (ts) => (ts > 0 ? new Date(ts).toISOString() : '-');
        return [
            `Connected: ${st.connected ? 'yes' : 'no'}`,
            `Server: ${st.serverEdition || '-'} ${st.serverVersion}`,
            `Logged states: ${this.registry.size} (conflicts: ${[...this.registry.values()].filter(r => r.conflict).length})`,
            `Tables: ${this.schema?.tableCount() ?? 0} of max. ${this.cfg.maxTables} (${this.cfg.schemaMode} mode)`,
            `Written / rejected / dropped: ${c.written} / ${c.rejected} / ${c.dropped}`,
            `Buffer: ${q?.queuedRecords ?? 0} records, disk ${q?.spoolBytes ?? 0} B, circuit ${q?.circuit ?? '-'}`,
            `Last write: ${fmt(st.lastWrite)}  Last read: ${fmt(st.lastRead)}`,
            `Last error: ${st.lastError || '-'}`,
        ].join('\n');
    }
    async handleTestConnection(msg) {
        const form = (msg.message?.config ?? msg.message ?? {});
        const merged = { ...this.config };
        for (const k of ['url', 'database', 'edition', 'tlsRejectUnauthorized', 'tlsCa', 'requestTimeoutMs']) {
            if (form[k] !== undefined && form[k] !== '') {
                merged[k] = form[k];
            }
        }
        if (typeof form.token === 'string' && form.token.trim()) {
            merged.token = form.token.trim();
        }
        const { config } = (0, config_1.normalizeInstanceConfig)(merged);
        (0, redact_1.registerSecret)(config.token);
        const client = this.createClient(config);
        try {
            const ping = await client.ping();
            if (!ping.ok) {
                return this.reply(msg, { error: `Connection failed: ${ping.error ?? `HTTP ${ping.status}`}` });
            }
            const lines = [`OK: InfluxDB 3 ${ping.edition ?? ''} ${ping.version ?? ''} (${ping.latencyMs} ms)`];
            if (config.edition !== 'cloud') {
                try {
                    const dbs = await client.listDatabases();
                    lines.push(dbs.includes(config.database)
                        ? `Database "${config.database}" exists`
                        : `Database "${config.database}" does not exist yet - it is created by the first write`);
                }
                catch (e) {
                    lines.push(`Database list not readable with this token (${(0, redact_1.redact)(e)})`);
                }
                const retention = await client.getRetentionMs();
                if (retention !== null) {
                    lines.push(`Retention: ${retention === undefined ? 'infinite' : `${Math.round(retention / 86400000)} days`}`);
                }
            }
            this.reply(msg, { result: lines.join('\n') });
        }
        finally {
            client.close();
        }
    }
    async handleCreateDatabase(msg) {
        const m = (msg.message ?? {});
        const name = typeof m.database === 'string' && m.database ? m.database : this.cfg.database;
        const retention = typeof m.retention === 'string' ? m.retention.trim() : this.cfg.createDatabaseRetention;
        if (!config_1.DATABASE_NAME_RE.test(name)) {
            return this.reply(msg, { error: `Invalid database name "${name}"` });
        }
        if (retention && !config_1.RETENTION_RE.test(retention)) {
            return this.reply(msg, { error: `Invalid retention "${retention}" (examples: 30d, 52w, 1y, none)` });
        }
        if (!this.client) {
            return this.reply(msg, { error: 'not connected' });
        }
        try {
            const dbs = await this.client.listDatabases();
            if (dbs.includes(name)) {
                return this.reply(msg, { error: `Database "${name}" already exists - its retention is not changed` });
            }
            await this.client.createDatabase(name, retention);
            this.log.info(`Database "${name}" created on request of ${msg.from} (retention ${retention || 'infinite'})`);
            this.reply(msg, { result: `Database "${name}" created (retention ${retention || 'infinite'})` });
        }
        catch (e) {
            this.reply(msg, { error: (0, redact_1.redact)(e) });
        }
    }
    async handlePreview(msg) {
        const m = (msg.message ?? {});
        if (typeof m.id !== 'string' || !m.id) {
            return this.reply(msg, { text: 'Save the object settings once to see the preview.' });
        }
        const obj = await this.getForeignObjectAsync(m.id);
        if (!obj) {
            return this.reply(msg, { text: `Object ${m.id} not found` });
        }
        const custom = { ...(m.config ?? this.customOf(obj) ?? {}), enabled: true };
        const map = await this.computeMapping(m.id, obj, custom);
        const check = this.schema.check(map.claim);
        const st = await this.getForeignStateAsync(m.id).catch(() => null);
        const lines = [
            `Table: ${map.target.table} (${map.target.source})   Field: ${map.target.field} (${map.target.valueType})`,
        ];
        if (!check.ok) {
            lines.push(`⚠ CONFLICT: ${check.reason}`);
        }
        const staticTags = {};
        for (const [k, v] of Object.entries(map.tags.tags)) {
            if (check.ok && check.droppedTags.includes(k)) {
                continue;
            }
            staticTags[k] = v;
        }
        staticTags.state_id = map.storeId;
        const fake = {
            id: m.id,
            storeId: map.storeId,
            cfg: map.cfg,
            key: map.key,
            target: map.target,
            tags: map.tags,
            staticTags,
            metaFields: map.metaFields,
            metaTags: map.metaTags,
            warnings: map.warnings,
            pipeline: undefined,
        };
        const sample = st ?? {
            val: map.target.valueType === 'number' ? 0 : map.target.valueType === 'boolean' ? false : '',
            ts: Date.now(),
            ack: true,
            q: 0,
            from: '',
            lc: 0,
        };
        const prepared = this.prepare(fake, sample);
        if (prepared) {
            try {
                lines.push(`Line protocol: ${(0, lineProtocol_1.serialize)(this.toRecord(fake, prepared))}`);
            }
            catch (e) {
                lines.push(`Line protocol error: ${(0, redact_1.redact)(e)}`);
            }
        }
        else {
            lines.push('Current value cannot be stored with this type');
        }
        if (map.tags.dynamic.length) {
            lines.push(`Dynamic tags: ${map.tags.dynamic.map(d => `${d.key}=${d.template}`).join(', ')}`);
        }
        const warnings = [...map.warnings, ...(check.ok ? check.warnings : [])];
        if (warnings.length) {
            lines.push(`Warnings: ${warnings.join(' | ')}`);
        }
        this.reply(msg, { text: lines.join('\n'), style: check.ok ? undefined : { color: 'red' } });
    }
    /** Options for the tag key dropdown of the per-state dialog (selectSendTo) */
    tagKeyOptions() {
        if (!this.cfg.tagDefinitions.length) {
            return [{ value: '', label: 'No tag keys defined - add them in the instance settings (tab Tags)' }];
        }
        return this.cfg.tagDefinitions.map(d => ({
            value: d.key,
            label: d.description ? `${d.key} – ${d.description}` : d.key,
        }));
    }
    /**
     * Options for the tag value field of the per-state dialog (autocompleteSendTo).
     *
     * @param key selected tag key
     */
    tagValueOptions(key) {
        const def = typeof key === 'string' ? this.cfg.tagDefinitions.find(d => d.key === key) : undefined;
        if (!def) {
            return [];
        }
        return def.values.map(v => ({ value: v, label: v === def.defaultValue ? `${v} (default)` : v }));
    }
    /**
     * Warnings for per-state tags that the catalog does not allow.
     *
     * @param tags per-state tags
     */
    catalogWarnings(tags) {
        const out = [];
        for (const t of tags) {
            const check = (0, tagResolver_1.checkTagAgainstCatalog)(t, this.cfg.tagDefinitions, this.cfg.tagKeyStyle);
            if (!check.ok) {
                out.push(`tag "${t.key}": ${check.reason} - it will be ignored`);
            }
        }
        return out;
    }
    /**
     * Options of the "influxdb instance" dropdowns: `{value, label}` objects (Admin cannot draw plain strings), with the
     * database version and the number of states each instance logs.
     */
    async handleListInfluxdbInstances(msg) {
        try {
            const objs = await this.getForeignObjectsAsync('system.adapter.influxdb.*', 'instance');
            const ids = Object.keys(objs)
                .map(id => id.replace('system.adapter.', ''))
                .filter(id => /^influxdb\.\d+$/.test(id))
                .sort();
            if (!ids.length) {
                return this.reply(msg, [{ value: '', label: 'no influxdb instance found' }]);
            }
            const view = await this.getObjectViewAsync('system', 'custom', {});
            const counts = new Map();
            for (const r of (view?.rows ?? [])) {
                for (const id of ids) {
                    if (r.value?.[id]?.enabled) {
                        counts.set(id, (counts.get(id) ?? 0) + 1);
                    }
                }
            }
            this.reply(msg, ids.map(id => {
                const n = (objs[`system.adapter.${id}`]?.native ?? {});
                const db = n.dbversion === '2.x' ? 'InfluxDB 2.x' : 'InfluxDB 1.x';
                return { value: id, label: `${id} (${db}, ${counts.get(id) ?? 0} state(s))` };
            }));
        }
        catch (e) {
            this.log.debug(`influxdb instances not listed: ${(0, redact_1.redact)(e)}`);
            this.reply(msg, []); // a dropdown needs a list
        }
    }
    async handleMigration(msg, mode) {
        const m = (msg.message ?? {});
        const source = typeof m.source === 'string' ? m.source : '';
        if (!/^influxdb\.\d+$/.test(source)) {
            const text = 'Select an influxdb instance (e.g. influxdb.0) first';
            return this.reply(msg, mode === 'html' ? { text: (0, migration_1.preformattedHtml)(text) } : { error: text });
        }
        const view = await this.getObjectViewAsync('system', 'custom', {});
        const rows = (view?.rows ?? []);
        // the Admin page sends the rules being edited (not saved yet); otherwise the saved ones are used
        const rules = (0, tagMapping_1.parseTagRules)(m.rules !== undefined ? m.rules : this.config.migrationTagRules);
        const plan = (0, migration_1.planSettingsMigration)(rows.map(r => ({ id: r.id, custom: r.value ?? {} })), source, this.namespace, {
            rules,
            // previews show the catalog being edited; apply always uses the saved one (Admin locks it otherwise)
            catalog: mode === 'apply' ? this.cfg.tagDefinitions : this.catalogFrom(m.catalog),
            style: this.cfg.tagKeyStyle,
            overwrite: m.overwrite === true,
            standardizeExisting: m.standardizeExisting !== false,
            ids: Array.isArray(m.ids)
                ? new Set(m.ids.filter((x) => typeof x === 'string'))
                : undefined,
        });
        if (mode === 'html') {
            return this.reply(msg, { text: (0, migration_1.preformattedHtml)((0, migration_1.formatSettingsPreview)(plan)) });
        }
        if (mode === 'preview') {
            return this.reply(msg, { result: (0, migration_1.formatSettingsPreview)(plan, 30), items: plan.todo.map(p => p.id) });
        }
        let done = 0;
        let standardized = 0;
        const failed = [];
        for (const item of plan.todo) {
            try {
                await this.writeCustomConfig(item.id, item.config);
                done++;
            }
            catch (e) {
                failed.push(`${item.id}: ${(0, redact_1.redact)(e)}`);
            }
        }
        for (const ex of plan.existing) {
            try {
                await this.writeCustomConfig(ex.id, { customTags: ex.customTags });
                standardized++;
            }
            catch (e) {
                failed.push(`${ex.id}: ${(0, redact_1.redact)(e)}`);
            }
        }
        this.log.info(`Migration from ${source}: ${done} state(s) configured by request of ${msg.from}${plan.unmatched.size ? ` (${plan.unmatched.size} old tag value(s) did not fit the tag catalog)` : ''}`);
        this.reply(msg, {
            result: `${done} state(s) configured for ${this.namespace}${standardized ? `, tags of ${standardized} already configured state(s) standardised` : ''}.${failed.length ? ` ${failed.length} failed.` : ''} ${source} was not changed.`,
            failed: failed.length ? failed : undefined,
        });
    }
    // ------------------------------------------------------------------ old tags of the influxdb settings (ADR-0006)
    /**
     * Used tags: custom tags of the influxdb settings and of this instance, plus (option) the tags stored in the
     * old database of tab "Migration of data".
     *
     * @param sourceRaw influxdb instance or '' for all
     * @param includePointsRaw read the old database (default: saved option)
     * @param waitForPointsMs how long to wait for the old database; afterwards the answer uses the settings (and an
     *     older old-database result) while the one read continues in the background
     */
    async oldTags(sourceRaw, includePointsRaw, waitForPointsMs = Number.POSITIVE_INFINITY) {
        const source = typeof sourceRaw === 'string' && /^influxdb\.\d+$/.test(sourceRaw) ? sourceRaw : '';
        const native = this.config;
        const includePoints = typeof includePointsRaw === 'boolean' ? includePointsRaw : native.migrationIncludeOldPoints !== false;
        const settings = await this.settingsTags.get(source, async () => {
            const view = await this.getObjectViewAsync('system', 'custom', {});
            const rows = (view?.rows ?? []).map(r => ({
                id: r.id,
                custom: r.value ?? {},
            }));
            return (0, tagMapping_1.mergeInventories)((0, migration_1.collectOldTags)(rows, source), (0, migration_1.collectInstanceTags)(rows, this.namespace));
        });
        const notes = [];
        if (!includePoints) {
            return { inventory: settings, notes };
        }
        const { source: db } = this.dmConfig();
        if ((0, dataMigration_1.validateSource)(db).length) {
            return { inventory: settings, notes }; // no old database configured
        }
        const ident = JSON.stringify([db.version, db.url, db.database, db.retentionPolicy, db.org, db.bucket]);
        const reading = this.pointTags.get(ident, async () => {
            const reader = (0, legacySource_1.createLegacyReader)(db);
            try {
                return await reader.tagInventory();
            }
            finally {
                reader.close();
            }
        });
        let uses;
        if (waitForPointsMs === Number.POSITIVE_INFINITY) {
            try {
                uses = await reading;
            }
            catch (e) {
                notes.push(`Tags of the old database not read: ${e instanceof Error ? e.message : (0, redact_1.redact)(e)}`);
            }
        }
        else {
            uses = (await this.within(reading, waitForPointsMs)) ?? this.pointTags.peek(ident);
            if (!uses) {
                notes.push('Tags of the old database are still being read');
            }
        }
        if (!uses) {
            return { inventory: settings, notes };
        }
        const points = { keys: new Map() };
        for (const u of uses) {
            (0, tagMapping_1.addToInventory)(points, u.key, u.value, u.measurements ?? 0, 'points');
        }
        return { inventory: (0, tagMapping_1.mergeInventories)(settings, points), notes };
    }
    /**
     * Result of a promise if it settles within the time, else undefined (the promise keeps running; a failure is
     * swallowed here - the cache remembers it).
     */
    within(promise, ms) {
        return new Promise(resolve => {
            let settled = false;
            const timer = {};
            const finish = (v) => {
                if (!settled) {
                    settled = true;
                    this.clearTimeout(timer.handle);
                    resolve(v);
                }
            };
            timer.handle = this.setTimeout(() => finish(undefined), ms);
            promise.then(finish, () => finish(undefined));
        });
    }
    /** Names of all ioBroker rooms or functions (system language) */
    async enumNames(prefix) {
        try {
            const objs = await this.getForeignObjectsAsync(`${prefix}*`, 'enum');
            const names = Object.entries(objs).map(([id, obj]) => this.nameOf(obj?.common?.name, id.split('.').pop() ?? id));
            return [...new Set(names.filter(Boolean))].sort((a, b) => a.localeCompare(b));
        }
        catch (e) {
            this.log.debug(`Cannot read ${prefix}*: ${(0, redact_1.redact)(e)}`);
            return [];
        }
    }
    /**
     * "Propose standard": matches old keys/values automatically (folding, synonyms, ioBroker rooms/functions) and
     * writes proposed catalog rows and rule rows into the form (sendTo `useNative`) - nothing is saved.
     */
    async handleProposeStandard(msg) {
        const m = (msg.message ?? {});
        const native = this.config;
        const { inventory, notes } = await this.oldTags(m.source, m.includeOldPoints);
        if (!inventory.keys.size) {
            return this.reply(msg, { result: 'noOldTags', args: [] });
        }
        const valueStyle = m.valueStyle === 'upper' || m.valueStyle === 'lower' || m.valueStyle === 'asUsed' ? m.valueStyle : 'auto';
        const useEnums = m.useEnums !== false;
        const p = (0, tagStandard_1.proposeStandard)(inventory, {
            catalogRows: m.catalog !== undefined ? m.catalog : native.tagDefinitions,
            ruleRows: m.rules !== undefined ? m.rules : native.migrationTagRules,
            keyStyle: this.cfg.tagKeyStyle,
            valueField: this.cfg.valueField,
            valueStyle,
            rooms: useEnums ? await this.enumNames('enum.rooms.') : [],
            functions: useEnums ? await this.enumNames('enum.functions.') : [],
        });
        const c = p.coverage;
        const coverage = c.uncovered.length === 0
            ? `All ${c.total} used tag value(s) are covered${c.removed ? ` (${c.removed} removed by rules)` : ''}.`
            : `${c.covered} of ${c.total} used tag value(s) are covered, ${c.uncovered.length} not.`;
        const nothing = !p.newKeys.length && !p.newValues && !p.newRules && !p.completedRows;
        const details = [
            `Proposal of ${new Date().toLocaleString()}`,
            '',
            coverage,
            ...c.uncovered.map(u => `  not covered: ${u}`),
            ...notes,
            nothing
                ? 'Nothing new to propose.'
                : `${p.newKeys.length} new key(s), ${p.newValues} new value(s), ${p.newRules} rule row(s) added${p.completedRows ? `, ${p.completedRows} incomplete row(s) filled in` : ''}.`,
            '',
            'Old key → new key: old values → standard value',
            ...p.summary.map(l => `  ${l}`),
        ].join('\n');
        await this.setStateAsync('info.migrationProposal', (0, migration_1.preformattedHtml)(details), true);
        if (nothing) {
            return this.reply(msg, { result: 'standardNothing', args: [], coverage: c });
        }
        this.reply(msg, {
            native: { tagDefinitions: p.catalogRows, migrationTagRules: p.ruleRows },
            result: 'standardProposedShort',
            args: [String(p.newKeys.length), String(p.newValues), String(p.newRules), coverage],
            summary: p.summary,
            coverage: c,
        });
    }
    /** Tag catalog sent by the Admin page (being edited), else the saved one */
    catalogFrom(raw) {
        return raw === undefined
            ? this.cfg.tagDefinitions
            : (0, config_1.parseTagDefinitions)(raw, this.cfg.tagKeyStyle, this.cfg.valueField, []);
    }
    /** Options of the "old key" column: keys of the old custom tags */
    async oldTagKeyOptions(message) {
        const m = (message ?? {});
        let inv;
        try {
            inv = (await this.oldTags(m.source, m.includeOldPoints, 2000)).inventory;
        }
        catch (e) {
            this.log.debug(`Old tag keys not available: ${(0, redact_1.redact)(e)}`);
            return []; // a dropdown needs a list, never an error object
        }
        return [...inv.keys.values()]
            .sort((a, b) => a.key.localeCompare(b.key))
            .map(e => ({
            value: e.key,
            label: `${e.key} (${e.values.size} value(s), ${e.states} state(s))`,
        }));
    }
    /** Options of the "old values" column: values of the selected old key */
    async oldTagValueOptions(message) {
        const m = (message ?? {});
        const key = typeof m.key === 'string' ? m.key.trim().toLowerCase() : '';
        let entry;
        try {
            entry = key ? (await this.oldTags(m.source, m.includeOldPoints, 2000)).inventory.keys.get(key) : undefined;
        }
        catch (e) {
            this.log.debug(`Old tag values not available: ${(0, redact_1.redact)(e)}`);
        }
        const options = entry
            ? [...entry.values.keys()]
                .sort((a, b) => a.localeCompare(b))
                .map(value => ({ value, label: `${value} (${(0, tagMapping_1.describeUse)(entry, value)})` }))
            : [];
        // values already selected in the row always get an option, so their chips have a label
        for (const v of (0, tagMapping_1.ruleValues)(m.selected)) {
            if (!options.some(o => o.value === v)) {
                options.push({ value: v, label: `${v} (not used any more)` });
            }
        }
        return options;
    }
    /** "Take over old tags": appends the rule rows that are still needed (sendTo `useNative`) */
    async handleSuggestRules(msg) {
        const m = (msg.message ?? {});
        const rawRows = Array.isArray(m.rules)
            ? m.rules
            : (0, config_1.tableRows)(this.config.migrationTagRules);
        const rules = (0, tagMapping_1.parseTagRules)(rawRows);
        const { inventory } = await this.oldTags(m.source, m.includeOldPoints);
        const added = (0, tagMapping_1.suggestTagRules)(inventory, rules, this.catalogFrom(m.catalog), this.cfg.tagKeyStyle);
        this.reply(msg, {
            native: { migrationTagRules: [...rawRows, ...added.map(tagMapping_1.ruleToRow)] },
            result: added.length ? 'rulesAdded' : inventory.keys.size ? 'rulesNone' : 'noOldTags',
            args: [String(added.length)],
        });
    }
    // ------------------------------------------------------------------ import of historical data (ADR-0006)
    dmFile() {
        return node_path_1.default.join(utils.getAbsoluteInstanceDataDir(this), 'data-migration.json');
    }
    dmConfig() {
        const warnings = [];
        const native = this.config;
        const { source, job } = (0, dataMigration_1.parseDataMigrationConfig)(native, warnings);
        const rules = (0, tagMapping_1.parseTagRules)(native.migrationTagRules, warnings);
        return { source, job, warnings, rules };
    }
    /** Copies the non-secret connection settings of an influxdb instance into the form (sendTo `useNative`). */
    async handleDmFill(msg) {
        const m = (msg.message ?? {});
        const inst = typeof m.instance === 'string' ? m.instance : '';
        if (!/^influxdb\.\d+$/.test(inst)) {
            return this.reply(msg, { error: 'Select an influxdb instance first' });
        }
        const obj = await this.getForeignObjectAsync(`system.adapter.${inst}`);
        if (!obj) {
            return this.reply(msg, { error: `${inst} not found` });
        }
        const n = (obj.native ?? {});
        const v2 = n.dbversion === '2.x';
        const protocol = n.protocol === 'https' ? 'https' : 'http';
        const rawHost = typeof n.host === 'string' && n.host.trim() ? n.host.trim() : 'localhost';
        const host = rawHost.includes(':') && !rawHost.startsWith('[') ? `[${rawHost}]` : rawHost;
        const port = Number(n.port) > 0 ? Number(n.port) : 8086;
        const p = v2 && typeof n.path === 'string' ? n.path.replace(/^\/+|\/+$/g, '') : '';
        const native = {
            dmInfluxdbInstance: inst,
            dmVersion: v2 ? '2.x' : '1.x',
            dmUrl: `${protocol}://${host}:${port}${p ? `/${p}` : ''}`,
            dmRejectUnauthorized: n.validateSSL !== false,
        };
        if (v2) {
            native.dmOrg = typeof n.organization === 'string' ? n.organization : '';
            native.dmBucket = typeof n.dbname === 'string' ? n.dbname : '';
        }
        else {
            native.dmDatabase = typeof n.dbname === 'string' ? n.dbname : '';
            native.dmUser = typeof n.user === 'string' ? n.user : '';
        }
        this.reply(msg, {
            native,
            result: `Connection settings of ${inst} copied. Enter the ${v2 ? 'token' : 'password'} (it cannot be read from ${inst}) and save.`,
        });
    }
    async handleDmTest(msg) {
        const { source, warnings } = this.dmConfig();
        const errors = (0, dataMigration_1.validateSource)(source);
        if (errors.length) {
            return this.reply(msg, { error: errors.join('; ') });
        }
        const reader = (0, legacySource_1.createLegacyReader)(source);
        try {
            const res = await reader.check();
            if (!res.ok) {
                return this.reply(msg, { error: `Old database: ${res.error}` });
            }
            const all = await reader.listMeasurements(20_000).catch(() => []);
            const where = source.version === '1.x' ? `database "${source.database}"` : `bucket "${source.bucket}"`;
            this.reply(msg, {
                result: `Connected to InfluxDB ${(res.version || source.version).replace(/^v/, '')}: ${all.length >= 20_000 ? 'more than 20000' : all.length} measurement(s) in ${where}.${warnings.length ? ` Warnings: ${warnings.join('; ')}` : ''}`,
            });
        }
        finally {
            reader.close();
        }
    }
    /** Source measurement of a state: alias of its influxdb settings, else its id (rule of the classic adapter). */
    oldMeasurement(id, custom, instance) {
        const blocks = instance
            ? [custom?.[instance]]
            : Object.entries(custom ?? {})
                .filter(([k]) => /^influxdb\.\d+$/.test(k))
                .map(([, v]) => v);
        for (const b of blocks) {
            const alias = b?.aliasId;
            if (typeof alias === 'string' && alias.trim()) {
                return alias.trim();
            }
        }
        return id;
    }
    /** Old measurement → ioBroker state of the influxdb settings (alias, else the id), for the explanations */
    async dmOwners() {
        const view = await this.getObjectViewAsync('system', 'custom', {});
        const owners = new Map();
        for (const r of (view?.rows ?? [])) {
            const custom = r.value ?? {};
            const here = !!custom[this.namespace]?.enabled;
            for (const [instance, c] of Object.entries(custom)) {
                const cfg = c;
                if (/^influxdb\.\d+$/.test(instance) && cfg?.enabled) {
                    const alias = typeof cfg.aliasId === 'string' ? cfg.aliasId.trim() : '';
                    owners.set(alias || r.id, { id: r.id, instance, configuredHere: here });
                }
            }
        }
        return owners;
    }
    async dmItems(job, rules) {
        const filter = (0, dataMigration_1.compileStateFilter)(job.stateFilter);
        const view = await this.getObjectViewAsync('system', 'custom', {});
        const customs = new Map((view?.rows ?? []).map(r => [
            r.id,
            r.value ?? {},
        ]));
        const items = [];
        for (const reg of [...this.registry.values()].sort((a, b) => a.id.localeCompare(b.id))) {
            if (reg.conflict || !filter(reg.id)) {
                continue;
            }
            items.push({
                id: reg.id,
                measurement: this.oldMeasurement(reg.id, customs.get(reg.id), job.influxdbInstance),
                table: reg.target.table,
                convert: points => this.convertLegacy(reg.id, points, job.keepOldTags, rules),
            });
        }
        return items;
    }
    /**
     * Converts old points with the live mapping of the state (type, rounding, table, tags, metadata). Tags of the
     * old points pass through the tag rules and the catalog and are added where the configuration sets no value.
     */
    convertLegacy(id, points, keepOldTags, rules) {
        const reg = this.registry.get(id);
        if (!reg || reg.conflict) {
            return { lines: [], skipped: points.length };
        }
        const lines = [];
        let skipped = 0;
        const tagCache = new Map();
        for (const p of points) {
            const res = (0, schemaMapper_1.coerceValue)(p.value, reg.target.valueType, {
                maxStringLength: this.cfg.maxStringLength,
                objectPolicy: this.cfg.objectPolicy,
            });
            if (!res.ok) {
                skipped++;
                continue;
            }
            const val = typeof res.value === 'number' ? (0, schemaMapper_1.roundTo)(res.value, reg.cfg.round) : res.value;
            const rec = this.toRecord(reg, { val, ts: Math.round(p.ts), ack: p.ack ?? false, q: p.q ?? 0, from: p.from ?? '' }, false);
            if (p.ack === undefined) {
                delete rec.fields.ack;
                delete rec.tags.ack;
            }
            if (p.q === undefined) {
                delete rec.fields.q;
                delete rec.tags.q;
            }
            if (keepOldTags) {
                for (const [k, v] of Object.entries(this.mappedOldTags(p.tags, rules, tagCache))) {
                    if (!(k in rec.tags) && !(k in rec.fields)) {
                        rec.tags[k] = v;
                    }
                }
            }
            try {
                lines.push((0, lineProtocol_1.serialize)(rec));
            }
            catch {
                skipped++;
            }
        }
        return { lines, skipped };
    }
    mappedOldTags(tags, rules, cache) {
        const keys = Object.keys(tags).sort();
        if (!keys.length) {
            return {};
        }
        const cacheKey = JSON.stringify(keys.map(k => [k, tags[k]]));
        let out = cache.get(cacheKey);
        if (!out) {
            out = {};
            const mapped = (0, tagMapping_1.applyTagRules)(keys.map(k => ({ key: k, value: tags[k] })), rules, this.cfg.tagDefinitions, this.cfg.tagKeyStyle);
            for (const t of mapped.tags) {
                const k = (0, tagResolver_1.normalizeTagKey)(t.key, this.cfg.tagKeyStyle);
                const v = t.value.slice(0, this.cfg.maxTagValueLength);
                if (k && v && !(this.cfg.blockSensitiveTags && (0, tagResolver_1.looksSensitive)(k, v))) {
                    out[k] = v;
                }
            }
            cache.set(cacheKey, out);
        }
        return out;
    }
    dmTarget() {
        return {
            firstTime: async (item) => {
                const reg = this.registry.get(item.id);
                if (!reg || !this.client) {
                    return undefined;
                }
                try {
                    const rows = await this.client.querySql(`SELECT CAST(MIN(time) AS BIGINT) / 1000000 AS first FROM ${(0, historyService_1.quoteIdent)(reg.target.table)} WHERE state_id = $sid`, { sid: reg.storeId });
                    const v = rows[0]?.first;
                    return v === null || v === undefined || !Number.isFinite(Number(v)) ? undefined : Number(v);
                }
                catch (e) {
                    if (e instanceof influxClient_1.HttpError && /not found|does not exist/i.test(`${e.message} ${e.body}`)) {
                        return undefined; // table not created yet
                    }
                    throw e;
                }
            },
            write: lines => this.client
                ? this.client.write(lines)
                : Promise.resolve({ kind: 'transient', message: 'not connected' }),
            busy: () => {
                const st = this.queue?.stats();
                return !!st && (st.circuit === 'open' || st.queuedRecords > this.cfg.bufferMaxRecords / 2);
            },
        };
    }
    dmSleep(ms, signal) {
        return new Promise(resolve => {
            if (signal.aborted) {
                return resolve();
            }
            const timer = {};
            const onAbort = () => {
                this.clearTimeout(timer.handle);
                resolve();
            };
            timer.handle = this.setTimeout(() => {
                signal.removeEventListener('abort', onAbort);
                resolve();
            }, ms);
            if (timer.handle === undefined) {
                return resolve(); // adapter is stopping
            }
            signal.addEventListener('abort', onAbort, { once: true });
        });
    }
    dmProgress(p) {
        const now = Date.now();
        if (p.running && now - this.dmLastProgress < 1000) {
            return;
        }
        this.dmLastProgress = now;
        void Promise.all([
            this.setStateAsync('info.dataMigration.running', p.running, true),
            this.setStateAsync('info.dataMigration.progress', p.percent, true),
            this.setStateAsync('info.dataMigration.status', p.status, true),
            this.setStateAsync('info.dataMigration.points', p.points, true),
            this.setStateAsync('info.dataMigration.errors', p.errors, true),
        ]).catch(e => this.log.debug(`Cannot update import status: ${(0, redact_1.redact)(e)}`));
    }
    dmKey(source, job, rules) {
        return (0, dataMigration_1.jobKey)(source, job, { rules, catalog: this.cfg.tagDefinitions, style: this.cfg.tagKeyStyle });
    }
    async handleDmPreview(msg) {
        const { source, job, warnings, rules } = this.dmConfig();
        const errors = (0, dataMigration_1.validateSource)(source);
        if (errors.length) {
            return this.reply(msg, { error: errors.join('; ') });
        }
        const items = await this.dmItems(job, rules);
        const reader = (0, legacySource_1.createLegacyReader)(source);
        try {
            const check = await reader.check();
            if (!check.ok) {
                return this.reply(msg, { error: `Old database: ${check.error}` });
            }
            const cp = await new dataMigration_1.FileCheckpointStore(this.dmFile()).load();
            let text = await (0, dataMigration_1.previewImport)(items, { source: reader, target: this.dmTarget() }, job, cp, this.dmKey(source, job, rules), { detailed: 15, samples: 2 }, await this.dmOwners(), this.namespace);
            const retention = await this.client?.getRetentionMs().catch(() => undefined);
            if (retention) {
                text += `\nNote: the InfluxDB 3 database keeps data only ${Math.round(retention / 86_400_000)} day(s) - older imported points are removed by the server.`;
            }
            if (warnings.length) {
                text = `Warnings: ${warnings.join('; ')}\n\n${text}`;
            }
            const stamp = `Preview of ${new Date().toLocaleString()}\n\n`;
            await this.setStateAsync('info.dataMigration.preview', (0, migration_1.preformattedHtml)(stamp + text), true);
            this.reply(msg, { result: 'Preview ready - see "Result of the preview" below.', text });
        }
        finally {
            reader.close();
        }
    }
    async handleDmStart(msg) {
        if (this.dmRun) {
            return this.reply(msg, { error: 'The import is already running' });
        }
        const { source, job, warnings, rules } = this.dmConfig();
        const errors = (0, dataMigration_1.validateSource)(source);
        if (errors.length) {
            return this.reply(msg, { error: errors.join('; ') });
        }
        const items = await this.dmItems(job, rules);
        if (!items.length) {
            const pending = new Map();
            for (const o of (await this.dmOwners()).values()) {
                if (!o.configuredHere) {
                    pending.set(o.instance, (pending.get(o.instance) ?? 0) + 1);
                }
            }
            const hint = [...pending.entries()]
                .map(([inst, n]) => `${n} state(s) of ${inst} are not configured for ${this.namespace} yet: tab "Migration of settings" → choose ${inst} → "Apply".`)
                .join(' ');
            return this.reply(msg, {
                error: `No state of this instance matches the selection, nothing to import. ${hint || 'Configure the states for this instance first (tab "Migration of settings").'}`,
            });
        }
        const key = this.dmKey(source, job, rules);
        const store = new dataMigration_1.FileCheckpointStore(this.dmFile());
        const cp = await store.load();
        if (cp && cp.jobKey === key && cp.finished) {
            return this.reply(msg, {
                error: 'An import with these settings is already finished. "Reset progress" imports again.',
            });
        }
        const reader = (0, legacySource_1.createLegacyReader)(source);
        const check = await reader.check();
        if (!check.ok) {
            reader.close();
            return this.reply(msg, { error: `Old database: ${check.error}` });
        }
        const ctrl = new AbortController();
        const runner = new dataMigration_1.DataMigrationJob({
            source: reader,
            target: this.dmTarget(),
            store,
            now: () => Date.now(),
            sleep: (ms, signal) => this.dmSleep(ms, signal),
            log: {
                info: m => this.log.info((0, redact_1.redact)(m)),
                warn: m => this.log.warn((0, redact_1.redact)(m)),
                debug: m => this.log.debug((0, redact_1.redact)(m)),
            },
            onProgress: p => this.dmProgress(p),
        }, job);
        const done = runner
            .run(items, key, ctrl.signal)
            .catch((e) => {
            this.log.error(`Data import: ${(0, redact_1.redact)(e)}`);
            return 'failed';
        })
            .finally(() => {
            reader.close();
            this.dmRun = undefined;
            void this.refreshSchema();
        });
        this.dmRun = { ctrl, done };
        this.log.info(`Data import started by ${msg.from}: ${items.length} state(s) from InfluxDB ${source.version}${warnings.length ? ` (warnings: ${warnings.join('; ')})` : ''}`);
        this.reply(msg, { result: `Import of ${items.length} state(s) started - see the progress below and the log.` });
    }
    handleDmStop(msg) {
        if (!this.dmRun) {
            return this.reply(msg, { result: 'No import is running.' });
        }
        this.dmRun.ctrl.abort();
        this.log.info(`Data import stopped by ${msg.from}`);
        this.reply(msg, { result: 'Stopping - the progress is saved, "Start" continues.' });
    }
    async handleDmReset(msg) {
        if (this.dmRun) {
            return this.reply(msg, { error: 'Stop the import first' });
        }
        const store = new dataMigration_1.FileCheckpointStore(this.dmFile());
        // keeps the "first live value" times - otherwise imported points would be taken for live values
        await store.save((0, dataMigration_1.newCheckpoint)('reset', Date.now(), await store.load()));
        this.dmProgress({ running: false, percent: 0, status: 'progress reset', points: 0, errors: 0, current: '' });
        this.log.info(`Data import progress reset by ${msg.from}`);
        this.reply(msg, {
            result: 'Progress reset - "Start" imports from the beginning. Points imported before stay in InfluxDB 3; writing them again overwrites them.',
        });
    }
}
exports.InfluxV3Adapter = InfluxV3Adapter;
if (require.main !== module) {
    // Export the constructor in compact mode
    module.exports = (options) => new InfluxV3Adapter(options);
}
else {
    // otherwise start the instance directly
    (() => new InfluxV3Adapter())();
}
//# sourceMappingURL=main.js.map