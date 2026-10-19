/**
 * ioBroker adapter "influx-v3" — logs selected states into InfluxDB 3 (Core / Enterprise).
 *
 * AdapterRuntime: lifecycle, subscriptions, per-state registry and sendTo commands.
 * The logic lives in src/lib/*; see docs/ARCHITECTURE.md.
 */
import * as utils from '@iobroker/adapter-core';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import {
    type InstanceConfig,
    normalizeInstanceConfig,
    normalizeStateConfig,
    type StateConfig,
    stateConfigKey,
    DATABASE_NAME_RE,
    RETENTION_RE,
    tableRows,
} from './lib/config';
import { type EmitReason, FilterPipeline, type PreparedState } from './lib/filterPipeline';
import {
    buildHistoryQueries,
    type BuiltQuery,
    type HistoryTarget,
    mapHistoryRows,
    normalizeHistoryOptions,
} from './lib/historyService';
import { HttpError, InfluxV3Client } from './lib/influxClient';
import { type LpRecord, LineProtocolError, serialize, type FieldValue } from './lib/lineProtocol';
import { buildMigrationPreview } from './lib/migration';
import { Observability } from './lib/observability';
import { checkReadOnlyInfluxQL, checkReadOnlySql, senderAllowed } from './lib/queryService';
import { redact, registerSecret } from './lib/redact';
import {
    type ClaimRequest,
    coerceValue,
    kindOfValueType,
    resolveTarget,
    roundTo,
    SchemaRegistry,
    type Target,
    toFieldValue,
} from './lib/schemaMapper';
import { Spool } from './lib/spool';
import {
    CardinalityGuard,
    type DynamicValue,
    checkTagAgainstCatalog,
    looksSensitive,
    type ResolvedTags,
    resolveDynamicTags,
    resolveTags,
    type TagContext,
} from './lib/tagResolver';
import { WriteQueue } from './lib/writeQueue';

/** One logged state */
interface Registered {
    id: string;
    storeId: string;
    cfg: StateConfig;
    key: string;
    target: Target;
    tags: ResolvedTags;
    /** final static tags incl. state_id (after schema/cardinality checks) */
    staticTags: Record<string, string>;
    metaFields: { ack: boolean; q: boolean; from: boolean };
    metaTags: { ack: boolean; q: boolean; from: boolean };
    conflict?: string;
    warnings: string[];
    pipeline: FilterPipeline;
    /** ts of the newest state change received through the subscription */
    lastSeenTs?: number;
}

interface ObjInfo {
    common: Record<string, unknown>;
    native: Record<string, unknown>;
    type: string;
}

const DYNAMIC_ALLOWED = /^[\p{L}\p{N} _.:/-]{1,128}$/u;

type Callback = ioBroker.MessageCallbackInfo | undefined;

export class InfluxV3Adapter extends utils.Adapter {
    private cfg!: InstanceConfig;
    private client?: InfluxV3Client;
    private queue?: WriteQueue;
    private schema!: SchemaRegistry;
    private cardinality!: CardinalityGuard;
    private dynamicGuard!: CardinalityGuard;
    private readonly stats = new Observability();
    private readonly registry = new Map<string, Registered>();
    private readonly storeIdToReal = new Map<string, string>();
    private readonly dynamicCache = new Map<string, DynamicValue>();
    private readonly dynamicSubs = new Map<string, Set<string>>();
    private readonly roomsByMember = new Map<string, string[]>();
    private readonly functionsByMember = new Map<string, string[]>();
    private readonly parentCache = new Map<string, { type: string; name: string } | null>();
    private readonly objectChains = new Map<string, Promise<void>>();
    private readonly warnedOnce = new Set<string>();
    private sysLanguage = 'en';
    private subscribedAll = false;
    private ready = false;
    private stopping = false;
    private pingTimer?: ioBroker.Interval;
    private statsTimer?: ioBroker.Interval;
    private enumReloadTimer?: ioBroker.Timeout;
    private influxConnected = false;

    public constructor(options: Partial<utils.AdapterOptions> = {}) {
        super({ ...options, name: 'influx-v3' });
        this.on('ready', this.onReady.bind(this));
        this.on('stateChange', this.onStateChange.bind(this));
        this.on('objectChange', this.onObjectChange.bind(this));
        this.on('message', this.onMessage.bind(this));
        this.on('unload', this.onUnload.bind(this));
    }

    // ------------------------------------------------------------------ lifecycle

    private async onReady(): Promise<void> {
        if (await this.migrateGlobalTagsToCatalog()) {
            return; // js-controller restarts the instance with the converted settings
        }
        const { config, warnings } = normalizeInstanceConfig(this.config as unknown as Record<string, unknown>);
        this.cfg = config;
        registerSecret(config.token);
        for (const w of warnings) {
            this.log.warn(`Configuration: ${w}`);
        }
        await this.setStateAsync('info.connection', false, true);

        this.schema = new SchemaRegistry(config.maxTables, config.maxColumns);
        this.cardinality = new CardinalityGuard(config.maxTagValuesPerKey, (key, limit) =>
            this.log.warn(
                `Tag "${key}" reached ${limit} distinct values - further values are written as "_other" (cardinality protection)`,
            ),
        );
        this.dynamicGuard = new CardinalityGuard(config.dynamicTagMaxValues, (key, limit) =>
            this.log.warn(
                `Dynamic tag "${key}" reached ${limit} distinct values - further values are written as "_other"`,
            ),
        );
        this.client = this.createClient(config);
        const spool = new Spool(path.join(utils.getAbsoluteInstanceDataDir(this), 'spool'), config.spoolFsync);
        this.queue = new WriteQueue(
            {
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
            },
            this.client,
            {
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
                log: (level, message) => this.log[level](redact(message)),
            },
            this.timers(),
            spool,
        );
        try {
            await this.queue.start();
        } catch (e) {
            this.log.error(`Disk buffer not usable: ${redact(e)}`);
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
        this.log.info(
            `InfluxDB 3 adapter ready: ${this.registry.size} state(s) logged into database "${config.database}" (${config.schemaMode} mode, ${config.bufferMode} buffer)`,
        );
    }

    /** Managed adapter timers (cleared by js-controller on unload) for the queue and the filters */
    private timers(): {
        now(): number;
        after(fn: () => void, ms: number): unknown;
        cancel(h: unknown): void;
        random(): number;
    } {
        return {
            now: () => Date.now(),
            after: (fn, ms) => this.setTimeout(fn, ms),
            cancel: h => this.clearTimeout(h as ioBroker.Timeout | undefined),
            random: () => Math.random(),
        };
    }

    /**
     * One-time conversion of the former "global tags" table into the tag catalog, saved in the
     * instance settings so that Admin shows it. Returns true when the settings were changed.
     */
    private async migrateGlobalTagsToCatalog(): Promise<boolean> {
        const native = this.config as unknown as Record<string, unknown>;
        const legacy = tableRows(native.globalTags) as Record<string, unknown>[];
        const catalog = tableRows(native.tagDefinitions);
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
            this.log.info(
                `${tagDefinitions.length} global tag(s) moved into the tag catalog - restarting with the new settings`,
            );
            return true;
        } catch (e) {
            this.log.warn(`Cannot save the tag catalog (${redact(e)}) - global tags are converted in memory only`);
            return false;
        }
    }

    private async onUnload(callback: () => void): Promise<void> {
        this.stopping = true;
        // js-controller must get the callback at the latest shortly before common.stopTimeout (10 s).
        // Adapter timers are refused while stopping, so the limit is an abortable promise delay.
        const abort = new AbortController();
        const work = this.shutdown().catch(e => this.log.error(`Error during shutdown: ${redact(e)}`));
        await Promise.race([work, delay(9000, undefined, { signal: abort.signal }).catch(() => undefined)]);
        abort.abort();
        callback();
    }

    private async shutdown(): Promise<void> {
        this.clearInterval(this.pingTimer);
        this.clearInterval(this.statsTimer);
        this.clearTimeout(this.enumReloadTimer);
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

    private createClient(config: InstanceConfig): InfluxV3Client {
        return new InfluxV3Client({
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

    private setConnected(ok: boolean, error?: string): void {
        if (error) {
            this.stats.error(redact(error));
        }
        this.stats.connected = ok;
        if (this.influxConnected !== ok) {
            this.influxConnected = ok;
            void this.setStateAsync('info.connection', ok, true);
            if (ok) {
                this.log.info('Connection to InfluxDB 3 established');
            } else if (error) {
                this.log.warn(`Connection to InfluxDB 3 lost: ${redact(error)}`);
            }
        }
    }

    private async connect(): Promise<void> {
        if (!this.client) {
            return;
        }
        const ping = await this.client.ping();
        if (!ping.ok) {
            this.setConnected(false, ping.error ?? `HTTP ${ping.status}`);
            this.log.warn(
                `InfluxDB 3 not reachable at ${this.cfg.url}: ${ping.error ?? ping.status} - data is buffered`,
            );
            return;
        }
        const edition = ping.edition ?? 'unknown';
        this.stats.serverEdition = edition;
        this.stats.serverVersion = ping.version ?? '';
        await this.setStateChangedAsync('info.serverVersion', `${edition} ${ping.version ?? ''}`.trim(), true);
        if (this.cfg.edition === 'auto' && edition === 'unknown') {
            this.log.warn('Could not detect the InfluxDB edition from /ping - assuming the Core/Enterprise API');
        } else if (
            this.cfg.edition !== 'auto' &&
            this.cfg.edition !== 'cloud' &&
            edition !== 'unknown' &&
            edition !== this.cfg.edition
        ) {
            this.log.warn(`Configured edition "${this.cfg.edition}" but the server reports "${edition}"`);
        }
        this.setConnected(true);
        await this.refreshSchema();
        if (this.cfg.edition !== 'cloud') {
            const retention = await this.client.getRetentionMs();
            if (retention === undefined) {
                this.log.info(`Database "${this.cfg.database}": retention infinite`);
            } else if (typeof retention === 'number') {
                this.log.info(`Database "${this.cfg.database}": retention ${Math.round(retention / 86400000)} days`);
            }
        }
    }

    private async refreshSchema(): Promise<void> {
        if (!this.client || this.cfg.edition === 'cloud') {
            return;
        }
        try {
            const columns = await this.client.getSchemaColumns();
            this.schema.seed(columns);
            this.stats.lastRead = Date.now();
        } catch (e) {
            this.log.warn(
                `Cannot read the database schema: ${redact(e)} - type checks use the adapter's own knowledge only`,
            );
        }
    }

    private async periodicPing(): Promise<void> {
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
        } else {
            this.setConnected(false, ping.error);
        }
    }

    private async publishStats(): Promise<void> {
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
        ]).catch(e => this.log.debug(`Cannot publish statistics: ${redact(e)}`));
        if (this.cfg.writeStatsToInflux && this.ready && !this.stopping) {
            try {
                const line = serialize({
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
            } catch {
                // statistics are best effort
            }
        }
    }

    // ------------------------------------------------------------------ objects & enums

    private async loadSystemLanguage(): Promise<void> {
        try {
            const sys = await this.getForeignObjectAsync('system.config');
            const lang = (sys?.common as { language?: string } | undefined)?.language;
            if (lang) {
                this.sysLanguage = lang;
            }
        } catch {
            // default "en"
        }
    }

    private nameOf(name: unknown, fallback: string): string {
        if (typeof name === 'string' && name) {
            return name;
        }
        if (name && typeof name === 'object') {
            const rec = name as Record<string, unknown>;
            const v = rec[this.sysLanguage] ?? rec.en ?? Object.values(rec)[0];
            if (typeof v === 'string' && v) {
                return v;
            }
        }
        return fallback;
    }

    private async loadEnums(): Promise<void> {
        this.roomsByMember.clear();
        this.functionsByMember.clear();
        for (const [prefix, target] of [
            ['enum.rooms.', this.roomsByMember],
            ['enum.functions.', this.functionsByMember],
        ] as const) {
            try {
                const objs = await this.getForeignObjectsAsync(`${prefix}*`, 'enum');
                for (const [id, obj] of Object.entries(objs)) {
                    const members = (obj?.common as { members?: unknown })?.members;
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
            } catch (e) {
                this.log.debug(`Cannot read ${prefix}*: ${redact(e)}`);
            }
        }
    }

    /** enum names for an id or its nearest parent that is a member */
    private enumFor(map: Map<string, string[]>, id: string): string[] {
        const parts = id.split('.');
        for (let i = parts.length; i > 0; i--) {
            const list = map.get(parts.slice(0, i).join('.'));
            if (list?.length) {
                return list;
            }
        }
        return [];
    }

    private needsParents(cfg: StateConfig): boolean {
        const templates = [...this.cfg.globalTags, ...cfg.customTags].map(t => t.value).join(' ');
        return (
            (cfg.useAutoTags && (this.cfg.autoTagDevice || this.cfg.autoTagChannel)) || templates.includes('{parent.')
        );
    }

    private async parentInfo(id: string): Promise<{ device?: string; channel?: string }> {
        const parts = id.split('.');
        const out: { device?: string; channel?: string } = {};
        for (let i = parts.length - 1; i >= 2; i--) {
            const pid = parts.slice(0, i).join('.');
            let info = this.parentCache.get(pid);
            if (info === undefined) {
                try {
                    const obj = await this.getForeignObjectAsync(pid);
                    info = obj ? { type: obj.type, name: this.nameOf(obj.common?.name, parts[i - 1]) } : null;
                } catch {
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

    private async loadCustomConfigs(): Promise<void> {
        let rows: { id: string; value: Record<string, unknown> }[] = [];
        try {
            const view = await this.getObjectViewAsync('system', 'custom', {});
            rows = view?.rows ?? [];
        } catch (e) {
            this.log.error(`Cannot read custom settings: ${redact(e)}`);
        }
        const ids = rows
            .filter(r => {
                const c = r.value?.[this.namespace] as Record<string, unknown> | undefined;
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

    private customOf(obj: ioBroker.Object | null | undefined): Record<string, unknown> | undefined {
        const custom = obj?.common?.custom as Record<string, unknown> | undefined;
        const c = custom?.[this.namespace];
        return c && typeof c === 'object' ? (c as Record<string, unknown>) : undefined;
    }

    private onObjectChange(id: string, obj: ioBroker.Object | null | undefined): void {
        if (!this.ready || this.stopping) {
            return;
        }
        if (id.startsWith('enum.rooms.') || id.startsWith('enum.functions.')) {
            this.scheduleEnumReload();
            return;
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
                } else if (this.registry.has(id)) {
                    this.unregister(id, 'disabled');
                    await this.updateSubscriptions();
                }
            })
            .catch(e => this.log.error(`Cannot apply configuration of ${id}: ${redact(e)}`))
            .finally(() => {
                if (this.objectChains.get(id) === next) {
                    this.objectChains.delete(id);
                }
            });
        this.objectChains.set(id, next);
    }

    private scheduleEnumReload(): void {
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
            })().catch(e => this.log.warn(`Cannot apply enum change: ${redact(e)}`));
        }, 2000);
    }

    private async buildContext(id: string, storeId: string, obj: ObjInfo, cfg: StateConfig): Promise<TagContext> {
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

    private reservedKeys(field: string): Set<string> {
        return new Set(['time', 'state_id', field, 'ack', 'q', 'from'].map(k => k.toLowerCase()));
    }

    /**
     * Computes the complete mapping of a state (shared by registration and preview).
     *
     * @param id state id
     * @param obj object
     * @param customRaw custom config of this instance
     */
    private async computeMapping(
        id: string,
        obj: ioBroker.Object,
        customRaw: Record<string, unknown>,
    ): Promise<{
        cfg: StateConfig;
        storeId: string;
        target: Target;
        tags: ResolvedTags;
        claim: ClaimRequest;
        metaFields: Registered['metaFields'];
        metaTags: Registered['metaTags'];
        warnings: string[];
        key: string;
    }> {
        const { config: cfg, warnings } = normalizeStateConfig(customRaw, this.cfg, id);
        const info: ObjInfo = {
            common: obj.common ?? {},
            native: (obj.native ?? {}) as Record<string, unknown>,
            type: obj.type,
        };
        if (obj.type !== 'state') {
            warnings.push(`${id} is a ${obj.type}, not a state - nothing will be logged`);
        }
        const storeId = cfg.aliasId || id;
        const target = resolveTarget(this.cfg, cfg, id, storeId, info.common);
        const ctx = await this.buildContext(id, storeId, info, cfg);
        const tags = resolveTags(ctx, {
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
        const extraFields: ClaimRequest['extraFields'] = {};
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
            ...(['ack', 'q', 'from'] as const).filter(k => metaTags[k]),
        ];
        const claim: ClaimRequest = {
            stateId: id,
            table: target.table,
            field: target.field,
            fieldKind: kindOfValueType(target.valueType),
            extraFields,
            tagKeys,
        };
        const key = JSON.stringify([stateConfigKey(cfg), target, tags.tags, tags.dynamic, metaFields, metaTags]);
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
    private async registerState(id: string, obj: ioBroker.Object, live: boolean, force = false): Promise<void> {
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

        let conflict: string | undefined;
        const staticTags: Record<string, string> = {};
        const result = this.schema.claim(m.claim);
        if (!result.ok) {
            conflict = result.reason;
            this.stats.counters.conflicts++;
            this.log.warn(`${id}: not logged - ${result.reason}`);
        } else {
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
        const pipeline =
            existing?.pipeline ??
            new FilterPipeline(
                m.cfg,
                (s, reason) => this.write(id, s, reason),
                `system.adapter.${this.namespace}`,
                this.timers(),
            );
        if (existing) {
            pipeline.update(m.cfg);
        }
        const reg: Registered = {
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
            this.log.info(
                `${existing ? 'updated' : 'enabled'} logging of ${id} → table "${m.target.table}" (${m.target.valueType}${conflict ? ', CONFLICT' : ''}) tags: ${tagText}`,
            );
        }
    }

    private unregister(id: string, reason: string): void {
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

    private warnOnce(key: string, text: string): void {
        if (this.warnedOnce.has(key)) {
            return;
        }
        this.warnedOnce.add(key);
        this.log.warn(text);
    }

    private updateDynamicSources(id: string, tags: ResolvedTags | undefined): void {
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

    private async primeDynamic(src: string): Promise<void> {
        try {
            if (!this.subscribedAll) {
                await this.subscribeForeignStatesAsync(src);
            }
            const st = await this.getForeignStateAsync(src);
            if (st && st.val !== null && st.val !== undefined) {
                this.dynamicCache.set(src, { value: String(st.val), ts: st.ts });
            }
        } catch (e) {
            this.log.debug(`Dynamic tag source ${src}: ${redact(e)}`);
        }
    }

    private async updateSubscriptions(): Promise<void> {
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

    private readonly subscribedIds = new Set<string>();

    /**
     * Reads the current value of a newly registered state.
     *
     * @param reg registration
     * @param enabledAt when logging was enabled while running: a value changed after that moment may have been
     *     set before the subscription was active, so it is logged like a state change instead of becoming the
     *     baseline of the filters
     */
    private async initialValue(reg: Registered, enabledAt?: number): Promise<void> {
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
        } catch (e) {
            this.log.debug(`Cannot read initial value of ${reg.id}: ${redact(e)}`);
        }
    }

    // ------------------------------------------------------------------ state changes → records

    private onStateChange(id: string, state: ioBroker.State | null | undefined): void {
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

    private prepare(reg: Registered, state: ioBroker.State): PreparedState | undefined {
        const res = coerceValue(state.val, reg.target.valueType, {
            maxStringLength: this.cfg.maxStringLength,
            objectPolicy: this.cfg.objectPolicy,
        });
        if (!res.ok) {
            if (res.reason === 'null') {
                this.stats.counters.filtered++;
            } else {
                this.stats.counters.conversionErrors++;
                this.warnOnce(
                    `conv:${reg.id}:${reg.target.valueType}`,
                    `${reg.id}: value not stored (${res.reason}) - further conversion errors of this state are only counted`,
                );
            }
            return undefined;
        }
        let val = res.value;
        if (typeof val === 'number') {
            val = roundTo(val, reg.cfg.round);
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
    private toRecord(reg: Registered, s: PreparedState): LpRecord {
        const fields: Record<string, FieldValue> = { [reg.target.field]: toFieldValue(s.val, reg.target.valueType) };
        if (reg.metaFields.ack) {
            fields.ack = { type: 'boolean', value: s.ack };
        }
        if (reg.metaFields.q) {
            fields.q = { type: 'integer', value: s.q };
        }
        if (reg.metaFields.from && s.from) {
            fields.from = { type: 'string', value: s.from };
        }
        const tags: Record<string, string> = { ...reg.staticTags };
        if (reg.tags.dynamic.length) {
            const dyn = resolveDynamicTags(
                reg.tags.dynamic,
                src => this.dynamicCache.get(src),
                Date.now(),
                this.cfg.dynamicTagTtlSec,
                DYNAMIC_ALLOWED,
                this.cfg.maxTagValueLength,
            );
            for (const [k, v] of Object.entries(dyn)) {
                if (this.cfg.blockSensitiveTags && !reg.cfg.allowSensitiveTags && looksSensitive(k, v)) {
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

    private write(id: string, s: PreparedState, reason: EmitReason): void {
        const reg = this.registry.get(id);
        if (!reg || reg.conflict || !this.queue) {
            return;
        }
        try {
            const line = serialize(this.toRecord(reg, s));
            if (this.queue.push(line)) {
                this.stats.counters.queued++;
            }
            if (reg.cfg.enableDebugLogs) {
                this.log.debug(`${id}: queued (${reason}) ${line}`);
            }
        } catch (e) {
            this.stats.counters.rejected++;
            this.warnOnce(
                `lp:${id}`,
                `${id}: record cannot be serialised: ${e instanceof LineProtocolError ? e.message : redact(e)}`,
            );
        }
    }

    private onRejected(items: { line: string; message: string }[]): void {
        this.stats.counters.rejected += items.length;
        for (const item of items) {
            if (!this.stats.mayLogReject(this.cfg.logRejectedLimitPerMin)) {
                this.log.debug(`Rejected by InfluxDB: ${redact(item.message)}`);
                continue;
            }
            const line = item.line.length > 300 ? `${item.line.slice(0, 300)}…` : item.line;
            this.log.warn(`InfluxDB rejected a record (${redact(item.message)}): ${redact(line)}`);
        }
        if (items.length) {
            this.stats.error(redact(items[items.length - 1].message));
            // the schema may have changed (e.g. a column type defined by another client) - refresh soon
            void this.refreshSchema();
        }
    }

    // ------------------------------------------------------------------ messages

    private reply(msg: ioBroker.Message, payload: unknown): void {
        if (msg.callback) {
            this.sendTo(msg.from, msg.command, payload, msg.callback as Callback);
        }
    }

    private async onMessage(msg: ioBroker.Message): Promise<void> {
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
                    return this.reply(
                        msg,
                        ok
                            ? { success: true }
                            : { error: 'buffer could not be flushed completely (server not reachable?)' },
                    );
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
                case 'getTagValues':
                    return this.reply(msg, this.tagValueOptions((msg.message as { key?: unknown } | undefined)?.key));
                case 'migrationPreview':
                    return await this.handleMigration(msg, false);
                case 'migrationApply':
                    return await this.handleMigration(msg, true);
                default:
                    this.log.debug(`Unknown command ${msg.command} from ${msg.from}`);
                    return this.reply(msg, { error: `Unknown command ${msg.command}` });
            }
        } catch (e) {
            this.log.error(`Cannot process ${msg.command}: ${redact(e)}`);
            this.reply(msg, { error: redact(e) });
        }
    }

    private historyTarget(reg: Registered): HistoryTarget {
        const columns = new Set<string>(['time', reg.target.field, 'state_id']);
        const placement = { ack: this.cfg.metaAck, q: this.cfg.metaQ, from: this.cfg.metaFrom };
        const meta: HistoryTarget['metaColumns'] = {};
        for (const k of ['ack', 'q', 'from'] as const) {
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

    private async handleGetHistory(msg: ioBroker.Message): Promise<void> {
        const started = Date.now();
        const message = (msg.message ?? {}) as { id?: string; options?: Record<string, unknown> };
        const { options, error } = normalizeHistoryOptions(message.options, {
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
        const built = buildHistoryQueries(target, options);
        try {
            const results = await Promise.all(
                built.queries.map(async (q: BuiltQuery) => ({
                    kind: q.kind,
                    rows: await this.client!.querySql(q.sql, q.params, {
                        timeoutMs: this.cfg.queryTimeoutMs,
                        maxBytes: this.cfg.queryMaxBytes,
                    }),
                })),
            );
            const result = mapHistoryRows(results, options, built.step, reg.storeId);
            this.stats.lastRead = Date.now();
            this.stats.counters.historyQueries++;
            if (reg.cfg.enableDebugLogs) {
                this.log.debug(
                    `getHistory ${reg.id}: ${result.length} values in ${Date.now() - started} ms (${built.aggregate})`,
                );
            }
            this.reply(msg, { result, step: built.step, error: null, sessionId });
        } catch (e) {
            if (e instanceof HttpError && /not found/i.test(e.message)) {
                // nothing written yet
                return this.reply(msg, { result: [], step: built.step, error: null, sessionId });
            }
            this.reply(msg, { result: [], step: 0, error: redact(e), sessionId });
        }
    }

    private handleStoreState(msg: ioBroker.Message): void {
        const m = msg.message as unknown;
        const items: { id: string; state: ioBroker.State }[] = [];
        const add = (id: unknown, state: unknown): void => {
            if (typeof id !== 'string') {
                return;
            }
            if (Array.isArray(state)) {
                state.forEach(s => add(id, s));
            } else if (state && typeof state === 'object' && 'val' in state) {
                items.push({ id, state: state as ioBroker.State });
            }
        };
        if (Array.isArray(m)) {
            m.forEach(e => add((e as { id?: unknown })?.id, (e as { state?: unknown })?.state));
        } else if (m && typeof m === 'object') {
            add((m as { id?: unknown }).id, (m as { state?: unknown }).state);
        }
        if (!items.length) {
            return this.reply(msg, { error: 'Invalid call: expected {id, state} or [{id, state}]' });
        }
        let stored = 0;
        const errors: string[] = [];
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

    private async writeCustomConfig(id: string, options: Record<string, unknown>): Promise<void> {
        const obj = await this.getForeignObjectAsync(id);
        if (!obj) {
            throw new Error(`object ${id} not found`);
        }
        obj.common ||= {} as ioBroker.StateCommon;
        const common = obj.common as { custom?: Record<string, unknown> };
        common.custom ||= {};
        const prev = (common.custom[this.namespace] as Record<string, unknown> | undefined) ?? {};
        common.custom[this.namespace] = { ...prev, ...options };
        await this.setForeignObjectAsync(id, obj);
    }

    private async handleEnableHistory(msg: ioBroker.Message): Promise<void> {
        const m = msg.message as { id?: unknown; options?: Record<string, unknown> } | undefined;
        if (!m || typeof m.id !== 'string' || !m.id) {
            return this.reply(msg, { error: 'Invalid call: id missing' });
        }
        const options = { ...(m.options ?? {}), enabled: true };
        // validate with the same rules the runtime uses
        const { config: stateCfg, warnings } = normalizeStateConfig(options, this.cfg, m.id);
        warnings.push(...this.catalogWarnings(stateCfg.customTags));
        await this.writeCustomConfig(m.id, options);
        this.log.info(
            `enableHistory ${m.id} by ${msg.from}${warnings.length ? ` (warnings: ${warnings.join('; ')})` : ''}`,
        );
        this.reply(msg, { success: true, warnings: warnings.length ? warnings : undefined });
    }

    private async handleDisableHistory(msg: ioBroker.Message): Promise<void> {
        const m = msg.message as { id?: unknown } | undefined;
        if (!m || typeof m.id !== 'string' || !m.id) {
            return this.reply(msg, { error: 'Invalid call: id missing' });
        }
        await this.writeCustomConfig(m.id, { enabled: false });
        this.log.info(`disableHistory ${m.id} by ${msg.from}`);
        this.reply(msg, { success: true });
    }

    private enabledDPs(): Record<string, StateConfig & { table: string; conflict?: string }> {
        const out: Record<string, StateConfig & { table: string; conflict?: string }> = {};
        for (const reg of this.registry.values()) {
            out[reg.id] = { ...reg.cfg, table: reg.target.table, conflict: reg.conflict };
        }
        return out;
    }

    private async handleQuery(msg: ioBroker.Message, lang: 'sql' | 'influxql'): Promise<void> {
        if (!this.cfg.allowUserQueries) {
            return this.reply(msg, { error: 'User queries are disabled in the instance settings' });
        }
        if (!senderAllowed(msg.from, this.cfg.querySenders)) {
            this.log.warn(`Query from ${msg.from} refused (not in the sender allow-list)`);
            return this.reply(msg, { error: `Sender ${msg.from} is not allowed to run queries` });
        }
        if (!this.client) {
            return this.reply(msg, { error: 'not connected' });
        }
        const m = msg.message as unknown;
        const text = typeof m === 'string' ? m : (m as { query?: unknown })?.query;
        const params = typeof m === 'object' && m ? (m as { params?: unknown }).params : undefined;
        const guard = lang === 'sql' ? checkReadOnlySql(text) : checkReadOnlyInfluxQL(text);
        if (!guard.ok) {
            return this.reply(msg, { error: guard.reason });
        }
        const cleanParams: Record<string, string | number | boolean> = {};
        if (params && typeof params === 'object') {
            for (const [k, v] of Object.entries(params as Record<string, unknown>)) {
                if (
                    /^[A-Za-z_][A-Za-z0-9_]*$/.test(k) &&
                    (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean')
                ) {
                    cleanParams[k] = v;
                }
            }
        }
        const opts = { timeoutMs: this.cfg.queryTimeoutMs, maxBytes: this.cfg.queryMaxBytes };
        try {
            const rows =
                lang === 'sql'
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
        } catch (e) {
            this.reply(msg, { result: [], error: redact(e) });
        }
    }

    private async handleGetSchema(msg: ioBroker.Message): Promise<void> {
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

    private adapterStats(): Record<string, unknown> {
        return {
            ...this.stats.snapshot(),
            queue: this.queue?.stats(),
            enabledStates: this.registry.size,
            subscribedAll: this.subscribedAll,
            tables: this.schema?.tableCount(),
            conflicts: [...this.registry.values()].filter(r => r.conflict).map(r => ({ id: r.id, reason: r.conflict })),
        };
    }

    private diagnosticsText(): string {
        const st = this.stats;
        const c = st.counters;
        const q = this.queue?.stats();
        const fmt = (ts: number): string => (ts > 0 ? new Date(ts).toISOString() : '-');
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

    private async handleTestConnection(msg: ioBroker.Message): Promise<void> {
        const form = ((msg.message as { config?: unknown })?.config ?? msg.message ?? {}) as Record<string, unknown>;
        const merged: Record<string, unknown> = { ...(this.config as unknown as Record<string, unknown>) };
        for (const k of ['url', 'database', 'edition', 'tlsRejectUnauthorized', 'tlsCa', 'requestTimeoutMs']) {
            if (form[k] !== undefined && form[k] !== '') {
                merged[k] = form[k];
            }
        }
        if (typeof form.token === 'string' && form.token.trim()) {
            merged.token = form.token.trim();
        }
        const { config } = normalizeInstanceConfig(merged);
        registerSecret(config.token);
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
                    lines.push(
                        dbs.includes(config.database)
                            ? `Database "${config.database}" exists`
                            : `Database "${config.database}" does not exist yet - it is created by the first write`,
                    );
                } catch (e) {
                    lines.push(`Database list not readable with this token (${redact(e)})`);
                }
                const retention = await client.getRetentionMs();
                if (retention !== null) {
                    lines.push(
                        `Retention: ${retention === undefined ? 'infinite' : `${Math.round(retention / 86400000)} days`}`,
                    );
                }
            }
            this.reply(msg, { result: lines.join('\n') });
        } finally {
            client.close();
        }
    }

    private async handleCreateDatabase(msg: ioBroker.Message): Promise<void> {
        const m = (msg.message ?? {}) as { database?: unknown; retention?: unknown };
        const name = typeof m.database === 'string' && m.database ? m.database : this.cfg.database;
        const retention = typeof m.retention === 'string' ? m.retention.trim() : this.cfg.createDatabaseRetention;
        if (!DATABASE_NAME_RE.test(name)) {
            return this.reply(msg, { error: `Invalid database name "${name}"` });
        }
        if (retention && !RETENTION_RE.test(retention)) {
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
            this.log.info(
                `Database "${name}" created on request of ${msg.from} (retention ${retention || 'infinite'})`,
            );
            this.reply(msg, { result: `Database "${name}" created (retention ${retention || 'infinite'})` });
        } catch (e) {
            this.reply(msg, { error: redact(e) });
        }
    }

    private async handlePreview(msg: ioBroker.Message): Promise<void> {
        const m = (msg.message ?? {}) as { id?: unknown; config?: unknown };
        if (typeof m.id !== 'string' || !m.id) {
            return this.reply(msg, { text: 'Save the object settings once to see the preview.' });
        }
        const obj = await this.getForeignObjectAsync(m.id);
        if (!obj) {
            return this.reply(msg, { text: `Object ${m.id} not found` });
        }
        const custom = { ...((m.config as Record<string, unknown>) ?? this.customOf(obj) ?? {}), enabled: true };
        const map = await this.computeMapping(m.id, obj, custom);
        const check = this.schema.check(map.claim);
        const st = await this.getForeignStateAsync(m.id).catch(() => null);
        const lines: string[] = [
            `Table: ${map.target.table} (${map.target.source})   Field: ${map.target.field} (${map.target.valueType})`,
        ];
        if (!check.ok) {
            lines.push(`⚠ CONFLICT: ${check.reason}`);
        }
        const staticTags: Record<string, string> = {};
        for (const [k, v] of Object.entries(map.tags.tags)) {
            if (check.ok && check.droppedTags.includes(k)) {
                continue;
            }
            staticTags[k] = v;
        }
        staticTags.state_id = map.storeId;
        const fake: Registered = {
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
            pipeline: undefined as unknown as FilterPipeline,
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
                lines.push(`Line protocol: ${serialize(this.toRecord(fake, prepared))}`);
            } catch (e) {
                lines.push(`Line protocol error: ${redact(e)}`);
            }
        } else {
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
    private tagKeyOptions(): { value: string; label: string }[] {
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
    private tagValueOptions(key: unknown): { value: string; label: string }[] {
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
    private catalogWarnings(tags: { key: string; value: string }[]): string[] {
        const out: string[] = [];
        for (const t of tags) {
            const check = checkTagAgainstCatalog(t, this.cfg.tagDefinitions, this.cfg.tagKeyStyle);
            if (!check.ok) {
                out.push(`tag "${t.key}": ${check.reason} - it will be ignored`);
            }
        }
        return out;
    }

    private async handleListInfluxdbInstances(msg: ioBroker.Message): Promise<void> {
        const objs = await this.getForeignObjectsAsync('system.adapter.influxdb.*', 'instance');
        const list = Object.keys(objs).map(id => id.replace('system.adapter.', ''));
        this.reply(msg, list.length ? list : [{ value: '', label: 'no influxdb instance found' }]);
    }

    private async handleMigration(msg: ioBroker.Message, apply: boolean): Promise<void> {
        const m = (msg.message ?? {}) as { source?: unknown; overwrite?: unknown; ids?: unknown };
        const source = typeof m.source === 'string' ? m.source : '';
        if (!/^influxdb\.\d+$/.test(source)) {
            return this.reply(msg, { error: 'Select an influxdb instance (e.g. influxdb.0) first' });
        }
        const view = await this.getObjectViewAsync('system', 'custom', {});
        const rows = (view?.rows ?? []) as unknown as { id: string; value: Record<string, unknown> }[];
        const preview = buildMigrationPreview(
            rows.map(r => ({ id: r.id, custom: r.value ?? {} })),
            source,
            this.namespace,
        );
        const overwrite = m.overwrite === true;
        const selected = Array.isArray(m.ids)
            ? new Set(m.ids.filter((x): x is string => typeof x === 'string'))
            : undefined;
        const todo = preview.filter(p => (!p.alreadyConfigured || overwrite) && (!selected || selected.has(p.id)));
        // custom tags of the classic adapter only work if their keys/values are in this instance's catalog
        const notInCatalog = new Set<string>();
        for (const item of todo) {
            const { config: stateCfg } = normalizeStateConfig(item.proposed, this.cfg, item.id);
            for (const t of stateCfg.customTags) {
                const check = checkTagAgainstCatalog(t, this.cfg.tagDefinitions, this.cfg.tagKeyStyle);
                if (!check.ok) {
                    notInCatalog.add(`${t.key}=${t.value}`);
                }
            }
        }
        if (!apply) {
            const text = [
                `${preview.length} state(s) are logged by ${source}.`,
                `${todo.length} would be configured for ${this.namespace} (${preview.length - todo.length} already configured, skipped).`,
                ...todo.slice(0, 30).map(p => `• ${p.id}`),
                todo.length > 30 ? `… and ${todo.length - 30} more` : '',
                notInCatalog.size
                    ? `Tags not in the tag catalog (ignored until you add their keys/values in the Tags tab): ${[...notInCatalog].slice(0, 20).join(', ')}`
                    : '',
                `Settings of ${source} are not changed. Historical data is not copied.`,
            ]
                .filter(Boolean)
                .join('\n');
            return this.reply(msg, { result: text, items: todo.map(p => p.id) });
        }
        let done = 0;
        const failed: string[] = [];
        for (const item of todo) {
            try {
                await this.writeCustomConfig(item.id, item.proposed);
                done++;
            } catch (e) {
                failed.push(`${item.id}: ${redact(e)}`);
            }
        }
        this.log.info(`Migration from ${source}: ${done} state(s) configured by request of ${msg.from}`);
        this.reply(msg, {
            result: `${done} state(s) configured for ${this.namespace}.${failed.length ? ` ${failed.length} failed.` : ''} ${source} was not changed.`,
            failed: failed.length ? failed : undefined,
        });
    }
}

if (require.main !== module) {
    // Export the constructor in compact mode
    module.exports = (options: Partial<utils.AdapterOptions> | undefined) => new InfluxV3Adapter(options);
} else {
    // otherwise start the instance directly
    (() => new InfluxV3Adapter())();
}
