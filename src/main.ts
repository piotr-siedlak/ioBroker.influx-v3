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
    parseTagDefinitions,
    type StateConfig,
    stateConfigKey,
    DATABASE_NAME_RE,
    RETENTION_RE,
    tableRows,
} from './lib/config';
import {
    compileStateFilter,
    DataMigrationJob,
    type DataMigrationSettings,
    FileCheckpointStore,
    type ImportItem,
    type ImportTarget,
    type JobOutcome,
    type MeasurementOwner,
    jobKey,
    newCheckpoint,
    parseDataMigrationConfig,
    previewImport,
    type Progress as ImportProgress,
    validateSource,
} from './lib/dataMigration';
import { type EmitReason, FilterPipeline, type PreparedState } from './lib/filterPipeline';
import {
    buildHistoryQueries,
    type BuiltQuery,
    type HistoryTarget,
    mapHistoryRows,
    normalizeHistoryOptions,
    quoteIdent,
} from './lib/historyService';
import { HttpError, InfluxV3Client } from './lib/influxClient';
import { createLegacyReader, type LegacyPoint, type LegacySourceConfig, type LegacyTagUse } from './lib/legacySource';
import { type LpRecord, LineProtocolError, serialize, type FieldValue } from './lib/lineProtocol';
import {
    collectInstanceTags,
    collectOldTags,
    formatSettingsPreview,
    planSettingsMigration,
    preformattedHtml,
} from './lib/migration';
import {
    addToInventory,
    applyTagRules,
    describeUse,
    type InventoryKey,
    mergeInventories,
    type OldTagInventory,
    parseTagRules,
    ruleToRow,
    ruleValues,
    suggestTagRules,
    type TagRule,
} from './lib/tagMapping';
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
    normalizeTagKey,
    type ResolvedTags,
    resolveDynamicTags,
    resolveTags,
    type TagContext,
} from './lib/tagResolver';
import { SharedCache } from './lib/sharedCache';
import { proposeStandard, type ValueStyle } from './lib/tagStandard';
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

/** Complete mapping of a state (registration, preview, import) */
interface StateMapping {
    cfg: StateConfig;
    storeId: string;
    target: Target;
    tags: ResolvedTags;
    claim: ClaimRequest;
    metaFields: Registered['metaFields'];
    metaTags: Registered['metaTags'];
    warnings: string[];
    key: string;
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
    /** running import of historical data */
    private dmRun?: { ctrl: AbortController; done: Promise<JobOutcome> };
    private dmLastProgress = 0;
    /**
     * Used tags of the settings and of the old database. Every cell of the rule table asks for them (two requests per
     * row), so concurrent requests share one computation (ADR-0006, extension 5).
     */
    private readonly settingsTags = new SharedCache<OldTagInventory>(() => Date.now(), 30_000);
    private readonly pointTags = new SharedCache<LegacyTagUse[]>(() => Date.now(), 600_000, 60_000);

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
        for (const k of ['dmPassword', 'dmToken']) {
            const secret = (this.config as unknown as Record<string, unknown>)[k];
            if (typeof secret === 'string' && secret) {
                registerSecret(secret);
            }
        }
        for (const w of warnings) {
            this.log.warn(`Configuration: ${w}`);
        }
        await this.setStateAsync('info.connection', false, true);
        await this.setStateAsync('info.dataMigration.running', false, true);

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
        if (this.dmRun) {
            // the import saves its progress and continues with "Start" after the restart
            this.dmRun.ctrl.abort();
            await Promise.race([this.dmRun.done, delay(3000)]);
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
        if ((obj?.common as { custom?: unknown } | undefined)?.custom || this.registry.has(id)) {
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
    ): Promise<StateMapping> {
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
     * Registration that is not logged: mapping preview and import of states not configured for this instance.
     *
     * @param id state id
     * @param map mapping
     * @param droppedTags tags the schema check removed
     */
    private unloggedRegistration(id: string, map: StateMapping, droppedTags: readonly string[]): Registered {
        const staticTags: Record<string, string> = {};
        for (const [k, v] of Object.entries(map.tags.tags)) {
            if (!droppedTags.includes(k)) {
                staticTags[k] = v;
            }
        }
        staticTags.state_id = map.storeId;
        return {
            id,
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
    private toRecord(reg: Registered, s: PreparedState, withDynamicTags = true): LpRecord {
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
        if (withDynamicTags && reg.tags.dynamic.length) {
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
                    return this.reply(msg, this.tagValueOptions((msg.message as { key?: unknown } | undefined)?.key));
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
        const fake = this.unloggedRegistration(m.id, map, check.ok ? check.droppedTags : []);
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

    /**
     * Options of the "influxdb instance" dropdowns: `{value, label}` objects (Admin cannot draw plain strings), with the
     * database version and the number of states each instance logs.
     */
    private async handleListInfluxdbInstances(msg: ioBroker.Message): Promise<void> {
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
            const counts = new Map<string, number>();
            for (const r of (view?.rows ?? []) as unknown as { value: Record<string, unknown> }[]) {
                for (const id of ids) {
                    if ((r.value?.[id] as { enabled?: unknown } | undefined)?.enabled) {
                        counts.set(id, (counts.get(id) ?? 0) + 1);
                    }
                }
            }
            this.reply(
                msg,
                ids.map(id => {
                    const n = (objs[`system.adapter.${id}`]?.native ?? {}) as Record<string, unknown>;
                    const db = n.dbversion === '2.x' ? 'InfluxDB 2.x' : 'InfluxDB 1.x';
                    return { value: id, label: `${id} (${db}, ${counts.get(id) ?? 0} state(s))` };
                }),
            );
        } catch (e) {
            this.log.debug(`influxdb instances not listed: ${redact(e)}`);
            this.reply(msg, []); // a dropdown needs a list
        }
    }

    private async handleMigration(msg: ioBroker.Message, mode: 'preview' | 'html' | 'apply'): Promise<void> {
        const m = (msg.message ?? {}) as {
            source?: unknown;
            overwrite?: unknown;
            ids?: unknown;
            rules?: unknown;
            catalog?: unknown;
            standardizeExisting?: unknown;
        };
        const source = typeof m.source === 'string' ? m.source : '';
        if (!/^influxdb\.\d+$/.test(source)) {
            const text = 'Select an influxdb instance (e.g. influxdb.0) first';
            return this.reply(msg, mode === 'html' ? { text: preformattedHtml(text) } : { error: text });
        }
        const view = await this.getObjectViewAsync('system', 'custom', {});
        const rows = (view?.rows ?? []) as unknown as { id: string; value: Record<string, unknown> }[];
        // the Admin page sends the rules being edited (not saved yet); otherwise the saved ones are used
        const rules = parseTagRules(
            m.rules !== undefined ? m.rules : (this.config as unknown as Record<string, unknown>).migrationTagRules,
        );
        const plan = planSettingsMigration(
            rows.map(r => ({ id: r.id, custom: r.value ?? {} })),
            source,
            this.namespace,
            {
                rules,
                // previews show the catalog being edited; apply always uses the saved one (Admin locks it otherwise)
                catalog: mode === 'apply' ? this.cfg.tagDefinitions : this.catalogFrom(m.catalog),
                style: this.cfg.tagKeyStyle,
                overwrite: m.overwrite === true,
                standardizeExisting: m.standardizeExisting !== false,
                ids: Array.isArray(m.ids)
                    ? new Set(m.ids.filter((x): x is string => typeof x === 'string'))
                    : undefined,
            },
        );
        if (mode === 'html') {
            return this.reply(msg, { text: preformattedHtml(formatSettingsPreview(plan)) });
        }
        if (mode === 'preview') {
            return this.reply(msg, { result: formatSettingsPreview(plan, 30), items: plan.todo.map(p => p.id) });
        }
        let done = 0;
        let standardized = 0;
        const failed: string[] = [];
        for (const item of plan.todo) {
            try {
                await this.writeCustomConfig(item.id, item.config);
                done++;
            } catch (e) {
                failed.push(`${item.id}: ${redact(e)}`);
            }
        }
        for (const ex of plan.existing) {
            try {
                await this.writeCustomConfig(ex.id, { customTags: ex.customTags });
                standardized++;
            } catch (e) {
                failed.push(`${ex.id}: ${redact(e)}`);
            }
        }
        this.log.info(
            `Migration from ${source}: ${done} state(s) configured by request of ${msg.from}${
                plan.unmatched.size ? ` (${plan.unmatched.size} old tag value(s) did not fit the tag catalog)` : ''
            }`,
        );
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
    private async oldTags(
        sourceRaw: unknown,
        includePointsRaw?: unknown,
        waitForPointsMs = Number.POSITIVE_INFINITY,
    ): Promise<{ inventory: OldTagInventory; notes: string[] }> {
        const source = typeof sourceRaw === 'string' && /^influxdb\.\d+$/.test(sourceRaw) ? sourceRaw : '';
        const native = this.config as unknown as Record<string, unknown>;
        const includePoints =
            typeof includePointsRaw === 'boolean' ? includePointsRaw : native.migrationIncludeOldPoints !== false;
        const settings = await this.settingsTags.get(source, async () => {
            const view = await this.getObjectViewAsync('system', 'custom', {});
            const rows = ((view?.rows ?? []) as unknown as { id: string; value: Record<string, unknown> }[]).map(r => ({
                id: r.id,
                custom: r.value ?? {},
            }));
            return mergeInventories(collectOldTags(rows, source), collectInstanceTags(rows, this.namespace));
        });
        const notes: string[] = [];
        if (!includePoints) {
            return { inventory: settings, notes };
        }
        const { source: db } = this.dmConfig();
        if (validateSource(db).length) {
            return { inventory: settings, notes }; // no old database configured
        }
        const ident = JSON.stringify([db.version, db.url, db.database, db.retentionPolicy, db.org, db.bucket]);
        const reading = this.pointTags.get(ident, async () => {
            const reader = createLegacyReader(db);
            try {
                return await reader.tagInventory();
            } finally {
                reader.close();
            }
        });
        let uses: LegacyTagUse[] | undefined;
        if (waitForPointsMs === Number.POSITIVE_INFINITY) {
            try {
                uses = await reading;
            } catch (e) {
                notes.push(`Tags of the old database not read: ${e instanceof Error ? e.message : redact(e)}`);
            }
        } else {
            uses = (await this.within(reading, waitForPointsMs)) ?? this.pointTags.peek(ident);
            if (!uses) {
                notes.push('Tags of the old database are still being read');
            }
        }
        if (!uses) {
            return { inventory: settings, notes };
        }
        const points: OldTagInventory = { keys: new Map() };
        for (const u of uses) {
            addToInventory(points, u.key, u.value, u.measurements ?? 0, 'points');
        }
        return { inventory: mergeInventories(settings, points), notes };
    }

    /**
     * Result of a promise if it settles within the time, else undefined (the promise keeps running; a failure is
     * swallowed here - the cache remembers it).
     */
    private within<T>(promise: Promise<T>, ms: number): Promise<T | undefined> {
        return new Promise(resolve => {
            let settled = false;
            const timer: { handle?: ioBroker.Timeout } = {};
            const finish = (v: T | undefined): void => {
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
    private async enumNames(prefix: 'enum.rooms.' | 'enum.functions.'): Promise<string[]> {
        try {
            const objs = await this.getForeignObjectsAsync(`${prefix}*`, 'enum');
            const names = Object.entries(objs).map(([id, obj]) =>
                this.nameOf(obj?.common?.name, id.split('.').pop() ?? id),
            );
            return [...new Set(names.filter(Boolean))].sort((a, b) => a.localeCompare(b));
        } catch (e) {
            this.log.debug(`Cannot read ${prefix}*: ${redact(e)}`);
            return [];
        }
    }

    /**
     * "Propose standard": matches old keys/values automatically (folding, synonyms, ioBroker rooms/functions) and
     * writes proposed catalog rows and rule rows into the form (sendTo `useNative`) - nothing is saved.
     */
    private async handleProposeStandard(msg: ioBroker.Message): Promise<void> {
        const m = (msg.message ?? {}) as {
            source?: unknown;
            rules?: unknown;
            catalog?: unknown;
            valueStyle?: unknown;
            useEnums?: unknown;
            includeOldPoints?: unknown;
        };
        const native = this.config as unknown as Record<string, unknown>;
        const { inventory, notes } = await this.oldTags(m.source, m.includeOldPoints);
        if (!inventory.keys.size) {
            return this.reply(msg, { result: 'noOldTags', args: [] });
        }
        const valueStyle: ValueStyle =
            m.valueStyle === 'upper' || m.valueStyle === 'lower' || m.valueStyle === 'asUsed' ? m.valueStyle : 'auto';
        const useEnums = m.useEnums !== false;
        const p = proposeStandard(inventory, {
            catalogRows: m.catalog !== undefined ? m.catalog : native.tagDefinitions,
            ruleRows: m.rules !== undefined ? m.rules : native.migrationTagRules,
            keyStyle: this.cfg.tagKeyStyle,
            valueField: this.cfg.valueField,
            valueStyle,
            rooms: useEnums ? await this.enumNames('enum.rooms.') : [],
            functions: useEnums ? await this.enumNames('enum.functions.') : [],
        });
        const c = p.coverage;
        const coverage =
            c.uncovered.length === 0
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
        await this.setStateAsync('info.migrationProposal', preformattedHtml(details), true);
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
    private catalogFrom(raw: unknown): InstanceConfig['tagDefinitions'] {
        return raw === undefined
            ? this.cfg.tagDefinitions
            : parseTagDefinitions(raw, this.cfg.tagKeyStyle, this.cfg.valueField, []);
    }

    /** Options of the "old key" column: keys of the old custom tags */
    private async oldTagKeyOptions(message: unknown): Promise<{ value: string; label: string }[]> {
        const m = (message ?? {}) as { source?: unknown; includeOldPoints?: unknown };
        let inv: OldTagInventory;
        try {
            inv = (await this.oldTags(m.source, m.includeOldPoints, 2000)).inventory;
        } catch (e) {
            this.log.debug(`Old tag keys not available: ${redact(e)}`);
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
    private async oldTagValueOptions(message: unknown): Promise<{ value: string; label: string }[]> {
        const m = (message ?? {}) as {
            source?: unknown;
            key?: unknown;
            includeOldPoints?: unknown;
            selected?: unknown;
        };
        const key = typeof m.key === 'string' ? m.key.trim().toLowerCase() : '';
        let entry: InventoryKey | undefined;
        try {
            entry = key ? (await this.oldTags(m.source, m.includeOldPoints, 2000)).inventory.keys.get(key) : undefined;
        } catch (e) {
            this.log.debug(`Old tag values not available: ${redact(e)}`);
        }
        const options = entry
            ? [...entry.values.keys()]
                  .sort((a, b) => a.localeCompare(b))
                  .map(value => ({ value, label: `${value} (${describeUse(entry, value)})` }))
            : [];
        // values already selected in the row always get an option, so their chips have a label
        for (const v of ruleValues(m.selected)) {
            if (!options.some(o => o.value === v)) {
                options.push({ value: v, label: `${v} (not used any more)` });
            }
        }
        return options;
    }

    /** "Take over old tags": appends the rule rows that are still needed (sendTo `useNative`) */
    private async handleSuggestRules(msg: ioBroker.Message): Promise<void> {
        const m = (msg.message ?? {}) as {
            source?: unknown;
            rules?: unknown;
            catalog?: unknown;
            includeOldPoints?: unknown;
        };
        const rawRows = Array.isArray(m.rules)
            ? m.rules
            : tableRows((this.config as unknown as Record<string, unknown>).migrationTagRules);
        const rules = parseTagRules(rawRows);
        const { inventory } = await this.oldTags(m.source, m.includeOldPoints);
        const added = suggestTagRules(inventory, rules, this.catalogFrom(m.catalog), this.cfg.tagKeyStyle);
        this.reply(msg, {
            native: { migrationTagRules: [...rawRows, ...added.map(ruleToRow)] },
            result: added.length ? 'rulesAdded' : inventory.keys.size ? 'rulesNone' : 'noOldTags',
            args: [String(added.length)],
        });
    }
    // ------------------------------------------------------------------ import of historical data (ADR-0006)

    private dmFile(): string {
        return path.join(utils.getAbsoluteInstanceDataDir(this), 'data-migration.json');
    }

    private dmConfig(): {
        source: LegacySourceConfig;
        job: DataMigrationSettings;
        warnings: string[];
        rules: TagRule[];
    } {
        const warnings: string[] = [];
        const native = this.config as unknown as Record<string, unknown>;
        const { source, job } = parseDataMigrationConfig(native, warnings);
        const rules = parseTagRules(native.migrationTagRules, warnings);
        return { source, job, warnings, rules };
    }

    /** Copies the non-secret connection settings of an influxdb instance into the form (sendTo `useNative`). */
    private async handleDmFill(msg: ioBroker.Message): Promise<void> {
        const m = (msg.message ?? {}) as { instance?: unknown };
        const inst = typeof m.instance === 'string' ? m.instance : '';
        if (!/^influxdb\.\d+$/.test(inst)) {
            return this.reply(msg, { error: 'Select an influxdb instance first' });
        }
        const obj = await this.getForeignObjectAsync(`system.adapter.${inst}`);
        if (!obj) {
            return this.reply(msg, { error: `${inst} not found` });
        }
        const n = (obj.native ?? {}) as Record<string, unknown>;
        const v2 = n.dbversion === '2.x';
        const protocol = n.protocol === 'https' ? 'https' : 'http';
        const rawHost = typeof n.host === 'string' && n.host.trim() ? n.host.trim() : 'localhost';
        const host = rawHost.includes(':') && !rawHost.startsWith('[') ? `[${rawHost}]` : rawHost;
        const port = Number(n.port) > 0 ? Number(n.port) : 8086;
        const p = v2 && typeof n.path === 'string' ? n.path.replace(/^\/+|\/+$/g, '') : '';
        const native: Record<string, unknown> = {
            dmInfluxdbInstance: inst,
            dmVersion: v2 ? '2.x' : '1.x',
            dmUrl: `${protocol}://${host}:${port}${p ? `/${p}` : ''}`,
            dmRejectUnauthorized: n.validateSSL !== false,
        };
        if (v2) {
            native.dmOrg = typeof n.organization === 'string' ? n.organization : '';
            native.dmBucket = typeof n.dbname === 'string' ? n.dbname : '';
        } else {
            native.dmDatabase = typeof n.dbname === 'string' ? n.dbname : '';
            native.dmUser = typeof n.user === 'string' ? n.user : '';
        }
        this.reply(msg, {
            native,
            result: `Connection settings of ${inst} copied. Enter the ${v2 ? 'token' : 'password'} (it cannot be read from ${inst}) and save.`,
        });
    }

    private async handleDmTest(msg: ioBroker.Message): Promise<void> {
        const { source, warnings } = this.dmConfig();
        const errors = validateSource(source);
        if (errors.length) {
            return this.reply(msg, { error: errors.join('; ') });
        }
        const reader = createLegacyReader(source);
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
        } finally {
            reader.close();
        }
    }

    /** Source measurement of a state: alias of its influxdb settings, else its id (rule of the classic adapter). */
    private oldMeasurement(id: string, custom: Record<string, unknown> | undefined, instance: string): string {
        const blocks = instance
            ? [custom?.[instance]]
            : Object.entries(custom ?? {})
                  .filter(([k]) => /^influxdb\.\d+$/.test(k))
                  .map(([, v]) => v);
        for (const b of blocks) {
            const alias = (b as Record<string, unknown> | undefined)?.aliasId;
            if (typeof alias === 'string' && alias.trim()) {
                return alias.trim();
            }
        }
        return id;
    }

    /**
     * Old measurement → ioBroker state of the influxdb settings (alias, else the id), with the reason why a state is
     * not imported - for the explanations of the preview.
     *
     * @param job import settings
     * @param items selected states
     * @param skipped states left out by {@link dmItems}, with the reason
     */
    private async dmOwners(
        job: DataMigrationSettings,
        items: readonly ImportItem[],
        skipped: ReadonlyMap<string, string>,
    ): Promise<Map<string, MeasurementOwner>> {
        const filter = compileStateFilter(job.stateFilter);
        const selected = new Set(items.map(i => i.id));
        const view = await this.getObjectViewAsync('system', 'custom', {});
        const owners = new Map<string, MeasurementOwner>();
        for (const r of (view?.rows ?? []) as unknown as { id: string; value: Record<string, unknown> }[]) {
            const custom = r.value ?? {};
            for (const [instance, c] of Object.entries(custom)) {
                const cfg = c as { enabled?: unknown; aliasId?: unknown } | undefined;
                if (!/^influxdb\.\d+$/.test(instance) || !cfg?.enabled) {
                    continue;
                }
                let reason: string | undefined;
                if (!selected.has(r.id)) {
                    const here = !!(custom[this.namespace] as { enabled?: unknown } | undefined)?.enabled;
                    reason =
                        job.influxdbInstance && instance !== job.influxdbInstance
                            ? `logged by ${instance}, not by the selected ${job.influxdbInstance}`
                            : !filter(r.id)
                              ? 'excluded by the state filter'
                              : (skipped.get(r.id) ??
                                (here
                                    ? `configured for ${this.namespace} but not logged (see its log)`
                                    : 'not selected'));
                }
                const alias = typeof cfg.aliasId === 'string' ? cfg.aliasId.trim() : '';
                owners.set(alias || r.id, { id: r.id, instance, reason });
            }
        }
        return owners;
    }

    /**
     * States to import: those configured for this instance (with their live mapping) and those only the influxdb
     * adapter logs (with the mapping "Apply" of the settings migration would give them).
     *
     * @param job import settings
     * @param rules tag rules
     */
    private async dmItems(
        job: DataMigrationSettings,
        rules: readonly TagRule[],
    ): Promise<{ items: ImportItem[]; skipped: Map<string, string> }> {
        const filter = compileStateFilter(job.stateFilter);
        const view = await this.getObjectViewAsync('system', 'custom', {});
        const rows = ((view?.rows ?? []) as unknown as { id: string; value: Record<string, unknown> }[]).map(r => ({
            id: r.id,
            custom: r.value ?? {},
        }));
        const customs = new Map(rows.map(r => [r.id, r.custom]));
        const items: ImportItem[] = [];
        const skipped = new Map<string, string>();
        for (const reg of this.registry.values()) {
            if (!filter(reg.id)) {
                continue;
            }
            if (reg.conflict) {
                skipped.set(reg.id, `mapping conflict in ${this.namespace}: ${reg.conflict}`);
                continue;
            }
            items.push({
                id: reg.id,
                measurement: this.oldMeasurement(reg.id, customs.get(reg.id), job.influxdbInstance),
                table: reg.target.table,
                convert: points => this.convertLegacy(this.registry.get(reg.id), points, job.keepOldTags, rules),
            });
        }
        for (const p of await this.dmPlanned(rows, job, rules, filter, skipped)) {
            items.push({
                id: p.reg.id,
                measurement: this.oldMeasurement(p.reg.id, customs.get(p.reg.id), p.instance),
                table: p.reg.target.table,
                pending: true,
                convert: points => this.convertLegacy(p.reg, points, job.keepOldTags, rules),
            });
        }
        return { items: items.sort((a, b) => a.id.localeCompare(b.id)), skipped };
    }

    /**
     * States the influxdb adapter logs that are not configured for this instance: the mapping that "Apply" of the
     * settings migration would give them (same tag rules, saved catalog), so that their history can be imported
     * before - or without - the settings migration. Nothing is registered and no object is changed.
     *
     * @param rows objects with custom settings
     * @param job import settings
     * @param rules tag rules
     * @param filter state filter
     * @param skipped collects states that cannot be imported, with the reason
     */
    private async dmPlanned(
        rows: { id: string; custom: Record<string, unknown> }[],
        job: DataMigrationSettings,
        rules: readonly TagRule[],
        filter: (id: string) => boolean,
        skipped: Map<string, string>,
    ): Promise<{ reg: Registered; instance: string }[]> {
        const instances = job.influxdbInstance
            ? [job.influxdbInstance]
            : [...new Set(rows.flatMap(r => Object.keys(r.custom).filter(k => /^influxdb\.\d+$/.test(k))))].sort();
        const planned = new Map<string, { config: Record<string, unknown>; instance: string }>();
        for (const instance of instances) {
            const plan = planSettingsMigration(rows, instance, this.namespace, {
                rules,
                catalog: this.cfg.tagDefinitions,
                style: this.cfg.tagKeyStyle,
                overwrite: false,
            });
            for (const item of plan.todo) {
                if (!planned.has(item.id) && !this.registry.has(item.id) && filter(item.id)) {
                    planned.set(item.id, { config: item.config, instance });
                }
            }
        }
        const mappings: { id: string; map: StateMapping; instance: string }[] = [];
        for (const [id, p] of [...planned.entries()].sort(([a], [b]) => a.localeCompare(b))) {
            const obj = await this.getForeignObjectAsync(id).catch(() => null);
            if (obj?.type !== 'state') {
                skipped.set(id, obj ? `${obj.type}, not a state` : 'object not found');
                continue;
            }
            mappings.push({ id, map: await this.computeMapping(id, obj, p.config), instance: p.instance });
        }
        // all planned states claim their columns together (conflicts between them count too), then release them -
        // synchronously, so live logging never sees these claims
        const results = mappings.map(m => this.schema.claim(m.map.claim));
        for (const m of mappings) {
            this.schema.release(m.id);
        }
        const out: { reg: Registered; instance: string }[] = [];
        mappings.forEach((m, i) => {
            const r = results[i];
            if (r.ok) {
                out.push({ reg: this.unloggedRegistration(m.id, m.map, r.droppedTags), instance: m.instance });
            } else {
                skipped.set(m.id, `mapping conflict: ${r.reason}`);
            }
        });
        return out;
    }

    /**
     * Converts old points with the live mapping of the state (type, rounding, table, tags, metadata). Tags of the
     * old points pass through the tag rules and the catalog and are added where the configuration sets no value.
     */
    private convertLegacy(
        reg: Registered | undefined,
        points: LegacyPoint[],
        keepOldTags: boolean,
        rules: readonly TagRule[],
    ): { lines: string[]; skipped: number } {
        if (!reg || reg.conflict) {
            return { lines: [], skipped: points.length };
        }
        const lines: string[] = [];
        let skipped = 0;
        const tagCache = new Map<string, Record<string, string>>();
        for (const p of points) {
            const res = coerceValue(p.value, reg.target.valueType, {
                maxStringLength: this.cfg.maxStringLength,
                objectPolicy: this.cfg.objectPolicy,
            });
            if (!res.ok) {
                skipped++;
                continue;
            }
            const val = typeof res.value === 'number' ? roundTo(res.value, reg.cfg.round) : res.value;
            const rec = this.toRecord(
                reg,
                { val, ts: Math.round(p.ts), ack: p.ack ?? false, q: p.q ?? 0, from: p.from ?? '' },
                false, // past values of dynamic tags are unknown
            );
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
                lines.push(serialize(rec));
            } catch {
                skipped++;
            }
        }
        return { lines, skipped };
    }

    private mappedOldTags(
        tags: Record<string, string>,
        rules: readonly TagRule[],
        cache: Map<string, Record<string, string>>,
    ): Record<string, string> {
        const keys = Object.keys(tags).sort();
        if (!keys.length) {
            return {};
        }
        const cacheKey = JSON.stringify(keys.map(k => [k, tags[k]]));
        let out = cache.get(cacheKey);
        if (!out) {
            out = {};
            const mapped = applyTagRules(
                keys.map(k => ({ key: k, value: tags[k] })),
                rules,
                this.cfg.tagDefinitions,
                this.cfg.tagKeyStyle,
            );
            for (const t of mapped.tags) {
                const k = normalizeTagKey(t.key, this.cfg.tagKeyStyle);
                const v = t.value.slice(0, this.cfg.maxTagValueLength);
                if (k && v && !(this.cfg.blockSensitiveTags && looksSensitive(k, v))) {
                    out[k] = v;
                }
            }
            cache.set(cacheKey, out);
        }
        return out;
    }

    private dmTarget(): ImportTarget {
        return {
            firstTime: async (item, after) => {
                const reg = this.registry.get(item.id);
                if (!reg || !this.client) {
                    return undefined;
                }
                const since =
                    after !== undefined && Number.isFinite(after)
                        ? ` AND time >= to_timestamp_millis(${Math.floor(after)})`
                        : '';
                try {
                    const rows = await this.client.querySql<{ first?: unknown }>(
                        `SELECT CAST(MIN(time) AS BIGINT) / 1000000 AS first FROM ${quoteIdent(reg.target.table)} WHERE state_id = $sid${since}`,
                        { sid: reg.storeId },
                    );
                    const v = rows[0]?.first;
                    return v === null || v === undefined || !Number.isFinite(Number(v)) ? undefined : Number(v);
                } catch (e) {
                    if (e instanceof HttpError && /not found|does not exist/i.test(`${e.message} ${e.body}`)) {
                        return undefined; // table not created yet
                    }
                    throw e;
                }
            },
            write: lines =>
                this.client
                    ? this.client.write(lines)
                    : Promise.resolve({ kind: 'transient' as const, message: 'not connected' }),
            busy: () => {
                const st = this.queue?.stats();
                return !!st && (st.circuit === 'open' || st.queuedRecords > this.cfg.bufferMaxRecords / 2);
            },
        };
    }

    private dmSleep(ms: number, signal: AbortSignal): Promise<void> {
        return new Promise(resolve => {
            if (signal.aborted) {
                return resolve();
            }
            const timer: { handle?: ioBroker.Timeout } = {};
            const onAbort = (): void => {
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

    private dmProgress(p: ImportProgress): void {
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
        ]).catch(e => this.log.debug(`Cannot update import status: ${redact(e)}`));
    }

    private dmKey(source: LegacySourceConfig, job: DataMigrationSettings, rules: readonly TagRule[]): string {
        return jobKey(source, job, { rules, catalog: this.cfg.tagDefinitions, style: this.cfg.tagKeyStyle });
    }

    private async handleDmPreview(msg: ioBroker.Message): Promise<void> {
        const { source, job, warnings, rules } = this.dmConfig();
        const errors = validateSource(source);
        if (errors.length) {
            return this.reply(msg, { error: errors.join('; ') });
        }
        const { items, skipped } = await this.dmItems(job, rules);
        const reader = createLegacyReader(source);
        try {
            const check = await reader.check();
            if (!check.ok) {
                return this.reply(msg, { error: `Old database: ${check.error}` });
            }
            const cp = await new FileCheckpointStore(this.dmFile()).load();
            let text = await previewImport(
                items,
                { source: reader, target: this.dmTarget() },
                job,
                cp,
                this.dmKey(source, job, rules),
                { detailed: 15, samples: 2 },
                await this.dmOwners(job, items, skipped),
                this.namespace,
            );
            const retention = await this.client?.getRetentionMs().catch(() => undefined);
            if (retention) {
                text += `\nNote: the InfluxDB 3 database keeps data only ${Math.round(retention / 86_400_000)} day(s) - older imported points are removed by the server.`;
            }
            if (warnings.length) {
                text = `Warnings: ${warnings.join('; ')}\n\n${text}`;
            }
            const stamp = `Preview of ${new Date().toLocaleString()}\n\n`;
            await this.setStateAsync('info.dataMigration.preview', preformattedHtml(stamp + text), true);
            this.reply(msg, { result: 'Preview ready - see "Result of the preview" below.', text });
        } finally {
            reader.close();
        }
    }

    private async handleDmStart(msg: ioBroker.Message): Promise<void> {
        if (this.dmRun) {
            return this.reply(msg, { error: 'The import is already running' });
        }
        const { source, job, warnings, rules } = this.dmConfig();
        const errors = validateSource(source);
        if (errors.length) {
            return this.reply(msg, { error: errors.join('; ') });
        }
        const { items, skipped } = await this.dmItems(job, rules);
        if (!items.length) {
            return this.reply(msg, {
                error: `Nothing to import: neither ${this.namespace} nor the influxdb adapter logs a state that matches the selection${skipped.size ? ` (${skipped.size} state(s) cannot be imported - see "Preview")` : ''}.`,
            });
        }
        const key = this.dmKey(source, job, rules);
        const store = new FileCheckpointStore(this.dmFile());
        const cp = await store.load();
        if (cp && cp.jobKey === key && cp.finished) {
            return this.reply(msg, {
                error: 'An import with these settings is already finished. "Reset progress" imports again.',
            });
        }
        const reader = createLegacyReader(source);
        const check = await reader.check();
        if (!check.ok) {
            reader.close();
            return this.reply(msg, { error: `Old database: ${check.error}` });
        }
        const ctrl = new AbortController();
        const runner = new DataMigrationJob(
            {
                source: reader,
                target: this.dmTarget(),
                store,
                now: () => Date.now(),
                sleep: (ms, signal) => this.dmSleep(ms, signal),
                log: {
                    info: m => this.log.info(redact(m)),
                    warn: m => this.log.warn(redact(m)),
                    debug: m => this.log.debug(redact(m)),
                },
                onProgress: p => this.dmProgress(p),
            },
            job,
        );
        const done = runner
            .run(items, key, ctrl.signal)
            .catch((e: unknown): JobOutcome => {
                this.log.error(`Data import: ${redact(e)}`);
                return 'failed';
            })
            .finally(() => {
                reader.close();
                this.dmRun = undefined;
                void this.refreshSchema();
            });
        this.dmRun = { ctrl, done };
        const pending = items.filter(i => i.pending).length;
        this.log.info(
            `Data import started by ${msg.from}: ${items.length} state(s)${pending ? ` (${pending} not configured for ${this.namespace} yet - mapping of the settings migration)` : ''} from InfluxDB ${source.version}${warnings.length ? ` (warnings: ${warnings.join('; ')})` : ''}`,
        );
        this.reply(msg, { result: `Import of ${items.length} state(s) started - see the progress below and the log.` });
    }

    private handleDmStop(msg: ioBroker.Message): void {
        if (!this.dmRun) {
            return this.reply(msg, { result: 'No import is running.' });
        }
        this.dmRun.ctrl.abort();
        this.log.info(`Data import stopped by ${msg.from}`);
        this.reply(msg, { result: 'Stopping - the progress is saved, "Start" continues.' });
    }

    private async handleDmReset(msg: ioBroker.Message): Promise<void> {
        if (this.dmRun) {
            return this.reply(msg, { error: 'Stop the import first' });
        }
        const store = new FileCheckpointStore(this.dmFile());
        // keeps the "first live value" times - otherwise imported points would be taken for live values
        await store.save(newCheckpoint('reset', Date.now(), await store.load()));
        this.dmProgress({ running: false, percent: 0, status: 'progress reset', points: 0, errors: 0, current: '' });
        this.log.info(`Data import progress reset by ${msg.from}`);
        this.reply(msg, {
            result: 'Progress reset - "Start" imports from the beginning. Points imported before stay in InfluxDB 3; writing them again overwrites them.',
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
