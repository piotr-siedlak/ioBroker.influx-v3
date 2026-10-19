/**
 * Migration helper: proposes `influx-v3.N` per-state settings from existing `influxdb.N` settings.
 *
 * Read-only towards `influxdb.N`: its configuration is never changed or removed. Historical data
 * is NOT migrated (InfluxDB 1.x/2.x → 3 needs an export/import outside of ioBroker).
 */

export interface MigrationItem {
    id: string;
    source: string;
    alreadyConfigured: boolean;
    proposed: Record<string, unknown>;
}

const COPY_KEYS = [
    'changesOnly',
    'debounceTime',
    'blockTime',
    'changesRelogInterval',
    'changesMinDelta',
    'ignoreZero',
    'ignoreBelowNumber',
    'ignoreAboveNumber',
    'round',
    'aliasId',
    'enableDebugLogs',
] as const;

/**
 * Maps one classic custom config to this adapter's custom config.
 *
 * @param src `common.custom["influxdb.N"]`
 */
export function mapInfluxdbConfig(src: Record<string, unknown>): Record<string, unknown> {
    const out: Record<string, unknown> = { enabled: true };
    for (const k of COPY_KEYS) {
        const v = src[k];
        if (v !== undefined && v !== null && v !== '') {
            out[k] = v;
        }
    }
    // the classic adapter used "debounce" as old name of blockTime
    if (out.blockTime === undefined && typeof src.debounce === 'number' && src.debounce > 0) {
        out.blockTime = src.debounce;
    }
    if (typeof src.disableSkippedValueLogging === 'boolean') {
        out.logSkippedBeforeChange = !src.disableSkippedValueLogging;
    }
    if (typeof src.storageType === 'string' && src.storageType) {
        out.storeAs = src.storageType.toLowerCase();
    }
    if (Array.isArray(src.customTags)) {
        out.customTags = src.customTags
            .filter(
                (t): t is { name: string; value: string } =>
                    !!t && typeof t === 'object' && typeof (t as { name?: unknown }).name === 'string',
            )
            .map(t => ({ key: t.name, value: String(t.value ?? '') }))
            .filter(t => t.key.trim() && t.value.trim());
    }
    return out;
}

/**
 * Builds the migration preview from the objects with custom settings.
 *
 * @param objects rows of the `system/custom` view: id → custom block
 * @param sourceInstance e.g. `influxdb.0`
 * @param targetNamespace e.g. `influx-v3.0`
 */
export function buildMigrationPreview(
    objects: { id: string; custom: Record<string, unknown> }[],
    sourceInstance: string,
    targetNamespace: string,
): MigrationItem[] {
    const items: MigrationItem[] = [];
    for (const o of objects) {
        const src = o.custom[sourceInstance] as Record<string, unknown> | undefined;
        if (!src || typeof src !== 'object' || !src.enabled) {
            continue;
        }
        const target = o.custom[targetNamespace] as Record<string, unknown> | undefined;
        items.push({
            id: o.id,
            source: sourceInstance,
            alreadyConfigured: !!target && typeof target === 'object' && !!target.enabled,
            proposed: mapInfluxdbConfig(src),
        });
    }
    return items.sort((a, b) => a.id.localeCompare(b.id));
}
