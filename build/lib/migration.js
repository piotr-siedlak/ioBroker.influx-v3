"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.mapInfluxdbConfig = mapInfluxdbConfig;
exports.buildMigrationPreview = buildMigrationPreview;
exports.standardizeTags = standardizeTags;
exports.collectInstanceTags = collectInstanceTags;
exports.collectOldTags = collectOldTags;
exports.planSettingsMigration = planSettingsMigration;
exports.formatSettingsPreview = formatSettingsPreview;
exports.preformattedHtml = preformattedHtml;
const tagMapping_1 = require("./tagMapping");
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
];
/**
 * Maps one classic custom config to this adapter's custom config.
 *
 * @param src `common.custom["influxdb.N"]`
 */
function mapInfluxdbConfig(src) {
    const out = { enabled: true };
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
            .filter((t) => !!t && typeof t === 'object' && typeof t.name === 'string')
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
function buildMigrationPreview(objects, sourceInstance, targetNamespace) {
    const items = [];
    for (const o of objects) {
        const src = o.custom[sourceInstance];
        if (!src || typeof src !== 'object' || !src.enabled) {
            continue;
        }
        const target = o.custom[targetNamespace];
        items.push({
            id: o.id,
            source: sourceInstance,
            alreadyConfigured: !!target && typeof target === 'object' && !!target.enabled,
            proposed: mapInfluxdbConfig(src),
        });
    }
    return items.sort((a, b) => a.id.localeCompare(b.id));
}
/**
 * Applies the rules to tags of this instance: a tag is replaced only when the rules translate it into an allowed
 * catalog tag; other tags (incl. templates) stay as they are. Duplicate keys after the translation: first wins.
 *
 * @param tags current custom tags
 * @param rules tag rules
 * @param catalog tag catalog
 * @param style key style
 */
function standardizeTags(tags, rules, catalog, style) {
    const out = [];
    const changes = [];
    const seen = new Set();
    for (const t of tags) {
        if (!t || typeof t.key !== 'string' || typeof t.value !== 'string') {
            continue;
        }
        let next = t;
        if (!/\{[^}]+\}/.test(t.value)) {
            const r = (0, tagMapping_1.applyTagRules)([t], rules, catalog, style);
            if (r.tags.length) {
                next = r.tags[0];
            }
        }
        const k = next.key.trim().toLowerCase();
        if (seen.has(k)) {
            changes.push(`${t.key}=${t.value} → removed (${next.key} is already set)`);
            continue;
        }
        seen.add(k);
        if (next.key !== t.key || next.value !== t.value) {
            changes.push(`${t.key}=${t.value} → ${next.key}=${next.value}`);
        }
        out.push(next);
    }
    return { tags: out, changes };
}
/**
 * Custom tags of the states configured for this instance (templates excluded) - also "used tags".
 *
 * @param objects rows of the `system/custom` view
 * @param namespace e.g. `influx-v3.0`
 */
function collectInstanceTags(objects, namespace) {
    const perState = [];
    for (const o of objects) {
        const c = o.custom?.[namespace];
        if (!c || !c.enabled || !Array.isArray(c.customTags)) {
            continue;
        }
        const tags = c.customTags.filter(t => t && typeof t.key === 'string' && typeof t.value === 'string' && !/\{[^}]+\}/.test(t.value));
        if (tags.length) {
            perState.push(tags);
        }
    }
    return (0, tagMapping_1.buildOldTagInventory)(perState, 'instance');
}
/**
 * Old custom tags of the states enabled in the given influxdb instance(s).
 *
 * @param objects rows of the `system/custom` view
 * @param source `influxdb.N`, or '' for all influxdb instances
 */
function collectOldTags(objects, source) {
    const perState = [];
    for (const o of objects) {
        const keys = source ? [source] : Object.keys(o.custom ?? {}).filter(k => /^influxdb\.\d+$/.test(k));
        const tags = [];
        for (const k of keys) {
            const src = o.custom?.[k];
            if (src && typeof src === 'object' && src.enabled) {
                tags.push(...(mapInfluxdbConfig(src).customTags ?? []));
            }
        }
        if (tags.length) {
            perState.push(tags);
        }
    }
    return (0, tagMapping_1.buildOldTagInventory)(perState);
}
/**
 * Plans the settings migration: which states are configured and with which tags.
 *
 * @param objects rows of the `system/custom` view
 * @param source e.g. `influxdb.0`
 * @param target e.g. `influx-v3.0`
 * @param opts rules, catalog, overwrite flag and optional selection of ids
 * @param opts.rules tag rules
 * @param opts.catalog tag catalog of the target instance
 * @param opts.style tag key style of the target instance
 * @param opts.overwrite also re-configure states that are already configured
 * @param opts.ids only these ids
 * @param opts.standardizeExisting also standardise the tags of states already configured for the target
 */
function planSettingsMigration(objects, source, target, opts) {
    const preview = buildMigrationPreview(objects, source, target);
    const todo = [];
    const unmatched = new Map();
    for (const item of preview) {
        if ((item.alreadyConfigured && !opts.overwrite) || (opts.ids && !opts.ids.has(item.id))) {
            continue;
        }
        const oldTags = item.proposed.customTags ?? [];
        const tagResult = (0, tagMapping_1.applyTagRules)(oldTags, opts.rules, opts.catalog, opts.style);
        for (const o of tagResult.outcomes) {
            if (o.status === 'rejected' || o.status === 'conflict') {
                const k = `${o.from.key}=${o.from.value}`;
                const e = unmatched.get(k);
                if (e) {
                    e.count++;
                }
                else {
                    unmatched.set(k, { count: 1, reason: o.reason });
                }
            }
        }
        const config = { ...item.proposed };
        if (oldTags.length) {
            config.customTags = tagResult.tags;
        }
        todo.push({ ...item, tagResult, config });
    }
    const existing = [];
    if (opts.standardizeExisting) {
        const todoIds = new Set(todo.map(t => t.id));
        for (const o of objects) {
            const c = o.custom?.[target];
            if (!c ||
                !c.enabled ||
                todoIds.has(o.id) ||
                (opts.ids && !opts.ids.has(o.id)) ||
                !Array.isArray(c.customTags)) {
                continue;
            }
            const r = standardizeTags(c.customTags, opts.rules, opts.catalog, opts.style);
            if (r.changes.length) {
                existing.push({ id: o.id, customTags: r.tags, changes: r.changes });
            }
        }
        existing.sort((a, b) => a.id.localeCompare(b.id));
    }
    return {
        existing,
        inventory: collectOldTags(objects, source),
        source,
        target,
        total: preview.length,
        todo,
        skipped: preview.filter(p => p.alreadyConfigured && !opts.overwrite).length,
        unmatched,
    };
}
/**
 * Text of the preview: per state the tag translation, then the old tags that still do not fit.
 *
 * @param plan migration plan
 * @param maxStates maximum number of listed states
 */
function formatSettingsPreview(plan, maxStates = 60) {
    const lines = [
        `${plan.total} state(s) are logged by ${plan.source}: ${plan.todo.length} will be configured for ${plan.target}${plan.skipped ? `, ${plan.skipped} already configured (skipped).` : '.'}`,
    ];
    const inventory = (0, tagMapping_1.describeInventory)(plan.inventory);
    if (inventory.length) {
        lines.push('', `Custom tags in the settings of ${plan.source} (value (states)):`, ...inventory.map(l => `    ${l}`));
    }
    const withTags = plan.todo.filter(p => p.tagResult.outcomes.length);
    const withoutTags = plan.todo.length - withTags.length;
    const listed = [...withTags, ...plan.todo.filter(p => !p.tagResult.outcomes.length)].slice(0, maxStates);
    if (listed.length) {
        lines.push('');
    }
    for (const item of listed) {
        if (!item.tagResult.outcomes.length) {
            lines.push(`• ${item.id}`);
            continue;
        }
        lines.push(`• ${item.id}`);
        for (const o of item.tagResult.outcomes) {
            lines.push(`    ${(0, tagMapping_1.describeOutcome)(o)}`);
        }
    }
    if (plan.todo.length > listed.length) {
        lines.push(`… and ${plan.todo.length - listed.length} more state(s)`);
    }
    if (withoutTags && listed.length < plan.todo.length) {
        lines.push(`(${withoutTags} state(s) without custom tags)`);
    }
    if (plan.unmatched.size) {
        lines.push('', 'Old tags that are NOT written - add a tag rule or extend the tag catalog (Tags tab):');
        const sorted = [...plan.unmatched.entries()].sort((a, b) => b[1].count - a[1].count);
        for (const [k, v] of sorted.slice(0, 40)) {
            lines.push(`    ${k} - ${v.count} state(s): ${v.reason}`);
        }
        if (sorted.length > 40) {
            lines.push(`    … and ${sorted.length - 40} more`);
        }
    }
    if (plan.existing.length) {
        lines.push('', `States already configured for ${plan.target} - tags standardised by "Apply" (${plan.existing.length}):`);
        for (const e of plan.existing.slice(0, 40)) {
            lines.push(`• ${e.id}`, ...e.changes.map(c => `    ${c}`));
        }
        if (plan.existing.length > 40) {
            lines.push(`… and ${plan.existing.length - 40} more state(s)`);
        }
    }
    lines.push('', `Settings of ${plan.source} are not changed. Historical data: tab "Migration of data".`);
    return lines.join('\n');
}
/**
 * Wraps plain text as HTML for a `textSendTo` control (`container: html`), everything escaped.
 *
 * @param text plain text
 * @param color optional text colour
 */
function preformattedHtml(text, color) {
    const esc = text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    const style = `white-space:pre-wrap;font-family:monospace;font-size:12px;max-height:480px;overflow:auto${color ? `;color:${color}` : ''}`;
    return `<div style="${style}">${esc}</div>`;
}
//# sourceMappingURL=migration.js.map