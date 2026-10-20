/**
 * Tag rules of the migration (ADR-0006): translate tags of the classic influxdb adapter into tags of the
 * tag catalog. Used by the settings migration and by the import of historical data.
 */
import { tableRows, type TagDefinition, type TagKeyStyle, type TagPair } from './config';
import { checkTagAgainstCatalog, normalizeTagKey } from './tagResolver';

/** One row of the rule table */
export interface TagRule {
    /** old key (compared case-insensitively) */
    fromKey: string;
    /** old values (case-insensitive); empty = every value */
    fromValues: string[];
    /** new key; empty = keep the key; {@link DROP_KEY} = remove the tag */
    toKey: string;
    /** new value; empty = keep the value */
    toValue: string;
}

/** `toKey` that removes the tag */
export const DROP_KEY = '-';

export type TagOutcome =
    /** unchanged and allowed by the catalog */
    | { status: 'kept'; from: TagPair; to: TagPair }
    /** changed by a rule (1-based rule number) */
    | { status: 'mapped'; from: TagPair; to: TagPair; rule: number }
    /** only the spelling was adapted to the catalog (key or value case) */
    | { status: 'adjusted'; from: TagPair; to: TagPair }
    /** removed by a rule */
    | { status: 'removed'; from: TagPair; rule: number }
    /** not allowed by the catalog - not written */
    | { status: 'rejected'; from: TagPair; to: TagPair; reason: string; rule?: number }
    /** another old tag already produced this key - not written */
    | { status: 'conflict'; from: TagPair; to: TagPair; reason: string };

export interface TagMappingResult {
    /** tags to write */
    tags: TagPair[];
    outcomes: TagOutcome[];
}

const str = (v: unknown): string => (typeof v === 'string' ? v.trim() : typeof v === 'number' ? String(v) : '');

/**
 * Old values of a rule row: a list (multi-select) or one string (rules saved before lists existed).
 *
 * @param v cell value
 */
export function ruleValues(v: unknown): string[] {
    const list = Array.isArray(v) ? v : [v];
    const out: string[] = [];
    for (const x of list) {
        const s = str(x);
        if (s && !out.some(o => o.toLowerCase() === s.toLowerCase())) {
            out.push(s);
        }
    }
    return out;
}

/**
 * Parses the rule table of the instance settings (or of a sendTo message).
 *
 * @param raw rows `{fromKey, fromValue, toKey, toValue}` - `fromValue` is a list of old values or one string
 * @param warnings collector
 */
export function parseTagRules(raw: unknown, warnings: string[] = []): TagRule[] {
    const rules: TagRule[] = [];
    tableRows(raw).forEach((row, i) => {
        if (!row || typeof row !== 'object') {
            return;
        }
        const r = row as Record<string, unknown>;
        const rule: TagRule = {
            fromKey: str(r.fromKey),
            fromValues: ruleValues(r.fromValue ?? r.fromValues),
            toKey: str(r.toKey),
            toValue: str(r.toValue),
        };
        if (!rule.fromKey) {
            if (rule.fromValues.length || rule.toKey || rule.toValue) {
                warnings.push(`migrationTagRules[${i}]: old key missing - rule ignored`);
            }
            return;
        }
        if (!rule.toKey && !rule.toValue) {
            warnings.push(`migrationTagRules[${i}]: rule for "${rule.fromKey}" changes nothing`);
        }
        rules.push(rule);
    });
    return rules;
}

/**
 * Finds the rule for one old tag: a rule whose value list contains the value wins over a rule for every value,
 * otherwise the first one in table order.
 *
 * @param tag old tag
 * @param rules rule table
 * @returns index of the rule or -1
 */
export function findTagRule(tag: TagPair, rules: readonly TagRule[]): number {
    const key = tag.key.trim().toLowerCase();
    const value = tag.value.trim().toLowerCase();
    let any = -1;
    for (let i = 0; i < rules.length; i++) {
        const r = rules[i];
        if (r.fromKey.toLowerCase() !== key) {
            continue;
        }
        if (r.fromValues.length) {
            if (r.fromValues.some(v => v.toLowerCase() === value)) {
                return i;
            }
        } else if (any < 0) {
            any = i;
        }
    }
    return any;
}

/**
 * Gives a tag the spelling of the catalog: catalog key spelling for an equal key (after normalisation,
 * ignoring case) and catalog value spelling for an allowed value equal ignoring case.
 *
 * @param tag tag
 * @param catalog tag catalog
 * @param style tag key style
 */
export function canonicalTag(tag: TagPair, catalog: readonly TagDefinition[], style: TagKeyStyle): TagPair {
    const key = normalizeTagKey(tag.key, style).toLowerCase();
    const def = catalog.find(d => normalizeTagKey(d.key, style).toLowerCase() === key);
    const value = tag.value.trim();
    if (!def) {
        return { key: tag.key.trim(), value };
    }
    const lower = value.toLowerCase();
    return { key: def.key, value: def.values.find(v => v.toLowerCase() === lower) ?? value };
}

/**
 * Applies the rules and the catalog to the tags of one state (or one old point).
 *
 * @param tags old tags
 * @param rules rule table
 * @param catalog tag catalog of the instance
 * @param style tag key style
 */
export function applyTagRules(
    tags: readonly TagPair[],
    rules: readonly TagRule[],
    catalog: readonly TagDefinition[],
    style: TagKeyStyle,
): TagMappingResult {
    const out: TagPair[] = [];
    const outcomes: TagOutcome[] = [];
    const used = new Map<string, TagPair>();
    for (const raw of tags) {
        const from = { key: raw.key.trim(), value: String(raw.value ?? '').trim() };
        if (!from.key) {
            continue;
        }
        const idx = findTagRule(from, rules);
        const rule = idx >= 0 ? rules[idx] : undefined;
        if (rule?.toKey === DROP_KEY) {
            outcomes.push({ status: 'removed', from, rule: idx + 1 });
            continue;
        }
        const renamed = rule ? { key: rule.toKey || from.key, value: rule.toValue || from.value } : from;
        const to = canonicalTag(renamed, catalog, style);
        const check = checkTagAgainstCatalog(to, catalog, style);
        if (!check.ok) {
            outcomes.push({ status: 'rejected', from, to, reason: check.reason, rule: rule ? idx + 1 : undefined });
            continue;
        }
        const norm = normalizeTagKey(to.key, style).toLowerCase();
        const first = used.get(norm);
        if (first) {
            outcomes.push({
                status: 'conflict',
                from,
                to,
                reason: `"${to.key}" is already set to "${first.value}" by another tag`,
            });
            continue;
        }
        used.set(norm, to);
        out.push(to);
        if (rule) {
            outcomes.push({ status: 'mapped', from, to, rule: idx + 1 });
        } else if (to.key !== from.key || to.value !== from.value) {
            outcomes.push({ status: 'adjusted', from, to });
        } else {
            outcomes.push({ status: 'kept', from, to });
        }
    }
    return { tags: out, outcomes };
}

/**
 * Short text of one outcome for previews, e.g. `room=Kotlownia → Location=KOTLOWNIA (rule 2)`.
 *
 * @param o outcome
 */
export function describeOutcome(o: TagOutcome): string {
    const f = `${o.from.key}=${o.from.value}`;
    switch (o.status) {
        case 'kept':
            return f;
        case 'mapped':
            return `${f} → ${o.to.key}=${o.to.value} (rule ${o.rule})`;
        case 'adjusted':
            return `${f} → ${o.to.key}=${o.to.value} (catalog spelling)`;
        case 'removed':
            return `${f} → removed (rule ${o.rule})`;
        case 'rejected':
            return `${f}${o.rule ? ` → ${o.to.key}=${o.to.value} (rule ${o.rule})` : ''} ✗ ${o.reason}`;
        case 'conflict':
            return `${f} → ${o.to.key}=${o.to.value} ✗ ${o.reason}`;
    }
}

/** Rule as a row of the Admin table (`fromValue` = list for the multi-select) */
export function ruleToRow(r: TagRule): { fromKey: string; fromValue: string[]; toKey: string; toValue: string } {
    return { fromKey: r.fromKey, fromValue: [...r.fromValues], toKey: r.toKey, toValue: r.toValue };
}

/**
 * Compares texts ignoring case, diacritics and punctuation (`Kotłownia` ≈ `KOTLOWNIA`). Only used for visible
 * suggestions, never for automatic mapping.
 *
 * @param s text
 */
export function foldText(s: string): string {
    return s
        .normalize('NFKD')
        .replace(/[\u0300-\u036f]/g, '')
        .replace(/ł/g, 'l')
        .replace(/Ł/g, 'L')
        .toLowerCase()
        .replace(/[^\p{L}\p{N}]+/gu, '');
}

/**
 * Comparison form of a tag value for suggestions: numbers exactly (sign and decimals matter: `-1` ≠ `1`,
 * `1,5` = `1.5`), other text with {@link foldText}.
 *
 * @param value tag value
 */
export function foldValue(value: string): string {
    const t = value.trim();
    return /^[+-]?\d+([.,]\d+)?$/.test(t) ? `#${t.replace(',', '.').replace(/^\+/, '')}` : foldText(t);
}

/** Where a used tag value was found */
export type TagOrigin = 'influxdb' | 'points' | 'instance';

/** One key of the inventory */
export interface InventoryKey {
    /** first spelling seen */
    key: string;
    /** states (settings) using the key */
    states: number;
    /** value → number of states using it in settings (0 if only found in old points) */
    values: Map<string, number>;
    /** value → number of old measurements containing it */
    points: Map<string, number>;
    /** value → sources it was found in */
    origins: Map<string, Set<TagOrigin>>;
}

/** Used tags: key (ignoring case) → spelling, values with counts and origins */
export interface OldTagInventory {
    keys: Map<string, InventoryKey>;
}

/**
 * Adds one used tag value.
 *
 * @param inv inventory
 * @param keyRaw tag key
 * @param valueRaw tag value
 * @param count states/measurements
 * @param origin source
 */
export function addToInventory(
    inv: OldTagInventory,
    keyRaw: string,
    valueRaw: unknown,
    count: number,
    origin: TagOrigin,
): void {
    const key = keyRaw.trim();
    const value =
        typeof valueRaw === 'string'
            ? valueRaw.trim()
            : typeof valueRaw === 'number' || typeof valueRaw === 'boolean'
              ? String(valueRaw)
              : '';
    if (!key || !value) {
        return;
    }
    const lk = key.toLowerCase();
    let e = inv.keys.get(lk);
    if (!e) {
        e = { key, states: 0, values: new Map(), points: new Map(), origins: new Map() };
        inv.keys.set(lk, e);
    }
    e.values.set(value, (e.values.get(value) ?? 0) + (origin === 'points' ? 0 : count));
    if (origin === 'points') {
        e.points.set(value, (e.points.get(value) ?? 0) + count);
    }
    const o = e.origins.get(value) ?? new Set<TagOrigin>();
    o.add(origin);
    e.origins.set(value, o);
}

/**
 * Counts the custom tags of states.
 *
 * @param tagsPerState custom tags of every state
 * @param origin source of the settings
 * @param into inventory to add to (new one if omitted)
 */
export function buildOldTagInventory(
    tagsPerState: readonly (readonly TagPair[])[],
    origin: TagOrigin = 'influxdb',
    into: OldTagInventory = { keys: new Map() },
): OldTagInventory {
    for (const tags of tagsPerState) {
        const seen = new Set<string>();
        for (const t of tags) {
            const key = t.key.trim();
            if (!key || !String(t.value ?? '').trim()) {
                continue;
            }
            addToInventory(into, key, t.value, 1, origin);
            const lk = key.toLowerCase();
            if (!seen.has(lk)) {
                into.keys.get(lk)!.states++;
                seen.add(lk);
            }
        }
    }
    return into;
}

/**
 * Merges inventories (counts are added, origins united).
 *
 * @param list inventories
 */
export function mergeInventories(...list: OldTagInventory[]): OldTagInventory {
    const out: OldTagInventory = { keys: new Map() };
    for (const inv of list) {
        for (const e of inv.keys.values()) {
            for (const [value, count] of e.values) {
                for (const origin of e.origins.get(value) ?? ['influxdb' as const]) {
                    addToInventory(out, e.key, value, 0, origin);
                }
                const target = out.keys.get(e.key.toLowerCase())!;
                target.values.set(value, (target.values.get(value) ?? 0) + count);
                target.points.set(value, (target.points.get(value) ?? 0) + (e.points.get(value) ?? 0));
                if (!target.points.get(value)) {
                    target.points.delete(value);
                }
            }
            const target = out.keys.get(e.key.toLowerCase());
            if (target) {
                target.states += e.states;
            }
        }
    }
    return out;
}

/**
 * How often a value is used (settings and old measurements) - for choosing the most used spelling.
 *
 * @param e inventory key
 * @param value value
 */
export function useCount(e: InventoryKey, value: string): number {
    return (e.values.get(value) ?? 0) + (e.points.get(value) ?? 0);
}

/**
 * Short text of the use of a value, e.g. `3 state(s)`, `3 state(s), 5 old measurement(s)`.
 *
 * @param e inventory key
 * @param value value
 */
export function describeUse(e: InventoryKey, value: string): string {
    const parts: string[] = [];
    const states = e.values.get(value) ?? 0;
    const points = e.points.get(value) ?? 0;
    if (states) {
        parts.push(`${states} state(s)`);
    }
    if (points) {
        parts.push(`${points} old measurement(s)`);
    } else if (e.origins.get(value)?.has('points')) {
        parts.push('old points'); // InfluxDB 2.x cannot count the measurements cheaply
    }
    return parts.join(', ') || 'used';
}

/**
 * Inventory as text lines: `room: Kotłownia (5), Salon (3)`.
 *
 * @param inv inventory
 * @param maxValues values listed per key
 */
export function describeInventory(inv: OldTagInventory, maxValues = 30): string[] {
    return [...inv.keys.values()]
        .sort((a, b) => a.key.localeCompare(b.key))
        .map(e => {
            const vals = [...e.values.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
            const text = vals
                .slice(0, maxValues)
                .map(([v, n]) => `${v} (${n})`)
                .join(', ');
            return `${e.key}: ${text}${vals.length > maxValues ? `, … ${vals.length - maxValues} more` : ''}`;
        });
}

/**
 * Rule rows that are still needed so that every old tag fits the catalog. Existing rules are never changed:
 *
 * - old key without catalog match and without rule: one row "key, every value" with an empty new key;
 * - values the catalog rejects for a known target key: grouped by a suggested catalog value (equal after
 *   {@link foldText}), several old values per row; values without suggestion: one row each, new value empty.
 *
 * @param inv old tags
 * @param rules existing rules
 * @param catalog tag catalog
 * @param style tag key style
 */
export function suggestTagRules(
    inv: OldTagInventory,
    rules: readonly TagRule[],
    catalog: readonly TagDefinition[],
    style: TagKeyStyle,
): TagRule[] {
    const out: TagRule[] = [];
    const entries = [...inv.keys.values()].sort((a, b) => a.key.localeCompare(b.key));
    for (const e of entries) {
        const lk = e.key.toLowerCase();
        const values = [...e.values.keys()].sort((a, b) => a.localeCompare(b));
        const keyRule = rules.find(r => r.fromKey.toLowerCase() === lk && !r.fromValues.length);
        if (keyRule?.toKey === DROP_KEY) {
            continue;
        }
        const target = keyRule?.toKey || e.key;
        const nk = normalizeTagKey(target, style).toLowerCase();
        const def = catalog.find(d => normalizeTagKey(d.key, style).toLowerCase() === nk);
        // values that already have a rule of their own are the user's decision
        const free = values.filter(v => {
            const idx = findTagRule({ key: e.key, value: v }, rules);
            return idx < 0 || !rules[idx].fromValues.length;
        });
        if (!def) {
            if (!keyRule && free.length) {
                out.push({ fromKey: e.key, fromValues: [], toKey: '', toValue: '' });
            }
            continue;
        }
        const groups = new Map<string, string[]>();
        const single: string[] = [];
        for (const v of free) {
            const res = applyTagRules([{ key: e.key, value: v }], rules, catalog, style);
            if (res.tags.length) {
                continue; // fits already
            }
            const fv = foldValue(v);
            const suggestion = fv ? def.values.find(a => foldValue(a) === fv) : undefined;
            if (suggestion) {
                const g = groups.get(suggestion) ?? [];
                g.push(v);
                groups.set(suggestion, g);
            } else {
                single.push(v);
            }
        }
        for (const [suggestion, vals] of groups) {
            out.push({ fromKey: e.key, fromValues: vals, toKey: def.key, toValue: suggestion });
        }
        for (const v of single) {
            out.push({ fromKey: e.key, fromValues: [v], toKey: def.key, toValue: '' });
        }
    }
    return out;
}
