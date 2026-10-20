"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DROP_KEY = void 0;
exports.ruleValues = ruleValues;
exports.parseTagRules = parseTagRules;
exports.findTagRule = findTagRule;
exports.canonicalTag = canonicalTag;
exports.applyTagRules = applyTagRules;
exports.describeOutcome = describeOutcome;
exports.ruleToRow = ruleToRow;
exports.foldText = foldText;
exports.foldValue = foldValue;
exports.buildOldTagInventory = buildOldTagInventory;
exports.describeInventory = describeInventory;
exports.suggestTagRules = suggestTagRules;
/**
 * Tag rules of the migration (ADR-0006): translate tags of the classic influxdb adapter into tags of the
 * tag catalog. Used by the settings migration and by the import of historical data.
 */
const config_1 = require("./config");
const tagResolver_1 = require("./tagResolver");
/** `toKey` that removes the tag */
exports.DROP_KEY = '-';
const str = (v) => (typeof v === 'string' ? v.trim() : typeof v === 'number' ? String(v) : '');
/**
 * Old values of a rule row: a list (multi-select) or one string (rules saved before lists existed).
 *
 * @param v cell value
 */
function ruleValues(v) {
    const list = Array.isArray(v) ? v : [v];
    const out = [];
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
function parseTagRules(raw, warnings = []) {
    const rules = [];
    (0, config_1.tableRows)(raw).forEach((row, i) => {
        if (!row || typeof row !== 'object') {
            return;
        }
        const r = row;
        const rule = {
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
function findTagRule(tag, rules) {
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
        }
        else if (any < 0) {
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
function canonicalTag(tag, catalog, style) {
    const key = (0, tagResolver_1.normalizeTagKey)(tag.key, style).toLowerCase();
    const def = catalog.find(d => (0, tagResolver_1.normalizeTagKey)(d.key, style).toLowerCase() === key);
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
function applyTagRules(tags, rules, catalog, style) {
    const out = [];
    const outcomes = [];
    const used = new Map();
    for (const raw of tags) {
        const from = { key: raw.key.trim(), value: String(raw.value ?? '').trim() };
        if (!from.key) {
            continue;
        }
        const idx = findTagRule(from, rules);
        const rule = idx >= 0 ? rules[idx] : undefined;
        if (rule?.toKey === exports.DROP_KEY) {
            outcomes.push({ status: 'removed', from, rule: idx + 1 });
            continue;
        }
        const renamed = rule ? { key: rule.toKey || from.key, value: rule.toValue || from.value } : from;
        const to = canonicalTag(renamed, catalog, style);
        const check = (0, tagResolver_1.checkTagAgainstCatalog)(to, catalog, style);
        if (!check.ok) {
            outcomes.push({ status: 'rejected', from, to, reason: check.reason, rule: rule ? idx + 1 : undefined });
            continue;
        }
        const norm = (0, tagResolver_1.normalizeTagKey)(to.key, style).toLowerCase();
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
        }
        else if (to.key !== from.key || to.value !== from.value) {
            outcomes.push({ status: 'adjusted', from, to });
        }
        else {
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
function describeOutcome(o) {
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
function ruleToRow(r) {
    return { fromKey: r.fromKey, fromValue: [...r.fromValues], toKey: r.toKey, toValue: r.toValue };
}
/**
 * Compares texts ignoring case, diacritics and punctuation (`Kotłownia` ≈ `KOTLOWNIA`). Only used for visible
 * suggestions, never for automatic mapping.
 *
 * @param s text
 */
function foldText(s) {
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
function foldValue(value) {
    const t = value.trim();
    return /^[+-]?\d+([.,]\d+)?$/.test(t) ? `#${t.replace(',', '.').replace(/^\+/, '')}` : foldText(t);
}
/**
 * Counts the old custom tags.
 *
 * @param tagsPerState custom tags of every state
 */
function buildOldTagInventory(tagsPerState) {
    const keys = new Map();
    for (const tags of tagsPerState) {
        const seen = new Set();
        for (const t of tags) {
            const key = t.key.trim();
            const value = String(t.value ?? '').trim();
            if (!key || !value) {
                continue;
            }
            const lk = key.toLowerCase();
            let e = keys.get(lk);
            if (!e) {
                e = { key, states: 0, values: new Map() };
                keys.set(lk, e);
            }
            if (!seen.has(lk)) {
                e.states++;
                seen.add(lk);
            }
            e.values.set(value, (e.values.get(value) ?? 0) + 1);
        }
    }
    return { keys };
}
/**
 * Inventory as text lines: `room: Kotłownia (5), Salon (3)`.
 *
 * @param inv inventory
 * @param maxValues values listed per key
 */
function describeInventory(inv, maxValues = 30) {
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
function suggestTagRules(inv, rules, catalog, style) {
    const out = [];
    const entries = [...inv.keys.values()].sort((a, b) => a.key.localeCompare(b.key));
    for (const e of entries) {
        const lk = e.key.toLowerCase();
        const values = [...e.values.keys()].sort((a, b) => a.localeCompare(b));
        const keyRule = rules.find(r => r.fromKey.toLowerCase() === lk && !r.fromValues.length);
        if (keyRule?.toKey === exports.DROP_KEY) {
            continue;
        }
        const target = keyRule?.toKey || e.key;
        const nk = (0, tagResolver_1.normalizeTagKey)(target, style).toLowerCase();
        const def = catalog.find(d => (0, tagResolver_1.normalizeTagKey)(d.key, style).toLowerCase() === nk);
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
        const groups = new Map();
        const single = [];
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
            }
            else {
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
//# sourceMappingURL=tagMapping.js.map