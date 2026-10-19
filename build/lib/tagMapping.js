"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DROP_KEY = void 0;
exports.parseTagRules = parseTagRules;
exports.findTagRule = findTagRule;
exports.canonicalTag = canonicalTag;
exports.applyTagRules = applyTagRules;
exports.describeOutcome = describeOutcome;
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
 * Parses the rule table of the instance settings (or of a sendTo message).
 *
 * @param raw rows `{fromKey, fromValue, toKey, toValue}`
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
            fromValue: str(r.fromValue),
            toKey: str(r.toKey),
            toValue: str(r.toValue),
        };
        if (!rule.fromKey) {
            if (rule.fromValue || rule.toKey || rule.toValue) {
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
 * Finds the rule for one old tag: a rule with matching key and value wins over a rule for every value,
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
        if (r.fromValue) {
            if (r.fromValue.toLowerCase() === value) {
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
//# sourceMappingURL=tagMapping.js.map