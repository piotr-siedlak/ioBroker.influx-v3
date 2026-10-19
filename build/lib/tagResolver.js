"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CardinalityGuard = void 0;
exports.checkTagAgainstCatalog = checkTagAgainstCatalog;
exports.normalizeTagKey = normalizeTagKey;
exports.looksSensitive = looksSensitive;
exports.sanitizeTagValue = sanitizeTagValue;
exports.evaluateTemplate = evaluateTemplate;
exports.resolveTags = resolveTags;
exports.resolveDynamicTags = resolveDynamicTags;
const lineProtocol_1 = require("./lineProtocol");
/**
 * Checks one per-state tag against the instance tag catalog.
 * Returns the catalog entry, or an error text when the key or the value is not allowed.
 *
 * @param tag per-state tag (key as entered, raw value incl. templates)
 * @param catalog tag catalog of the instance
 * @param style tag key style (keys are compared after normalisation)
 */
function checkTagAgainstCatalog(tag, catalog, style) {
    const key = normalizeTagKey(tag.key, style);
    const definition = catalog.find(d => normalizeTagKey(d.key, style) === key);
    if (!definition) {
        return {
            ok: false,
            reason: `key "${tag.key}" is not defined in the tag catalog of the instance (instance settings → Tags)`,
        };
    }
    const value = tag.value.trim();
    if (!definition.allowOther && !definition.values.includes(value)) {
        return {
            ok: false,
            reason: `value "${value}" is not allowed for "${definition.key}" (allowed: ${definition.values.join(', ') || '-'})`,
        };
    }
    return { ok: true, definition };
}
/**
 * Normalises a tag key according to the configured style.
 *
 * @param key raw key
 * @param style `snake` or `preserve`
 */
function normalizeTagKey(key, style) {
    const clean = (0, lineProtocol_1.sanitizeIdentifier)(key);
    if (style === 'preserve') {
        return clean;
    }
    return clean
        .normalize('NFKD')
        .replace(/[̀-ͯ]/g, '') // strip diacritics: "Kotłownia" keys become ascii-ish
        .replace(/ł/g, 'l')
        .replace(/Ł/g, 'L')
        .toLowerCase()
        .replace(/[^a-z0-9_]+/g, '_')
        .replace(/_+/g, '_')
        .replace(/^_+|_+$/g, '');
}
const SENSITIVE_KEY = /(pass(word)?|pwd|secret|token|api[_-]?key|auth|session|cookie|bearer|credential)/i;
const EMAIL = /[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/;
const TOKEN_PREFIX = /^(apiv3_|eyJ|ghp_|xox[bp]-|sk-)/;
function looksLikeRandomToken(value) {
    if (/^[a-f0-9]{40,}$/i.test(value)) {
        return true; // long hex secrets (sha1/sha256 style keys)
    }
    // base64/url-safe secrets mix lower case, upper case and digits; device serials usually do not
    return (value.length >= 32 &&
        /^[A-Za-z0-9+/=_-]+$/.test(value) &&
        /[a-z]/.test(value) &&
        /[A-Z]/.test(value) &&
        /\d/.test(value));
}
/**
 * True if a tag key or value looks like it carries secrets or personal data.
 *
 * @param key tag key
 * @param value tag value
 */
function looksSensitive(key, value) {
    return SENSITIVE_KEY.test(key) || EMAIL.test(value) || TOKEN_PREFIX.test(value) || looksLikeRandomToken(value);
}
/**
 * Sanitises a tag value: control chars → space, trimmed, trailing backslashes removed, truncated.
 *
 * @param value raw value
 * @param maxLen maximum length
 */
function sanitizeTagValue(value, maxLen) {
    const clean = (0, lineProtocol_1.sanitizeIdentifier)(value);
    return clean.length > maxLen ? (0, lineProtocol_1.sanitizeIdentifier)(clean.slice(0, maxLen)) : clean;
}
function nameToString(name, language) {
    if (typeof name === 'string') {
        return name;
    }
    if (name && typeof name === 'object') {
        const rec = name;
        const v = rec[language] ?? rec.en ?? Object.values(rec)[0];
        return typeof v === 'string' ? v : '';
    }
    return '';
}
const PLACEHOLDER = /\{([^{}]+)\}/g;
/**
 * Evaluates the static part of a template. Unknown placeholders resolve to '' and produce a warning.
 * `{state:<id>}` placeholders are left untouched (dynamic).
 *
 * @param template the template text
 * @param ctx state context
 * @param nativeAllowlist native keys allowed in `{native.x}`
 * @param warnings collector
 */
function evaluateTemplate(template, ctx, nativeAllowlist, warnings) {
    let variable = false;
    const dynamicSources = [];
    const text = template.replace(PLACEHOLDER, (match, exprRaw) => {
        const expr = exprRaw.trim();
        variable = true;
        if (expr.startsWith('state:')) {
            const src = expr.slice(6).trim();
            if (src) {
                dynamicSources.push(src);
            }
            return match; // resolved at write time
        }
        const segments = ctx.id.split('.');
        if (expr === 'id') {
            return ctx.id;
        }
        if (expr === 'storeId') {
            return ctx.storeId;
        }
        if (expr === 'id.adapter') {
            return segments[0] ?? '';
        }
        if (expr === 'id.instance') {
            return segments.length > 1 ? `${segments[0]}.${segments[1]}` : '';
        }
        const seg = /^id\.segment\[(-?\d+)\]$/.exec(expr);
        if (seg) {
            const idx = parseInt(seg[1], 10);
            const v = idx < 0 ? segments[segments.length + idx] : segments[idx];
            return v ?? '';
        }
        if (expr === 'common.unit' || expr === 'common.role' || expr === 'common.type') {
            const v = ctx.common[expr.slice(7)];
            return typeof v === 'string' ? v : '';
        }
        if (expr === 'common.name') {
            return nameToString(ctx.common.name, ctx.language);
        }
        if (expr.startsWith('native.')) {
            const key = expr.slice(7);
            if (!nativeAllowlist.includes(key)) {
                warnings.push(`template ${match}: native key "${key}" is not in the allow-list - empty value used`);
                return '';
            }
            const v = ctx.native[key];
            return typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean' ? String(v) : '';
        }
        if (expr === 'enum.room') {
            return ctx.rooms[0] ?? '';
        }
        if (expr === 'enum.function') {
            return ctx.functions[0] ?? '';
        }
        if (expr === 'parent.device') {
            return ctx.parentDevice ?? '';
        }
        if (expr === 'parent.channel') {
            return ctx.parentChannel ?? '';
        }
        warnings.push(`template ${match}: unknown placeholder - empty value used`);
        return '';
    });
    return { text, variable, dynamicSources };
}
/**
 * Computes the tag set of one state.
 *
 * @param ctx state context
 * @param settings instance + state settings
 */
function resolveTags(ctx, settings) {
    const inst = settings.instance;
    const st = settings.state;
    const warnings = [];
    const tags = {};
    const origin = {};
    const variable = new Set();
    const dynamic = [];
    const seenInLayer = new Map();
    const add = (layer, rawKey, rawValue, isVariable) => {
        const key = normalizeTagKey(rawKey, inst.tagKeyStyle);
        if (!key) {
            warnings.push(`tag "${rawKey}": key is empty after normalisation - ignored`);
            return;
        }
        if (settings.reserved.has(key.toLowerCase()) || key.startsWith('_') || key.startsWith('iox::')) {
            warnings.push(`tag "${rawKey}": key "${key}" is reserved - ignored`);
            return;
        }
        const value = sanitizeTagValue(rawValue, inst.maxTagValueLength);
        if (!value) {
            return; // empty values are not written
        }
        if (inst.blockSensitiveTags && !st.allowSensitiveTags && looksSensitive(key, value)) {
            warnings.push(`tag "${key}": looks like a secret or personal data - not written (enable "allow sensitive tags" to override)`);
            return;
        }
        const prev = seenInLayer.get(key);
        if (prev === layer) {
            warnings.push(`tag "${key}": defined twice in ${layer} tags (after normalisation) - first value kept`);
            return;
        }
        if (prev && rawKey !== key && tags[key] !== undefined) {
            warnings.push(`tag "${rawKey}" overrides "${key}" from ${prev} tags`);
        }
        seenInLayer.set(key, layer);
        tags[key] = value;
        origin[key] = layer;
        if (isVariable) {
            variable.add(key);
        }
        else {
            variable.delete(key);
        }
    };
    // global tags (constants or templates)
    if (st.useGlobalTags) {
        for (const t of inst.globalTags) {
            const ev = evaluateTemplate(t.value, ctx, inst.nativeAllowlist, warnings);
            if (ev.dynamicSources.length) {
                warnings.push(`global tag "${t.key}": {state:...} placeholders are only allowed in per-state tags - ignored`);
                continue;
            }
            add('global', t.key, ev.text, ev.variable);
        }
    }
    // automatic tags
    if (st.useAutoTags) {
        const segments = ctx.id.split('.');
        if (inst.autoTagRoom && ctx.rooms.length) {
            add('auto', 'room', ctx.rooms[0], false);
        }
        if (inst.autoTagFunction && ctx.functions.length) {
            add('auto', 'function', ctx.functions[0], false);
        }
        if (inst.autoTagAdapter && segments.length > 1) {
            add('auto', 'adapter', segments[0], false);
        }
        if (inst.autoTagInstance && segments.length > 1) {
            add('auto', 'instance', `${segments[0]}.${segments[1]}`, false);
        }
        if (inst.autoTagDevice && ctx.parentDevice) {
            add('auto', 'device', ctx.parentDevice, true);
        }
        if (inst.autoTagChannel && ctx.parentChannel) {
            add('auto', 'channel', ctx.parentChannel, true);
        }
        if (inst.autoTagUnit && typeof ctx.common.unit === 'string') {
            add('auto', 'unit', ctx.common.unit, false);
        }
        if (inst.autoTagRole && typeof ctx.common.role === 'string') {
            add('auto', 'role', ctx.common.role, false);
        }
        if (inst.autoTagName) {
            add('auto', 'name', nameToString(ctx.common.name, ctx.language), true);
        }
    }
    // per-state tags: only keys (and values) of the instance tag catalog
    for (const t of st.customTags) {
        const check = checkTagAgainstCatalog(t, inst.tagDefinitions, inst.tagKeyStyle);
        if (!check.ok) {
            warnings.push(`tag "${t.key}": ${check.reason} - ignored`);
            continue;
        }
        const ev = evaluateTemplate(t.value, ctx, inst.nativeAllowlist, warnings);
        if (ev.dynamicSources.length) {
            if (!inst.dynamicTagsEnabled) {
                warnings.push(`tag "${t.key}": dynamic tags are disabled in the instance settings - ignored`);
                continue;
            }
            const key = normalizeTagKey(t.key, inst.tagKeyStyle);
            if (!key || settings.reserved.has(key.toLowerCase())) {
                warnings.push(`tag "${t.key}": key is empty or reserved - ignored`);
                continue;
            }
            // a dynamic tag replaces a static one with the same key
            delete tags[key];
            delete origin[key];
            dynamic.push({ key, template: ev.text, sources: ev.dynamicSources });
            continue;
        }
        add('state', t.key, ev.text, ev.variable);
    }
    // explicit removal
    for (const ex of st.excludeTags) {
        const key = normalizeTagKey(ex, inst.tagKeyStyle);
        delete tags[key];
        delete origin[key];
        variable.delete(key);
        const i = dynamic.findIndex(d => d.key === key);
        if (i >= 0) {
            dynamic.splice(i, 1);
        }
    }
    return { tags, origin, variableKeys: [...variable].filter(k => k in tags), dynamic, warnings };
}
/**
 * Limits the number of distinct values per tag key across all series of an instance.
 */
class CardinalityGuard {
    maxPerKey;
    onWarn;
    values = new Map();
    warned = new Set();
    constructor(maxPerKey, onWarn = () => undefined) {
        this.maxPerKey = maxPerKey;
        this.onWarn = onWarn;
    }
    /**
     * Returns the value to write: the value itself, or `_other` when the key exceeded its limit.
     *
     * @param key tag key
     * @param value tag value
     * @param limit optional limit overriding the default
     */
    check(key, value, limit = this.maxPerKey) {
        let set = this.values.get(key);
        if (!set) {
            set = new Set();
            this.values.set(key, set);
        }
        if (set.has(value)) {
            return value;
        }
        if (set.size >= limit) {
            if (!this.warned.has(key)) {
                this.warned.add(key);
                this.onWarn(key, limit);
            }
            return '_other';
        }
        set.add(value);
        return value;
    }
    /** @param key tag key */
    count(key) {
        return this.values.get(key)?.size ?? 0;
    }
}
exports.CardinalityGuard = CardinalityGuard;
/**
 * Resolves dynamic tags at write time.
 *
 * @param dyn dynamic tag definitions
 * @param lookup returns the cached value of a source state
 * @param now current time (ms)
 * @param ttlSec maximum age of a source value, 0 = unlimited
 * @param allowed optional pattern a resolved value must match
 * @param maxLen maximum value length
 */
function resolveDynamicTags(dyn, lookup, now, ttlSec, allowed, maxLen) {
    const out = {};
    for (const d of dyn) {
        let missing = false;
        const text = d.template.replace(PLACEHOLDER, (match, exprRaw) => {
            const expr = exprRaw.trim();
            if (!expr.startsWith('state:')) {
                return match;
            }
            const v = lookup(expr.slice(6).trim());
            if (!v || (ttlSec > 0 && now - v.ts > ttlSec * 1000)) {
                missing = true;
                return '';
            }
            return v.value;
        });
        if (missing) {
            continue;
        }
        const value = sanitizeTagValue(text, maxLen);
        if (value && allowed.test(value)) {
            out[d.key] = value;
        }
    }
    return out;
}
//# sourceMappingURL=tagResolver.js.map