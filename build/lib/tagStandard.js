"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.keyGroup = keyGroup;
exports.valueGroup = valueGroup;
exports.sameKey = sameKey;
exports.styleValue = styleValue;
exports.resolveValueStyle = resolveValueStyle;
exports.sortRuleRows = sortRuleRows;
exports.proposeStandard = proposeStandard;
/**
 * Automatic matching of the old custom tags and a proposed standard (ADR-0006, extension 2).
 *
 * From the tags found in the influxdb settings, the tag catalog and the tag rules being edited, `proposeStandard`
 * computes catalog rows (new keys, missing values) and the minimal rule rows that translate every old tag into the
 * standard. Similarity (folding, synonym dictionaries) is only used here, for a proposal the user reviews - the
 * runtime never maps by similarity.
 */
const config_1 = require("./config");
const tagMapping_1 = require("./tagMapping");
const tagResolver_1 = require("./tagResolver");
/** Synonyms of tag keys (EN, DE, PL, FR, IT, ES, PT); the first entry is the proposed key name */
const KEY_GROUPS = [
    [
        'location',
        'room',
        'raum',
        'zimmer',
        'ort',
        'standort',
        'lokalizacja',
        'pomieszczenie',
        'pokój',
        'miejsce',
        'place',
        'pièce',
        'stanza',
        'habitación',
        'lieu',
        'ubicación',
        'posizione',
        'localização',
    ],
    ['zone', 'zona', 'strefa', 'area', 'bereich', 'obszar'],
    [
        'floor',
        'etage',
        'stockwerk',
        'geschoss',
        'level',
        'piętro',
        'kondygnacja',
        'poziom',
        'storey',
        'story',
        'piano',
        'planta',
        'andar',
    ],
    ['building', 'gebäude', 'haus', 'house', 'budynek', 'dom', 'bâtiment', 'edificio'],
    ['device', 'gerät', 'urządzenie', 'appliance', 'equipment', 'apparat', 'appareil', 'dispositivo', 'aparato'],
    [
        'type',
        'typ',
        'art',
        'kind',
        'rodzaj',
        'sort',
        'category',
        'kategorie',
        'kategoria',
        'catégorie',
        'categoria',
        'tipo',
    ],
    [
        'function',
        'funktion',
        'funkcja',
        'gewerk',
        'trade',
        'purpose',
        'zweck',
        'przeznaczenie',
        'fonction',
        'funzione',
        'función',
    ],
    ['circuit', 'heating circuit', 'heizkreis', 'hk', 'obieg', 'obwód', 'loop', 'kreis', 'circuito'],
    ['source', 'quelle', 'źródło', 'origin', 'origine', 'fuente', 'fonte'],
];
/** Synonyms of common values (rooms and functions), multilingual; the first entry names the group */
const VALUE_GROUPS = [
    [
        'boiler room',
        'heating room',
        'heizungsraum',
        'heizraum',
        'heizungskeller',
        'kotłownia',
        'chaufferie',
        'locale caldaia',
        'sala de calderas',
    ],
    ['living room', 'lounge', 'wohnzimmer', 'salon', 'pokój dzienny', 'séjour', 'soggiorno', 'sala de estar', 'salone'],
    ['kitchen', 'küche', 'kueche', 'kuchnia', 'cuisine', 'cucina', 'cocina', 'cozinha'],
    ['bedroom', 'schlafzimmer', 'sypialnia', 'chambre', 'camera da letto', 'dormitorio', 'quarto'],
    [
        'bathroom',
        'bath',
        'bad',
        'badezimmer',
        'łazienka',
        'salle de bain',
        'salle de bains',
        'bagno',
        'baño',
        'casa de banho',
    ],
    ['toilet', 'wc', 'toaleta', 'gäste wc', 'toilettes'],
    [
        'hall',
        'hallway',
        'corridor',
        'flur',
        'diele',
        'korytarz',
        'przedpokój',
        'couloir',
        'entrée',
        'corridoio',
        'pasillo',
    ],
    ['garage', 'garaż', 'garaje', 'garagem'],
    ['basement', 'cellar', 'keller', 'piwnica', 'cave', 'cantina', 'sótano'],
    ['attic', 'loft', 'dachboden', 'speicher', 'strych', 'poddasze', 'grenier', 'soffitta', 'ático'],
    [
        'office',
        'study',
        'büro',
        'buero',
        'arbeitszimmer',
        'gabinet',
        'biuro',
        'bureau',
        'ufficio',
        'oficina',
        'despacho',
    ],
    ['dining room', 'esszimmer', 'jadalnia', 'salle à manger', 'sala da pranzo', 'comedor'],
    [
        "children's room",
        'kids room',
        'nursery',
        'kinderzimmer',
        'pokój dziecięcy',
        'pokój dziecka',
        "chambre d'enfant",
        'cameretta',
    ],
    ['guest room', 'gästezimmer', 'gaestezimmer', 'pokój gościnny', "chambre d'amis", 'camera degli ospiti'],
    ['laundry', 'laundry room', 'waschküche', 'waschraum', 'pralnia', 'buanderie', 'lavanderia'],
    ['utility room', 'hauswirtschaftsraum', 'hwr', 'pomieszczenie gospodarcze', 'cellier'],
    [
        'storage',
        'storeroom',
        'abstellraum',
        'abstellkammer',
        'lager',
        'schowek',
        'spiżarnia',
        'pantry',
        'speisekammer',
        'dispensa',
        'despensa',
    ],
    ['garden', 'yard', 'garten', 'ogród', 'jardin', 'giardino', 'jardim'],
    ['terrace', 'patio', 'terrasse', 'taras', 'terrazza', 'terraza'],
    ['balcony', 'balkon', 'balcon', 'balcone', 'balcón'],
    ['outside', 'outdoor', 'außen', 'aussen', 'draußen', 'zewnątrz', 'na zewnątrz', 'extérieur', 'esterno', 'exterior'],
    ['workshop', 'werkstatt', 'warsztat', 'atelier', 'taller'],
    ['staircase', 'stairs', 'treppenhaus', 'treppe', 'klatka schodowa', 'schody', 'escalier', 'escalera'],
    ['technical room', 'technikraum', 'pomieszczenie techniczne', 'local technique', 'locale tecnico'],
    ['heating', 'heizung', 'ogrzewanie', 'chauffage', 'riscaldamento', 'calefacción'],
    [
        'lighting',
        'light',
        'lights',
        'licht',
        'beleuchtung',
        'oświetlenie',
        'światło',
        'éclairage',
        'illuminazione',
        'iluminación',
    ],
    [
        'blinds',
        'shutter',
        'shutters',
        'rollladen',
        'rolladen',
        'jalousie',
        'rolety',
        'żaluzje',
        'volets',
        'tapparelle',
        'persianas',
    ],
    ['security', 'sicherheit', 'alarm', 'bezpieczeństwo', 'sécurité', 'sicurezza', 'seguridad'],
    ['energy', 'energie', 'energia', 'strom', 'power', 'prąd', 'electricity', 'elektryka'],
    ['ventilation', 'lüftung', 'wentylacja', 'aération', 'ventilazione', 'ventilación'],
    ['water', 'wasser', 'woda', 'eau', 'acqua', 'agua'],
    ['weather', 'wetter', 'pogoda', 'meteo'],
    ['climate', 'klima', 'klimatyzacja', 'air conditioning', 'climatisation', 'clima'],
];
function buildIndex(groups) {
    const index = new Map();
    for (const g of groups) {
        for (const word of g) {
            const f = (0, tagMapping_1.foldText)(word);
            if (f && !index.has(f)) {
                index.set(f, (0, tagMapping_1.foldText)(g[0]));
            }
        }
    }
    return index;
}
const KEY_INDEX = buildIndex(KEY_GROUPS);
const VALUE_INDEX = buildIndex(VALUE_GROUPS);
const KEY_CANONICAL = new Map(KEY_GROUPS.map(g => [(0, tagMapping_1.foldText)(g[0]), g[0]]));
/** Synonym group of a key (folded canonical name) or undefined */
function keyGroup(key) {
    const f = (0, tagMapping_1.foldText)(key);
    return KEY_INDEX.get(f) ?? (f.length > 3 && f.endsWith('s') ? KEY_INDEX.get(f.slice(0, -1)) : undefined);
}
/** Synonym group of a value (folded name of the group) or undefined */
function valueGroup(value) {
    return VALUE_INDEX.get((0, tagMapping_1.foldText)(value));
}
/** True when two keys are the same after folding, also singular/plural (`rooms` = `room`) */
function sameKey(a, b) {
    const fa = (0, tagMapping_1.foldText)(a);
    const fb = (0, tagMapping_1.foldText)(b);
    return !!fa && (fa === fb || fa === `${fb}s` || fb === `${fa}s`);
}
const NUMERIC = /^[+-]?\d+([.,]\d+)?$/;
/**
 * Spelling of a proposed value.
 *
 * @param value old spelling
 * @param style `upper` → `BOILER_ROOM`, `lower` → `boiler_room`, `asUsed` → unchanged
 */
function styleValue(value, style) {
    const trimmed = value.trim();
    if (NUMERIC.test(trimmed)) {
        return trimmed.replace(',', '.'); // the catalog separates values with commas
    }
    const v = trimmed.replace(/,/g, ' ').replace(/\s+/g, ' ');
    if (style === 'asUsed') {
        return v;
    }
    const ascii = v
        .normalize('NFKD')
        .replace(/[̀-ͯ]/g, '')
        .replace(/ł/g, 'l')
        .replace(/Ł/g, 'L')
        .replace(/ß/g, 'ss')
        .replace(/[^A-Za-z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '');
    if (!ascii) {
        return v;
    }
    return style === 'upper' ? ascii.toUpperCase() : ascii.toLowerCase();
}
/**
 * Resolves `auto`: follows the values already in the catalog (all upper → upper, all lower → lower, otherwise as
 * used); an empty catalog → upper.
 *
 * @param style setting
 * @param catalog tag catalog
 */
function resolveValueStyle(style, catalog) {
    if (style !== 'auto') {
        return style;
    }
    const values = catalog.flatMap(d => d.values).filter(v => /[A-Za-zÀ-ž]/.test(v));
    if (!values.length) {
        return 'upper';
    }
    if (values.every(v => v === v.toUpperCase())) {
        return 'upper';
    }
    if (values.every(v => v === v.toLowerCase())) {
        return 'lower';
    }
    return 'asUsed';
}
/**
 * Groups the rule table by old key (stable: the order of rows with the same old key - and therefore which rule
 * wins - is kept); per key the "every value" rows first. Rows without old key stay at the end.
 *
 * @param rows rule rows as in the form
 */
function sortRuleRows(rows) {
    const keyOf = (r) => {
        const k = r?.fromKey;
        return typeof k === 'string' ? k.trim().toLowerCase() : '';
    };
    const isEvery = (r) => !(0, tagMapping_1.ruleValues)(r?.fromValue).length;
    return rows
        .map((r, i) => ({ r, i }))
        .sort((a, b) => {
        const ka = keyOf(a.r);
        const kb = keyOf(b.r);
        if (!ka || !kb) {
            return !ka && !kb ? a.i - b.i : ka ? -1 : 1;
        }
        return ka.localeCompare(kb) || Number(isEvery(b.r)) - Number(isEvery(a.r)) || a.i - b.i;
    })
        .map(x => x.r);
}
const keyNorm = (k, style) => (0, tagResolver_1.normalizeTagKey)(k, style).toLowerCase();
/**
 * True when a rule row is only a placeholder: an empty new key/value that still leads to a rejected tag with the
 * current catalog (e.g. a row added by "Take over old tags" and not filled in yet). Complete rows and removals are
 * decisions of the user.
 *
 * @param r rule
 * @param inv used tags
 * @param catalog current catalog
 * @param style key style
 */
function isPlaceholder(r, inv, catalog, style) {
    if (r.toKey === tagMapping_1.DROP_KEY) {
        return false;
    }
    if (!r.fromValues.length) {
        if (r.toKey) {
            return false; // key translation: the values are handled separately
        }
        const used = [...(inv.keys.get(r.fromKey.toLowerCase())?.values.keys() ?? [])];
        return (!used.length ||
            used.some(v => !(0, tagMapping_1.applyTagRules)([{ key: r.fromKey, value: v }], [r], catalog, style).tags.length));
    }
    if (r.toKey && r.toValue) {
        return false;
    }
    return r.fromValues.some(v => !(0, tagMapping_1.applyTagRules)([{ key: r.fromKey, value: v }], [r], catalog, style).tags.length);
}
/**
 * Proposes catalog rows and rule rows so that every used tag fits a standard.
 *
 * @param inv used tags (influxdb settings, old points, this instance)
 * @param input catalog, rules and options from the form
 */
function proposeStandard(inv, input) {
    const style = input.keyStyle;
    const catalogRaw = (0, config_1.tableRows)(input.catalogRows).filter(r => r && typeof r === 'object');
    const ruleRaw = (0, config_1.tableRows)(input.ruleRows);
    const catalog = (0, config_1.parseTagDefinitions)(catalogRaw, style, input.valueField, []);
    const allRules = (0, tagMapping_1.parseTagRules)(ruleRaw);
    /** decisions of the user: complete rows and removals (placeholders are completed below) */
    const rules = allRules.filter(r => !isPlaceholder(r, inv, catalog, style));
    const valueStyle = resolveValueStyle(input.valueStyle, catalog);
    const maxList = input.maxListValues ?? 25;
    const maxOpen = input.maxOpenListValues ?? 1000;
    const reserved = new Set([...config_1.RESERVED_TAG_KEYS, input.valueField.toLowerCase()]);
    const plans = new Map(); // by normalised standard key
    const planByOldKey = new Map(); // old key (lower case) → plan
    const planOf = (key, def) => {
        const n = keyNorm(key, style);
        let p = plans.get(n);
        if (!p) {
            p = { key, def, oldKeys: [], values: new Map() };
            plans.set(n, p);
        }
        return p;
    };
    const ownRule = (key, value) => {
        const idx = (0, tagMapping_1.findTagRule)({ key, value }, rules);
        return idx >= 0 && rules[idx].fromValues.length > 0;
    };
    // ---- keys: rule (user decision) → catalog (folding/synonyms) → other old key of the same group → new key
    const entries = [...inv.keys.values()].sort((a, b) => b.states - a.states || a.key.localeCompare(b.key));
    const groupKey = new Map(); // synonym group / folded key → chosen standard key
    for (const e of entries) {
        const keyRule = rules.find(r => r.fromKey.toLowerCase() === e.key.toLowerCase() && !r.fromValues.length);
        if (keyRule?.toKey === tagMapping_1.DROP_KEY) {
            continue;
        }
        const group = keyGroup(e.key);
        const fold = (0, tagMapping_1.foldText)(e.key);
        let def;
        let key;
        if (keyRule?.toKey) {
            def = catalog.find(d => keyNorm(d.key, style) === keyNorm(keyRule.toKey, style));
            key = def?.key ?? keyRule.toKey;
        }
        else {
            def =
                catalog.find(d => sameKey(d.key, e.key)) ??
                    (group ? catalog.find(d => keyGroup(d.key) === group) : undefined);
            key = def?.key ?? groupKey.get(group ?? fold);
            if (!key) {
                key = group ? (KEY_CANONICAL.get(group) ?? e.key) : (0, tagResolver_1.normalizeTagKey)(e.key, 'snake') || e.key;
                if (reserved.has(keyNorm(key, style))) {
                    key = `${key}_tag`;
                }
            }
        }
        groupKey.set(group ?? fold, key);
        const plan = planOf(key, def);
        plan.oldKeys.push(e.key);
        planByOldKey.set(e.key.toLowerCase(), plan);
        plan.values.set(e.key, new Map([...e.values.keys()].map(v => [v, undefined])));
    }
    // ---- target values of complete rules (user decisions, rows of earlier proposals) must exist in the catalog
    const ruleTargets = new Map();
    for (const r of rules) {
        if (r.toKey && r.toKey !== tagMapping_1.DROP_KEY && r.toValue) {
            const n = keyNorm(r.toKey, style);
            const t = ruleTargets.get(n) ?? { key: r.toKey, values: [] };
            if (!t.values.includes(r.toValue)) {
                t.values.push(r.toValue);
            }
            ruleTargets.set(n, t);
        }
    }
    // ---- values per standard key (closed and open lists)
    const newCatalogRows = [];
    const extraValues = new Map(); // existing catalog key (normalised) → appended values
    const summary = [];
    let newValues = 0;
    for (const plan of plans.values()) {
        const allOld = [];
        for (const oldKey of plan.oldKeys) {
            const entry = inv.keys.get(oldKey.toLowerCase());
            for (const value of entry?.values.keys() ?? []) {
                if (entry && !ownRule(oldKey, value)) {
                    allOld.push({ oldKey, value, count: (0, tagMapping_1.useCount)(entry, value) });
                }
            }
        }
        const groups = new Map();
        for (const o of allOld) {
            const f = (0, tagMapping_1.foldValue)(o.value) || o.value;
            const g = groups.get(f) ?? { count: 0, members: [] };
            g.count += o.count;
            g.members.push(o);
            groups.set(f, g);
        }
        const openList = plan.def ? plan.def.allowOther : groups.size > maxList;
        // candidates: catalog values, then ioBroker rooms/functions for location/function keys
        const candidates = [];
        const addCandidate = (std) => {
            if (!candidates.some(c => c.std === std)) {
                candidates.push({ std, fold: (0, tagMapping_1.foldValue)(std), group: valueGroup(std) });
            }
        };
        for (const v of plan.def?.values ?? []) {
            addCandidate(v);
        }
        const added = [];
        for (const v of ruleTargets.get(keyNorm(plan.key, style))?.values ?? []) {
            if (!candidates.some(c => c.std === v)) {
                addCandidate(v);
                added.push(v);
            }
        }
        ruleTargets.delete(keyNorm(plan.key, style));
        const kg = keyGroup(plan.key);
        const enumNames = kg === 'location' ? (input.rooms ?? []) : kg === 'function' ? (input.functions ?? []) : [];
        const enumCandidates = enumNames.map(n => ({ name: n, fold: (0, tagMapping_1.foldValue)(n), group: valueGroup(n) }));
        const perStd = new Map(); // standard value → old spellings (for the summary)
        const sortedGroups = [...groups.entries()].sort((a, b) => b[1].count - a[1].count || a[0].localeCompare(b[0]));
        for (const [fold, g] of sortedGroups) {
            const spelling = [...g.members].sort((a, b) => b.count - a.count)[0].value;
            const vg = valueGroup(spelling);
            let std = candidates.find(c => c.fold === fold)?.std ??
                (vg ? candidates.find(c => c.group === vg)?.std : undefined);
            if (!std) {
                const en = enumCandidates.find(c => c.fold === fold) ??
                    (vg ? enumCandidates.find(c => c.group === vg) : undefined);
                // open lists keep the most used spelling (free text such as device names); closed lists get the style
                std = styleValue(en ? en.name : spelling, openList && !en ? 'asUsed' : valueStyle);
                addCandidate(std);
                added.push(std);
            }
            for (const m of g.members) {
                plan.values.get(m.oldKey)?.set(m.value, std);
            }
            const olds = perStd.get(std) ?? [];
            olds.push(...g.members.map(m => m.value).filter(v => !olds.includes(v)));
            perStd.set(std, olds);
        }
        const lines = [...perStd.entries()]
            .filter(([std, olds]) => olds.length > 1 || olds[0] !== std)
            .map(([std, olds]) => `${olds.join(', ')} → ${std}`);
        if (!plan.def) {
            const values = [...candidates.map(c => c.std)];
            if (!openList) {
                // unused ioBroker rooms/functions complete the standard list of a new key
                for (const en of enumCandidates) {
                    const std = styleValue(en.name, valueStyle);
                    if (!values.some(v => (0, tagMapping_1.foldValue)(v) === (0, tagMapping_1.foldValue)(std))) {
                        values.push(std);
                    }
                }
            }
            const listed = values.slice(0, openList ? maxOpen : values.length);
            newValues += listed.length;
            newCatalogRows.push({
                key: plan.key,
                values: listed.join(', '),
                allowOther: openList,
                defaultValue: '',
                description: `proposed from ${plan.oldKeys.join(', ')}`,
            });
        }
        else if (added.length) {
            const room = Math.max(0, (openList ? maxOpen : Number.MAX_SAFE_INTEGER) - plan.def.values.length);
            const listed = added.slice(0, room);
            if (listed.length) {
                extraValues.set(keyNorm(plan.def.key, style), listed);
                newValues += listed.length;
            }
        }
        summary.push(`${plan.oldKeys.join(', ')} → ${plan.key}${plan.def ? '' : ' (new key)'}${openList ? ' [other values allowed]' : ''}${lines.length ? `: ${lines.join('; ')}` : ''}${added.length && plan.def ? ` (+${added.length} value(s))` : ''}`);
    }
    // rule targets of keys without old tags of their own
    for (const t of ruleTargets.values()) {
        const def = catalog.find(d => keyNorm(d.key, style) === keyNorm(t.key, style));
        if (def) {
            if (!def.allowOther || def.values.length < maxOpen) {
                const missing = t.values.filter(v => !def.values.includes(v));
                if (missing.length) {
                    const n = keyNorm(def.key, style);
                    extraValues.set(n, [...(extraValues.get(n) ?? []), ...missing]);
                    newValues += missing.length;
                }
            }
        }
        else if (!reserved.has(keyNorm(t.key, style))) {
            newCatalogRows.push({
                key: t.key,
                values: t.values.join(', '),
                allowOther: false,
                defaultValue: '',
                description: 'from the tag rules',
            });
            newValues += t.values.length;
        }
    }
    // ---- catalog rows: existing rows kept, missing values appended, new keys added
    const catalogRows = catalogRaw.map(row => {
        const extra = typeof row.key === 'string' ? extraValues.get(keyNorm(row.key, style)) : undefined;
        if (!extra) {
            return row;
        }
        const current = typeof row.values === 'string' ? row.values.trim() : '';
        return { ...row, values: [current, ...extra].filter(Boolean).join(', ') };
    });
    catalogRows.push(...newCatalogRows);
    const newCatalog = (0, config_1.parseTagDefinitions)(catalogRows, style, input.valueField, []);
    // ---- complete placeholder rows in place (split when their old values need different standard values)
    let completedRows = 0;
    const baseRows = [];
    for (const raw of ruleRaw) {
        const [r] = (0, tagMapping_1.parseTagRules)([raw]);
        const placeholder = !!r && isPlaceholder(r, inv, catalog, style);
        const plan = r ? planByOldKey.get(r.fromKey.toLowerCase()) : undefined;
        if (!r || !placeholder || !plan) {
            baseRows.push(raw);
            continue;
        }
        const row = raw;
        completedRows++;
        if (!r.fromValues.length) {
            baseRows.push({ ...row, toKey: r.toKey || plan.key });
            continue;
        }
        const valueMap = plan.values.get(plan.oldKeys.find(k => k.toLowerCase() === r.fromKey.toLowerCase()) ?? r.fromKey);
        const byStd = new Map();
        for (const v of r.fromValues) {
            const std = valueMap?.get(v) ??
                valueMap?.get([...valueMap.keys()].find(k => k.toLowerCase() === v.toLowerCase()) ?? '') ??
                v;
            byStd.set(std, [...(byStd.get(std) ?? []), v]);
        }
        for (const [std, olds] of byStd) {
            baseRows.push({ ...row, fromValue: olds, toKey: r.toKey || plan.key, toValue: r.toValue || std });
        }
    }
    // ---- complete mapping: per old key one "every value" row (key translation, also for values that appear
    //      later) and per standard value one row with all its old spellings - also where the automatic spelling
    //      would already give the result, so that every used key and value is visible and editable in the table.
    //      Keys with very many values (identifiers) only get rows for values that change.
    const working = (0, tagMapping_1.parseTagRules)(baseRows);
    const added = [];
    const sameRow = (a, b) => a.fromKey.toLowerCase() === b.fromKey.toLowerCase() &&
        keyNorm(a.toKey, style) === keyNorm(b.toKey, style) &&
        a.toValue === b.toValue &&
        a.fromValues.length === b.fromValues.length &&
        a.fromValues.every(v => b.fromValues.some(x => x.toLowerCase() === v.toLowerCase()));
    const push = (r) => {
        if (!working.some(w => sameRow(w, r))) {
            working.push(r);
            added.push(r);
        }
    };
    const maxExplicit = input.maxExplicitValues ?? 100;
    const sortedPlans = [...plans.values()].sort((a, b) => a.key.localeCompare(b.key));
    for (const plan of sortedPlans) {
        for (const oldKey of [...plan.oldKeys].sort((a, b) => a.localeCompare(b))) {
            const lk = oldKey.toLowerCase();
            const valueMap = plan.values.get(oldKey) ?? new Map();
            const wanted = (v) => ({
                key: plan.key,
                value: valueMap.get(v) ?? v,
            });
            const fits = (v) => {
                const res = (0, tagMapping_1.applyTagRules)([{ key: oldKey, value: v }], working, newCatalog, style).tags[0];
                const w = wanted(v);
                return !!res && keyNorm(res.key, style) === keyNorm(w.key, style) && res.value === w.value;
            };
            if (!working.some(r => r.fromKey.toLowerCase() === lk && !r.fromValues.length)) {
                push({ fromKey: oldKey, fromValues: [], toKey: plan.key, toValue: '' });
            }
            // a value with a row of its own (user decision, completed placeholder, earlier proposal) is mapped
            const hasRow = (v) => working.some(r => r.fromKey.toLowerCase() === lk && r.fromValues.some(x => x.toLowerCase() === v.toLowerCase()));
            const values = [...valueMap.keys()].filter(v => !ownRule(oldKey, v) && !hasRow(v));
            const explicitAll = new Set([...valueMap.values()]).size <= maxExplicit;
            const byStd = new Map();
            for (const v of values.sort((a, b) => a.localeCompare(b))) {
                if (!explicitAll && fits(v)) {
                    continue;
                }
                const std = wanted(v).value;
                byStd.set(std, [...(byStd.get(std) ?? []), v]);
            }
            for (const std of [...byStd.keys()].sort((a, b) => a.localeCompare(b))) {
                push({ fromKey: oldKey, fromValues: byStd.get(std) ?? [], toKey: plan.key, toValue: std });
            }
        }
    }
    const ruleRows = sortRuleRows([...baseRows, ...added.map(tagMapping_1.ruleToRow)]);
    // ---- coverage: every used key/value with the final rules and catalog
    const finalRules = (0, tagMapping_1.parseTagRules)(ruleRows);
    const coverage = { total: 0, covered: 0, removed: 0, uncovered: [] };
    for (const e of inv.keys.values()) {
        for (const v of e.values.keys()) {
            coverage.total++;
            const idx = (0, tagMapping_1.findTagRule)({ key: e.key, value: v }, finalRules);
            if (idx >= 0 && finalRules[idx].toKey === tagMapping_1.DROP_KEY) {
                coverage.removed++;
                continue;
            }
            const res = (0, tagMapping_1.applyTagRules)([{ key: e.key, value: v }], finalRules, newCatalog, style);
            if (res.tags.length) {
                coverage.covered++;
            }
            else {
                const o = res.outcomes[0];
                coverage.uncovered.push(`${e.key}=${v}: ${o && 'reason' in o ? o.reason : 'not translated'}`);
            }
        }
    }
    return {
        catalogRows,
        ruleRows,
        newKeys: newCatalogRows.map(r => String(r.key)),
        newValues,
        newRules: added.length,
        completedRows,
        summary,
        coverage,
    };
}
//# sourceMappingURL=tagStandard.js.map