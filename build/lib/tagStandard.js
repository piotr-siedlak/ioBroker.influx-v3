"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.keyGroup = keyGroup;
exports.valueGroup = valueGroup;
exports.sameKey = sameKey;
exports.styleValue = styleValue;
exports.resolveValueStyle = resolveValueStyle;
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
const keyNorm = (k, style) => (0, tagResolver_1.normalizeTagKey)(k, style).toLowerCase();
/**
 * Proposes catalog rows and rule rows for all old tags.
 *
 * @param inv old custom tags of the influxdb settings
 * @param input catalog, rules and options from the form
 */
function proposeStandard(inv, input) {
    const style = input.keyStyle;
    const catalogRaw = (0, config_1.tableRows)(input.catalogRows).filter(r => r && typeof r === 'object');
    const ruleRaw = (0, config_1.tableRows)(input.ruleRows);
    const catalog = (0, config_1.parseTagDefinitions)(catalogRaw, style, input.valueField, []);
    const rules = (0, tagMapping_1.parseTagRules)(ruleRaw);
    const valueStyle = resolveValueStyle(input.valueStyle, catalog);
    const maxList = input.maxListValues ?? 25;
    const reserved = new Set([...config_1.RESERVED_TAG_KEYS, input.valueField.toLowerCase()]);
    const plans = new Map(); // by normalised standard key
    const planOf = (key, def) => {
        const n = keyNorm(key, style);
        let p = plans.get(n);
        if (!p) {
            p = { key, def, oldKeys: [], values: new Map() };
            plans.set(n, p);
        }
        return p;
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
        plan.values.set(e.key, new Map([...e.values.keys()].map(v => [v, undefined])));
    }
    // ---- values per standard key
    const newCatalogRows = [];
    const extraValues = new Map(); // existing catalog key (normalised) → appended values
    const summary = [];
    let newValues = 0;
    for (const plan of plans.values()) {
        const allOld = [];
        for (const oldKey of plan.oldKeys) {
            const entry = inv.keys.get(oldKey.toLowerCase());
            for (const [value, count] of entry?.values ?? []) {
                // values with a rule of their own are the user's decision
                const idx = (0, tagMapping_1.findTagRule)({ key: oldKey, value }, rules);
                if (idx < 0 || !rules[idx].fromValues.length) {
                    allOld.push({ oldKey, value, count });
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
        const kg = keyGroup(plan.key);
        const enumNames = kg === 'location' ? (input.rooms ?? []) : kg === 'function' ? (input.functions ?? []) : [];
        const enumCandidates = enumNames.map(n => ({ name: n, fold: (0, tagMapping_1.foldValue)(n), group: valueGroup(n) }));
        const added = [];
        const perStd = new Map(); // standard value → old spellings (for the summary)
        const sortedGroups = [...groups.entries()].sort((a, b) => b[1].count - a[1].count || a[0].localeCompare(b[0]));
        for (const [fold, g] of sortedGroups) {
            if (openList) {
                continue; // other values allowed: values stay as they are
            }
            const spelling = [...g.members].sort((a, b) => b.count - a.count)[0].value;
            const vg = valueGroup(spelling);
            let std = candidates.find(c => c.fold === fold)?.std ??
                (vg ? candidates.find(c => c.group === vg)?.std : undefined);
            if (!std) {
                const en = enumCandidates.find(c => c.fold === fold) ??
                    (vg ? enumCandidates.find(c => c.group === vg) : undefined);
                std = styleValue(en ? en.name : spelling, valueStyle);
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
            // unused ioBroker rooms/functions complete the standard list of a new key
            const values = openList ? [] : [...candidates.map(c => c.std)];
            if (!openList) {
                for (const en of enumCandidates) {
                    const std = styleValue(en.name, valueStyle);
                    if (!values.some(v => (0, tagMapping_1.foldValue)(v) === (0, tagMapping_1.foldValue)(std))) {
                        values.push(std);
                    }
                }
            }
            newValues += values.length;
            newCatalogRows.push({
                key: plan.key,
                values: values.join(', '),
                allowOther: openList,
                defaultValue: '',
                description: `proposed from ${plan.oldKeys.join(', ')}`,
            });
        }
        else if (added.length) {
            extraValues.set(keyNorm(plan.def.key, style), added);
            newValues += added.length;
        }
        summary.push(`${plan.oldKeys.join(', ')} → ${plan.key}${plan.def ? '' : ' (new key)'}${lines.length ? `: ${lines.join('; ')}` : ''}${added.length && plan.def ? ` (+${added.length} value(s))` : ''}`);
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
    // ---- minimal rule rows
    const working = [...rules];
    const added = [];
    const push = (r) => {
        working.push(r);
        added.push(r);
    };
    for (const plan of plans.values()) {
        for (const oldKey of plan.oldKeys) {
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
            const values = [...valueMap.keys()].filter(v => {
                const idx = (0, tagMapping_1.findTagRule)({ key: oldKey, value: v }, rules);
                return idx < 0 || !rules[idx].fromValues.length;
            });
            const hasKeyRule = working.some(r => r.fromKey.toLowerCase() === oldKey.toLowerCase() && !r.fromValues.length);
            if (!hasKeyRule && keyNorm(oldKey, style) !== keyNorm(plan.key, style) && values.some(v => !fits(v))) {
                push({ fromKey: oldKey, fromValues: [], toKey: plan.key, toValue: '' });
            }
            const byStd = new Map();
            for (const v of values) {
                if (fits(v)) {
                    continue;
                }
                const std = wanted(v).value;
                const list = byStd.get(std) ?? [];
                list.push(v);
                byStd.set(std, list);
            }
            for (const [std, olds] of byStd) {
                push({ fromKey: oldKey, fromValues: olds, toKey: plan.key, toValue: std });
            }
        }
    }
    return {
        catalogRows,
        ruleRows: [...ruleRaw, ...added.map(tagMapping_1.ruleToRow)],
        newKeys: newCatalogRows.map(r => String(r.key)),
        newValues,
        newRules: added.length,
        summary,
    };
}
//# sourceMappingURL=tagStandard.js.map