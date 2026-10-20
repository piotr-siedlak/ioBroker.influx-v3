/**
 * Automatic matching of the old custom tags and a proposed standard (ADR-0006, extension 2).
 *
 * From the tags found in the influxdb settings, the tag catalog and the tag rules being edited, `proposeStandard`
 * computes catalog rows (new keys, missing values) and the minimal rule rows that translate every old tag into the
 * standard. Similarity (folding, synonym dictionaries) is only used here, for a proposal the user reviews - the
 * runtime never maps by similarity.
 */
import { parseTagDefinitions, RESERVED_TAG_KEYS, tableRows, type TagDefinition, type TagKeyStyle } from './config';
import {
    applyTagRules,
    DROP_KEY,
    findTagRule,
    foldText,
    foldValue,
    type OldTagInventory,
    parseTagRules,
    ruleToRow,
    type TagRule,
    useCount,
} from './tagMapping';
import { normalizeTagKey } from './tagResolver';

export type ValueStyle = 'auto' | 'upper' | 'lower' | 'asUsed';

/** Synonyms of tag keys (EN, DE, PL, FR, IT, ES, PT); the first entry is the proposed key name */
const KEY_GROUPS: string[][] = [
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
const VALUE_GROUPS: string[][] = [
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

function buildIndex(groups: string[][]): Map<string, string> {
    const index = new Map<string, string>();
    for (const g of groups) {
        for (const word of g) {
            const f = foldText(word);
            if (f && !index.has(f)) {
                index.set(f, foldText(g[0]));
            }
        }
    }
    return index;
}

const KEY_INDEX = buildIndex(KEY_GROUPS);
const VALUE_INDEX = buildIndex(VALUE_GROUPS);
const KEY_CANONICAL = new Map(KEY_GROUPS.map(g => [foldText(g[0]), g[0]]));

/** Synonym group of a key (folded canonical name) or undefined */
export function keyGroup(key: string): string | undefined {
    const f = foldText(key);
    return KEY_INDEX.get(f) ?? (f.length > 3 && f.endsWith('s') ? KEY_INDEX.get(f.slice(0, -1)) : undefined);
}

/** Synonym group of a value (folded name of the group) or undefined */
export function valueGroup(value: string): string | undefined {
    return VALUE_INDEX.get(foldText(value));
}

/** True when two keys are the same after folding, also singular/plural (`rooms` = `room`) */
export function sameKey(a: string, b: string): boolean {
    const fa = foldText(a);
    const fb = foldText(b);
    return !!fa && (fa === fb || fa === `${fb}s` || fb === `${fa}s`);
}

const NUMERIC = /^[+-]?\d+([.,]\d+)?$/;

/**
 * Spelling of a proposed value.
 *
 * @param value old spelling
 * @param style `upper` → `BOILER_ROOM`, `lower` → `boiler_room`, `asUsed` → unchanged
 */
export function styleValue(value: string, style: Exclude<ValueStyle, 'auto'>): string {
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
export function resolveValueStyle(style: ValueStyle, catalog: readonly TagDefinition[]): Exclude<ValueStyle, 'auto'> {
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

export interface ProposalInput {
    /** catalog rows as in the form (`values` comma separated) */
    catalogRows: unknown;
    /** rule rows as in the form */
    ruleRows: unknown;
    keyStyle: TagKeyStyle;
    valueField: string;
    valueStyle: ValueStyle;
    /** names of the ioBroker rooms / functions, offered as standard values of location / function keys */
    rooms?: string[];
    functions?: string[];
    /** distinct values above which a new key allows other values instead of a closed list */
    maxListValues?: number;
    /** values listed in the catalog for keys that allow other values */
    maxOpenListValues?: number;
}

export interface Coverage {
    /** used key/value pairs */
    total: number;
    covered: number;
    /** removed on purpose by a rule (`-`) */
    removed: number;
    /** `key=value: reason` of pairs that still do not fit */
    uncovered: string[];
}

export interface StandardProposal {
    catalogRows: Record<string, unknown>[];
    ruleRows: unknown[];
    newKeys: string[];
    newValues: number;
    newRules: number;
    /** incomplete rows (empty new key/value) that were completed */
    completedRows: number;
    /** readable summary, e.g. `room, Raum → location: Kotłownia, Heizungsraum → KOTLOWNIA` */
    summary: string[];
    coverage: Coverage;
}

interface KeyPlan {
    /** standard key (catalog spelling) */
    key: string;
    def?: TagDefinition;
    oldKeys: string[];
    /** old key → old value → standard value (undefined = keep the value) */
    values: Map<string, Map<string, string | undefined>>;
}

const keyNorm = (k: string, style: TagKeyStyle): string => normalizeTagKey(k, style).toLowerCase();

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
function isPlaceholder(
    r: TagRule,
    inv: OldTagInventory,
    catalog: readonly TagDefinition[],
    style: TagKeyStyle,
): boolean {
    if (r.toKey === DROP_KEY) {
        return false;
    }
    if (!r.fromValues.length) {
        if (r.toKey) {
            return false; // key translation: the values are handled separately
        }
        const used = [...(inv.keys.get(r.fromKey.toLowerCase())?.values.keys() ?? [])];
        return (
            !used.length ||
            used.some(v => !applyTagRules([{ key: r.fromKey, value: v }], [r], catalog, style).tags.length)
        );
    }
    if (r.toKey && r.toValue) {
        return false;
    }
    return r.fromValues.some(v => !applyTagRules([{ key: r.fromKey, value: v }], [r], catalog, style).tags.length);
}

/**
 * Proposes catalog rows and rule rows so that every used tag fits a standard.
 *
 * @param inv used tags (influxdb settings, old points, this instance)
 * @param input catalog, rules and options from the form
 */
export function proposeStandard(inv: OldTagInventory, input: ProposalInput): StandardProposal {
    const style = input.keyStyle;
    const catalogRaw = tableRows(input.catalogRows).filter(r => r && typeof r === 'object') as Record<
        string,
        unknown
    >[];
    const ruleRaw = tableRows(input.ruleRows);
    const catalog = parseTagDefinitions(catalogRaw, style, input.valueField, []);
    const allRules = parseTagRules(ruleRaw);
    /** decisions of the user: complete rows and removals (placeholders are completed below) */
    const rules = allRules.filter(r => !isPlaceholder(r, inv, catalog, style));
    const valueStyle = resolveValueStyle(input.valueStyle, catalog);
    const maxList = input.maxListValues ?? 25;
    const maxOpen = input.maxOpenListValues ?? 1000;
    const reserved = new Set([...RESERVED_TAG_KEYS, input.valueField.toLowerCase()]);

    const plans = new Map<string, KeyPlan>(); // by normalised standard key
    const planByOldKey = new Map<string, KeyPlan>(); // old key (lower case) → plan
    const planOf = (key: string, def: TagDefinition | undefined): KeyPlan => {
        const n = keyNorm(key, style);
        let p = plans.get(n);
        if (!p) {
            p = { key, def, oldKeys: [], values: new Map() };
            plans.set(n, p);
        }
        return p;
    };
    const ownRule = (key: string, value: string): boolean => {
        const idx = findTagRule({ key, value }, rules);
        return idx >= 0 && rules[idx].fromValues.length > 0;
    };

    // ---- keys: rule (user decision) → catalog (folding/synonyms) → other old key of the same group → new key
    const entries = [...inv.keys.values()].sort((a, b) => b.states - a.states || a.key.localeCompare(b.key));
    const groupKey = new Map<string, string>(); // synonym group / folded key → chosen standard key
    for (const e of entries) {
        const keyRule = rules.find(r => r.fromKey.toLowerCase() === e.key.toLowerCase() && !r.fromValues.length);
        if (keyRule?.toKey === DROP_KEY) {
            continue;
        }
        const group = keyGroup(e.key);
        const fold = foldText(e.key);
        let def: TagDefinition | undefined;
        let key: string | undefined;
        if (keyRule?.toKey) {
            def = catalog.find(d => keyNorm(d.key, style) === keyNorm(keyRule.toKey, style));
            key = def?.key ?? keyRule.toKey;
        } else {
            def =
                catalog.find(d => sameKey(d.key, e.key)) ??
                (group ? catalog.find(d => keyGroup(d.key) === group) : undefined);
            key = def?.key ?? groupKey.get(group ?? fold);
            if (!key) {
                key = group ? (KEY_CANONICAL.get(group) ?? e.key) : normalizeTagKey(e.key, 'snake') || e.key;
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

    // ---- values per standard key (closed and open lists)
    const newCatalogRows: Record<string, unknown>[] = [];
    const extraValues = new Map<string, string[]>(); // existing catalog key (normalised) → appended values
    const summary: string[] = [];
    let newValues = 0;
    for (const plan of plans.values()) {
        const allOld: { oldKey: string; value: string; count: number }[] = [];
        for (const oldKey of plan.oldKeys) {
            const entry = inv.keys.get(oldKey.toLowerCase());
            for (const value of entry?.values.keys() ?? []) {
                if (entry && !ownRule(oldKey, value)) {
                    allOld.push({ oldKey, value, count: useCount(entry, value) });
                }
            }
        }
        const groups = new Map<string, { count: number; members: typeof allOld }>();
        for (const o of allOld) {
            const f = foldValue(o.value) || o.value;
            const g = groups.get(f) ?? { count: 0, members: [] };
            g.count += o.count;
            g.members.push(o);
            groups.set(f, g);
        }
        const openList = plan.def ? plan.def.allowOther : groups.size > maxList;
        // candidates: catalog values, then ioBroker rooms/functions for location/function keys
        const candidates: { std: string; fold: string; group?: string }[] = [];
        const addCandidate = (std: string): void => {
            if (!candidates.some(c => c.std === std)) {
                candidates.push({ std, fold: foldValue(std), group: valueGroup(std) });
            }
        };
        for (const v of plan.def?.values ?? []) {
            addCandidate(v);
        }
        const kg = keyGroup(plan.key);
        const enumNames = kg === 'location' ? (input.rooms ?? []) : kg === 'function' ? (input.functions ?? []) : [];
        const enumCandidates = enumNames.map(n => ({ name: n, fold: foldValue(n), group: valueGroup(n) }));
        const added: string[] = [];
        const perStd = new Map<string, string[]>(); // standard value → old spellings (for the summary)
        const sortedGroups = [...groups.entries()].sort((a, b) => b[1].count - a[1].count || a[0].localeCompare(b[0]));
        for (const [fold, g] of sortedGroups) {
            const spelling = [...g.members].sort((a, b) => b.count - a.count)[0].value;
            const vg = valueGroup(spelling);
            let std =
                candidates.find(c => c.fold === fold)?.std ??
                (vg ? candidates.find(c => c.group === vg)?.std : undefined);
            if (!std) {
                const en =
                    enumCandidates.find(c => c.fold === fold) ??
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
                    if (!values.some(v => foldValue(v) === foldValue(std))) {
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
        } else if (added.length) {
            const room = Math.max(0, (openList ? maxOpen : Number.MAX_SAFE_INTEGER) - plan.def.values.length);
            const listed = added.slice(0, room);
            if (listed.length) {
                extraValues.set(keyNorm(plan.def.key, style), listed);
                newValues += listed.length;
            }
        }
        summary.push(
            `${plan.oldKeys.join(', ')} → ${plan.key}${plan.def ? '' : ' (new key)'}${openList ? ' [other values allowed]' : ''}${lines.length ? `: ${lines.join('; ')}` : ''}${added.length && plan.def ? ` (+${added.length} value(s))` : ''}`,
        );
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
    const newCatalog = parseTagDefinitions(catalogRows, style, input.valueField, []);

    // ---- complete placeholder rows in place (split when their old values need different standard values)
    let completedRows = 0;
    const baseRows: unknown[] = [];
    for (const raw of ruleRaw) {
        const [r] = parseTagRules([raw]);
        const placeholder = !!r && isPlaceholder(r, inv, catalog, style);
        const plan = r ? planByOldKey.get(r.fromKey.toLowerCase()) : undefined;
        if (!r || !placeholder || !plan) {
            baseRows.push(raw);
            continue;
        }
        const row = raw as Record<string, unknown>;
        completedRows++;
        if (!r.fromValues.length) {
            baseRows.push({ ...row, toKey: r.toKey || plan.key });
            continue;
        }
        const valueMap = plan.values.get(
            plan.oldKeys.find(k => k.toLowerCase() === r.fromKey.toLowerCase()) ?? r.fromKey,
        );
        const byStd = new Map<string, string[]>();
        for (const v of r.fromValues) {
            const std =
                valueMap?.get(v) ??
                valueMap?.get([...valueMap.keys()].find(k => k.toLowerCase() === v.toLowerCase()) ?? '') ??
                v;
            byStd.set(std, [...(byStd.get(std) ?? []), v]);
        }
        for (const [std, olds] of byStd) {
            baseRows.push({ ...row, fromValue: olds, toKey: r.toKey || plan.key, toValue: r.toValue || std });
        }
    }

    // ---- minimal rule rows
    const working: TagRule[] = parseTagRules(baseRows);
    const added: TagRule[] = [];
    const push = (r: TagRule): void => {
        working.push(r);
        added.push(r);
    };
    for (const plan of plans.values()) {
        for (const oldKey of plan.oldKeys) {
            const valueMap = plan.values.get(oldKey) ?? new Map<string, string | undefined>();
            const wanted = (v: string): { key: string; value: string } => ({
                key: plan.key,
                value: valueMap.get(v) ?? v,
            });
            const fits = (v: string): boolean => {
                const res = applyTagRules([{ key: oldKey, value: v }], working, newCatalog, style).tags[0];
                const w = wanted(v);
                return !!res && keyNorm(res.key, style) === keyNorm(w.key, style) && res.value === w.value;
            };
            const values = [...valueMap.keys()].filter(v => !ownRule(oldKey, v));
            const hasKeyRule = working.some(
                r => r.fromKey.toLowerCase() === oldKey.toLowerCase() && !r.fromValues.length,
            );
            if (!hasKeyRule && keyNorm(oldKey, style) !== keyNorm(plan.key, style) && values.some(v => !fits(v))) {
                push({ fromKey: oldKey, fromValues: [], toKey: plan.key, toValue: '' });
            }
            const byStd = new Map<string, string[]>();
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
    const ruleRows = [...baseRows, ...added.map(ruleToRow)];

    // ---- coverage: every used key/value with the final rules and catalog
    const finalRules = parseTagRules(ruleRows);
    const coverage: Coverage = { total: 0, covered: 0, removed: 0, uncovered: [] };
    for (const e of inv.keys.values()) {
        for (const v of e.values.keys()) {
            coverage.total++;
            const idx = findTagRule({ key: e.key, value: v }, finalRules);
            if (idx >= 0 && finalRules[idx].toKey === DROP_KEY) {
                coverage.removed++;
                continue;
            }
            const res = applyTagRules([{ key: e.key, value: v }], finalRules, newCatalog, style);
            if (res.tags.length) {
                coverage.covered++;
            } else {
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
