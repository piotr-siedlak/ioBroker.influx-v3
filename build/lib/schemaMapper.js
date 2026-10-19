"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SchemaRegistry = void 0;
exports.globToRegExp = globToRegExp;
exports.decideValueType = decideValueType;
exports.resolveTarget = resolveTarget;
exports.coerceValue = coerceValue;
exports.roundTo = roundTo;
exports.toFieldValue = toFieldValue;
exports.columnKindFromDataType = columnKindFromDataType;
exports.kindOfValueType = kindOfValueType;
/**
 * Converts a glob (`*` and `?`) into an anchored regular expression.
 *
 * @param glob the pattern
 */
function globToRegExp(glob) {
    const escaped = glob
        .replace(/[.+^${}()|[\]\\]/g, '\\$&')
        .replace(/\*/g, '.*')
        .replace(/\?/g, '.');
    return new RegExp(`^${escaped}$`, 'i');
}
/**
 * Determines the value type of a state from its configuration and its object.
 *
 * @param storeAs per-state override
 * @param commonType `common.type` of the object
 */
function decideValueType(storeAs, commonType) {
    if (storeAs) {
        return storeAs;
    }
    switch (commonType) {
        case 'number':
            return 'number';
        case 'boolean':
            return 'boolean';
        default:
            // string, mixed, json, array, object, file or missing → stored as text
            return 'string';
    }
}
function ruleMatches(rule, id, role, valueType) {
    const typeOk = rule.valueType === 'any' || (rule.valueType === '' ? valueType === 'number' : rule.valueType === valueType);
    if (!typeOk) {
        return false;
    }
    const subject = rule.match === 'id' ? id : role;
    return !!subject && globToRegExp(rule.pattern).test(subject);
}
/**
 * Resolves the target table/field of a state.
 *
 * @param inst instance config
 * @param st state config
 * @param id real state id
 * @param storeId alias or id
 * @param common object common
 */
function resolveTarget(inst, st, id, storeId, common) {
    const valueType = decideValueType(st.storeAs, common.type);
    if (inst.schemaMode === 'perState') {
        return {
            table: st.table || storeId,
            field: st.field || inst.valueField,
            valueType,
            source: st.table ? 'explicit' : 'perState',
        };
    }
    if (st.table) {
        return { table: st.table, field: inst.valueField, valueType, source: 'explicit' };
    }
    const role = typeof common.role === 'string' ? common.role : '';
    for (const rule of inst.tableRules) {
        if (ruleMatches(rule, id, role, valueType)) {
            return { table: rule.table, field: inst.valueField, valueType, source: 'rule' };
        }
    }
    const table = valueType === 'number' ? inst.tableNumber : valueType === 'boolean' ? inst.tableBoolean : inst.tableString;
    return { table, field: inst.valueField, valueType, source: 'default' };
}
/**
 * Converts an ioBroker value into the target value type.
 *
 * Rules: numbers accept finite numbers, numeric strings and booleans (1/0); booleans accept
 * booleans, 0/1, "true"/"false"/"on"/"off"; strings accept everything, objects are serialised as
 * JSON (or rejected) and limited in length. null/undefined/NaN/Infinity are rejected.
 *
 * @param val the value
 * @param type target type
 * @param opts limits
 * @param opts.maxStringLength maximum length of text values
 * @param opts.objectPolicy what to do with objects/arrays
 */
function coerceValue(val, type, opts) {
    if (val === null || val === undefined) {
        return { ok: false, reason: 'null' };
    }
    switch (type) {
        case 'number': {
            let n;
            if (typeof val === 'number') {
                n = val;
            }
            else if (typeof val === 'boolean') {
                n = val ? 1 : 0;
            }
            else if (typeof val === 'string' && val.trim() !== '' && Number.isFinite(Number(val.trim()))) {
                n = Number(val.trim());
            }
            else {
                return { ok: false, reason: `not a number: ${preview(val)}` };
            }
            if (!Number.isFinite(n)) {
                return { ok: false, reason: `non-finite number ${n}` };
            }
            return { ok: true, value: n };
        }
        case 'boolean': {
            if (typeof val === 'boolean') {
                return { ok: true, value: val };
            }
            if (val === 1 || val === 0) {
                return { ok: true, value: val === 1 };
            }
            if (typeof val === 'string') {
                const s = val.trim().toLowerCase();
                if (s === 'true' || s === 'on' || s === '1') {
                    return { ok: true, value: true };
                }
                if (s === 'false' || s === 'off' || s === '0') {
                    return { ok: true, value: false };
                }
            }
            return { ok: false, reason: `not a boolean: ${preview(val)}` };
        }
        case 'string': {
            let s;
            if (typeof val === 'string') {
                s = val;
            }
            else if (typeof val === 'number') {
                if (!Number.isFinite(val)) {
                    return { ok: false, reason: `non-finite number ${val}` };
                }
                s = String(val);
            }
            else if (typeof val === 'boolean') {
                s = String(val);
            }
            else if (opts.objectPolicy === 'json') {
                try {
                    s = JSON.stringify(val);
                }
                catch {
                    return { ok: false, reason: 'object cannot be serialised' };
                }
            }
            else {
                return { ok: false, reason: 'objects are rejected by configuration' };
            }
            if (Buffer.byteLength(s, 'utf8') > opts.maxStringLength) {
                return { ok: false, reason: `text longer than ${opts.maxStringLength} bytes` };
            }
            return { ok: true, value: s };
        }
    }
}
function preview(v) {
    const s = typeof v === 'string' ? JSON.stringify(v) : String(v);
    return s.length > 40 ? `${s.slice(0, 40)}…` : s;
}
/**
 * Rounds a number to `decimals` decimal places (null = unchanged).
 *
 * @param n number
 * @param decimals decimal places
 */
function roundTo(n, decimals) {
    if (decimals === null) {
        return n;
    }
    const f = 10 ** decimals;
    return Math.round(n * f) / f;
}
/**
 * @param value coerced value
 * @param type value type
 */
function toFieldValue(value, type) {
    switch (type) {
        case 'number':
            return { type: 'float', value: value };
        case 'boolean':
            return { type: 'boolean', value: value };
        default:
            return { type: 'string', value: value };
    }
}
/**
 * Maps an `information_schema.columns.data_type` value to a column kind.
 *
 * @param dataType Arrow type name
 */
function columnKindFromDataType(dataType) {
    if (dataType.startsWith('Dictionary')) {
        return 'tag';
    }
    if (dataType.startsWith('Timestamp')) {
        return 'time';
    }
    switch (dataType) {
        case 'Float64':
            return 'float';
        case 'Int64':
        case 'UInt64':
            return 'integer';
        case 'Boolean':
            return 'boolean';
        case 'Utf8':
        case 'LargeUtf8':
        case 'Utf8View':
            return 'string';
        default:
            return 'other';
    }
}
/** @param t value type */
function kindOfValueType(t) {
    return t === 'number' ? 'float' : t;
}
/**
 * Registry of tables and columns. Seeded from the database schema and from the claims of all
 * enabled states. Claims are evaluated in a deterministic order (sorted by state id) by the caller.
 */
class SchemaRegistry {
    maxTables;
    maxColumns;
    tables = new Map();
    claims = new Map();
    constructor(maxTables, maxColumns) {
        this.maxTables = maxTables;
        this.maxColumns = maxColumns;
    }
    /**
     * Replaces knowledge from the database (called after connect / refresh).
     *
     * @param columns rows of information_schema.columns
     */
    seed(columns) {
        for (const info of this.tables.values()) {
            info.fromDb = false;
        }
        for (const c of columns) {
            const info = this.table(c.table);
            info.fromDb = true;
            info.columns.set(c.column, columnKindFromDataType(c.dataType));
        }
    }
    table(name) {
        let info = this.tables.get(name);
        if (!info) {
            info = { columns: new Map(), owners: new Set(), fromDb: false };
            this.tables.set(name, info);
        }
        return info;
    }
    /** number of tables known (database + claimed) */
    tableCount() {
        let n = 0;
        for (const info of this.tables.values()) {
            if (info.fromDb || info.owners.size) {
                n++;
            }
        }
        return n;
    }
    /** @param table table name */
    columnCount(table) {
        return this.tables.get(table)?.columns.size ?? 0;
    }
    /** @param table table name */
    hasTable(table) {
        const info = this.tables.get(table);
        return !!info && (info.fromDb || info.owners.size > 0);
    }
    /**
     * @param table table name
     * @param column column name
     */
    columnKind(table, column) {
        return this.tables.get(table)?.columns.get(column);
    }
    /** @param stateId state id whose claim is removed */
    release(stateId) {
        const claim = this.claims.get(stateId);
        if (!claim) {
            return;
        }
        this.claims.delete(stateId);
        const info = this.tables.get(claim.table);
        info?.owners.delete(stateId);
        if (info && !info.fromDb && !info.owners.size) {
            this.tables.delete(claim.table);
        }
        else if (info && !info.fromDb) {
            this.rebuildColumns(claim.table);
        }
    }
    rebuildColumns(table) {
        const info = this.tables.get(table);
        if (!info || info.fromDb) {
            return;
        }
        info.columns.clear();
        for (const owner of info.owners) {
            const c = this.claims.get(owner);
            if (c) {
                this.applyColumns(info, c, c.tagKeys);
            }
        }
    }
    applyColumns(info, req, tagKeys) {
        info.columns.set('time', 'time');
        info.columns.set(req.field, req.fieldKind);
        for (const [k, kind] of Object.entries(req.extraFields)) {
            info.columns.set(k, kind);
        }
        for (const k of tagKeys) {
            info.columns.set(k, 'tag');
        }
    }
    /**
     * Checks a claim against the registry and records it when valid.
     *
     * @param req claim
     */
    claim(req) {
        this.release(req.stateId);
        const warnings = [];
        const exists = this.hasTable(req.table);
        if (!exists && this.tableCount() >= this.maxTables) {
            return {
                ok: false,
                reason: `table limit reached (${this.maxTables} tables) - cannot create table "${req.table}"`,
            };
        }
        const info = this.table(req.table);
        const existingValue = info.columns.get(req.field);
        if (existingValue && existingValue !== req.fieldKind) {
            if (!info.fromDb && !info.owners.size) {
                info.columns.clear();
            }
            else {
                return {
                    ok: false,
                    reason: `type conflict: column "${req.field}" of table "${req.table}" is ${existingValue}, this state needs ${req.fieldKind}`,
                };
            }
        }
        for (const [k, kind] of Object.entries(req.extraFields)) {
            const ex = info.columns.get(k);
            if (ex && ex !== kind) {
                return {
                    ok: false,
                    reason: `type conflict: column "${k}" of table "${req.table}" is ${ex}, needs ${kind} (check metadata placement)`,
                };
            }
        }
        const droppedTags = [];
        const tagKeys = [];
        for (const k of req.tagKeys) {
            const ex = info.columns.get(k);
            if (ex && ex !== 'tag') {
                droppedTags.push(k);
                warnings.push(`tag "${k}" dropped: table "${req.table}" already has a ${ex} field with this name`);
                continue;
            }
            tagKeys.push(k);
        }
        const newColumns = [req.field, ...Object.keys(req.extraFields), ...tagKeys, 'time'].filter(c => !info.columns.has(c));
        const total = info.columns.size + newColumns.length;
        if (total > this.maxColumns) {
            if (!info.owners.size && !info.fromDb) {
                this.tables.delete(req.table);
            }
            return {
                ok: false,
                reason: `column limit: table "${req.table}" would have ${total} columns (limit ${this.maxColumns})`,
            };
        }
        if (total >= this.maxColumns * 0.8) {
            warnings.push(`table "${req.table}" uses ${total} of ${this.maxColumns} columns`);
        }
        this.applyColumns(info, req, tagKeys);
        info.owners.add(req.stateId);
        this.claims.set(req.stateId, { ...req, tagKeys });
        if (!exists && this.tableCount() >= this.maxTables * 0.8) {
            warnings.push(`${this.tableCount()} of ${this.maxTables} tables used`);
        }
        return { ok: true, droppedTags, warnings };
    }
    /**
     * Evaluates a claim without changing the registry (used by the mapping preview).
     *
     * @param req claim
     */
    check(req) {
        const previous = this.claims.get(req.stateId);
        const result = this.claim(req);
        this.release(req.stateId);
        if (previous) {
            this.claim(previous);
        }
        return result;
    }
    /** list of tables with the number of claiming states (for diagnostics) */
    summary() {
        return [...this.tables.entries()]
            .map(([table, info]) => ({
            table,
            states: info.owners.size,
            columns: info.columns.size,
            inDb: info.fromDb,
        }))
            .sort((a, b) => a.table.localeCompare(b.table));
    }
}
exports.SchemaRegistry = SchemaRegistry;
//# sourceMappingURL=schemaMapper.js.map