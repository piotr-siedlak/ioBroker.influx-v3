/**
 * SchemaMapper: maps an ioBroker state to table / field / value type, coerces values and keeps a
 * registry that enforces "one value type per table" and the server's table/column limits.
 * See docs/adr/0001-table-model-grouped-vs-per-state.md.
 */
import type { InstanceConfig, StateConfig, TableRule, ValueType } from './config';
import type { FieldValue } from './lineProtocol';

export type TableSource = 'explicit' | 'rule' | 'default' | 'perState';

export interface Target {
    table: string;
    field: string;
    valueType: ValueType;
    source: TableSource;
}

/**
 * Converts a glob (`*` and `?`) into an anchored regular expression.
 *
 * @param glob the pattern
 */
export function globToRegExp(glob: string): RegExp {
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
export function decideValueType(storeAs: StateConfig['storeAs'], commonType: unknown): ValueType {
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

function ruleMatches(rule: TableRule, id: string, role: string, valueType: ValueType): boolean {
    const typeOk =
        rule.valueType === 'any' || (rule.valueType === '' ? valueType === 'number' : rule.valueType === valueType);
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
export function resolveTarget(
    inst: Pick<
        InstanceConfig,
        'schemaMode' | 'tableNumber' | 'tableBoolean' | 'tableString' | 'tableRules' | 'valueField'
    >,
    st: Pick<StateConfig, 'table' | 'storeAs' | 'field'>,
    id: string,
    storeId: string,
    common: Record<string, unknown>,
): Target {
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
    const table =
        valueType === 'number' ? inst.tableNumber : valueType === 'boolean' ? inst.tableBoolean : inst.tableString;
    return { table, field: inst.valueField, valueType, source: 'default' };
}

export type CoerceResult = { ok: true; value: number | boolean | string } | { ok: false; reason: string };

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
export function coerceValue(
    val: unknown,
    type: ValueType,
    opts: { maxStringLength: number; objectPolicy: 'json' | 'reject' },
): CoerceResult {
    if (val === null || val === undefined) {
        return { ok: false, reason: 'null' };
    }
    switch (type) {
        case 'number': {
            let n: number;
            if (typeof val === 'number') {
                n = val;
            } else if (typeof val === 'boolean') {
                n = val ? 1 : 0;
            } else if (typeof val === 'string' && val.trim() !== '' && Number.isFinite(Number(val.trim()))) {
                n = Number(val.trim());
            } else {
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
            let s: string;
            if (typeof val === 'string') {
                s = val;
            } else if (typeof val === 'number') {
                if (!Number.isFinite(val)) {
                    return { ok: false, reason: `non-finite number ${val}` };
                }
                s = String(val);
            } else if (typeof val === 'boolean') {
                s = String(val);
            } else if (opts.objectPolicy === 'json') {
                try {
                    s = JSON.stringify(val);
                } catch {
                    return { ok: false, reason: 'object cannot be serialised' };
                }
            } else {
                return { ok: false, reason: 'objects are rejected by configuration' };
            }
            if (Buffer.byteLength(s, 'utf8') > opts.maxStringLength) {
                return { ok: false, reason: `text longer than ${opts.maxStringLength} bytes` };
            }
            return { ok: true, value: s };
        }
    }
}

function preview(v: unknown): string {
    const s = typeof v === 'string' ? JSON.stringify(v) : String(v);
    return s.length > 40 ? `${s.slice(0, 40)}…` : s;
}

/**
 * Rounds a number to `decimals` decimal places (null = unchanged).
 *
 * @param n number
 * @param decimals decimal places
 */
export function roundTo(n: number, decimals: number | null): number {
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
export function toFieldValue(value: number | boolean | string, type: ValueType): FieldValue {
    switch (type) {
        case 'number':
            return { type: 'float', value: value as number };
        case 'boolean':
            return { type: 'boolean', value: value as boolean };
        default:
            return { type: 'string', value: value as string };
    }
}

/** Column kinds as known from the database or from our own claims */
export type ColumnKind = 'tag' | 'float' | 'integer' | 'boolean' | 'string' | 'time' | 'other';

/**
 * Maps an `information_schema.columns.data_type` value to a column kind.
 *
 * @param dataType Arrow type name
 */
export function columnKindFromDataType(dataType: string): ColumnKind {
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
export function kindOfValueType(t: ValueType): ColumnKind {
    return t === 'number' ? 'float' : t;
}

export interface SchemaColumn {
    table: string;
    column: string;
    dataType: string;
}

interface TableInfo {
    columns: Map<string, ColumnKind>;
    owners: Set<string>;
    fromDb: boolean;
}

export interface ClaimRequest {
    stateId: string;
    table: string;
    /** value field and its kind */
    field: string;
    fieldKind: ColumnKind;
    /** additional field columns (metadata) */
    extraFields: Record<string, ColumnKind>;
    tagKeys: string[];
}

export type ClaimResult = { ok: true; droppedTags: string[]; warnings: string[] } | { ok: false; reason: string };

/**
 * Registry of tables and columns. Seeded from the database schema and from the claims of all
 * enabled states. Claims are evaluated in a deterministic order (sorted by state id) by the caller.
 */
export class SchemaRegistry {
    private readonly tables = new Map<string, TableInfo>();
    private readonly claims = new Map<string, ClaimRequest>();

    constructor(
        private readonly maxTables: number,
        private readonly maxColumns: number,
    ) {}

    /**
     * Replaces knowledge from the database (called after connect / refresh).
     *
     * @param columns rows of information_schema.columns
     */
    seed(columns: SchemaColumn[]): void {
        for (const info of this.tables.values()) {
            info.fromDb = false;
        }
        for (const c of columns) {
            const info = this.table(c.table);
            info.fromDb = true;
            info.columns.set(c.column, columnKindFromDataType(c.dataType));
        }
    }

    private table(name: string): TableInfo {
        let info = this.tables.get(name);
        if (!info) {
            info = { columns: new Map(), owners: new Set(), fromDb: false };
            this.tables.set(name, info);
        }
        return info;
    }

    /** number of tables known (database + claimed) */
    tableCount(): number {
        let n = 0;
        for (const info of this.tables.values()) {
            if (info.fromDb || info.owners.size) {
                n++;
            }
        }
        return n;
    }

    /** @param table table name */
    columnCount(table: string): number {
        return this.tables.get(table)?.columns.size ?? 0;
    }

    /** @param table table name */
    hasTable(table: string): boolean {
        const info = this.tables.get(table);
        return !!info && (info.fromDb || info.owners.size > 0);
    }

    /**
     * @param table table name
     * @param column column name
     */
    columnKind(table: string, column: string): ColumnKind | undefined {
        return this.tables.get(table)?.columns.get(column);
    }

    /** @param stateId state id whose claim is removed */
    release(stateId: string): void {
        const claim = this.claims.get(stateId);
        if (!claim) {
            return;
        }
        this.claims.delete(stateId);
        const info = this.tables.get(claim.table);
        info?.owners.delete(stateId);
        if (info && !info.fromDb && !info.owners.size) {
            this.tables.delete(claim.table);
        } else if (info && !info.fromDb) {
            this.rebuildColumns(claim.table);
        }
    }

    private rebuildColumns(table: string): void {
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

    private applyColumns(info: TableInfo, req: ClaimRequest, tagKeys: string[]): void {
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
    claim(req: ClaimRequest): ClaimResult {
        this.release(req.stateId);
        const warnings: string[] = [];
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
            } else {
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
        const droppedTags: string[] = [];
        const tagKeys: string[] = [];
        for (const k of req.tagKeys) {
            const ex = info.columns.get(k);
            if (ex && ex !== 'tag') {
                droppedTags.push(k);
                warnings.push(`tag "${k}" dropped: table "${req.table}" already has a ${ex} field with this name`);
                continue;
            }
            tagKeys.push(k);
        }
        const newColumns = [req.field, ...Object.keys(req.extraFields), ...tagKeys, 'time'].filter(
            c => !info.columns.has(c),
        );
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
    check(req: ClaimRequest): ClaimResult {
        const previous = this.claims.get(req.stateId);
        const result = this.claim(req);
        this.release(req.stateId);
        if (previous) {
            this.claim(previous);
        }
        return result;
    }

    /** list of tables with the number of claiming states (for diagnostics) */
    summary(): { table: string; states: number; columns: number; inDb: boolean }[] {
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
