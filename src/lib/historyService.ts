/**
 * HistoryService: ioBroker `getHistory` → parameterised InfluxDB 3 SQL.
 *
 * - raw (`none`, `onchange`): rows within [start, end], optional border values outside the range
 * - aggregated (`average`, `min`, `max`, `total`, `count`, `percentile`, `quantile`): `date_bin`
 * - `minmax`: `selector_min` / `selector_max` per bin (two points per bin, with their real times)
 * Identifiers are quoted, all values are `$params`; time is selected as epoch ms.
 */
import type { ValueType } from './config';

export type Aggregate =
    'none' | 'onchange' | 'average' | 'min' | 'max' | 'minmax' | 'total' | 'count' | 'percentile' | 'quantile';

export interface HistoryOptions {
    start: number;
    end: number;
    count: number;
    limit: number;
    step?: number;
    aggregate: Aggregate;
    ack: boolean;
    q: boolean;
    from: boolean;
    addId: boolean;
    returnNewestEntries: boolean;
    removeBorderValues: boolean;
    round: number | null;
    percentile: number;
    quantile: number;
    sessionId?: unknown;
}

export interface HistoryTarget {
    table: string;
    field: string;
    valueType: ValueType;
    storeId: string;
    /** columns present in the table (from the schema registry); used to select only existing columns */
    columns: Set<string>;
    /** tag/field placement of metadata */
    metaColumns: { ack?: string; q?: string; from?: string };
}

export interface BuiltQuery {
    sql: string;
    params: Record<string, string | number | boolean>;
    kind: 'raw' | 'agg' | 'minmax' | 'borderBefore' | 'borderAfter';
}

export interface IobHistoryEntry {
    val: number | boolean | string | null;
    ts: number;
    ack?: boolean;
    q?: number;
    from?: string;
    id?: string;
}

/**
 * Quotes an SQL identifier.
 *
 * @param name identifier
 */
export function quoteIdent(name: string): string {
    return `"${name.replace(/"/g, '""')}"`;
}

const AGGREGATES: readonly Aggregate[] = [
    'none',
    'onchange',
    'average',
    'min',
    'max',
    'minmax',
    'total',
    'count',
    'percentile',
    'quantile',
];

function toNumber(v: unknown): number | undefined {
    if (typeof v === 'number' && Number.isFinite(v)) {
        return v;
    }
    if (typeof v === 'string' && v.trim() !== '') {
        const n = Number(v);
        if (Number.isFinite(n)) {
            return n;
        }
        const d = Date.parse(v);
        if (Number.isFinite(d)) {
            return d;
        }
    }
    return undefined;
}

/**
 * Normalises the options of a getHistory message (accepts the classic option names).
 *
 * @param raw `msg.message.options`
 * @param defaults instance defaults
 * @param defaults.limit default row limit
 * @param defaults.hardLimit absolute row limit
 * @param defaults.aggregate default aggregate
 * @param now current time
 */
export function normalizeHistoryOptions(
    raw: Record<string, unknown> | undefined,
    defaults: { limit: number; hardLimit: number; aggregate: string },
    now = Date.now(),
): { options: HistoryOptions; error?: string } {
    const o = raw ?? {};
    let end = o.end === undefined || o.end === null || o.end === '' ? now + 5000000 : toNumber(o.end);
    if (end === undefined) {
        return { options: {} as HistoryOptions, error: `Invalid end ${JSON.stringify(o.end)}` };
    }
    let start = o.start === undefined || o.start === null || o.start === '' ? undefined : toNumber(o.start);
    if (o.start !== undefined && o.start !== null && o.start !== '' && start === undefined) {
        return { options: {} as HistoryOptions, error: `Invalid start ${JSON.stringify(o.start)}` };
    }
    const countRaw = toNumber(o.count);
    const limitRaw = toNumber(o.limit);
    let aggregate = (
        typeof o.aggregate === 'string' && (AGGREGATES as readonly string[]).includes(o.aggregate)
            ? o.aggregate
            : defaults.aggregate
    ) as Aggregate;
    if (!(AGGREGATES as readonly string[]).includes(aggregate)) {
        aggregate = 'average';
    }
    let returnNewestEntries = o.returnNewestEntries === true;
    if (start === undefined && countRaw) {
        returnNewestEntries = true;
        start = 0;
    }
    if (start === undefined) {
        start = end - 86400000;
    }
    if (start > end) {
        [start, end] = [end, start];
    }
    const limit = Math.min(defaults.hardLimit, Math.max(1, Math.round(limitRaw ?? countRaw ?? defaults.limit)));
    const isRaw = aggregate === 'none' || aggregate === 'onchange';
    const count = Math.min(defaults.hardLimit, Math.max(1, Math.round(countRaw ?? (isRaw ? limit : 500))));
    let round: number | null = null;
    const r = toNumber(o.round);
    if (r !== undefined && Number.isInteger(r) && r >= 0 && r <= 15) {
        round = r;
    }
    const step = toNumber(o.step);
    let percentile = toNumber(o.percentile) ?? 50;
    if (percentile < 0 || percentile > 100) {
        percentile = 50;
    }
    let quantile = toNumber(o.quantile) ?? 0.5;
    if (quantile < 0 || quantile > 1) {
        quantile = 0.5;
    }
    return {
        options: {
            start: Math.round(start),
            end: Math.round(end),
            count,
            limit,
            step: step && step > 0 ? Math.round(step) : undefined,
            aggregate,
            ack: o.ack === true,
            q: o.q === true,
            from: o.from === true,
            addId: o.addId === true,
            returnNewestEntries,
            removeBorderValues: o.removeBorderValues === true,
            round,
            percentile,
            quantile,
            sessionId: o.sessionId,
        },
    };
}

/**
 * Effective aggregate for the value type (strings cannot be averaged → raw values).
 *
 * @param agg requested aggregate
 * @param type value type of the table
 */
export function effectiveAggregate(agg: Aggregate, type: ValueType): Aggregate {
    if (type === 'string' && agg !== 'none' && agg !== 'onchange' && agg !== 'count') {
        return 'none';
    }
    return agg;
}

/**
 * Builds the SQL statements for one getHistory request.
 *
 * @param t target table/field/state
 * @param o normalised options
 */
export function buildHistoryQueries(
    t: HistoryTarget,
    o: HistoryOptions,
): { queries: BuiltQuery[]; step: number; aggregate: Aggregate } {
    const table = quoteIdent(t.table);
    const field = quoteIdent(t.field);
    const startIso = new Date(o.start).toISOString();
    const endIso = new Date(o.end).toISOString();
    const base: string[] = [];
    const baseParams: Record<string, string> = {};
    if (t.columns.has('state_id')) {
        base.push('"state_id" = $sid');
        baseParams.sid = t.storeId;
    }
    base.push(`${field} IS NOT NULL`);
    const where = [...base, 'time >= $start', 'time <= $end'];
    const params: Record<string, string | number | boolean> = { ...baseParams, start: startIso, end: endIso };
    const aggregate = effectiveAggregate(o.aggregate, t.valueType);
    const ts = 'CAST(time AS BIGINT) / 1000000';

    if (aggregate === 'none' || aggregate === 'onchange') {
        const cols = [`${ts} AS ts`, `${field} AS val`];
        const meta: [keyof HistoryTarget['metaColumns'], boolean][] = [
            ['ack', o.ack],
            ['q', o.q],
            ['from', o.from],
        ];
        for (const [name, wanted] of meta) {
            const col = t.metaColumns[name];
            if (wanted && col && t.columns.has(col)) {
                cols.push(`${quoteIdent(col)} AS ${quoteIdent(name)}`);
            }
        }
        const select = cols.join(', ');
        const order = o.returnNewestEntries ? 'DESC' : 'ASC';
        const queries: BuiltQuery[] = [
            {
                kind: 'raw',
                sql: `SELECT ${select} FROM ${table} WHERE ${where.join(' AND ')} ORDER BY time ${order} LIMIT ${o.limit}`,
                params,
            },
        ];
        if (!o.removeBorderValues && !o.returnNewestEntries && o.start > 0) {
            // one value on each side of the range, so charts start and end at the borders
            queries.push({
                kind: 'borderBefore',
                sql: `SELECT ${select} FROM ${table} WHERE ${[...base, 'time < $start'].join(' AND ')} ORDER BY time DESC LIMIT 1`,
                params: { ...baseParams, start: startIso },
            });
            queries.push({
                kind: 'borderAfter',
                sql: `SELECT ${select} FROM ${table} WHERE ${[...base, 'time > $end'].join(' AND ')} ORDER BY time ASC LIMIT 1`,
                params: { ...baseParams, end: endIso },
            });
        }
        return { queries, step: 0, aggregate };
    }

    const span = Math.max(1, o.end - o.start);
    const step = Math.max(1, o.step ?? Math.ceil(span / Math.max(1, o.count)));
    const bin = `date_bin(INTERVAL '${step} milliseconds', time, TIMESTAMP '1970-01-01T00:00:00Z')`;
    const numeric = t.valueType === 'boolean' ? `CAST(${field} AS DOUBLE)` : field;
    const maxBins = Math.min(o.limit * 2, 1000000);

    if (aggregate === 'minmax') {
        return {
            queries: [
                {
                    kind: 'minmax',
                    sql:
                        `SELECT CAST(selector_min(${numeric}, time)['time'] AS BIGINT) / 1000000 AS tmin, ` +
                        `selector_min(${numeric}, time)['value'] AS vmin, ` +
                        `CAST(selector_max(${numeric}, time)['time'] AS BIGINT) / 1000000 AS tmax, ` +
                        `selector_max(${numeric}, time)['value'] AS vmax ` +
                        `FROM ${table} WHERE ${where.join(' AND ')} GROUP BY ${bin} ORDER BY 1 LIMIT ${maxBins}`,
                    params,
                },
            ],
            step,
            aggregate,
        };
    }

    let expr: string;
    switch (aggregate) {
        case 'min':
            expr = `min(${numeric})`;
            break;
        case 'max':
            expr = `max(${numeric})`;
            break;
        case 'total':
            expr = `sum(${numeric})`;
            break;
        case 'count':
            expr = `count(${field})`;
            break;
        case 'percentile':
            expr = `approx_percentile_cont(${numeric}, ${Number((o.percentile / 100).toFixed(6))})`;
            break;
        case 'quantile':
            expr = `approx_percentile_cont(${numeric}, ${Number(o.quantile.toFixed(6))})`;
            break;
        default:
            expr = `avg(${numeric})`;
    }
    return {
        queries: [
            {
                kind: 'agg',
                sql: `SELECT CAST(${bin} AS BIGINT) / 1000000 AS ts, ${expr} AS val FROM ${table} WHERE ${where.join(' AND ')} GROUP BY 1 ORDER BY 1 LIMIT ${o.limit}`,
                params,
            },
        ],
        step,
        aggregate,
    };
}

function roundVal(v: unknown, round: number | null): number | boolean | string | null {
    if (typeof v === 'number') {
        if (round === null) {
            return v;
        }
        const f = 10 ** round;
        return Math.round(v * f) / f;
    }
    if (typeof v === 'boolean' || typeof v === 'string') {
        return v;
    }
    return null;
}

/**
 * Converts result rows into ioBroker history entries (sorted ascending by ts).
 *
 * @param results rows per query
 * @param o options
 * @param step bin size (aggregated queries)
 * @param id state id for `addId`
 */
export function mapHistoryRows(
    results: { kind: BuiltQuery['kind']; rows: Record<string, unknown>[] }[],
    o: HistoryOptions,
    step: number,
    id: string,
): IobHistoryEntry[] {
    const out: IobHistoryEntry[] = [];
    const push = (e: IobHistoryEntry): void => {
        if (o.addId) {
            e.id = id;
        }
        out.push(e);
    };
    for (const { kind, rows } of results) {
        for (const row of rows) {
            if (kind === 'minmax') {
                const tmin = Number(row.tmin);
                const tmax = Number(row.tmax);
                const pts: IobHistoryEntry[] = [
                    { ts: tmin, val: roundVal(row.vmin, o.round) },
                    { ts: tmax, val: roundVal(row.vmax, o.round) },
                ].filter(p => Number.isFinite(p.ts));
                pts.sort((a, b) => a.ts - b.ts);
                if (pts.length === 2 && pts[0].ts === pts[1].ts) {
                    pts.pop();
                }
                pts.forEach(push);
                continue;
            }
            let ts = Number(row.ts);
            if (!Number.isFinite(ts)) {
                continue;
            }
            if (kind === 'agg') {
                ts += Math.floor(step / 2); // middle of the interval
            }
            const e: IobHistoryEntry = { ts, val: roundVal(row.val, o.round) };
            if (typeof row.ack === 'boolean') {
                e.ack = row.ack;
            } else if (row.ack === 'true' || row.ack === 'false') {
                e.ack = row.ack === 'true';
            }
            if (row.q !== undefined && row.q !== null && Number.isFinite(Number(row.q))) {
                e.q = Number(row.q);
            }
            if (typeof row.from === 'string') {
                e.from = row.from;
            }
            push(e);
        }
    }
    out.sort((a, b) => a.ts - b.ts);
    return out;
}
