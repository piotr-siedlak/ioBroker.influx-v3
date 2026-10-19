import { strict as assert } from 'node:assert';
import {
    buildHistoryQueries,
    effectiveAggregate,
    type HistoryTarget,
    mapHistoryRows,
    normalizeHistoryOptions,
    quoteIdent,
} from '../../src/lib/historyService';

const defaults = { limit: 2000, hardLimit: 100000, aggregate: 'average' };
const NOW = Date.UTC(2026, 9, 19, 12, 0, 0);

const target = (over: Partial<HistoryTarget> = {}): HistoryTarget => ({
    table: 'numeric_states',
    field: 'value',
    valueType: 'number',
    storeId: 'heat.0.hc1.temperature',
    columns: new Set(['time', 'value', 'state_id', 'ack', 'q']),
    metaColumns: { ack: 'ack', q: 'q' },
    ...over,
});

describe('historyService', () => {
    describe('normalizeHistoryOptions', () => {
        it('defaults: last 24 h, average, default limit', () => {
            const { options } = normalizeHistoryOptions({}, defaults, NOW);
            assert.equal(options.end, NOW + 5000000);
            assert.equal(options.start, options.end - 86400000);
            assert.equal(options.aggregate, 'average');
            assert.equal(options.limit, 2000);
            assert.equal(options.count, 500);
        });
        it('accepts ISO strings and swaps start/end', () => {
            const { options } = normalizeHistoryOptions(
                { start: '2026-10-19T14:00:00Z', end: '2026-10-19T13:00:00Z', aggregate: 'none' },
                defaults,
                NOW,
            );
            assert.equal(options.start, Date.UTC(2026, 9, 19, 13));
            assert.equal(options.end, Date.UTC(2026, 9, 19, 14));
        });
        it('count without start means newest entries', () => {
            const { options } = normalizeHistoryOptions({ count: 10, aggregate: 'none' }, defaults, NOW);
            assert.equal(options.returnNewestEntries, true);
            assert.equal(options.start, 0);
            assert.equal(options.limit, 10);
        });
        it('limits are capped by the hard limit; invalid values rejected', () => {
            assert.equal(normalizeHistoryOptions({ limit: 10_000_000 }, defaults, NOW).options.limit, 100000);
            assert.ok(normalizeHistoryOptions({ start: 'yesterday' }, defaults, NOW).error);
            assert.equal(normalizeHistoryOptions({ aggregate: 'bogus' }, defaults, NOW).options.aggregate, 'average');
            assert.equal(normalizeHistoryOptions({ percentile: 300 }, defaults, NOW).options.percentile, 50);
            assert.equal(normalizeHistoryOptions({ round: 2 }, defaults, NOW).options.round, 2);
            assert.equal(normalizeHistoryOptions({ round: -1 }, defaults, NOW).options.round, null);
        });
    });

    it('quotes identifiers', () => {
        assert.equal(quoteIdent('a"b'), '"a""b"');
        assert.equal(quoteIdent('hm-rpc.0.x'), '"hm-rpc.0.x"');
    });

    it('strings cannot be aggregated (raw values instead)', () => {
        assert.equal(effectiveAggregate('average', 'string'), 'none');
        assert.equal(effectiveAggregate('count', 'string'), 'count');
        assert.equal(effectiveAggregate('average', 'boolean'), 'average');
    });

    describe('buildHistoryQueries', () => {
        const opts = (o: Record<string, unknown>) => normalizeHistoryOptions(o, defaults, NOW).options;

        it('raw query: parameterised id and time range, requested metadata, border values', () => {
            const { queries } = buildHistoryQueries(
                target(),
                opts({ start: 1000, end: 2000, aggregate: 'none', ack: true, q: true }),
            );
            assert.equal(queries.length, 3);
            const raw = queries[0];
            assert.equal(
                raw.sql,
                'SELECT CAST(time AS BIGINT) / 1000000 AS ts, "value" AS val, "ack" AS "ack", "q" AS "q" FROM "numeric_states" WHERE "state_id" = $sid AND "value" IS NOT NULL AND time >= $start AND time <= $end ORDER BY time ASC LIMIT 2000',
            );
            assert.deepEqual(raw.params, {
                sid: 'heat.0.hc1.temperature',
                start: '1970-01-01T00:00:01.000Z',
                end: '1970-01-01T00:00:02.000Z',
            });
            assert.ok(queries[1].sql.includes('time < $start ORDER BY time DESC LIMIT 1'));
            assert.deepEqual(Object.keys(queries[1].params).sort(), ['sid', 'start']);
            assert.ok(queries[2].sql.includes('time > $end ORDER BY time ASC LIMIT 1'));
        });

        it('the state id is never interpolated into SQL', () => {
            const evil = "x' OR 1=1 --";
            const { queries } = buildHistoryQueries(
                target({ storeId: evil, table: 'a"b' }),
                opts({ aggregate: 'none' }),
            );
            for (const q of queries) {
                assert.ok(!q.sql.includes(evil));
                assert.ok(q.sql.includes('FROM "a""b"'));
            }
        });

        it('metadata is only selected when requested and present', () => {
            const t = target({ columns: new Set(['time', 'value', 'state_id']), metaColumns: {} });
            const { queries } = buildHistoryQueries(
                t,
                opts({ aggregate: 'none', ack: true, from: true, removeBorderValues: true }),
            );
            assert.equal(queries.length, 1);
            assert.ok(!queries[0].sql.includes('ack'));
        });

        it('per-state tables without state_id are queried without the id filter', () => {
            const t = target({ table: 'hm-rpc.0.x', columns: new Set(['time', 'value']) });
            const { queries } = buildHistoryQueries(t, opts({ aggregate: 'none', removeBorderValues: true }));
            assert.ok(!queries[0].sql.includes('state_id'));
            assert.ok(!('sid' in queries[0].params));
        });

        it('newest entries are read descending, without border values', () => {
            const { queries } = buildHistoryQueries(target(), opts({ count: 5, aggregate: 'none' }));
            assert.equal(queries.length, 1);
            assert.ok(queries[0].sql.includes('ORDER BY time DESC LIMIT 5'));
        });

        it('aggregates use date_bin with a computed step', () => {
            const { queries, step } = buildHistoryQueries(
                target(),
                opts({ start: 0, end: 3600_000, count: 60, aggregate: 'average' }),
            );
            assert.equal(step, 60_000);
            assert.ok(
                queries[0].sql.includes(
                    "date_bin(INTERVAL '60000 milliseconds', time, TIMESTAMP '1970-01-01T00:00:00Z')",
                ),
            );
            assert.ok(queries[0].sql.includes('avg("value") AS val'));
            for (const [agg, expr] of [
                ['min', 'min("value")'],
                ['max', 'max("value")'],
                ['total', 'sum("value")'],
                ['count', 'count("value")'],
                ['percentile', 'approx_percentile_cont("value", 0.9)'],
                ['quantile', 'approx_percentile_cont("value", 0.25)'],
            ]) {
                const q = buildHistoryQueries(
                    target(),
                    opts({ aggregate: agg, percentile: 90, quantile: 0.25, step: 1000 }),
                ).queries[0];
                assert.ok(q.sql.includes(expr), `${agg}: ${q.sql}`);
                assert.ok(q.sql.includes("INTERVAL '1000 milliseconds'"));
            }
        });

        it('booleans are cast to double for numeric aggregates (duty cycle)', () => {
            const { queries } = buildHistoryQueries(
                target({ valueType: 'boolean', table: 'boolean_states' }),
                opts({ aggregate: 'average', step: 1000 }),
            );
            assert.ok(queries[0].sql.includes('avg(CAST("value" AS DOUBLE))'));
        });

        it('minmax uses selector_min/selector_max', () => {
            const { queries } = buildHistoryQueries(target(), opts({ aggregate: 'minmax', step: 1000 }));
            assert.equal(queries[0].kind, 'minmax');
            assert.ok(queries[0].sql.includes('selector_min("value", time)[\'value\'] AS vmin'));
            assert.ok(queries[0].sql.includes('selector_max("value", time)[\'time\']'));
        });
    });

    describe('mapHistoryRows', () => {
        const o = normalizeHistoryOptions({ aggregate: 'none', addId: true, round: 1 }, defaults, NOW).options;
        it('maps raw rows incl. metadata and border values, sorted', () => {
            const out = mapHistoryRows(
                [
                    { kind: 'raw', rows: [{ ts: 2000, val: 21.26, ack: true, q: 0 }] },
                    { kind: 'borderBefore', rows: [{ ts: 1000, val: 20.04 }] },
                    { kind: 'borderAfter', rows: [] },
                ],
                o,
                0,
                'x.0.t',
            );
            assert.deepEqual(out, [
                { ts: 1000, val: 20, id: 'x.0.t' },
                { ts: 2000, val: 21.3, ack: true, q: 0, id: 'x.0.t' },
            ]);
        });
        it('aggregated rows get the middle of the interval', () => {
            const out = mapHistoryRows(
                [{ kind: 'agg', rows: [{ ts: 0, val: 1 }] }],
                { ...o, addId: false, round: null },
                60_000,
                'x',
            );
            assert.deepEqual(out, [{ ts: 30_000, val: 1 }]);
        });
        it('minmax rows produce two points with real times (one if equal)', () => {
            const out = mapHistoryRows(
                [
                    {
                        kind: 'minmax',
                        rows: [
                            { tmin: 50, vmin: 1, tmax: 10, vmax: 9 },
                            { tmin: 100, vmin: 5, tmax: 100, vmax: 5 },
                        ],
                    },
                ],
                { ...o, addId: false, round: null },
                1000,
                'x',
            );
            assert.deepEqual(out, [
                { ts: 10, val: 9 },
                { ts: 50, val: 1 },
                { ts: 100, val: 5 },
            ]);
        });
    });
});
