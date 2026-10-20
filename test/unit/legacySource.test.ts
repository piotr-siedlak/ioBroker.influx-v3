import { strict as assert } from 'node:assert';
import {
    buildFluxCount,
    buildFluxFirstTime,
    buildFluxRead,
    buildFluxReadMany,
    buildFluxTimeRange,
    buildInfluxQLRead,
    buildInfluxQLReadMany,
    countTagUses,
    fluxTablesToPointMap,
    fluxTablesToPoints,
    influxQLSeriesToPointMap,
    influxQLSeriesToPoints,
    LegacyReadError,
    parseAnnotatedCsv,
    parseCsvRecords,
    parseInfluxQLResponse,
    parseRfc3339,
    quoteFluxString,
    quoteInfluxQLIdentifier,
    toLegacyPoint,
} from '../../src/lib/legacySource';

describe('legacy source (InfluxDB 1.x/2.x reader)', () => {
    describe('query text', () => {
        it('quotes identifiers and strings so that ids cannot break out', () => {
            assert.equal(quoteInfluxQLIdentifier('hm-rpc.0.ABC.1.TEMP'), '"hm-rpc.0.ABC.1.TEMP"');
            assert.equal(quoteInfluxQLIdentifier('a"; DROP DATABASE x; "'), '"a\\"; DROP DATABASE x; \\""');
            assert.equal(quoteInfluxQLIdentifier('a\\b'), '"a\\\\b"');
            assert.equal(quoteFluxString('x") |> drop() //'), '"x\\") |> drop() //"');
            assert.equal(quoteFluxString('a${b}'), '"a\\${b}"');
        });

        it('InfluxQL read: half-open window, tags separated, optional retention policy', () => {
            assert.equal(
                buildInfluxQLRead('a.0.b', '', 1000, 2000),
                'SELECT * FROM "a.0.b" WHERE time >= 1000ms AND time < 2000ms GROUP BY *',
            );
            assert.match(buildInfluxQLRead('a.0.b', 'autogen', 1, 2), /FROM "autogen"\."a\.0\.b" WHERE/);
        });

        it('reads of many measurements: a list (quoted) or the whole database', () => {
            assert.equal(
                buildInfluxQLReadMany(['a.0.b', 'BUFOR-TOP'], '', 1000, 2000),
                'SELECT * FROM "a.0.b" WHERE time >= 1000ms AND time < 2000ms GROUP BY *; SELECT * FROM "BUFOR-TOP" WHERE time >= 1000ms AND time < 2000ms GROUP BY *',
            );
            assert.match(buildInfluxQLReadMany(['x'], 'autogen', 1, 2), /FROM "autogen"\."x" WHERE/);
            const q = buildFluxReadMany('iobroker', ['a', 'b"x'], 0, 1000);
            assert.match(q, /filter\(fn: \(r\) => r\._measurement == "a" or r\._measurement == "b\\"x"\)/);
            assert.match(q, /pivot\(rowKey: \["_time"\]/);
            assert.doesNotMatch(buildFluxReadMany('iobroker', 'all', 0, 1000), /_measurement ==/);
            assert.match(
                buildFluxCount('iobroker', ['a'], 0, 1000),
                /r\._measurement == "a"\)\n {2}\|> filter\(fn: \(r\) => r\._field == "value"\)\n {2}\|> count\(\) \|> group\(\) \|> sum\(\)$/,
            );
            const f = buildFluxFirstTime('iobroker', ['a']);
            assert.match(f, /r\._measurement == "a"/);
            assert.match(f, /r\._field == "value"/);
            assert.match(
                f,
                /first\(\) \|> keep\(columns: \["_time"\]\) \|> group\(\) \|> sort\(columns: \["_time"\]\) \|> limit\(n: 1\)/,
            );
        });

        it('Flux read: range, measurement filter and pivot', () => {
            const q = buildFluxRead('iobroker', 'a.0.b', Date.UTC(2024, 0, 1), Date.UTC(2024, 0, 2));
            assert.match(q, /from\(bucket: "iobroker"\)/);
            assert.match(
                q,
                /start: time\(v: "2024-01-01T00:00:00.000Z"\), stop: time\(v: "2024-01-02T00:00:00.000Z"\)/,
            );
            assert.match(q, /r\._measurement == "a\.0\.b"/);
            assert.match(q, /pivot\(rowKey: \["_time"\], columnKey: \["_field"\], valueColumn: "_value"\)/);
            assert.match(buildFluxTimeRange('b', 'm'), /yield\(name: "first"\)[\s\S]*yield\(name: "last"\)/);
        });
    });

    it('counts tag uses per measurement without metadata keys', () => {
        assert.deepEqual(
            countTagUses([
                { key: 'room', value: 'Salon' },
                { key: 'room', value: 'Salon' },
                { key: 'room', value: 'Garaż' },
                { key: 'ack', value: 'true' },
                { key: 'from', value: 'js.0' },
                { key: '_measurement', value: 'x' },
                { key: 'floor', value: '' },
            ]),
            [
                { key: 'room', value: 'Salon', measurements: 2 },
                { key: 'room', value: 'Garaż', measurements: 1 },
            ],
        );
    });

    it('parses RFC 3339 times with any fraction and offset', () => {
        assert.equal(parseRfc3339('2024-01-01T00:00:00Z'), Date.UTC(2024, 0, 1));
        assert.equal(parseRfc3339('2024-01-01T00:00:00.123456789Z'), Date.UTC(2024, 0, 1) + 123);
        assert.equal(parseRfc3339('2024-01-01T02:00:00.5+02:00'), Date.UTC(2024, 0, 1) + 500);
        assert.ok(Number.isNaN(parseRfc3339('yesterday')));
    });

    it('metadata comes from fields or tags, custom tags stay tags', () => {
        assert.deepEqual(
            toLegacyPoint(5, { value: 1.5, ack: true, q: 0, from: 'system.adapter.x.0' }, { room: 'Kitchen' }),
            { ts: 5, value: 1.5, ack: true, q: 0, from: 'system.adapter.x.0', tags: { room: 'Kitchen' } },
        );
        assert.deepEqual(toLegacyPoint(5, { value: 'on' }, { ack: 'false', q: '16', from: 'js.0', room: '' }), {
            ts: 5,
            value: 'on',
            ack: false,
            q: 16,
            from: 'js.0',
            tags: {},
        });
        assert.equal(toLegacyPoint(5, { ack: true }, {}), undefined, 'no value field');
        assert.equal(toLegacyPoint(NaN, { value: 1 }, {}), undefined);
    });

    describe('InfluxDB 1.x JSON', () => {
        it('converts series with tags into sorted points', () => {
            const body = JSON.stringify({
                results: [
                    {
                        statement_id: 0,
                        series: [
                            {
                                name: 'a.0.b',
                                tags: { room: 'Kitchen' },
                                columns: ['time', 'ack', 'from', 'q', 'value'],
                                values: [
                                    [2000, true, 'system.adapter.js.0', 0, 21.5],
                                    [1000, false, null, 0, 20],
                                ],
                            },
                            {
                                name: 'a.0.b',
                                tags: { room: '' },
                                columns: ['time', 'ack', 'from', 'q', 'value'],
                                values: [[1500, true, '', 0, 20.5]],
                            },
                        ],
                    },
                ],
            });
            const [series] = parseInfluxQLResponse(body);
            const points = influxQLSeriesToPoints(series);
            assert.deepEqual(
                points.map(p => [p.ts, p.value, p.tags.room]),
                [
                    [1000, 20, 'Kitchen'],
                    [1500, 20.5, undefined],
                    [2000, 21.5, 'Kitchen'],
                ],
            );
            assert.equal(points[2].from, 'system.adapter.js.0');
            assert.equal(points[0].from, undefined);
        });

        it('groups series of several measurements by name', () => {
            const map = influxQLSeriesToPointMap([
                { name: 'b', columns: ['time', 'value'], values: [[2, 1]] },
                { name: 'a', tags: { room: 'x' }, columns: ['time', 'value'], values: [[5, 1]] },
                { name: 'a', tags: { room: 'y' }, columns: ['time', 'value'], values: [[3, 2]] },
            ]);
            assert.deepEqual(
                map.get('a')?.map(p => [p.ts, p.tags.room]),
                [
                    [3, 'y'],
                    [5, 'x'],
                ],
            );
            assert.equal(map.get('b')?.length, 1);
        });

        it('reports statement errors and invalid answers', () => {
            assert.throws(
                () => parseInfluxQLResponse('{"results":[{"statement_id":0,"error":"database not found: x"}]}'),
                (e: unknown) => e instanceof LegacyReadError && e.kind === 'notFound',
            );
            assert.throws(() => parseInfluxQLResponse('{"error":"authorization failed"}'), LegacyReadError);
            assert.throws(() => parseInfluxQLResponse('<html>'), LegacyReadError);
            assert.deepEqual(parseInfluxQLResponse('{"results":[{"statement_id":0}]}'), [[]]);
        });
    });

    describe('InfluxDB 2.x annotated CSV', () => {
        it('line breaks inside cells come back as \\n (InfluxDB writes them as CRLF)', () => {
            assert.deepEqual(parseCsvRecords('"a\r\nb",c\r\n'), [['a\nb', 'c']]);
        });

        it('splits CSV records with quoted commas, quotes and line breaks', () => {
            assert.deepEqual(parseCsvRecords('a,"b,c","d ""e""",\r\n"x\ny",z\n'), [
                ['a', 'b,c', 'd "e"', ''],
                ['x\ny', 'z'],
            ]);
        });

        const csv = [
            '#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,boolean,string,double,double',
            '#group,false,false,true,true,false,true,true,false,false,false,false',
            '#default,_result,,,,,,,,,,',
            ',result,table,_start,_stop,_time,_measurement,room,ack,from,q,value',
            ',,0,2024-01-01T00:00:00Z,2024-01-02T00:00:00Z,2024-01-01T10:00:00.5Z,a.0.b,"Kitchen, north",true,system.adapter.js.0,0,21.5',
            ',,0,2024-01-01T00:00:00Z,2024-01-02T00:00:00Z,2024-01-01T09:00:00Z,a.0.b,"Kitchen, north",false,,0,20',
            '',
            '#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,string,string,string',
            '#group,false,false,true,true,false,true,true,true,true,false',
            '#default,_result,,,,,,,,,',
            ',result,table,_start,_stop,_time,_measurement,ack,from,q,value',
            ',,1,2024-01-01T00:00:00Z,2024-01-02T00:00:00Z,2024-01-01T11:00:00Z,a.0.b,true,admin.0,16,"text, with ""quotes"""',
            '',
        ].join('\r\n');

        it('reads blocks with different schemas (meta as fields and as tags)', () => {
            const tables = parseAnnotatedCsv(csv);
            assert.equal(tables.length, 2);
            const points = fluxTablesToPoints(tables);
            assert.deepEqual(
                points.map(p => p.ts),
                [Date.UTC(2024, 0, 1, 9), Date.UTC(2024, 0, 1, 10) + 500, Date.UTC(2024, 0, 1, 11)],
            );
            assert.deepEqual(points[1], {
                ts: Date.UTC(2024, 0, 1, 10) + 500,
                value: 21.5,
                ack: true,
                q: 0,
                from: 'system.adapter.js.0',
                tags: { room: 'Kitchen, north' },
            });
            assert.equal(points[0].from, undefined, 'empty from is dropped');
            assert.deepEqual(points[2], {
                ts: Date.UTC(2024, 0, 1, 11),
                value: 'text, with "quotes"',
                ack: true,
                q: 16,
                from: 'admin.0',
                tags: {},
            });
        });

        it('groups the points of several measurements', () => {
            const map = fluxTablesToPointMap(parseAnnotatedCsv(csv));
            assert.deepEqual([...map.keys()], ['a.0.b']);
            assert.equal(map.get('a.0.b')?.length, 3);
            const many = fluxTablesToPointMap(
                parseAnnotatedCsv(csv.replace(/,a\.0\.b,true,admin/, ',other,true,admin')),
            );
            assert.deepEqual(
                [...many.entries()].map(([m, p]) => [m, p.length]),
                [
                    ['a.0.b', 2],
                    ['other', 1],
                ],
            );
        });

        it('throws on error tables and accepts empty answers', () => {
            const err =
                '#datatype,string,string\r\n#group,true,true\r\n#default,,\r\n,error,reference\r\n,"bucket ""x"" not found",\r\n';
            assert.throws(() => parseAnnotatedCsv(err), /bucket "x" not found/);
            assert.deepEqual(parseAnnotatedCsv(''), []);
            assert.deepEqual(fluxTablesToPoints(parseAnnotatedCsv('\r\n')), []);
        });
    });
});
