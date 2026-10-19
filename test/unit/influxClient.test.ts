import { strict as assert } from 'node:assert';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { gunzipSync } from 'node:zlib';
import { classifyWrite, HttpError, InfluxV3Client, parseRetryAfter, TransportError } from '../../src/lib/influxClient';
import { clearSecrets } from '../../src/lib/redact';

const res = (status: number, body = '', headers: Record<string, string> = {}) => ({ status, body, headers });

describe('InfluxV3Client', () => {
    describe('classifyWrite (real response bodies of Core 3.12.1)', () => {
        it('204 → ok', () => assert.deepEqual(classifyWrite(res(204)), { kind: 'ok' }));
        it('partial write', () => {
            const r = classifyWrite(
                res(
                    400,
                    '{"error":"partial write of line protocol occurred","data":[{"error_message":"invalid column type for column \'value\', expected iox::column_type::field::float, got iox::column_type::field::string","line_number":1,"original_line":"temperature,state_id"}]}',
                ),
            );
            assert.equal(r.kind, 'partial');
            assert.equal(r.kind === 'partial' && r.rejected[0].line, 1);
        });
        it('limits (422) per line are partial too', () => {
            const r = classifyWrite(
                res(
                    422,
                    '{"error":"partial write of line protocol occurred","data":[{"error_message":"Schema update for table \'wide\' would exceed the column limit","line_number":3,"original_line":"wide"}]}',
                ),
            );
            assert.equal(r.kind, 'partial');
        });
        it('accept_partial=false parse error → invalid', () => {
            const r = classifyWrite(
                res(
                    400,
                    '{"error":"line protocol parsing error","data":{"error_message":"bad","line_number":1,"original_line":"x"}}',
                ),
            );
            assert.equal(r.kind, 'invalid');
        });
        it('database limit (422 without lines) → invalid', () => {
            assert.equal(
                classifyWrite(res(422, '{"error":"Adding a new database would exceed limit of 5 databases"}')).kind,
                'invalid',
            );
        });
        it('413 → tooLarge', () =>
            assert.equal(classifyWrite(res(413, 'max request size (10485760 bytes) exceeded')).kind, 'tooLarge'));
        it('401/403 → auth', () => {
            assert.equal(classifyWrite(res(401, '{"error": "the request was not authenticated"}')).kind, 'auth');
            assert.equal(classifyWrite(res(403, '')).kind, 'auth');
        });
        it('404 → config', () => assert.equal(classifyWrite(res(404, 'not found')).kind, 'config'));
        it('5xx/429/408 and network errors → transient', () => {
            assert.equal(classifyWrite(res(500)).kind, 'transient');
            assert.equal(classifyWrite(res(503)).kind, 'transient');
            const r = classifyWrite(res(429, '', { 'retry-after': '7' }));
            assert.deepEqual(r, { kind: 'transient', status: 429, message: 'HTTP 429', retryAfterMs: 7000 });
            assert.equal(classifyWrite(res(408)).kind, 'transient');
            assert.equal(classifyWrite(new TransportError('ECONNREFUSED', 'ECONNREFUSED')).kind, 'transient');
        });
        it('removes secrets from messages', () => {
            const r = classifyWrite(res(400, '{"error":"bad token apiv3_secretsecret"}'));
            assert.ok(r.kind === 'invalid' && !r.message.includes('apiv3_secretsecret'));
        });
        it('parseRetryAfter', () => {
            assert.equal(parseRetryAfter('3'), 3000);
            assert.equal(parseRetryAfter(undefined), undefined);
            assert.equal(
                parseRetryAfter('Wed, 21 Oct 2015 07:28:10 GMT', Date.parse('Wed, 21 Oct 2015 07:28:00 GMT')),
                10000,
            );
            assert.equal(parseRetryAfter('99999'), 3600000);
        });
    });

    describe('against a fake HTTP server', () => {
        let server: http.Server;
        let url: string;
        let last: { method?: string; url?: string; headers: http.IncomingHttpHeaders; body: Buffer };
        let respond: (req: http.IncomingMessage, res: http.ServerResponse, body: Buffer) => void;

        before(async () => {
            server = http.createServer((req, res) => {
                const chunks: Buffer[] = [];
                req.on('data', (c: Buffer) => chunks.push(c));
                req.on('end', () => {
                    const body = Buffer.concat(chunks);
                    last = { method: req.method, url: req.url, headers: req.headers, body };
                    respond(req, res, body);
                });
            });
            await new Promise<void>(r => server.listen(0, '127.0.0.1', () => r()));
            url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
        });
        after(() => {
            server.close();
            clearSecrets();
        });

        const client = (over: Partial<ConstructorParameters<typeof InfluxV3Client>[0]> = {}) =>
            new InfluxV3Client({
                url,
                token: 'apiv3_testtoken',
                database: 'iob',
                edition: 'auto',
                timeoutMs: 500,
                rejectUnauthorized: true,
                gzip: true,
                noSync: false,
                maxSockets: 2,
                ...over,
            });

        it('ping detects edition and version and sends the bearer token', async () => {
            respond = (_req, res) => {
                res.writeHead(200, {
                    'x-influxdb-build': 'Core',
                    'x-influxdb-version': '3.12.1',
                    'content-type': 'application/json',
                });
                res.end('{"product_name":"InfluxDB 3 Core","version":"3.12.1"}');
            };
            const c = client();
            const p = await c.ping();
            c.close();
            assert.equal(p.ok, true);
            assert.equal(p.edition, 'core');
            assert.equal(p.version, '3.12.1');
            assert.equal(last.headers.authorization, 'Bearer apiv3_testtoken');
        });

        it('ping reports authentication problems without the token', async () => {
            respond = (_req, res) => {
                res.writeHead(401);
                res.end('{"error": "the request was not authenticated"}');
            };
            const c = client();
            const p = await c.ping();
            c.close();
            assert.equal(p.ok, false);
            assert.ok(p.error?.includes('authentication'));
            assert.ok(!JSON.stringify(p).includes('apiv3_testtoken'));
        });

        it('write uses /api/v3/write_lp with db, precision=millisecond and gzip', async () => {
            respond = (_req, res) => {
                res.writeHead(204);
                res.end();
            };
            const c = client({ noSync: true });
            const lines = Array.from({ length: 100 }, (_, i) => `t,state_id=s v=${i} ${1792418400000 + i}`);
            const r = await c.write(lines);
            c.close();
            assert.deepEqual(r, { kind: 'ok' });
            assert.equal(last.method, 'POST');
            assert.equal(last.url, '/api/v3/write_lp?db=iob&precision=millisecond&no_sync=true');
            assert.equal(last.headers['content-encoding'], 'gzip');
            assert.equal(gunzipSync(last.body).toString(), lines.join('\n'));
        });

        it('small bodies are not compressed; cloud edition uses /api/v2/write with Token auth', async () => {
            respond = (_req, res) => {
                res.writeHead(204);
                res.end();
            };
            const c = client({ edition: 'cloud' });
            await c.write(['t v=1 1']);
            c.close();
            assert.equal(last.url, '/api/v2/write?bucket=iob&precision=ms');
            assert.equal(last.headers['content-encoding'], undefined);
            assert.equal(last.headers.authorization, 'Token apiv3_testtoken');
            await assert.rejects(() => client({ edition: 'cloud' }).querySql('SELECT 1'), HttpError);
        });

        it('keeps a path prefix of the URL (reverse proxy)', async () => {
            respond = (_req, res) => {
                res.writeHead(204);
                res.end();
            };
            const c = client({ url: `${url}/influx` });
            await c.write(['t v=1 1']);
            c.close();
            assert.ok(last.url?.startsWith('/influx/api/v3/write_lp?'));
        });

        it('timeouts are transient write errors', async () => {
            respond = () => undefined; // never answers
            const c = client({ timeoutMs: 100 });
            const r = await c.write(['t v=1 1']);
            c.close();
            assert.equal(r.kind, 'transient');
            assert.ok(r.kind === 'transient' && r.message.includes('timeout'));
        });

        it('connection refused is transient', async () => {
            const c = client({ url: 'http://127.0.0.1:1' });
            const r = await c.write(['t v=1 1']);
            c.close();
            assert.equal(r.kind, 'transient');
        });

        it('querySql posts JSON with params and parses rows', async () => {
            respond = (_req, res) => {
                res.writeHead(200, { 'content-type': 'application/json' });
                res.end('[{"ts":1792418400000,"val":38.7}]');
            };
            const c = client();
            const rows = await c.querySql('SELECT 1 WHERE x = $a', { a: 'b' });
            c.close();
            assert.deepEqual(rows, [{ ts: 1792418400000, val: 38.7 }]);
            assert.equal(last.url, '/api/v3/query_sql');
            assert.deepEqual(JSON.parse(last.body.toString()), {
                db: 'iob',
                q: 'SELECT 1 WHERE x = $a',
                format: 'json',
                params: { a: 'b' },
            });
        });

        it('query errors become HttpError with the first line of the server message', async () => {
            respond = (_req, res) => {
                res.writeHead(400);
                res.end("Error during planning: table 'public.iox.nope' not found\nmore");
            };
            const c = client();
            await assert.rejects(
                () => c.querySql('SELECT * FROM nope'),
                (e: unknown) =>
                    e instanceof HttpError &&
                    e.status === 400 &&
                    e.message === "Error during planning: table 'public.iox.nope' not found",
            );
            c.close();
        });

        it('response size limit aborts large responses', async () => {
            respond = (_req, res) => {
                res.writeHead(200);
                res.end(`[${'1,'.repeat(10000)}1]`);
            };
            const c = client();
            await assert.rejects(() => c.querySql('SELECT 1', undefined, { maxBytes: 1000 }), TransportError);
            c.close();
        });

        it('listDatabases and createDatabase', async () => {
            respond = (req, res) => {
                if (req.method === 'GET') {
                    res.writeHead(200);
                    res.end('[{"iox::database":"_internal"},{"iox::database":"iob"}]');
                } else {
                    res.writeHead(200);
                    res.end();
                }
            };
            const c = client();
            assert.deepEqual(await c.listDatabases(), ['_internal', 'iob']);
            await c.createDatabase('new_db', '30d');
            c.close();
            assert.deepEqual(JSON.parse(last.body.toString()), { db: 'new_db', retention_period: '30d' });
        });

        it('a closed client rejects new requests', async () => {
            const c = client();
            c.close();
            await assert.rejects(() => c.request('GET', '/ping'), TransportError);
        });
    });
});
