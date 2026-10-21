/**
 * Connection health against a real InfluxDB 3 Core. Core has admin tokens only, so the restrictions of an
 * InfluxDB 3 Enterprise database token (403 for /ping and /health, optionally no read or no write permission) are
 * imitated by a small proxy in front of the server. Skipped unless INFLUX3_URL and INFLUX3_TOKEN are set:
 *
 *   eval "$(scripts/influx3-test-server.sh start)" && npm run test:influx
 */
import { strict as assert } from 'node:assert';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { assessHealth, needsDatabaseCheck, needsWriteProbe } from '../../src/lib/health';
import { type ClientOptions, InfluxV3Client } from '../../src/lib/influxClient';

const URL = process.env.INFLUX3_URL;
const TOKEN = process.env.INFLUX3_TOKEN;
const DB = 'iobtest';

/** Proxy that refuses some endpoints with 403 like InfluxDB 3 Enterprise does for a database token */
function restrictingProxy(deny: { ping?: boolean; read?: boolean; write?: boolean }): Promise<http.Server> {
    const target = new globalThis.URL(URL ?? '');
    const server = http.createServer((req, res) => {
        const path = req.url ?? '';
        if (
            (deny.ping && /^\/(ping|health)\b/.test(path)) ||
            (deny.read && path.startsWith('/api/v3/query_')) ||
            (deny.write && path.startsWith('/api/v3/write_lp'))
        ) {
            req.resume();
            res.writeHead(403, { 'content-type': 'application/json' });
            res.end('{"error":"permission denied"}');
            return;
        }
        const up = http.request(
            { hostname: target.hostname, port: target.port, path, method: req.method, headers: req.headers },
            r => {
                res.writeHead(r.statusCode ?? 502, r.headers);
                r.pipe(res);
            },
        );
        up.on('error', () => {
            res.writeHead(502);
            res.end();
        });
        req.pipe(up);
    });
    return new Promise(resolve => server.listen(0, '127.0.0.1', () => resolve(server)));
}

const options = (over: Partial<ClientOptions> = {}): ClientOptions => ({
    url: URL ?? '',
    token: TOKEN ?? '',
    database: DB,
    edition: 'auto',
    timeoutMs: 10_000,
    rejectUnauthorized: true,
    gzip: false,
    noSync: false,
    maxSockets: 2,
    ...over,
});

/** the adapter's decision for one client (no live writes) */
async function health(client: InfluxV3Client): Promise<ReturnType<typeof assessHealth>> {
    const base = { ping: await client.ping(), now: Date.now(), recentMs: 120_000 };
    if (!needsDatabaseCheck(base.ping)) {
        return assessHealth(base);
    }
    const read = await client.probeRead();
    const write = needsWriteProbe({ ...base, read }) ? await client.probeWrite() : undefined;
    return assessHealth({ ...base, read, write });
}

(URL && TOKEN ? describe : describe.skip)('connection health with restricted tokens (real InfluxDB 3 Core)', () => {
    const proxies: http.Server[] = [];
    const clients: InfluxV3Client[] = [];
    const via = async (deny: { ping?: boolean; read?: boolean; write?: boolean }): Promise<InfluxV3Client> => {
        const p = await restrictingProxy(deny);
        proxies.push(p);
        const c = new InfluxV3Client(options({ url: `http://127.0.0.1:${(p.address() as AddressInfo).port}` }));
        clients.push(c);
        return c;
    };

    before(async () => {
        // the database must exist for the write probe (an empty write would create it)
        const c = new InfluxV3Client(options());
        assert.equal((await c.write([`health_probe value=1 ${Date.UTC(2026, 9, 21)}`])).kind, 'ok');
        c.close();
    });
    after(() => {
        clients.forEach(c => c.close());
        proxies.forEach(p => p.close());
    });

    it('admin token: /ping, reading and writing allowed', async () => {
        const c = new InfluxV3Client(options());
        clients.push(c);
        assert.equal((await c.ping()).ok, true);
        assert.equal((await c.probeRead()).status, 'ok');
        const w = await c.probeWrite();
        assert.deepEqual([w.status, w.httpStatus], ['ok', 400], 'empty write: authorised, nothing written');
        assert.deepEqual(await health(c), { ok: true, pingAllowed: true });
    });

    it('database token (/ping refused): connected through the database', async () => {
        const c = await via({ ping: true });
        const ping = await c.ping();
        assert.deepEqual([ping.ok, ping.status], [false, 403]);
        assert.deepEqual(await health(c), { ok: true, pingAllowed: false, canRead: true });
    });

    it('write-only database token: connected through the empty write', async () => {
        const c = await via({ ping: true, read: true });
        assert.equal((await c.probeRead()).status, 'denied');
        assert.deepEqual(await health(c), { ok: true, pingAllowed: false, canRead: false });
    });

    it('read-only token: reachable, writing refused', async () => {
        const c = await via({ ping: true, write: true });
        assert.equal((await c.probeWrite()).status, 'denied');
        assert.equal((await c.write(['health_probe value=2 1'])).kind, 'auth');
    });

    it('wrong token: not connected, with a clear reason', async () => {
        const c = new InfluxV3Client(options({ token: 'apiv3_wrong' }));
        clients.push(c);
        const h = await health(c);
        assert.equal(h.ok, false);
        assert.match(h.error ?? '', /authentication failed - the token may neither read \/ping nor read or write/);
    });

    it('reading a missing database creates nothing', async () => {
        const c = new InfluxV3Client(options({ database: 'iob_missing_probe' }));
        clients.push(c);
        assert.equal((await c.probeRead()).status, 'missing');
        assert.ok(!(await c.listDatabases()).includes('iob_missing_probe'));
    });
});
