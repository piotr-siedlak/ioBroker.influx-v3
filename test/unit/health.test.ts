import { strict as assert } from 'node:assert';
import { assessHealth, type HealthInput, needsDatabaseCheck, needsWriteProbe } from '../../src/lib/health';
import type { PingResult, ProbeResult } from '../../src/lib/influxClient';

const NOW = 1_000_000;
const pingOk: PingResult = { ok: true, status: 200, edition: 'enterprise', version: '3.12.1', latencyMs: 3 };
const pingDenied: PingResult = {
    ok: false,
    status: 403,
    latencyMs: 3,
    error: 'authentication failed (check the API token)',
};
const pingDown: PingResult = { ok: false, status: 0, latencyMs: 3, error: 'ECONNREFUSED: connect ECONNREFUSED' };
const probe = (status: ProbeResult['status'], httpStatus = 200): ProbeResult => ({ status, httpStatus });
const input = (over: Partial<HealthInput>): HealthInput => ({ ping: pingOk, now: NOW, recentMs: 120_000, ...over });

describe('connection health (info.connection)', () => {
    it('/ping allowed: connected, edition known', () => {
        assert.deepEqual(assessHealth(input({})), { ok: true, pingAllowed: true });
        assert.equal(needsDatabaseCheck(pingOk), false);
    });

    it('server not reachable: not connected, the database is not probed', () => {
        assert.equal(needsDatabaseCheck(pingDown), false);
        assert.deepEqual(assessHealth(input({ ping: pingDown })), {
            ok: false,
            pingAllowed: false,
            error: 'ECONNREFUSED: connect ECONNREFUSED',
        });
        assert.equal(assessHealth(input({ ping: { ok: false, status: 503, latencyMs: 1 } })).error, 'HTTP 503');
    });

    it('Enterprise database token (/ping refused, database readable): connected', () => {
        assert.equal(needsDatabaseCheck(pingDenied), true);
        assert.deepEqual(assessHealth(input({ ping: pingDenied, read: probe('ok') })), {
            ok: true,
            pingAllowed: false,
            canRead: true,
        });
        assert.equal(
            assessHealth(input({ ping: pingDenied, read: probe('missing', 404) })).ok,
            true,
            'database not created yet - the first write creates it',
        );
        assert.equal(needsWriteProbe(input({ ping: pingDenied, read: probe('ok') })), false);
    });

    it('write-only token: a recent write proves the connection, else the empty write does', () => {
        const base = input({ ping: pingDenied, read: probe('denied', 403) });
        assert.equal(needsWriteProbe({ ...base, lastWriteOk: NOW - 60_000 }), false);
        assert.deepEqual(assessHealth({ ...base, lastWriteOk: NOW - 60_000 }), {
            ok: true,
            pingAllowed: false,
            canRead: false,
        });
        assert.equal(needsWriteProbe({ ...base, lastWriteOk: NOW - 600_000 }), true, 'too old');
        assert.equal(needsWriteProbe(base), true);
        assert.equal(assessHealth({ ...base, write: probe('ok', 400) }).ok, true);
    });

    it('wrong token: neither /ping nor the database - not connected with a clear reason', () => {
        const h = assessHealth(input({ ping: pingDenied, read: probe('denied', 401), write: probe('denied', 401) }));
        assert.equal(h.ok, false);
        assert.match(h.error ?? '', /^authentication failed - the token may neither read \/ping nor read or write/);
        const missing = assessHealth(
            input({ ping: pingDenied, read: probe('denied', 403), write: probe('missing', 404) }),
        );
        assert.match(missing.error ?? '', /does not exist and this token may not create it/);
    });

    it('writes refused by the server win over a successful ping (no flapping)', () => {
        const h = assessHealth(input({ writeRefused: 'HTTP 403 not authorized for database iobroker' }));
        assert.deepEqual(h, {
            ok: false,
            pingAllowed: true,
            error: 'writes refused: HTTP 403 not authorized for database iobroker',
        });
    });

    it('a failing database probe (server error) is not connected', () => {
        const h = assessHealth(
            input({ ping: pingDenied, read: { status: 'error', httpStatus: 500, message: 'HTTP 500 boom' } }),
        );
        assert.deepEqual(h, { ok: false, pingAllowed: false, error: 'HTTP 500 boom' });
    });
});
