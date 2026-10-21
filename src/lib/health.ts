/**
 * Connection health (`info.connection`) judged by what the adapter needs: writing (and reading) the configured
 * database. `/ping` is only the first, cheapest check - InfluxDB 3 Enterprise answers it with 401/403 for a database
 * token (least privilege: `db:NAME:read,write`), although that token writes and reads without problems.
 */
import type { PingResult, ProbeResult } from './influxClient';

export interface HealthInput {
    ping: PingResult;
    /** `SELECT 1` on the database - only when /ping was refused */
    read?: ProbeResult;
    /** empty write - only when /ping and reading were refused and no write proved the connection recently */
    write?: ProbeResult;
    /** time of the last successful write of the live queue */
    lastWriteOk?: number;
    /** the server refuses the writes of the live queue (401/403/404), newest error */
    writeRefused?: string;
    now: number;
    /** a successful write within this time proves the connection */
    recentMs: number;
}

export interface Health {
    ok: boolean;
    /** /ping answered: edition and version are known */
    pingAllowed: boolean;
    /** the database can be read (history, schema); undefined = not checked */
    canRead?: boolean;
    /** reason when not ok */
    error?: string;
}

/**
 * The status means "authenticated, but this endpoint is not allowed" or "not authenticated".
 *
 * @param status HTTP status
 */
export function isDenied(status: number): boolean {
    return status === 401 || status === 403;
}

/**
 * /ping was refused: the database has to be checked (read probe, then possibly the write probe).
 *
 * @param ping answer of /ping
 */
export function needsDatabaseCheck(ping: PingResult): boolean {
    return !ping.ok && isDenied(ping.status);
}

/**
 * The write probe is needed: reading was refused and no write proved the connection recently.
 *
 * @param i results so far
 */
export function needsWriteProbe(i: Omit<HealthInput, 'write'>): boolean {
    return (
        needsDatabaseCheck(i.ping) &&
        i.read?.status === 'denied' &&
        !(i.lastWriteOk !== undefined && i.now - i.lastWriteOk <= i.recentMs)
    );
}

/**
 * Decides the connection state.
 *
 * @param i ping, probes and the state of the live writes
 */
export function assessHealth(i: HealthInput): Health {
    const pingAllowed = i.ping.ok;
    if (i.writeRefused) {
        return { ok: false, pingAllowed, error: `writes refused: ${i.writeRefused}` };
    }
    if (i.ping.ok) {
        return { ok: true, pingAllowed };
    }
    if (!isDenied(i.ping.status)) {
        return { ok: false, pingAllowed, error: i.ping.error ?? `HTTP ${i.ping.status}` };
    }
    // the server answered, the token may not read /ping (database token): check the database instead
    const read = i.read;
    if (read?.status === 'ok' || read?.status === 'missing') {
        return { ok: true, pingAllowed, canRead: true };
    }
    if (read && read.status !== 'denied') {
        return { ok: false, pingAllowed, error: read.message ?? `HTTP ${read.httpStatus}` };
    }
    const canRead = read ? false : undefined;
    if (i.lastWriteOk !== undefined && i.now - i.lastWriteOk <= i.recentMs) {
        return { ok: true, pingAllowed, canRead };
    }
    const write = i.write;
    if (write?.status === 'ok') {
        return { ok: true, pingAllowed, canRead };
    }
    if (write?.status === 'missing') {
        return {
            ok: false,
            pingAllowed,
            canRead,
            error: 'the database does not exist and this token may not create it',
        };
    }
    if (write?.status === 'error') {
        return { ok: false, pingAllowed, canRead, error: write.message ?? `HTTP ${write.httpStatus}` };
    }
    return {
        ok: false,
        pingAllowed,
        canRead,
        error: 'authentication failed - the token may neither read /ping nor read or write the database (check the API token)',
    };
}
