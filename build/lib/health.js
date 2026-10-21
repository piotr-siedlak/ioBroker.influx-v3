"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isDenied = isDenied;
exports.needsDatabaseCheck = needsDatabaseCheck;
exports.needsWriteProbe = needsWriteProbe;
exports.assessHealth = assessHealth;
/**
 * The status means "authenticated, but this endpoint is not allowed" or "not authenticated".
 *
 * @param status HTTP status
 */
function isDenied(status) {
    return status === 401 || status === 403;
}
/**
 * /ping was refused: the database has to be checked (read probe, then possibly the write probe).
 *
 * @param ping answer of /ping
 */
function needsDatabaseCheck(ping) {
    return !ping.ok && isDenied(ping.status);
}
/**
 * The write probe is needed: reading was refused and no write proved the connection recently.
 *
 * @param i results so far
 */
function needsWriteProbe(i) {
    return (needsDatabaseCheck(i.ping) &&
        i.read?.status === 'denied' &&
        !(i.lastWriteOk !== undefined && i.now - i.lastWriteOk <= i.recentMs));
}
/**
 * Decides the connection state.
 *
 * @param i ping, probes and the state of the live writes
 */
function assessHealth(i) {
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
//# sourceMappingURL=health.js.map