/**
 * ObservabilityService: counters and timestamps of the instance. Published periodically to
 * `info.*` (never on every write) and returned by `getAdapterStats`.
 */

export interface Counters {
    received: number;
    filtered: number;
    queued: number;
    written: number;
    rejected: number;
    dropped: number;
    conversionErrors: number;
    conflicts: number;
    historyQueries: number;
    userQueries: number;
}

export class Observability {
    readonly counters: Counters = {
        received: 0,
        filtered: 0,
        queued: 0,
        written: 0,
        rejected: 0,
        dropped: 0,
        conversionErrors: 0,
        conflicts: 0,
        historyQueries: 0,
        userQueries: 0,
    };
    lastWrite = 0;
    lastRead = 0;
    lastError = '';
    lastErrorTs = 0;
    connected = false;
    serverVersion = '';
    serverEdition = '';
    readonly startedAt = Date.now();
    private readonly dropReasons = new Map<string, number>();
    private readonly rejectLog: number[] = [];

    /**
     * @param n number of records
     * @param reason reason text
     */
    drop(n: number, reason: string): void {
        this.counters.dropped += n;
        this.dropReasons.set(reason, (this.dropReasons.get(reason) ?? 0) + n);
    }

    /** @param message redacted error text */
    error(message: string): void {
        this.lastError = message;
        this.lastErrorTs = Date.now();
    }

    /**
     * Rate limiter for logging rejected lines.
     *
     * @param limitPerMin maximum log entries per minute
     */
    mayLogReject(limitPerMin: number): boolean {
        const now = Date.now();
        while (this.rejectLog.length && now - this.rejectLog[0] > 60000) {
            this.rejectLog.shift();
        }
        if (this.rejectLog.length >= limitPerMin) {
            return false;
        }
        this.rejectLog.push(now);
        return true;
    }

    snapshot(): Record<string, unknown> {
        return {
            ...this.counters,
            dropReasons: Object.fromEntries(this.dropReasons),
            lastWrite: this.lastWrite,
            lastRead: this.lastRead,
            lastError: this.lastError,
            lastErrorTs: this.lastErrorTs,
            connected: this.connected,
            serverVersion: this.serverVersion,
            serverEdition: this.serverEdition,
            uptimeSec: Math.round((Date.now() - this.startedAt) / 1000),
        };
    }
}
