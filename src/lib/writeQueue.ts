/**
 * WriteQueue: batching, retry with exponential backoff and jitter, circuit breaker,
 * backpressure, optional disk spool and bounded shutdown flush.
 * See docs/adr/0002-write-path-and-reliability.md.
 */
import { setTimeout as delay } from 'node:timers/promises';
import type { WriteResult } from './influxClient';
import { lineBytes } from './lineProtocol';
import type { Spool } from './spool';

export interface QueueOptions {
    writeMode: 'batched' | 'direct';
    batchMaxLines: number;
    batchMaxBytes: number;
    flushIntervalMs: number;
    maxConcurrentWrites: number;
    retryBaseDelayMs: number;
    retryMaxDelayMs: number;
    authRetryMs: number;
    bufferMode: 'memory' | 'disk';
    bufferMaxRecords: number;
    bufferMaxBytes: number;
    spoolMaxBytes: number;
    bufferMaxAgeMs: number;
    overflowPolicy: 'dropOldest' | 'dropNewest';
    persistOnShutdown: boolean;
}

export interface QueueEvents {
    written(count: number): void;
    rejected(items: { line: string; message: string }[]): void;
    dropped(count: number, reason: string): void;
    /** `kind` of a failure: auth/config = the server refuses the writes, transient = not reachable or overloaded */
    connection(ok: boolean, error?: string, kind?: 'auth' | 'config' | 'transient'): void;
    log(level: 'debug' | 'info' | 'warn' | 'error', message: string): void;
}

export interface WriteTarget {
    write(lines: string[]): Promise<WriteResult>;
}

interface Batch {
    seq: number;
    /** undefined when only stored on disk */
    lines?: string[];
    count: number;
    bytes: number;
    createdAt: number;
    onDisk: boolean;
    diskBytes: number;
    persistFailed: boolean;
    inFlight: boolean;
    attempts: number;
}

export interface QueueStats {
    pendingBatches: number;
    queuedRecords: number;
    memoryRecords: number;
    memoryBytes: number;
    spoolBytes: number;
    inFlight: number;
    circuit: 'closed' | 'open' | 'halfOpen';
    consecutiveFailures: number;
    nextAttemptAt: number;
}

/**
 * Clock, timers and randomness. The adapter passes its managed timers (`adapter.setTimeout`), which
 * js-controller clears on unload and which are not available while the adapter is stopping.
 */
export interface Timers {
    now(): number;
    /** run `fn` after `ms` milliseconds; may return undefined while the adapter is stopping */
    after(fn: () => void, ms: number): unknown;
    cancel(h: unknown): void;
    random(): number;
}

/**
 * Exponential backoff with jitter: base·2^(n-1), capped, randomised between base and that value.
 *
 * @param failures consecutive failures (≥ 1)
 * @param base base delay
 * @param cap maximum delay
 * @param random random source in [0,1)
 */
export function backoffDelay(failures: number, base: number, cap: number, random: () => number = Math.random): number {
    const exp = Math.min(cap, base * 2 ** Math.min(Math.max(failures - 1, 0), 30));
    return Math.round(Math.min(cap, base + random() * Math.max(0, exp - base)));
}

export class WriteQueue {
    private current: string[] = [];
    private currentBytes = 0;
    private readonly pending: Batch[] = [];
    private nextSeq = 1;
    private inFlight = 0;
    private memRecords = 0;
    private memBytes = 0;
    private spoolBytes = 0;
    private flushTimer: unknown;
    private retryTimer: unknown;
    private circuitUntil = 0;
    private halfOpen = false;
    private failures = 0;
    private closed = false;
    private persistChain: Promise<void> = Promise.resolve();
    private drainWaiters: (() => void)[] = [];
    /** set when the adapter is stopping: no new timers are started */
    private draining = false;
    private lastConnectionOk: boolean | undefined;

    constructor(
        private readonly opts: QueueOptions,
        private readonly target: WriteTarget,
        private readonly events: QueueEvents,
        private readonly timers: Timers,
        private readonly spool?: Spool,
    ) {}

    /** Loads batches left in the spool directory by a previous run. */
    async start(): Promise<number> {
        if (!this.spool) {
            return 0;
        }
        await this.spool.init();
        const entries = await this.spool.list();
        let records = 0;
        for (const e of entries) {
            this.pending.push({
                seq: e.seq,
                count: e.count,
                bytes: e.bytes,
                createdAt: e.createdAt,
                onDisk: true,
                diskBytes: e.bytes,
                persistFailed: false,
                inFlight: false,
                attempts: 0,
            });
            this.spoolBytes += e.bytes;
            records += e.count;
            this.nextSeq = Math.max(this.nextSeq, e.seq + 1);
        }
        if (entries.length) {
            this.events.log('info', `Recovered ${records} records in ${entries.length} batches from the disk buffer`);
            this.pump();
        }
        return records;
    }

    stats(): QueueStats {
        let queued = this.current.length;
        for (const b of this.pending) {
            queued += b.count;
        }
        const now = this.timers.now();
        return {
            pendingBatches: this.pending.length,
            queuedRecords: queued,
            memoryRecords: this.memRecords + this.current.length,
            memoryBytes: this.memBytes + this.currentBytes,
            spoolBytes: this.spoolBytes,
            inFlight: this.inFlight,
            circuit: this.halfOpen ? 'halfOpen' : now < this.circuitUntil ? 'open' : 'closed',
            consecutiveFailures: this.failures,
            nextAttemptAt: this.circuitUntil,
        };
    }

    /**
     * Adds one serialised line. Returns false if the line was dropped (buffer full or closed).
     *
     * @param line line protocol line
     */
    push(line: string): boolean {
        if (this.closed) {
            this.events.dropped(1, 'adapter is stopping');
            return false;
        }
        const bytes = lineBytes(line);
        if (!this.makeRoom(1, bytes)) {
            this.events.dropped(1, 'buffer full (dropNewest)');
            return false;
        }
        this.current.push(line);
        this.currentBytes += bytes;
        if (
            this.opts.writeMode === 'direct' ||
            this.current.length >= this.opts.batchMaxLines ||
            this.currentBytes >= this.opts.batchMaxBytes
        ) {
            this.seal();
        } else if (this.flushTimer === undefined && !this.draining) {
            this.flushTimer = this.timers.after(() => {
                this.flushTimer = undefined;
                this.seal();
            }, this.opts.flushIntervalMs);
        }
        return true;
    }

    /** Ensures that a new record fits into the memory limits. */
    private makeRoom(records: number, bytes: number): boolean {
        const fits = (): boolean =>
            this.memRecords + this.current.length + records <= this.opts.bufferMaxRecords &&
            this.memBytes + this.currentBytes + bytes <= this.opts.bufferMaxBytes;
        if (fits()) {
            return true;
        }
        if (this.opts.bufferMode === 'disk') {
            // evict lines of persisted batches from RAM - they stay on disk
            for (const b of this.pending) {
                if (fits()) {
                    break;
                }
                if (b.onDisk && b.lines && !b.inFlight) {
                    this.memRecords -= b.count;
                    this.memBytes -= b.bytes;
                    b.lines = undefined;
                }
            }
            if (fits()) {
                return true;
            }
        }
        if (this.opts.overflowPolicy === 'dropNewest') {
            return false;
        }
        while (!fits()) {
            const idx = this.pending.findIndex(b => !b.inFlight && b.lines);
            if (idx < 0) {
                break;
            }
            const [victim] = this.pending.splice(idx, 1);
            this.forget(victim);
            this.events.dropped(victim.count, 'buffer full (dropOldest)');
        }
        if (fits()) {
            return true;
        }
        // the open batch alone exceeds the limit: seal it so it can be sent, drop the new record
        return false;
    }

    /** Removes a batch from memory/disk accounting (and from disk). */
    private forget(b: Batch): void {
        if (b.lines) {
            this.memRecords -= b.count;
            this.memBytes -= b.bytes;
            b.lines = undefined;
        }
        if (b.onDisk && this.spool) {
            this.spoolBytes -= b.diskBytes;
            b.onDisk = false;
            const spool = this.spool;
            this.persistChain = this.persistChain.then(() => spool.remove(b.seq).catch(() => undefined));
        }
    }

    /** Closes the open batch and hands it over for sending. */
    private seal(): void {
        if (this.flushTimer !== undefined) {
            this.timers.cancel(this.flushTimer);
            this.flushTimer = undefined;
        }
        if (!this.current.length) {
            this.pump();
            return;
        }
        const batch: Batch = {
            seq: this.nextSeq++,
            lines: this.current,
            count: this.current.length,
            bytes: this.currentBytes,
            createdAt: this.timers.now(),
            onDisk: false,
            diskBytes: 0,
            persistFailed: false,
            inFlight: false,
            attempts: 0,
        };
        this.current = [];
        this.currentBytes = 0;
        this.memRecords += batch.count;
        this.memBytes += batch.bytes;
        this.pending.push(batch);
        if (this.opts.bufferMode === 'disk' && this.spool) {
            this.persist(batch);
        } else {
            this.pump();
        }
    }

    private persist(batch: Batch): void {
        const spool = this.spool;
        if (!spool || !batch.lines) {
            return;
        }
        const lines = batch.lines;
        this.persistChain = this.persistChain
            .then(async () => {
                if (!this.pending.includes(batch)) {
                    return; // already sent or dropped
                }
                const size = await spool.write(batch.seq, lines, batch.createdAt);
                batch.onDisk = true;
                batch.diskBytes = size;
                this.spoolBytes += size;
                this.enforceDiskLimit();
            })
            .catch((e: unknown) => {
                batch.persistFailed = true;
                this.events.log(
                    'error',
                    `Cannot write disk buffer: ${(e as Error).message} - batch kept in memory only`,
                );
            })
            .finally(() => this.pump());
    }

    private enforceDiskLimit(): void {
        while (this.spoolBytes > this.opts.spoolMaxBytes) {
            const idx = this.pending.findIndex(b => b.onDisk && !b.inFlight);
            if (idx < 0) {
                return;
            }
            const [victim] = this.pending.splice(idx, 1);
            this.forget(victim);
            this.events.dropped(victim.count, 'disk buffer full');
        }
    }

    private dropExpired(): void {
        const limit = this.timers.now() - this.opts.bufferMaxAgeMs;
        for (let i = this.pending.length - 1; i >= 0; i--) {
            const b = this.pending[i];
            if (!b.inFlight && b.createdAt < limit) {
                this.pending.splice(i, 1);
                this.forget(b);
                this.events.dropped(b.count, 'older than the maximum buffer age');
            }
        }
    }

    private sendable(b: Batch): boolean {
        if (b.inFlight) {
            return false;
        }
        if (this.opts.bufferMode === 'disk' && this.spool) {
            return b.onDisk || b.persistFailed;
        }
        return true;
    }

    /** Starts as many sends as allowed. */
    private pump(): void {
        this.dropExpired();
        if (!this.pending.length) {
            this.notifyDrained();
            return;
        }
        const now = this.timers.now();
        if (now < this.circuitUntil) {
            this.scheduleRetry(this.circuitUntil - now);
            return;
        }
        if (this.failures > 0 && !this.halfOpen && this.inFlight === 0) {
            this.halfOpen = true; // probe with one batch
        }
        while (this.inFlight < this.opts.maxConcurrentWrites) {
            if (this.halfOpen && this.inFlight > 0) {
                break;
            }
            const batch = this.pending.find(b => this.sendable(b));
            if (!batch) {
                break;
            }
            void this.send(batch);
        }
    }

    private scheduleRetry(ms: number): void {
        if (this.retryTimer !== undefined || this.closed || this.draining) {
            return;
        }
        this.retryTimer = this.timers.after(
            () => {
                this.retryTimer = undefined;
                this.pump();
            },
            Math.max(0, ms),
        );
    }

    private async send(batch: Batch): Promise<void> {
        batch.inFlight = true;
        this.inFlight++;
        let lines = batch.lines;
        if (!lines && this.spool) {
            try {
                lines = (await this.spool.read(batch.seq)).lines;
            } catch (e) {
                this.events.log(
                    'error',
                    `Disk buffer batch ${batch.seq} unreadable (${(e as Error).message}) - moved aside`,
                );
                await this.spool.quarantine(batch.seq).catch(() => undefined);
                this.spoolBytes -= batch.diskBytes;
                batch.onDisk = false;
                this.remove(batch);
                this.events.dropped(batch.count, 'unreadable disk buffer file');
                this.finishSend(batch);
                return;
            }
        }
        if (!lines) {
            this.remove(batch);
            this.finishSend(batch);
            return;
        }
        let result: WriteResult;
        try {
            result = await this.target.write(lines);
        } catch (e) {
            result = { kind: 'transient', message: (e as Error).message };
        }
        batch.attempts++;
        this.handleResult(batch, lines, result);
        this.finishSend(batch);
    }

    private finishSend(batch: Batch): void {
        batch.inFlight = false;
        this.inFlight--;
        this.pump();
    }

    private remove(batch: Batch): void {
        const idx = this.pending.indexOf(batch);
        if (idx >= 0) {
            this.pending.splice(idx, 1);
        }
        this.forget(batch);
    }

    private setConnection(ok: boolean, error?: string, kind?: 'auth' | 'config' | 'transient'): void {
        if (this.lastConnectionOk !== ok || !ok) {
            this.events.connection(ok, error, kind);
        }
        this.lastConnectionOk = ok;
    }

    private success(): void {
        this.failures = 0;
        this.halfOpen = false;
        this.circuitUntil = 0;
        this.setConnection(true);
    }

    private handleResult(batch: Batch, lines: string[], result: WriteResult): void {
        switch (result.kind) {
            case 'ok':
                this.remove(batch);
                this.events.written(lines.length);
                this.success();
                return;
            case 'partial': {
                this.remove(batch);
                const rejected = result.rejected
                    .filter(r => r.line >= 1 && r.line <= lines.length)
                    .map(r => ({ line: lines[r.line - 1], message: r.message }));
                this.events.written(lines.length - rejected.length);
                this.events.rejected(rejected);
                this.success();
                return;
            }
            case 'invalid':
                this.remove(batch);
                this.events.rejected(lines.map(line => ({ line, message: result.message })));
                this.success();
                return;
            case 'tooLarge': {
                if (lines.length <= 1) {
                    this.remove(batch);
                    this.events.rejected(lines.map(line => ({ line, message: result.message })));
                    return;
                }
                const idx = this.pending.indexOf(batch);
                this.remove(batch);
                const mid = Math.ceil(lines.length / 2);
                const halves = [lines.slice(0, mid), lines.slice(mid)].map(part => {
                    const b: Batch = {
                        seq: this.nextSeq++,
                        lines: part,
                        count: part.length,
                        bytes: part.reduce((s, l) => s + lineBytes(l), 0),
                        createdAt: batch.createdAt,
                        onDisk: false,
                        diskBytes: 0,
                        persistFailed: false,
                        inFlight: false,
                        attempts: 0,
                    };
                    this.memRecords += b.count;
                    this.memBytes += b.bytes;
                    return b;
                });
                this.pending.splice(Math.max(0, idx), 0, ...halves);
                this.events.log('warn', `Batch of ${lines.length} records too large for the server - split into two`);
                if (this.opts.bufferMode === 'disk') {
                    halves.forEach(h => this.persist(h));
                }
                return;
            }
            case 'auth':
            case 'config': {
                this.failures++;
                this.circuitUntil = this.timers.now() + this.opts.authRetryMs;
                this.halfOpen = false;
                this.setConnection(false, result.message, result.kind);
                this.events.log(
                    'error',
                    `InfluxDB refused the write (${result.status}: ${result.message}) - data is kept, next attempt in ${Math.round(this.opts.authRetryMs / 1000)} s`,
                );
                return;
            }
            case 'transient': {
                this.failures++;
                let delay = backoffDelay(this.failures, this.opts.retryBaseDelayMs, this.opts.retryMaxDelayMs, () =>
                    this.timers.random(),
                );
                if (result.retryAfterMs !== undefined) {
                    delay = Math.max(delay, result.retryAfterMs);
                }
                this.circuitUntil = this.timers.now() + delay;
                this.halfOpen = false;
                this.setConnection(false, result.message, 'transient');
                this.events.log(
                    this.failures === 1 ? 'warn' : 'debug',
                    `Write failed (${result.status ?? 'network'}: ${result.message}) - retry ${this.failures} in ${delay} ms`,
                );
                return;
            }
        }
    }

    private notifyDrained(): void {
        if (this.pending.length || this.current.length) {
            return;
        }
        const waiters = this.drainWaiters;
        this.drainWaiters = [];
        waiters.forEach(w => w());
    }

    /**
     * Sends everything that is buffered and waits up to `timeoutMs`.
     *
     * @param timeoutMs maximum wait time
     */
    async flush(timeoutMs: number): Promise<boolean> {
        this.seal();
        if (!this.pending.length) {
            return true;
        }
        // ignore an open circuit for an explicit flush, but only once
        if (this.circuitUntil > this.timers.now() && this.failures > 0) {
            return false;
        }
        // A promise delay instead of adapter timers: flush() is also used while the adapter is stopping,
        // where adapter.setTimeout() refuses new timers. The delay is aborted as soon as the queue drains.
        const abort = new AbortController();
        const drained = new Promise<boolean>(resolve => {
            this.drainWaiters.push(() => resolve(true));
        });
        const timedOut = delay(timeoutMs, false, { signal: abort.signal }).catch(() => false);
        this.pump();
        const result = await Promise.race([drained, timedOut]);
        abort.abort();
        return result;
    }

    /** Called first when the adapter stops: accepts records, but starts no timers any more. */
    beginShutdown(): void {
        this.draining = true;
        if (this.flushTimer !== undefined) {
            this.timers.cancel(this.flushTimer);
            this.flushTimer = undefined;
        }
    }

    /**
     * Stops intake, tries to send for at most `timeoutMs`, then persists the rest
     * (disk mode, or memory mode with persistOnShutdown).
     *
     * @param timeoutMs maximum time for sending
     */
    async shutdown(timeoutMs: number): Promise<{ persisted: number; lost: number }> {
        this.seal();
        this.closed = true;
        if (this.flushTimer !== undefined) {
            this.timers.cancel(this.flushTimer);
            this.flushTimer = undefined;
        }
        const start = this.timers.now();
        if (this.pending.length && this.circuitUntil <= start) {
            await this.flush(timeoutMs);
        }
        if (this.retryTimer !== undefined) {
            this.timers.cancel(this.retryTimer);
            this.retryTimer = undefined;
        }
        await this.persistChain;
        let persisted = 0;
        let lost = 0;
        for (const b of [...this.pending]) {
            if (b.onDisk) {
                persisted += b.count;
                continue;
            }
            if (b.lines && this.spool && (this.opts.bufferMode === 'disk' || this.opts.persistOnShutdown)) {
                try {
                    await this.spool.init();
                    const size = await this.spool.write(b.seq, b.lines, b.createdAt);
                    b.onDisk = true;
                    b.diskBytes = size;
                    persisted += b.count;
                    continue;
                } catch (e) {
                    this.events.log('error', `Cannot persist buffer on shutdown: ${(e as Error).message}`);
                }
            }
            lost += b.count;
        }
        if (lost) {
            this.events.dropped(lost, 'not sent before shutdown');
        }
        return { persisted, lost };
    }
}
