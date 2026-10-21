"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.WriteQueue = void 0;
exports.backoffDelay = backoffDelay;
/**
 * WriteQueue: batching, retry with exponential backoff and jitter, circuit breaker,
 * backpressure, optional disk spool and bounded shutdown flush.
 * See docs/adr/0002-write-path-and-reliability.md.
 */
const promises_1 = require("node:timers/promises");
const lineProtocol_1 = require("./lineProtocol");
/**
 * Exponential backoff with jitter: base·2^(n-1), capped, randomised between base and that value.
 *
 * @param failures consecutive failures (≥ 1)
 * @param base base delay
 * @param cap maximum delay
 * @param random random source in [0,1)
 */
function backoffDelay(failures, base, cap, random = Math.random) {
    const exp = Math.min(cap, base * 2 ** Math.min(Math.max(failures - 1, 0), 30));
    return Math.round(Math.min(cap, base + random() * Math.max(0, exp - base)));
}
class WriteQueue {
    opts;
    target;
    events;
    timers;
    spool;
    current = [];
    currentBytes = 0;
    pending = [];
    nextSeq = 1;
    inFlight = 0;
    memRecords = 0;
    memBytes = 0;
    spoolBytes = 0;
    flushTimer;
    retryTimer;
    circuitUntil = 0;
    halfOpen = false;
    failures = 0;
    closed = false;
    persistChain = Promise.resolve();
    drainWaiters = [];
    /** set when the adapter is stopping: no new timers are started */
    draining = false;
    lastConnectionOk;
    constructor(opts, target, events, timers, spool) {
        this.opts = opts;
        this.target = target;
        this.events = events;
        this.timers = timers;
        this.spool = spool;
    }
    /** Loads batches left in the spool directory by a previous run. */
    async start() {
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
    stats() {
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
    push(line) {
        if (this.closed) {
            this.events.dropped(1, 'adapter is stopping');
            return false;
        }
        const bytes = (0, lineProtocol_1.lineBytes)(line);
        if (!this.makeRoom(1, bytes)) {
            this.events.dropped(1, 'buffer full (dropNewest)');
            return false;
        }
        this.current.push(line);
        this.currentBytes += bytes;
        if (this.opts.writeMode === 'direct' ||
            this.current.length >= this.opts.batchMaxLines ||
            this.currentBytes >= this.opts.batchMaxBytes) {
            this.seal();
        }
        else if (this.flushTimer === undefined && !this.draining) {
            this.flushTimer = this.timers.after(() => {
                this.flushTimer = undefined;
                this.seal();
            }, this.opts.flushIntervalMs);
        }
        return true;
    }
    /** Ensures that a new record fits into the memory limits. */
    makeRoom(records, bytes) {
        const fits = () => this.memRecords + this.current.length + records <= this.opts.bufferMaxRecords &&
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
    forget(b) {
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
    seal() {
        if (this.flushTimer !== undefined) {
            this.timers.cancel(this.flushTimer);
            this.flushTimer = undefined;
        }
        if (!this.current.length) {
            this.pump();
            return;
        }
        const batch = {
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
        }
        else {
            this.pump();
        }
    }
    persist(batch) {
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
            .catch((e) => {
            batch.persistFailed = true;
            this.events.log('error', `Cannot write disk buffer: ${e.message} - batch kept in memory only`);
        })
            .finally(() => this.pump());
    }
    enforceDiskLimit() {
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
    dropExpired() {
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
    sendable(b) {
        if (b.inFlight) {
            return false;
        }
        if (this.opts.bufferMode === 'disk' && this.spool) {
            return b.onDisk || b.persistFailed;
        }
        return true;
    }
    /** Starts as many sends as allowed. */
    pump() {
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
    scheduleRetry(ms) {
        if (this.retryTimer !== undefined || this.closed || this.draining) {
            return;
        }
        this.retryTimer = this.timers.after(() => {
            this.retryTimer = undefined;
            this.pump();
        }, Math.max(0, ms));
    }
    async send(batch) {
        batch.inFlight = true;
        this.inFlight++;
        let lines = batch.lines;
        if (!lines && this.spool) {
            try {
                lines = (await this.spool.read(batch.seq)).lines;
            }
            catch (e) {
                this.events.log('error', `Disk buffer batch ${batch.seq} unreadable (${e.message}) - moved aside`);
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
        let result;
        try {
            result = await this.target.write(lines);
        }
        catch (e) {
            result = { kind: 'transient', message: e.message };
        }
        batch.attempts++;
        this.handleResult(batch, lines, result);
        this.finishSend(batch);
    }
    finishSend(batch) {
        batch.inFlight = false;
        this.inFlight--;
        this.pump();
    }
    remove(batch) {
        const idx = this.pending.indexOf(batch);
        if (idx >= 0) {
            this.pending.splice(idx, 1);
        }
        this.forget(batch);
    }
    setConnection(ok, error, kind) {
        if (this.lastConnectionOk !== ok || !ok) {
            this.events.connection(ok, error, kind);
        }
        this.lastConnectionOk = ok;
    }
    success() {
        this.failures = 0;
        this.halfOpen = false;
        this.circuitUntil = 0;
        this.setConnection(true);
    }
    handleResult(batch, lines, result) {
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
                    const b = {
                        seq: this.nextSeq++,
                        lines: part,
                        count: part.length,
                        bytes: part.reduce((s, l) => s + (0, lineProtocol_1.lineBytes)(l), 0),
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
                this.events.log('error', `InfluxDB refused the write (${result.status}: ${result.message}) - data is kept, next attempt in ${Math.round(this.opts.authRetryMs / 1000)} s`);
                return;
            }
            case 'transient': {
                this.failures++;
                let delay = backoffDelay(this.failures, this.opts.retryBaseDelayMs, this.opts.retryMaxDelayMs, () => this.timers.random());
                if (result.retryAfterMs !== undefined) {
                    delay = Math.max(delay, result.retryAfterMs);
                }
                this.circuitUntil = this.timers.now() + delay;
                this.halfOpen = false;
                this.setConnection(false, result.message, 'transient');
                this.events.log(this.failures === 1 ? 'warn' : 'debug', `Write failed (${result.status ?? 'network'}: ${result.message}) - retry ${this.failures} in ${delay} ms`);
                return;
            }
        }
    }
    notifyDrained() {
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
    async flush(timeoutMs) {
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
        const drained = new Promise(resolve => {
            this.drainWaiters.push(() => resolve(true));
        });
        const timedOut = (0, promises_1.setTimeout)(timeoutMs, false, { signal: abort.signal }).catch(() => false);
        this.pump();
        const result = await Promise.race([drained, timedOut]);
        abort.abort();
        return result;
    }
    /** Called first when the adapter stops: accepts records, but starts no timers any more. */
    beginShutdown() {
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
    async shutdown(timeoutMs) {
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
                }
                catch (e) {
                    this.events.log('error', `Cannot persist buffer on shutdown: ${e.message}`);
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
exports.WriteQueue = WriteQueue;
//# sourceMappingURL=writeQueue.js.map