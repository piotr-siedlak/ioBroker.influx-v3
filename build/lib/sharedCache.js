"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SharedCache = void 0;
class SharedCache {
    now;
    ttlMs;
    errorTtlMs;
    entries = new Map();
    /**
     * @param now clock
     * @param ttlMs how long a result is reused
     * @param errorTtlMs how long a failure is reported without asking again
     */
    constructor(now, ttlMs, errorTtlMs = ttlMs) {
        this.now = now;
        this.ttlMs = ttlMs;
        this.errorTtlMs = errorTtlMs;
    }
    /**
     * Fresh result, the running computation, or a new computation.
     *
     * @param key cache key
     * @param load computes the value
     */
    get(key, load) {
        const now = this.now();
        const e = this.entries.get(key);
        if (e?.pending) {
            return e.pending;
        }
        if (e && !e.stale && e.error && e.errorAt >= e.valueAt && now - e.errorAt < this.errorTtlMs) {
            return Promise.reject(e.error);
        }
        if (e && !e.stale && e.value !== undefined && e.valueAt >= e.errorAt && now - e.valueAt < this.ttlMs) {
            return Promise.resolve(e.value);
        }
        const entry = e ?? { valueAt: 0, errorAt: 0 };
        let pending;
        try {
            pending = load();
        }
        catch (err) {
            pending = Promise.reject(err instanceof Error ? err : new Error(String(err)));
        }
        entry.pending = pending.then(value => {
            entry.value = value;
            entry.error = undefined;
            entry.stale = false;
            entry.valueAt = this.now();
            entry.pending = undefined;
            return value;
        }, (err) => {
            entry.error = err instanceof Error ? err : new Error(String(err));
            entry.stale = false;
            entry.errorAt = this.now();
            entry.pending = undefined;
            throw entry.error;
        });
        this.entries.set(key, entry);
        return entry.pending;
    }
    /**
     * Last successful result, also when it is stale or a new computation is running.
     *
     * @param key cache key
     */
    peek(key) {
        return this.entries.get(key)?.value;
    }
    /**
     * Marks results as outdated (all keys if omitted); a running computation is not interrupted.
     *
     * @param key cache key
     */
    invalidate(key) {
        for (const [k, e] of this.entries) {
            if (key === undefined || k === key) {
                e.stale = true;
            }
        }
    }
}
exports.SharedCache = SharedCache;
//# sourceMappingURL=sharedCache.js.map