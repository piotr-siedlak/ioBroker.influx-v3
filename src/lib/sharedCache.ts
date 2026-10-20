/**
 * Single-flight cache (ADR-0006, extension 5): concurrent callers share one computation per key, a fresh result is
 * reused, the last result stays available (stale) while a new computation runs, and failures are remembered for a
 * while so that a slow or broken source is not asked again by every caller.
 */
interface Entry<T> {
    /** time of the last result */
    valueAt: number;
    value?: T;
    /** time of the last failure (newer than the result) */
    errorAt: number;
    error?: Error;
    pending?: Promise<T>;
    /** dropped by invalidate(): the next get() computes anew */
    stale?: boolean;
}

export class SharedCache<T> {
    private readonly entries = new Map<string, Entry<T>>();

    /**
     * @param now clock
     * @param ttlMs how long a result is reused
     * @param errorTtlMs how long a failure is reported without asking again
     */
    constructor(
        private readonly now: () => number,
        private readonly ttlMs: number,
        private readonly errorTtlMs = ttlMs,
    ) {}

    /**
     * Fresh result, the running computation, or a new computation.
     *
     * @param key cache key
     * @param load computes the value
     */
    get(key: string, load: () => Promise<T>): Promise<T> {
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
        const entry: Entry<T> = e ?? { valueAt: 0, errorAt: 0 };
        let pending: Promise<T>;
        try {
            pending = load();
        } catch (err) {
            pending = Promise.reject(err instanceof Error ? err : new Error(String(err)));
        }
        entry.pending = pending.then(
            value => {
                entry.value = value;
                entry.error = undefined;
                entry.stale = false;
                entry.valueAt = this.now();
                entry.pending = undefined;
                return value;
            },
            (err: unknown) => {
                entry.error = err instanceof Error ? err : new Error(String(err));
                entry.stale = false;
                entry.errorAt = this.now();
                entry.pending = undefined;
                throw entry.error;
            },
        );
        this.entries.set(key, entry);
        return entry.pending;
    }

    /**
     * Last successful result, also when it is stale or a new computation is running.
     *
     * @param key cache key
     */
    peek(key: string): T | undefined {
        return this.entries.get(key)?.value;
    }

    /**
     * Marks results as outdated (all keys if omitted); a running computation is not interrupted.
     *
     * @param key cache key
     */
    invalidate(key?: string): void {
        for (const [k, e] of this.entries) {
            if (key === undefined || k === key) {
                e.stale = true;
            }
        }
    }
}
