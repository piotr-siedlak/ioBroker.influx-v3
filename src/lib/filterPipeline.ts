/**
 * FilterPipeline: decides which state changes of one datapoint are written.
 *
 * Fixed order of the checks (documented in README and tested):
 *  1. ack filter            (ackFilter)
 *  2. quality filter        (qFilter)
 *  3. ignore filters        (ignoreZero, ignoreBelowNumber, ignoreAboveNumber)
 *  4. running debounce      (same value as the pending one → keep waiting; other value → restart)
 *  5. blockTime             (values inside the block window are held back; the newest is written when the window ends)
 *  6. changesOnly           (unchanged value suppressed unless the relog interval has passed)
 *  7. changesMinDelta       (numbers only)
 *  8. emit                  (optionally preceded by the last suppressed value, then debounced or immediate)
 * After every emitted value a relog timer is armed when changesOnly && changesRelogInterval > 0.
 *
 * Values arriving here are already coerced to the target type (number/boolean/string) and never null.
 */
import type { StateConfig } from './config';

export type Val = number | boolean | string;

export interface PreparedState {
    val: Val;
    ts: number;
    ack: boolean;
    q: number;
    from: string;
}

export type EmitReason = 'change' | 'relog' | 'skipped' | 'initial' | 'debounced' | 'blockReleased';

export type Decision =
    | 'emitted'
    | 'filtered:ack'
    | 'filtered:q'
    | 'filtered:ignore'
    | 'debounce:waiting'
    | 'debounce:restarted'
    | 'blocked'
    | 'unchanged'
    | 'minDelta';

/**
 * Clock and timers. The adapter passes its managed timers (`adapter.setTimeout`), which
 * js-controller clears on unload; tests pass a fake clock.
 */
export interface Scheduler {
    now(): number;
    /** run `fn` after `ms` milliseconds; may return undefined while the adapter is stopping */
    after(fn: () => void, ms: number): unknown;
    cancel(handle: unknown): void;
}

export type FilterSettings = Pick<
    StateConfig,
    | 'ackFilter'
    | 'qFilter'
    | 'ignoreZero'
    | 'ignoreBelowNumber'
    | 'ignoreAboveNumber'
    | 'debounceTime'
    | 'blockTime'
    | 'changesOnly'
    | 'changesRelogInterval'
    | 'changesMinDelta'
    | 'logSkippedBeforeChange'
>;

export class FilterPipeline {
    private lastLogged: PreparedState | undefined;
    private lastLogTime = 0;
    private skipped: PreparedState | undefined;
    private debounceTimer: unknown;
    private debouncePending: PreparedState | undefined;
    private blockTimer: unknown;
    private blockPending: PreparedState | undefined;
    private relogTimer: unknown;
    private stopped = false;

    constructor(
        private settings: FilterSettings,
        private readonly emit: (s: PreparedState, reason: EmitReason) => void,
        private readonly relogFrom: string,
        private readonly scheduler: Scheduler,
    ) {}

    /** last value that was written (or accepted as reference) */
    get last(): PreparedState | undefined {
        return this.lastLogged;
    }

    /**
     * Sets the reference value at start without writing it (or writing it when `store` is true).
     *
     * @param s current state
     * @param store write the value as initial value (ts = now)
     */
    initialize(s: PreparedState, store: boolean): void {
        if (store) {
            const initial: PreparedState = { ...s, ts: this.scheduler.now(), from: this.relogFrom };
            this.lastLogged = initial;
            this.lastLogTime = initial.ts;
            this.emit(initial, 'initial');
        } else {
            this.lastLogged = s;
            this.lastLogTime = s.ts;
        }
        this.armRelog();
    }

    /**
     * Processes one incoming state.
     *
     * @param s the prepared state
     * @param blockReleased internal: the value comes from the end of a block window
     */
    process(s: PreparedState, blockReleased = false): Decision {
        if (this.stopped) {
            return 'filtered:ignore';
        }
        const cfg = this.settings;
        if (cfg.ackFilter === 'ackOnly' && !s.ack) {
            return 'filtered:ack';
        }
        if (cfg.ackFilter === 'notAckOnly' && s.ack) {
            return 'filtered:ack';
        }
        if (cfg.qFilter === 'goodOnly' && s.q !== 0) {
            return 'filtered:q';
        }
        if (typeof s.val === 'number') {
            if (cfg.ignoreZero && s.val === 0) {
                return 'filtered:ignore';
            }
            if (cfg.ignoreBelowNumber !== null && s.val < cfg.ignoreBelowNumber) {
                return 'filtered:ignore';
            }
            if (cfg.ignoreAboveNumber !== null && s.val > cfg.ignoreAboveNumber) {
                return 'filtered:ignore';
            }
        }

        let restarted = false;
        if (this.debounceTimer !== undefined) {
            if (this.debouncePending && this.debouncePending.val === s.val) {
                return 'debounce:waiting';
            }
            this.scheduler.cancel(this.debounceTimer);
            this.debounceTimer = undefined;
            this.debouncePending = undefined;
            restarted = true;
        }

        if (
            !restarted &&
            !blockReleased &&
            cfg.blockTime > 0 &&
            this.lastLogged &&
            s.ts < this.lastLogged.ts + cfg.blockTime
        ) {
            this.holdBlocked(s, this.lastLogged.ts + cfg.blockTime);
            return 'blocked';
        }

        if (this.lastLogged && cfg.changesOnly) {
            // a value that bounced back during a debounce window cancels the pending change
            const unchanged = this.lastLogged.val === s.val;
            if (unchanged) {
                const relogDue =
                    cfg.changesRelogInterval > 0 && s.ts - this.lastLogTime >= cfg.changesRelogInterval * 1000;
                if (!relogDue) {
                    if (!restarted) {
                        this.rememberSkipped(s);
                    }
                    return 'unchanged';
                }
            } else if (
                typeof s.val === 'number' &&
                typeof this.lastLogged.val === 'number' &&
                cfg.changesMinDelta > 0 &&
                Math.abs(this.lastLogged.val - s.val) < cfg.changesMinDelta
            ) {
                if (!restarted) {
                    this.rememberSkipped(s);
                }
                return 'minDelta';
            }
        }

        this.clearRelog();
        if (!blockReleased) {
            this.clearBlocked();
        }
        if (cfg.changesOnly && cfg.logSkippedBeforeChange && this.skipped && this.skipped.ts < s.ts) {
            const sk = this.skipped;
            this.skipped = undefined;
            this.emit(sk, 'skipped');
        }
        this.skipped = undefined;

        if (cfg.debounceTime > 0) {
            this.debouncePending = s;
            this.debounceTimer = this.scheduler.after(() => {
                this.debounceTimer = undefined;
                const pending = this.debouncePending;
                this.debouncePending = undefined;
                if (!pending || this.stopped) {
                    return;
                }
                this.accept(pending, 'debounced');
            }, cfg.debounceTime);
            return restarted ? 'debounce:restarted' : 'debounce:waiting';
        }
        this.accept(s, blockReleased ? 'blockReleased' : 'change');
        return 'emitted';
    }

    private accept(s: PreparedState, reason: EmitReason): void {
        this.lastLogged = s;
        this.lastLogTime = s.ts;
        this.emit(s, reason);
        this.armRelog();
    }

    private rememberSkipped(s: PreparedState): void {
        if (this.settings.logSkippedBeforeChange) {
            this.skipped = s;
        }
    }

    private holdBlocked(s: PreparedState, until: number): void {
        this.blockPending = s;
        if (this.blockTimer !== undefined) {
            return; // the running timer will release the newest pending value
        }
        const delay = Math.max(0, until - this.scheduler.now());
        this.blockTimer = this.scheduler.after(() => {
            this.blockTimer = undefined;
            const pending = this.blockPending;
            this.blockPending = undefined;
            if (pending && !this.stopped) {
                this.process(pending, true);
            }
        }, delay);
    }

    private clearBlocked(): void {
        if (this.blockTimer !== undefined) {
            this.scheduler.cancel(this.blockTimer);
            this.blockTimer = undefined;
        }
        this.blockPending = undefined;
    }

    private armRelog(): void {
        this.clearRelog();
        const cfg = this.settings;
        if (!cfg.changesOnly || cfg.changesRelogInterval <= 0 || this.stopped) {
            return;
        }
        this.relogTimer = this.scheduler.after(() => {
            this.relogTimer = undefined;
            if (!this.lastLogged || this.stopped) {
                return;
            }
            const now = this.scheduler.now();
            const relog: PreparedState = { ...this.lastLogged, ts: now, from: this.relogFrom };
            this.lastLogTime = now;
            this.skipped = undefined;
            this.emit(relog, 'relog');
            this.armRelog();
        }, cfg.changesRelogInterval * 1000);
    }

    private clearRelog(): void {
        if (this.relogTimer !== undefined) {
            this.scheduler.cancel(this.relogTimer);
            this.relogTimer = undefined;
        }
    }

    /**
     * Writes values that are waiting in a debounce or block window immediately
     * (used before re-configuration and on shutdown).
     */
    flushPending(): void {
        if (this.debounceTimer !== undefined) {
            this.scheduler.cancel(this.debounceTimer);
            this.debounceTimer = undefined;
            const pending = this.debouncePending;
            this.debouncePending = undefined;
            if (pending) {
                this.accept(pending, 'debounced');
            }
        }
        if (this.blockTimer !== undefined) {
            this.scheduler.cancel(this.blockTimer);
            this.blockTimer = undefined;
            const pending = this.blockPending;
            this.blockPending = undefined;
            if (pending) {
                this.accept(pending, 'blockReleased');
            }
        }
    }

    /**
     * Replaces the settings (on configuration change) and keeps the reference value.
     *
     * @param settings new settings
     */
    update(settings: FilterSettings): void {
        this.flushPending();
        this.settings = settings;
        this.armRelog();
    }

    /**
     * For the adapter shutdown: stops all timers and writes values that wait in a debounce or block
     * window, without starting new timers.
     */
    shutdown(): void {
        this.stopped = true;
        this.clearRelog();
        this.flushPending();
    }

    /** Stops all timers; nothing is emitted afterwards. */
    stop(): void {
        this.stopped = true;
        this.clearRelog();
        this.clearBlocked();
        if (this.debounceTimer !== undefined) {
            this.scheduler.cancel(this.debounceTimer);
            this.debounceTimer = undefined;
        }
        this.debouncePending = undefined;
    }
}
