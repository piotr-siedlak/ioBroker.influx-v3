"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.FilterPipeline = void 0;
class FilterPipeline {
    settings;
    emit;
    relogFrom;
    scheduler;
    lastLogged;
    lastLogTime = 0;
    skipped;
    debounceTimer;
    debouncePending;
    blockTimer;
    blockPending;
    relogTimer;
    stopped = false;
    constructor(settings, emit, relogFrom, scheduler) {
        this.settings = settings;
        this.emit = emit;
        this.relogFrom = relogFrom;
        this.scheduler = scheduler;
    }
    /** last value that was written (or accepted as reference) */
    get last() {
        return this.lastLogged;
    }
    /**
     * Sets the reference value at start without writing it (or writing it when `store` is true).
     *
     * @param s current state
     * @param store write the value as initial value (ts = now)
     */
    initialize(s, store) {
        if (store) {
            const initial = { ...s, ts: this.scheduler.now(), from: this.relogFrom };
            this.lastLogged = initial;
            this.lastLogTime = initial.ts;
            this.emit(initial, 'initial');
        }
        else {
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
    process(s, blockReleased = false) {
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
        if (!restarted &&
            !blockReleased &&
            cfg.blockTime > 0 &&
            this.lastLogged &&
            s.ts < this.lastLogged.ts + cfg.blockTime) {
            this.holdBlocked(s, this.lastLogged.ts + cfg.blockTime);
            return 'blocked';
        }
        if (this.lastLogged && cfg.changesOnly) {
            // a value that bounced back during a debounce window cancels the pending change
            const unchanged = this.lastLogged.val === s.val;
            if (unchanged) {
                const relogDue = cfg.changesRelogInterval > 0 && s.ts - this.lastLogTime >= cfg.changesRelogInterval * 1000;
                if (!relogDue) {
                    if (!restarted) {
                        this.rememberSkipped(s);
                    }
                    return 'unchanged';
                }
            }
            else if (typeof s.val === 'number' &&
                typeof this.lastLogged.val === 'number' &&
                cfg.changesMinDelta > 0 &&
                Math.abs(this.lastLogged.val - s.val) < cfg.changesMinDelta) {
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
    accept(s, reason) {
        this.lastLogged = s;
        this.lastLogTime = s.ts;
        this.emit(s, reason);
        this.armRelog();
    }
    rememberSkipped(s) {
        if (this.settings.logSkippedBeforeChange) {
            this.skipped = s;
        }
    }
    holdBlocked(s, until) {
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
    clearBlocked() {
        if (this.blockTimer !== undefined) {
            this.scheduler.cancel(this.blockTimer);
            this.blockTimer = undefined;
        }
        this.blockPending = undefined;
    }
    armRelog() {
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
            const relog = { ...this.lastLogged, ts: now, from: this.relogFrom };
            this.lastLogTime = now;
            this.skipped = undefined;
            this.emit(relog, 'relog');
            this.armRelog();
        }, cfg.changesRelogInterval * 1000);
    }
    clearRelog() {
        if (this.relogTimer !== undefined) {
            this.scheduler.cancel(this.relogTimer);
            this.relogTimer = undefined;
        }
    }
    /**
     * Writes values that are waiting in a debounce or block window immediately
     * (used before re-configuration and on shutdown).
     */
    flushPending() {
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
    update(settings) {
        this.flushPending();
        this.settings = settings;
        this.armRelog();
    }
    /**
     * For the adapter shutdown: stops all timers and writes values that wait in a debounce or block
     * window, without starting new timers.
     */
    shutdown() {
        this.stopped = true;
        this.clearRelog();
        this.flushPending();
    }
    /** Stops all timers; nothing is emitted afterwards. */
    stop() {
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
exports.FilterPipeline = FilterPipeline;
//# sourceMappingURL=filterPipeline.js.map