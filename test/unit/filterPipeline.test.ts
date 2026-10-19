import { strict as assert } from 'node:assert';
import {
    type EmitReason,
    FilterPipeline,
    type FilterSettings,
    type PreparedState,
    type Scheduler,
} from '../../src/lib/filterPipeline';

/** deterministic scheduler */
class FakeClock implements Scheduler {
    t = 1_000_000;
    private seq = 0;
    private readonly timers = new Map<number, { at: number; fn: () => void }>();
    now(): number {
        return this.t;
    }
    setTimeout(fn: () => void, ms: number): unknown {
        const id = ++this.seq;
        this.timers.set(id, { at: this.t + ms, fn });
        return id;
    }
    clearTimeout(h: unknown): void {
        this.timers.delete(h as number);
    }
    advance(ms: number): void {
        const end = this.t + ms;
        for (;;) {
            let next: [number, { at: number; fn: () => void }] | undefined;
            for (const e of this.timers) {
                if (e[1].at <= end && (!next || e[1].at < next[1].at)) {
                    next = e;
                }
            }
            if (!next) {
                break;
            }
            this.timers.delete(next[0]);
            this.t = next[1].at;
            next[1].fn();
        }
        this.t = end;
    }
    get pending(): number {
        return this.timers.size;
    }
}

const base: FilterSettings = {
    ackFilter: 'all',
    qFilter: 'all',
    ignoreZero: false,
    ignoreBelowNumber: null,
    ignoreAboveNumber: null,
    debounceTime: 0,
    blockTime: 0,
    changesOnly: false,
    changesRelogInterval: 0,
    changesMinDelta: 0,
    logSkippedBeforeChange: false,
};

function setup(over: Partial<FilterSettings> = {}): {
    p: FilterPipeline;
    clock: FakeClock;
    out: { val: unknown; ts: number; reason: EmitReason; from: string }[];
    s: (val: number | boolean | string, extra?: Partial<PreparedState>) => PreparedState;
} {
    const clock = new FakeClock();
    const out: { val: unknown; ts: number; reason: EmitReason; from: string }[] = [];
    const p = new FilterPipeline(
        { ...base, ...over },
        (st, reason) => out.push({ val: st.val, ts: st.ts, reason, from: st.from }),
        'system.adapter.influx_v3.0',
        clock,
    );
    const s = (val: number | boolean | string, extra: Partial<PreparedState> = {}): PreparedState => ({
        val,
        ts: clock.now(),
        ack: true,
        q: 0,
        from: 'system.adapter.test.0',
        ...extra,
    });
    return { p, clock, out, s };
}

describe('FilterPipeline', () => {
    it('without filters every value is emitted', () => {
        const { p, out, s, clock } = setup();
        assert.equal(p.process(s(1)), 'emitted');
        clock.advance(10);
        assert.equal(p.process(s(1)), 'emitted');
        assert.deepEqual(
            out.map(o => o.val),
            [1, 1],
        );
    });

    it('ack and quality filters', () => {
        const a = setup({ ackFilter: 'ackOnly' });
        assert.equal(a.p.process(a.s(1, { ack: false })), 'filtered:ack');
        assert.equal(a.p.process(a.s(1, { ack: true })), 'emitted');
        const b = setup({ ackFilter: 'notAckOnly' });
        assert.equal(b.p.process(b.s(1, { ack: true })), 'filtered:ack');
        const q = setup({ qFilter: 'goodOnly' });
        assert.equal(q.p.process(q.s(1, { q: 0x44 })), 'filtered:q');
        assert.equal(q.p.process(q.s(1, { q: 0 })), 'emitted');
    });

    it('ignore zero / below / above apply to numbers only', () => {
        const { p, s } = setup({ ignoreZero: true, ignoreBelowNumber: -10, ignoreAboveNumber: 100 });
        assert.equal(p.process(s(0)), 'filtered:ignore');
        assert.equal(p.process(s(-11)), 'filtered:ignore');
        assert.equal(p.process(s(101)), 'filtered:ignore');
        assert.equal(p.process(s(50)), 'emitted');
        assert.equal(p.process(s('0')), 'emitted');
    });

    it('changesOnly suppresses unchanged values', () => {
        const { p, s, out, clock } = setup({ changesOnly: true });
        p.process(s(1));
        clock.advance(1000);
        assert.equal(p.process(s(1)), 'unchanged');
        assert.equal(p.process(s(2)), 'emitted');
        assert.deepEqual(
            out.map(o => o.val),
            [1, 2],
        );
    });

    it('changesOnly with logSkippedBeforeChange writes the last skipped value first', () => {
        const { p, s, out, clock } = setup({ changesOnly: true, logSkippedBeforeChange: true });
        p.process(s(1));
        clock.advance(1000);
        p.process(s(1));
        clock.advance(1000);
        p.process(s(1));
        const skippedTs = clock.now();
        clock.advance(1000);
        p.process(s(5));
        assert.deepEqual(
            out.map(o => [o.val, o.reason]),
            [
                [1, 'change'],
                [1, 'skipped'],
                [5, 'change'],
            ],
        );
        assert.equal(out[1].ts, skippedTs);
    });

    it('min delta (numbers)', () => {
        const { p, s } = setup({ changesOnly: true, changesMinDelta: 0.5 });
        p.process(s(20));
        assert.equal(p.process(s(20.3)), 'minDelta');
        assert.equal(p.process(s(19.6)), 'minDelta');
        assert.equal(p.process(s(20.5)), 'emitted');
    });

    it('relog interval re-writes an unchanged value with the current time', () => {
        const { p, s, out, clock } = setup({ changesOnly: true, changesRelogInterval: 60 });
        p.process(s(7));
        clock.advance(60_000);
        assert.deepEqual(
            out.map(o => [o.val, o.reason]),
            [
                [7, 'change'],
                [7, 'relog'],
            ],
        );
        assert.equal(out[1].ts, clock.now());
        assert.equal(out[1].from, 'system.adapter.influx_v3.0');
        clock.advance(30_000);
        p.process(s(8)); // change resets the relog timer
        clock.advance(59_000);
        assert.equal(out.length, 3);
        clock.advance(1_000);
        assert.equal(out.length, 4);
        assert.equal(out[3].reason, 'relog');
    });

    it('an unchanged value is accepted when the relog interval has passed', () => {
        const { p, s, clock } = setup({ changesOnly: true, changesRelogInterval: 10 });
        p.process(s(1));
        p.stop(); // no timer-based relog; check the value-based path
        const q = setup({ changesOnly: true, changesRelogInterval: 10 });
        q.p.process(q.s(1));
        q.clock.t += 11_000; // time passes without timers firing
        assert.equal(q.p.process(q.s(1)), 'emitted');
        assert.ok(clock.now() > 0);
    });

    it('debounce writes the value after it was stable', () => {
        const { p, s, out, clock } = setup({ debounceTime: 1000 });
        assert.equal(p.process(s(1)), 'debounce:waiting');
        clock.advance(500);
        assert.equal(p.process(s(2)), 'debounce:restarted');
        clock.advance(500);
        assert.equal(p.process(s(2)), 'debounce:waiting', 'same value keeps the timer');
        assert.equal(out.length, 0);
        clock.advance(500);
        assert.deepEqual(
            out.map(o => [o.val, o.reason]),
            [[2, 'debounced']],
        );
    });

    it('debounce: a value bouncing back to the logged one cancels the pending change', () => {
        const { p, s, out, clock } = setup({ debounceTime: 1000, changesOnly: true });
        p.initialize(s(1), false);
        p.process(s(2));
        clock.advance(300);
        assert.equal(p.process(s(1)), 'unchanged');
        clock.advance(5000);
        assert.equal(out.length, 0);
    });

    it('blockTime holds values back and writes the newest one at the end of the window', () => {
        const { p, s, out, clock } = setup({ blockTime: 1000 });
        p.process(s(1));
        clock.advance(100);
        assert.equal(p.process(s(2)), 'blocked');
        clock.advance(100);
        assert.equal(p.process(s(3)), 'blocked');
        assert.equal(out.length, 1);
        clock.advance(800);
        assert.deepEqual(
            out.map(o => [o.val, o.reason]),
            [
                [1, 'change'],
                [3, 'blockReleased'],
            ],
        );
        clock.advance(1000);
        assert.equal(p.process(s(4)), 'emitted');
    });

    it('fixed filter order: ack before ignore before changesOnly', () => {
        const { p, s } = setup({ ackFilter: 'ackOnly', ignoreZero: true, changesOnly: true });
        assert.equal(p.process(s(0, { ack: false })), 'filtered:ack');
        assert.equal(p.process(s(0)), 'filtered:ignore');
        assert.equal(p.process(s(1)), 'emitted');
        assert.equal(p.process(s(1)), 'unchanged');
    });

    it('initial value: stored with start time or only used as reference', () => {
        const a = setup({ changesOnly: true });
        a.p.initialize(a.s(5, { ts: 1 }), true);
        assert.deepEqual(
            a.out.map(o => [o.val, o.reason, o.ts]),
            [[5, 'initial', a.clock.now()]],
        );
        assert.equal(a.p.process(a.s(5)), 'unchanged');
        const b = setup({ changesOnly: true });
        b.p.initialize(b.s(5), false);
        assert.equal(b.out.length, 0);
        assert.equal(b.p.process(b.s(5)), 'unchanged');
    });

    it('stop() cancels timers and nothing is emitted afterwards', () => {
        const { p, s, out, clock } = setup({ debounceTime: 1000, changesOnly: true, changesRelogInterval: 1 });
        p.process(s(1));
        p.stop();
        clock.advance(10_000);
        assert.equal(out.length, 0);
        assert.equal(clock.pending, 0);
        assert.equal(p.process(s(2)), 'filtered:ignore');
    });

    it('update() flushes pending values and applies new settings without losing the reference', () => {
        const { p, s, out, clock } = setup({ debounceTime: 1000, changesOnly: true });
        p.process(s(1));
        p.update({ ...base, changesOnly: true });
        assert.deepEqual(
            out.map(o => o.val),
            [1],
        );
        clock.advance(10);
        assert.equal(p.process(s(1)), 'unchanged');
        assert.equal(p.process(s(2)), 'emitted');
    });
});
