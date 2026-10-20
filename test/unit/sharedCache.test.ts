import { strict as assert } from 'node:assert';
import { SharedCache } from '../../src/lib/sharedCache';

/** promise that the test resolves or rejects by hand */
function deferred<T>(): { promise: Promise<T>; resolve: (v: T) => void; reject: (e: Error) => void } {
    let resolve!: (v: T) => void;
    let reject!: (e: Error) => void;
    const promise = new Promise<T>((res, rej) => {
        resolve = res;
        reject = rej;
    });
    return { promise, resolve, reject };
}

describe('SharedCache (single flight)', () => {
    let now = 0;
    const clock = (): number => now;
    beforeEach(() => (now = 1000));

    it('150 concurrent callers share one computation', async () => {
        const cache = new SharedCache<string[]>(clock, 10_000);
        const d = deferred<string[]>();
        let loads = 0;
        const load = (): Promise<string[]> => {
            loads++;
            return d.promise;
        };
        const callers = Array.from({ length: 150 }, () => cache.get('db', load));
        d.resolve(['a']);
        const results = await Promise.all(callers);
        assert.equal(loads, 1);
        assert.ok(results.every(r => r[0] === 'a'));
    });

    it('reuses a fresh result, computes again after the TTL', async () => {
        const cache = new SharedCache<number>(clock, 10_000);
        let loads = 0;
        const load = (): Promise<number> => Promise.resolve(++loads);
        assert.equal(await cache.get('k', load), 1);
        now += 9_000;
        assert.equal(await cache.get('k', load), 1);
        now += 2_000;
        assert.equal(await cache.get('k', load), 2);
        assert.equal(await cache.get('other', load), 3, 'keys are separate');
    });

    it('keeps the last result available while a new computation runs', async () => {
        const cache = new SharedCache<number>(clock, 1_000);
        await cache.get('k', () => Promise.resolve(1));
        now += 5_000;
        const d = deferred<number>();
        const running = cache.get('k', () => d.promise);
        assert.equal(cache.peek('k'), 1, 'stale value while loading');
        d.resolve(2);
        assert.equal(await running, 2);
        assert.equal(cache.peek('k'), 2);
    });

    it('remembers a failure for the error TTL, keeps the older value', async () => {
        const cache = new SharedCache<number>(clock, 1_000, 60_000);
        await cache.get('k', () => Promise.resolve(7));
        now += 2_000;
        let loads = 0;
        const failing = (): Promise<number> => {
            loads++;
            return Promise.reject(new Error('timeout'));
        };
        await assert.rejects(cache.get('k', failing), /timeout/);
        await assert.rejects(cache.get('k', failing), /timeout/);
        assert.equal(loads, 1, 'not asked again during the error TTL');
        assert.equal(cache.peek('k'), 7);
        now += 61_000;
        assert.equal(await cache.get('k', () => Promise.resolve(8)), 8);
    });

    it('a loader that throws synchronously is a failure, not a crash', async () => {
        const cache = new SharedCache<number>(clock, 1_000);
        await assert.rejects(
            cache.get('k', () => {
                throw new Error('bad url');
            }),
            /bad url/,
        );
    });

    it('invalidate makes the next caller compute anew', async () => {
        const cache = new SharedCache<number>(clock, 60_000);
        let loads = 0;
        const load = (): Promise<number> => Promise.resolve(++loads);
        await cache.get('a', load);
        await cache.get('b', load);
        cache.invalidate('a');
        assert.equal(await cache.get('a', load), 3);
        assert.equal(await cache.get('b', load), 2);
        cache.invalidate();
        assert.equal(await cache.get('b', load), 4);
    });
});
