import { strict as assert } from 'node:assert';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { LiveStartStore } from '../../src/lib/liveStart';

describe('LiveStartStore (first live value per state)', () => {
    let dir: string;
    beforeEach(() => (dir = mkdtempSync(path.join(os.tmpdir(), 'iob-live-'))));
    afterEach(() => rmSync(dir, { recursive: true, force: true }));

    it('keeps only the first time of a state and survives a restart', async () => {
        const file = path.join(dir, 'sub', 'live-start.json');
        const a = new LiveStartStore(file);
        await a.load();
        assert.equal(a.mark('x', 1000), true);
        assert.equal(a.mark('x', 500), false, 'never changed');
        assert.equal(a.mark('y', NaN), false);
        await a.save();
        assert.deepEqual(JSON.parse(readFileSync(file, 'utf8')), { x: 1000 });
        const b = new LiveStartStore(file);
        await b.load();
        assert.equal(b.get('x'), 1000);
        assert.equal(b.get('y'), undefined);
        assert.equal(b.size, 1);
    });

    it('a missing or broken file means no times; saving without changes writes nothing', async () => {
        const file = path.join(dir, 'live-start.json');
        writeFileSync(file, '{broken');
        const s = new LiveStartStore(file);
        await s.load();
        assert.equal(s.size, 0);
        await s.save();
        assert.equal(readFileSync(file, 'utf8'), '{broken');
        writeFileSync(file, JSON.stringify({ a: 5, b: 'x', c: null }));
        await s.load();
        assert.equal(s.get('a'), 5);
        assert.equal(s.size, 1);
    });
});
