/**
 * Time of the first live value this instance queued per state (ADR-0006, extension 7). The import of historical data
 * ends each state there instead of asking InfluxDB 3 for its first value - a query over the whole table that becomes
 * slower with every imported state and fails on InfluxDB 3 Core once it touches more Parquet files than allowed.
 *
 * Kept as a small JSON file in the instance data directory; entries are never changed or removed.
 */
import { promises as fsp } from 'node:fs';
import path from 'node:path';

export class LiveStartStore {
    private readonly times = new Map<string, number>();
    private dirty = false;

    constructor(private readonly file: string) {}

    /** Reads the saved times (a missing or broken file means: none yet). */
    async load(): Promise<void> {
        let raw: unknown;
        try {
            raw = JSON.parse(await fsp.readFile(this.file, 'utf8'));
        } catch {
            return;
        }
        if (raw && typeof raw === 'object' && !Array.isArray(raw)) {
            for (const [id, ts] of Object.entries(raw as Record<string, unknown>)) {
                if (typeof ts === 'number' && Number.isFinite(ts) && !this.times.has(id)) {
                    this.times.set(id, ts);
                }
            }
        }
    }

    /**
     * Time of the first live value of a state, undefined if none was queued yet.
     *
     * @param id state id
     */
    get(id: string): number | undefined {
        return this.times.get(id);
    }

    /**
     * Remembers `ts` as the first live value of the state unless one is known.
     *
     * @param id state id
     * @param ts timestamp of the queued value
     * @returns true when the time is new (the file needs saving)
     */
    mark(id: string, ts: number): boolean {
        if (this.times.has(id) || !Number.isFinite(ts)) {
            return false;
        }
        this.times.set(id, ts);
        this.dirty = true;
        return true;
    }

    get size(): number {
        return this.times.size;
    }

    /** Writes the file when something changed (temporary file + rename). */
    async save(): Promise<void> {
        if (!this.dirty) {
            return;
        }
        this.dirty = false;
        try {
            await fsp.mkdir(path.dirname(this.file), { recursive: true });
            const tmp = `${this.file}.tmp`;
            await fsp.writeFile(tmp, JSON.stringify(Object.fromEntries(this.times)), 'utf8');
            await fsp.rename(tmp, this.file);
        } catch (e) {
            this.dirty = true;
            throw e;
        }
    }
}
