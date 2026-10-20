"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.LiveStartStore = void 0;
/**
 * Time of the first live value this instance queued per state (ADR-0006, extension 7). The import of historical data
 * ends each state there instead of asking InfluxDB 3 for its first value - a query over the whole table that becomes
 * slower with every imported state and fails on InfluxDB 3 Core once it touches more Parquet files than allowed.
 *
 * Kept as a small JSON file in the instance data directory; entries are never changed or removed.
 */
const node_fs_1 = require("node:fs");
const node_path_1 = __importDefault(require("node:path"));
class LiveStartStore {
    file;
    times = new Map();
    dirty = false;
    constructor(file) {
        this.file = file;
    }
    /** Reads the saved times (a missing or broken file means: none yet). */
    async load() {
        let raw;
        try {
            raw = JSON.parse(await node_fs_1.promises.readFile(this.file, 'utf8'));
        }
        catch {
            return;
        }
        if (raw && typeof raw === 'object' && !Array.isArray(raw)) {
            for (const [id, ts] of Object.entries(raw)) {
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
    get(id) {
        return this.times.get(id);
    }
    /**
     * Remembers `ts` as the first live value of the state unless one is known.
     *
     * @param id state id
     * @param ts timestamp of the queued value
     * @returns true when the time is new (the file needs saving)
     */
    mark(id, ts) {
        if (this.times.has(id) || !Number.isFinite(ts)) {
            return false;
        }
        this.times.set(id, ts);
        this.dirty = true;
        return true;
    }
    get size() {
        return this.times.size;
    }
    /** Writes the file when something changed (temporary file + rename). */
    async save() {
        if (!this.dirty) {
            return;
        }
        this.dirty = false;
        try {
            await node_fs_1.promises.mkdir(node_path_1.default.dirname(this.file), { recursive: true });
            const tmp = `${this.file}.tmp`;
            await node_fs_1.promises.writeFile(tmp, JSON.stringify(Object.fromEntries(this.times)), 'utf8');
            await node_fs_1.promises.rename(tmp, this.file);
        }
        catch (e) {
            this.dirty = true;
            throw e;
        }
    }
}
exports.LiveStartStore = LiveStartStore;
//# sourceMappingURL=liveStart.js.map