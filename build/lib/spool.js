"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Spool = void 0;
/**
 * Spool: durable storage of batches on disk.
 *
 * One file per batch: `batch-<seq>-<records>.json` = `{"v":1,"createdAt":<ms>,"lines":[...]}`.
 * Files are written as `<name>.tmp`, fsync'ed (optional) and renamed, so a crash never leaves a
 * half-written batch file. Leftover `.tmp` files are removed on start; unreadable files are moved
 * to `corrupt-*.json` and reported.
 */
const promises_1 = __importDefault(require("node:fs/promises"));
const node_path_1 = __importDefault(require("node:path"));
const FILE_RE = /^batch-(\d{12})-(\d+)\.json$/;
class Spool {
    dir;
    fsyncEnabled;
    constructor(dir, fsyncEnabled) {
        this.dir = dir;
        this.fsyncEnabled = fsyncEnabled;
    }
    counts = new Map();
    fileOf(seq, count = this.counts.get(seq) ?? 0) {
        return node_path_1.default.join(this.dir, `batch-${String(seq).padStart(12, '0')}-${count}.json`);
    }
    async init() {
        await promises_1.default.mkdir(this.dir, { recursive: true });
    }
    /**
     * Writes a batch durably.
     *
     * @param seq sequence number
     * @param lines line protocol lines
     * @param createdAt creation time
     */
    async write(seq, lines, createdAt) {
        this.counts.set(seq, lines.length);
        const file = this.fileOf(seq, lines.length);
        const tmp = `${file}.tmp`;
        const content = { v: 1, createdAt, lines };
        const data = Buffer.from(JSON.stringify(content), 'utf8');
        const fh = await promises_1.default.open(tmp, 'w');
        try {
            await fh.writeFile(data);
            if (this.fsyncEnabled) {
                await fh.sync();
            }
        }
        finally {
            await fh.close();
        }
        await promises_1.default.rename(tmp, file);
        return data.length;
    }
    /**
     * Reads the lines of a batch.
     *
     * @param seq sequence number
     */
    async read(seq) {
        const raw = await promises_1.default.readFile(this.fileOf(seq), 'utf8');
        const parsed = JSON.parse(raw);
        if (parsed?.v !== 1 || !Array.isArray(parsed.lines) || parsed.lines.some(l => typeof l !== 'string')) {
            throw new Error('invalid spool file format');
        }
        return { lines: parsed.lines, createdAt: Number(parsed.createdAt) || 0 };
    }
    /** @param seq sequence number */
    async remove(seq) {
        await promises_1.default.rm(this.fileOf(seq), { force: true });
        this.counts.delete(seq);
    }
    /**
     * Moves an unreadable file aside.
     *
     * @param seq sequence number
     */
    async quarantine(seq) {
        const file = this.fileOf(seq);
        await promises_1.default
            .rename(file, node_path_1.default.join(this.dir, `corrupt-${Date.now()}-${node_path_1.default.basename(file)}`))
            .catch(() => promises_1.default.rm(file, { force: true }));
        this.counts.delete(seq);
    }
    /** Lists stored batches in order and removes leftover temporary files. */
    async list() {
        let names;
        try {
            names = await promises_1.default.readdir(this.dir);
        }
        catch {
            return [];
        }
        const entries = [];
        for (const name of names) {
            if (name.endsWith('.tmp')) {
                await promises_1.default.rm(node_path_1.default.join(this.dir, name), { force: true });
                continue;
            }
            const m = FILE_RE.exec(name);
            if (!m) {
                continue;
            }
            const file = node_path_1.default.join(this.dir, name);
            try {
                const st = await promises_1.default.stat(file);
                const seq = parseInt(m[1], 10);
                const count = parseInt(m[2], 10);
                this.counts.set(seq, count);
                entries.push({ seq, count, bytes: st.size, createdAt: st.mtimeMs, file });
            }
            catch {
                // removed concurrently
            }
        }
        return entries.sort((a, b) => a.seq - b.seq);
    }
}
exports.Spool = Spool;
//# sourceMappingURL=spool.js.map