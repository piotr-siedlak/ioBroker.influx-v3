/**
 * Spool: durable storage of batches on disk.
 *
 * One file per batch: `batch-<seq>-<records>.json` = `{"v":1,"createdAt":<ms>,"lines":[...]}`.
 * Files are written as `<name>.tmp`, fsync'ed (optional) and renamed, so a crash never leaves a
 * half-written batch file. Leftover `.tmp` files are removed on start; unreadable files are moved
 * to `corrupt-*.json` and reported.
 */
import fs from 'node:fs/promises';
import path from 'node:path';

export interface SpoolEntry {
    seq: number;
    count: number;
    bytes: number;
    createdAt: number;
    file: string;
}

interface SpoolFile {
    v: 1;
    createdAt: number;
    lines: string[];
}

const FILE_RE = /^batch-(\d{12})-(\d+)\.json$/;

export class Spool {
    constructor(
        readonly dir: string,
        private readonly fsyncEnabled: boolean,
    ) {}

    private readonly counts = new Map<number, number>();

    private fileOf(seq: number, count = this.counts.get(seq) ?? 0): string {
        return path.join(this.dir, `batch-${String(seq).padStart(12, '0')}-${count}.json`);
    }

    async init(): Promise<void> {
        await fs.mkdir(this.dir, { recursive: true });
    }

    /**
     * Writes a batch durably.
     *
     * @param seq sequence number
     * @param lines line protocol lines
     * @param createdAt creation time
     */
    async write(seq: number, lines: string[], createdAt: number): Promise<number> {
        this.counts.set(seq, lines.length);
        const file = this.fileOf(seq, lines.length);
        const tmp = `${file}.tmp`;
        const content: SpoolFile = { v: 1, createdAt, lines };
        const data = Buffer.from(JSON.stringify(content), 'utf8');
        const fh = await fs.open(tmp, 'w');
        try {
            await fh.writeFile(data);
            if (this.fsyncEnabled) {
                await fh.sync();
            }
        } finally {
            await fh.close();
        }
        await fs.rename(tmp, file);
        return data.length;
    }

    /**
     * Reads the lines of a batch.
     *
     * @param seq sequence number
     */
    async read(seq: number): Promise<{ lines: string[]; createdAt: number }> {
        const raw = await fs.readFile(this.fileOf(seq), 'utf8');
        const parsed = JSON.parse(raw) as SpoolFile;
        if (parsed?.v !== 1 || !Array.isArray(parsed.lines) || parsed.lines.some(l => typeof l !== 'string')) {
            throw new Error('invalid spool file format');
        }
        return { lines: parsed.lines, createdAt: Number(parsed.createdAt) || 0 };
    }

    /** @param seq sequence number */
    async remove(seq: number): Promise<void> {
        await fs.rm(this.fileOf(seq), { force: true });
        this.counts.delete(seq);
    }

    /**
     * Moves an unreadable file aside.
     *
     * @param seq sequence number
     */
    async quarantine(seq: number): Promise<void> {
        const file = this.fileOf(seq);
        await fs
            .rename(file, path.join(this.dir, `corrupt-${Date.now()}-${path.basename(file)}`))
            .catch(() => fs.rm(file, { force: true }));
        this.counts.delete(seq);
    }

    /** Lists stored batches in order and removes leftover temporary files. */
    async list(): Promise<SpoolEntry[]> {
        let names: string[];
        try {
            names = await fs.readdir(this.dir);
        } catch {
            return [];
        }
        const entries: SpoolEntry[] = [];
        for (const name of names) {
            if (name.endsWith('.tmp')) {
                await fs.rm(path.join(this.dir, name), { force: true });
                continue;
            }
            const m = FILE_RE.exec(name);
            if (!m) {
                continue;
            }
            const file = path.join(this.dir, name);
            try {
                const st = await fs.stat(file);
                const seq = parseInt(m[1], 10);
                const count = parseInt(m[2], 10);
                this.counts.set(seq, count);
                entries.push({ seq, count, bytes: st.size, createdAt: st.mtimeMs, file });
            } catch {
                // removed concurrently
            }
        }
        return entries.sort((a, b) => a.seq - b.seq);
    }
}
