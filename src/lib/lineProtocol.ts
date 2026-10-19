/**
 * Line protocol serialisation for InfluxDB 3.
 *
 * Escaping rules verified against InfluxDB 3 Core 3.12.1 (see docs/RESEARCH.md §4.2):
 * - table (measurement): `\`, `,` and space are escaped
 * - tag keys, tag values, field keys: `\`, `,`, `=` and space are escaped
 * - string field values: `\` and `"` are escaped, the value is wrapped in double quotes
 * - identifiers must not be empty, must not end with a backslash and must not contain line breaks
 * Timestamps are always integer milliseconds (`precision=millisecond`).
 */

export type FieldValue =
    | { type: 'float'; value: number }
    | { type: 'integer'; value: number }
    | { type: 'boolean'; value: boolean }
    | { type: 'string'; value: string };

export interface LpRecord {
    table: string;
    /** tag key → value; serialised sorted by key */
    tags: Record<string, string>;
    /** field key → value; at least one field is required */
    fields: Record<string, FieldValue>;
    /** epoch milliseconds */
    ts: number;
}

export class LineProtocolError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'LineProtocolError';
    }
}

// eslint-disable-next-line no-control-regex
const CONTROL_CHARS = /[\u0000-\u001f\u007f]/g;

/**
 * Makes an arbitrary string usable as identifier (table, tag key/value, field key):
 * control characters become spaces, the string is trimmed and trailing backslashes are removed.
 *
 * @param input the raw identifier
 */
export function sanitizeIdentifier(input: string): string {
    return input.replace(CONTROL_CHARS, ' ').trim().replace(/\\+$/, '').trim();
}

/** @param s table name */
export function escapeTable(s: string): string {
    return s.replace(/\\/g, '\\\\').replace(/,/g, '\\,').replace(/ /g, '\\ ');
}

/** @param s tag key, tag value or field key */
export function escapeKey(s: string): string {
    return s.replace(/\\/g, '\\\\').replace(/,/g, '\\,').replace(/=/g, '\\=').replace(/ /g, '\\ ');
}

/** @param s string field value (without the surrounding quotes) */
export function escapeStringField(s: string): string {
    return s.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}

/**
 * Formats a finite JS number as line protocol float. NaN/Infinity are rejected by InfluxDB
 * (measured), so they must never reach this function.
 *
 * @param n the number
 */
export function formatFloat(n: number): string {
    if (!Number.isFinite(n)) {
        throw new LineProtocolError(`Non-finite number ${n} cannot be written`);
    }
    if (Object.is(n, -0)) {
        return '0';
    }
    return String(n);
}

function formatField(v: FieldValue): string {
    switch (v.type) {
        case 'float':
            return formatFloat(v.value);
        case 'integer':
            if (!Number.isSafeInteger(v.value)) {
                throw new LineProtocolError(`Invalid integer ${v.value}`);
            }
            return `${v.value}i`;
        case 'boolean':
            return v.value ? 'true' : 'false';
        case 'string':
            return `"${escapeStringField(v.value)}"`;
    }
}

function checkIdentifier(kind: string, s: string): void {
    if (!s) {
        throw new LineProtocolError(`Empty ${kind}`);
    }
    if (/[\r\n]/.test(s)) {
        throw new LineProtocolError(`${kind} must not contain line breaks`);
    }
    if (s.endsWith('\\')) {
        throw new LineProtocolError(`${kind} must not end with a backslash`);
    }
}

/**
 * Serialises one record to a single line protocol line (without trailing newline).
 *
 * @param rec the record
 */
export function serialize(rec: LpRecord): string {
    checkIdentifier('table', rec.table);
    if (!Number.isFinite(rec.ts) || !Number.isInteger(rec.ts)) {
        throw new LineProtocolError(`Invalid timestamp ${rec.ts}`);
    }
    let line = escapeTable(rec.table);
    const tagKeys = Object.keys(rec.tags).sort();
    for (const key of tagKeys) {
        const value = rec.tags[key];
        if (value === '') {
            continue; // empty tag values are not allowed - omit the tag
        }
        checkIdentifier('tag key', key);
        checkIdentifier('tag value', value);
        line += `,${escapeKey(key)}=${escapeKey(value)}`;
    }
    const fieldKeys = Object.keys(rec.fields);
    if (!fieldKeys.length) {
        throw new LineProtocolError('At least one field is required');
    }
    const parts: string[] = [];
    for (const key of fieldKeys) {
        checkIdentifier('field key', key);
        parts.push(`${escapeKey(key)}=${formatField(rec.fields[key])}`);
    }
    return `${line} ${parts.join(',')} ${rec.ts}`;
}

/** @param line one serialised line; returns its UTF-8 size including the separating newline */
export function lineBytes(line: string): number {
    return Buffer.byteLength(line, 'utf8') + 1;
}
