"use strict";
/**
 * QueryService helpers: read-only guard for user supplied SQL / InfluxQL and sender allow-list.
 * See docs/adr/0005-security-and-sendto.md.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.stripSql = stripSql;
exports.checkReadOnlySql = checkReadOnlySql;
exports.checkReadOnlyInfluxQL = checkReadOnlyInfluxQL;
exports.senderAllowed = senderAllowed;
/**
 * Removes comments and the content of string literals / quoted identifiers, so keywords and
 * semicolons inside them are not misinterpreted.
 *
 * @param sql query text
 */
function stripSql(sql) {
    let out = '';
    let i = 0;
    while (i < sql.length) {
        const c = sql[i];
        const next = sql[i + 1];
        if (c === '-' && next === '-') {
            const end = sql.indexOf('\n', i);
            i = end < 0 ? sql.length : end + 1;
            out += ' ';
            continue;
        }
        if (c === '/' && next === '*') {
            const end = sql.indexOf('*/', i + 2);
            i = end < 0 ? sql.length : end + 2;
            out += ' ';
            continue;
        }
        if (c === "'" || c === '"') {
            // skip literal / quoted identifier; doubled quote = escaped quote
            let j = i + 1;
            while (j < sql.length) {
                if (sql[j] === c) {
                    if (sql[j + 1] === c) {
                        j += 2;
                        continue;
                    }
                    break;
                }
                j++;
            }
            out += c === "'" ? "''" : '""';
            i = j + 1;
            continue;
        }
        out += c;
        i++;
    }
    return out;
}
const SQL_ALLOWED_START = new Set(['SELECT', 'WITH', 'SHOW', 'DESCRIBE', 'EXPLAIN', 'VALUES']);
// `replace(...)` is a string function and therefore not listed; keywords only count outside literals
const SQL_FORBIDDEN = /\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|COPY|SET|GRANT|REVOKE|ATTACH|DETACH|MERGE|PREPARE|EXECUTE|DEALLOCATE)\b/i;
/**
 * Accepts a single read-only SQL statement.
 *
 * @param sql user query
 * @param maxLength maximum query length
 */
function checkReadOnlySql(sql, maxLength = 20000) {
    if (typeof sql !== 'string' || !sql.trim()) {
        return { ok: false, reason: 'query must be a non-empty string' };
    }
    if (sql.length > maxLength) {
        return { ok: false, reason: `query longer than ${maxLength} characters` };
    }
    const stripped = stripSql(sql).trim().replace(/;\s*$/, '');
    if (stripped.includes(';')) {
        return { ok: false, reason: 'only a single statement is allowed' };
    }
    const words = stripped
        .split(/\s+/)
        .filter(Boolean)
        .map(w => w.toUpperCase());
    const first = words[0]?.replace(/\($/, '');
    if (!first || !SQL_ALLOWED_START.has(first)) {
        return {
            ok: false,
            reason: `only read statements are allowed (SELECT, WITH, SHOW, DESCRIBE, EXPLAIN), got "${words[0] ?? ''}"`,
        };
    }
    if (SQL_FORBIDDEN.test(stripped)) {
        const m = SQL_FORBIDDEN.exec(stripped);
        return { ok: false, reason: `statement contains the forbidden keyword ${m?.[1]?.toUpperCase()}` };
    }
    return { ok: true, statement: sql.trim().replace(/;\s*$/, '') };
}
/**
 * Accepts a single read-only InfluxQL statement (SELECT / SHOW / EXPLAIN SELECT).
 *
 * @param q user query
 * @param maxLength maximum query length
 */
function checkReadOnlyInfluxQL(q, maxLength = 20000) {
    if (typeof q !== 'string' || !q.trim()) {
        return { ok: false, reason: 'query must be a non-empty string' };
    }
    if (q.length > maxLength) {
        return { ok: false, reason: `query longer than ${maxLength} characters` };
    }
    const stripped = stripSql(q).trim().replace(/;\s*$/, '');
    if (stripped.includes(';')) {
        return { ok: false, reason: 'only a single statement is allowed' };
    }
    const first = stripped.split(/\s+/)[0]?.toUpperCase();
    if (first !== 'SELECT' && first !== 'SHOW' && first !== 'EXPLAIN') {
        return { ok: false, reason: 'only SELECT, SHOW and EXPLAIN are allowed' };
    }
    if (/\b(INTO|DROP|DELETE|CREATE|ALTER|GRANT|REVOKE|KILL)\b/i.test(stripped)) {
        return { ok: false, reason: 'statement contains a forbidden keyword' };
    }
    return { ok: true, statement: q.trim().replace(/;\s*$/, '') };
}
/**
 * Checks whether a sendTo sender may run user queries.
 *
 * @param from `msg.from`, e.g. `system.adapter.javascript.0`
 * @param allowList configured prefixes (empty = everybody)
 */
function senderAllowed(from, allowList) {
    if (!allowList.length) {
        return true;
    }
    const sender = (from ?? '').replace(/^system\.adapter\./, '');
    return allowList.some(p => {
        const prefix = p.replace(/^system\.adapter\./, '');
        return sender === prefix || sender.startsWith(`${prefix}.`);
    });
}
//# sourceMappingURL=queryService.js.map