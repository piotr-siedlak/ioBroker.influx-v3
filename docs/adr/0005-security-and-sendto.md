# ADR-0005 — Security model of secrets and `sendTo` commands

Status: accepted (2026-10-19)

* Token: `native.token`, listed in `encryptedNative` and `protectedNative` (top-level keys of `io-package.json`).
  It is only placed in the `Authorization` header. A central `redact()` removes `Bearer …`, `Token …`,
  `apiv3_…` and the configured token from every log line, error message and `sendTo` answer.
* `testConnection` from Admin receives the unsaved form values; when the token field still contains the
  encrypted placeholder or is empty, the stored token is used. The answer contains only status, edition,
  version, latency, database existence and retention — never headers or the token.
* `query` / `queryInfluxQL`: only `SELECT`, `WITH`, `SHOW`, `EXPLAIN`, `DESCRIBE` (SQL) or `SELECT`/`SHOW`
  (InfluxQL); a single statement (no `;` outside string literals); comments stripped before checking;
  row limit (`queryMaxRows`), byte limit and timeout. InfluxDB 3 Core plans `INSERT INTO … VALUES` via SQL
  **(measured)**, so this guard is necessary.
* Optional allow-list `querySenders` (comma-separated adapter instance prefixes, e.g. `admin, javascript.0`).
  Empty = every adapter instance may query (same as the classic adapter). `enableHistory`/`disableHistory`
  keep the classic behaviour (any instance may call them) because scripts rely on them.
* All identifiers interpolated into SQL (table, column, tag keys) are quoted with `"` and internal quotes doubled;
  all values (state id, time range) are `$params`.
* The adapter opens no network port.
