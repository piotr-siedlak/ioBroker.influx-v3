# ADR-0005 — Security model of secrets and `sendTo` commands

Status: accepted (2026-10-19)

* Token: `native.token`, listed in `encryptedNative` and `protectedNative` (top-level keys of `io-package.json`).
  It is only placed in the `Authorization` header. A central `redact()` removes `Bearer …`, `Token …`,
  `apiv3_…` and the configured token from every log line, error message and `sendTo` answer.
* The password/token of the old database for the history import (`native.dmPassword`, `native.dmToken`) is treated
  like the token: `encryptedNative` + `protectedNative`, registered with `redact()`, never copied from another
  adapter (*Copy connection settings* takes only URL, version, database/bucket, organisation and user). The old
  database is only read (InfluxQL `SELECT`/`SHOW`, Flux `from()`/`schema`) – a read-only user/token is enough
  (ADR-0006).
* Commands that change objects run only on an explicit request: `migrationApply` writes `common.custom["influx-v3.N"]`
  of the listed states and never changes `influxdb.N`; the data import (`dmStart`) writes only into InfluxDB 3;
  `createDatabase` never runs automatically and never deletes.
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
