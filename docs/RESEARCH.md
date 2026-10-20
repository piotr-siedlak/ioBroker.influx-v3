# Research report — ioBroker adapter for InfluxDB 3 (`influx-v3`)

Date of research: **2026-10-19**. Every number in this document was either read from an
official source listed in [Sources](#sources) or measured against a real server
(InfluxDB 3 Core **3.12.1**, Docker image `influxdb:3-core`, revision `692ea4a994`).
Measured facts are marked **(measured)**; facts only taken from documentation are marked
**(docs)**. Nothing here is derived "by analogy" from InfluxDB 1.x/2.x.

## 1. State of the target repository

| Item | Finding |
| --- | --- |
| URL | <https://github.com/piotr-siedlak/iobroker-influx_v3> (at the time of the research; target name `ioBroker.influx-v3`, see below) |
| Visibility | public |
| Refs | `git ls-remote` returned **no refs** → the repository is empty (no branches, no commits, no license file) |
| Consequence | the adapter is created from scratch; license chosen in this project: **MIT** (same family as the ioBroker ecosystem, compatible with all used dependencies) |
| Naming problem | the ioBroker repository checker requires the GitHub repository to be named `ioBroker.<adaptername>` (checks `E0002`/`E0003` in `M0000_PackageJson.js`). `iobroker-influx_v3` fails these checks. **Before the first push the repository must be renamed to `ioBroker.influx-v3`** (GitHub keeps a redirect from the old name). `package.json`/`io-package.json` already use the new URL. |

## 2. Versions chosen (and why)

| Component | Latest on 2026-10-19 | Chosen requirement | Reason |
| --- | --- | --- | --- |
| Node.js | 26.x current, 24.x active LTS, 22.x maintenance LTS | `engines.node >= 22` | Repository checker (v5.23.2) requires `>=22`; test matrix must contain 22, 24, 26 (`config.js → testingNodeJs.requiredMatrixNodeVersions`). Node 20 is EOL. |
| js-controller | 7.2.5 | `>= 6.0.11` | Checker's required/recommended minimum. Nothing used needs 7.x (no dotted `encryptedNative` keys, which would require 7.0.7). |
| Admin | 8.2.0 | `>= 7.6.20` | Checker requires 7.6.17, recommends 7.6.20. `jsonCustom` + `textSendTo` with `alsoDependsOn` + `customObj` in patterns are available there. |
| `@iobroker/adapter-core` | 3.4.3 | `^3.4.3` | Checker: required 3.2.3, recommended/actual 3.4.3. |
| `@iobroker/testing` | 6.3.1 | `^6.3.1` | Checker: required 6.2.1. Provides `tests.packageFiles` and `tests.integration` (real js-controller). |
| `@iobroker/eslint-config` | 2.3.4 | `^2.3.4` | Official lint rules (ESLint 9 flat config, Prettier). |
| `@iobroker/types` | 7.2.5 | `^7.2.5` | Type definitions for the adapter API. |
| TypeScript | **7.0.2** (native Go port) | `~6.0.3` | `typescript-eslint` 8.71.2 declares peer `typescript >=4.8.4 <6.1.0`; TS 7 would break linting. The reference adapter `ioBroker.influxdb` 5.1.6 also pins `~6.0.3`. |
| `@alcalzone/release-script` | 5.2.1 | `^5.2.1` | Checker's recommended release tooling. |
| CI actions | — | `ioBroker/testing-action-check@v2`, `testing-action-adapter@v1`, `testing-action-deploy@v1` | Minimum versions required by the checker. Check job on Node 22, deploy job on Node 24. OS matrix ubuntu/windows/macos. |

## 3. Reference adapter `ioBroker.influxdb` (5.1.6) — behaviour reproduced, code not copied

Analysed: `src/main.ts` (4 301 lines), `admin/jsonCustom.json`, `io-package.json`.

| Behaviour | Reference implementation | What `influx-v3` does |
| --- | --- | --- |
| Per-state configuration | `common.custom["influxdb.N"]`, `supportCustoms: true`, `adminUI.custom: "json"` | identical mechanism with key `influx-v3.N`; never reads or writes `influxdb.N` except in the explicit, read-only migration preview |
| Discovery at start | `getObjectView('system','custom')` | same |
| Live re-configuration | `subscribeForeignObjects('*')` + `objectChange` | same, plus enum objects trigger re-computation of room/function tags |
| State subscription | individual subscriptions, switch to `'*'` above 19 states | same idea, threshold configurable (default 20) |
| Filters | debounce, blockTime, changesOnly, changesMinDelta, changesRelogInterval, ignoreZero, ignoreBelow/Above, "log skipped value before change" | all reproduced in a pure, unit-tested `FilterPipeline` with a fixed, documented order |
| Initial value | `relogLastValueOnStart` writes current value on start | `storeOnStart` per state (default from instance) |
| Messages | `features`, `getHistory`, `query`, `enableHistory`, `disableHistory`, `getEnabledDPs`, `storeState`, `update`, `delete*`, `flushBuffer`, `getConflictingPoints` … | `features`, `getHistory`, `query` (SQL), `queryInfluxQL`, `enableHistory`, `disableHistory`, `getEnabledDPs`, `storeState`, `flushBuffer`, `getSchema`, `testConnection`, `getAdapterStats`, `previewMapping`, migration wizard. **No `update`/`delete*`**: InfluxDB 3 SQL has no DML (`DELETE` → "DML not supported" **(measured)**), so they are not advertised in `features`. |
| `enableHistory` write | reads the whole object and replaces only this instance's custom block (to avoid array-merge problems with `extendObject`) | same approach |
| History response | `{ result: [{val, ts, ack, q, from}], step, error, sessionId }` | same shape |
| Retention | creates/changes retention policies itself | **not done**: retention in v3 is a database property (see §5) |

## 4. InfluxDB 3 HTTP API — verified behaviour (Core 3.12.1)

### 4.1 Endpoints used

| Endpoint | Purpose | Verified result |
| --- | --- | --- |
| `GET /ping` | version + edition | 200, JSON `{"product_name":"InfluxDB 3 Core","version":"3.12.1",…}`, headers `x-influxdb-build: Core`, `x-influxdb-version: 3.12.1` **(measured)**. Requires the token (401 without). |
| `GET /health` | liveness | 200 `OK` with token; 401 without **(measured)** |
| `POST /api/v3/write_lp?db=&precision=&accept_partial=&no_sync=` | native write | 204 on success. Precision values accepted: `auto, s, second, millisecond, ms, microsecond, u, us, n, nanosecond, ns` **(measured, from the 400 error text)**. Unknown database is **created implicitly** on first write **(measured)**. |
| `POST /api/v3/query_sql` (JSON body `{db,q,params,format}`) | SQL | formats `json`, `jsonl`, `csv`, `pretty`, `parquet`; `$name` parameters work **(measured)** |
| `POST /api/v3/query_influxql` | InfluxQL | works on Core **(measured)** |
| `GET /api/v3/configure/database?format=json` | list databases | `[{"iox::database":"name"}]` **(measured)** |
| `POST /api/v3/configure/database` `{db, retention_period}` | create database | 200 **(measured)** — only used by the optional, explicit "create database" button, never automatically |
| `/api/v2/write`, `/write`, `/query` | v1/v2 compatibility | all accept data on Core **(measured)**; not used by default |

### 4.2 Write semantics

| Case | Result **(measured)** |
| --- | --- |
| Default `accept_partial=true`, one bad line among good ones | HTTP **400** `{"error":"partial write of line protocol occurred","data":[{"error_message","line_number","original_line"}]}`; good lines **are stored** |
| `accept_partial=false` | HTTP 400 `"line protocol parsing error"`, **nothing** stored |
| `line_number` | counts **records**, not physical lines (a string field containing a raw newline does not shift numbering) |
| Field type conflict (string into float column, int `5i` into float column, tag name used as field) | per-line rejection (`invalid column type for column 'value' …`) |
| `NaN`, `Infinity` | rejected (`No fields were provided`) — the adapter must never emit them |
| String field > 1 MB | rejected `String is greater than 1MB` |
| Request body > 10 485 760 bytes | HTTP **413** `max request size (10485760 bytes) exceeded` |
| Gzip body (`Content-Encoding: gzip`) | accepted |
| `no_sync=true` | 204, faster acknowledgement (no WAL flush wait) |
| Same table + tag set + timestamp written twice | **fields are merged, later values win** (`value=1,ack=true` then `value=2` → `value=2, ack=true`). Re-sending an identical batch is therefore idempotent. |
| Escaping | measurement `my\ table`, tag `state_id=x\,y\=z\ w`, tag key `Sub-Type`, string `"q\"uo\\te"` stored exactly as intended; UTF-8 (`Kotłownia`, `ż`) fine |
| Raw newline inside a quoted string field | accepted on Core 3.12.1 (escaped `\n` is stored literally as backslash-n) |
| Invalid token | 401 `{"error": "the request was not authenticated"}` |
| Invalid database name (`bad name`) | 400 `invalid character in database or rp name: must be ASCII, containing only letters, numbers, underscores, or hyphens` |

### 4.3 Query semantics

| Case | Result **(measured)** |
| --- | --- |
| `time` column in JSON | ISO string **without timezone suffix**, UTC, e.g. `2026-10-19T14:00:11.123`. The adapter therefore selects `CAST(time AS BIGINT) / 1000000 AS ts` → integer epoch milliseconds (safe in JSON). |
| Integer `$param` | arrives as `UInt64`; `to_timestamp_millis($p)` fails, `to_timestamp_millis(CAST($p AS BIGINT))` works; RFC 3339 strings compare directly with `time` |
| `date_bin` | needs an `INTERVAL` literal (a duration parameter is rejected). The adapter builds `INTERVAL 'N milliseconds'` from a validated integer. |
| `selector_min/selector_max(value, time)` | available; return struct with `time` and `value` |
| `approx_percentile_cont(value, p)` | available |
| `DELETE FROM` | `DML not supported: Delete` |
| `INSERT INTO … VALUES` | **reaches the planner** (fails only on column count) → the adapter's `query` command must block non-read statements itself |
| Unknown database | 404 `database not found` |
| Unknown table | 400 `table 'public.iox.x' not found` |
| Schema | `information_schema.columns` gives `data_type` (`Float64`, `Boolean`, `Int64`, `Utf8`, `Dictionary(Int32, Utf8)` = tag, `Timestamp(ns)`) |
| Retention metadata | `SELECT * FROM system.databases` in database `_internal` → `retention_period_ns` |

### 4.4 Server limits (Core 3.12.1 defaults)

| Limit | Value | How verified |
| --- | --- | --- |
| Databases | **5** (excluding `_internal`) → HTTP 422 `Adding a new database would exceed limit of 5 databases` | measured |
| Tables (whole server) | **2 000** → 422 per line `exceed number of tables limit … (limit: 2000)` | measured |
| Columns per table (incl. `time` and tags) | **500** → 422 `would exceed the column limit … limit is 500` | measured |
| Request size | 10 MiB | measured |
| String field | 1 MB | measured |
| Parquet files per query | **432** (`--query-file-limit`; ≈ 3 days at the default `--gen1-duration` of 10 min, one file per table and block) → `Query would scan 432 Parquet files, exceeding the file limit` | measured 2026-10-20 (user system and load test); Core never compacts, Enterprise does |
| Write acknowledgement | after the WAL flush (`--wal-flush-interval`, default 1 s) → one connection writes ≈ one request per second: 5 000 lines ≈ 5 000 points/s, 50 000 lines ≈ 50 000/s, 2 × 50 000 ≈ 100 000/s | measured 2026-10-20 ([PERFORMANCE.md](PERFORMANCE.md)) |
| Enterprise databases | default 100 | docs |
| Enterprise tables/columns | configurable, different defaults | docs (not measured — no Enterprise license available) |

Consequence: the `per-state` model (one table per state) **cannot exceed ~2 000 states on Core**, and
less if other databases also have tables. The adapter estimates the table count before
writing and warns at 80 % (configurable limit, default 2 000).

## 5. Retention

* **Core (docs):** retention is a database property, set at creation (`--retention-period`
  or `retention_period` in `POST /api/v3/configure/database`), units `h d w mo y`, `none` = infinite,
  `0d` = *everything is expired*, enforced **at query time** first, physically deleted later.
  Docs state it "cannot be changed afterward"; no table-level retention.
* **Discrepancy found:** on Core 3.12.1 `PUT /api/v3/configure/database {"db":"probe_ret","retention_period":"60d"}`
  returned 200 and `system.databases.retention_period_ns` changed from 30 d to 60 d **(measured)**.
  Because this contradicts the documentation, the adapter **does not rely on it** and never changes retention.
* **Enterprise (docs):** retention can be updated after creation and table-level retention exists.
* **Adapter decision:** no fake per-state retention. The user chooses the target database (which has
  the desired retention). An optional, explicit "Create database with retention" button exists in the
  instance settings; it never runs automatically and never deletes anything. Diagnostics show the
  retention of the configured database (from `_internal.system.databases`) when the token can read it.

## 6. Official JavaScript client

`@influxdata/influxdb3-client` 2.4.0 (2026-09-08, Node ≥ 20):

* Write: supports `/api/v3/write_lp` with `acceptPartial`, `noSync`, gzip, `PartialWriteError` with line numbers.
* Query: **only through Arrow Flight (gRPC over HTTP/2)**; no HTTP `query_sql`.
* Dependencies: `apache-arrow` (5.8 MB unpacked), `@grpc/grpc-js` (2.6 MB), `grpc-web`, `@protobuf-ts/*`.

Decision (ADR-0003): use a **small own HTTP client** built on `node:http`/`node:https`:
1. No native modules either way, but the HTTP client adds **zero** runtime dependencies — relevant on
   Raspberry Pi / SD-card installs and for `npm install` time inside ioBroker.
2. `query_sql` over HTTP/1.1 works through ordinary reverse proxies; Flight needs HTTP/2/gRPC
   pass-through which many home setups (nginx, Synology, Traefik defaults) do not provide.
3. Full control of TLS (`ca`, `rejectUnauthorized`), keep-alive pool, timeouts, gzip and parsing of
   partial-write errors, which must be classified exactly for the retry logic.
4. History reads are small (≤ tens of thousands of rows) – JSON is adequate; Flight's columnar speed
   is not needed. The client is isolated behind an interface so a Flight transport can be added later.

## 7. Editions — feature matrix

| Feature | Core | Enterprise | Cloud Serverless / Dedicated / Clustered |
| --- | --- | --- | --- |
| `/api/v3/write_lp` | ✅ verified | ✅ same API (docs), **not tested** | ❌ not documented (docs recommend `/api/v2/write`) |
| `/api/v3/query_sql` (HTTP) | ✅ verified | ✅ docs, not tested | ❌ (Flight SQL / v1 `/query` only) |
| `/api/v3/query_influxql` | ✅ verified | ✅ docs | ❌ |
| Auto-detect via `/ping` | ✅ `x-influxdb-build: Core` | ✅ `Enterprise` (docs) | ❌ |
| Tokens | admin token only (all-powerful) | resource tokens per database (read/write) | database tokens |
| Retention change after creation | ❌ docs (✅ observed 3.12.1, not relied on) | ✅ | ✅ |
| Limits | 5 DB / 2 000 tables / 500 columns | 100 DB default, configurable | service-specific |
| **Adapter support in 0.1.0** | **supported, integration-tested** | **supported (same code path), untested** | **experimental write-only** via `/api/v2/write` (path verified against Core's compatibility endpoint, never against Cloud); history/SQL return a clear "not supported for this edition" error |

## 8. Naming analysis

| Candidate | npm | ioBroker repo | Note |
| --- | --- | --- | --- |
| `influx_v3` (`iobroker.influx_v3`) | free (E404) | not listed | preferred working name of the master prompt; allowed characters (checker `E0006`), **but rejected for new adapters** by checker `E4021` (see below) |
| **`influx-v3`** (`iobroker.influx-v3`) | free (E404) | not listed | **chosen** (owner's decision 2026-10-19): passes `E0006`/`E4021`, clearly distinct from `influxdb` |
| `influxdb3` | free | — | visually too close to `influxdb`, risk of confusion in admin lists |
| `influx3`, `influxv3`, `influxdb-v3` | free | — | no advantage |

Instance objects live under `influx-v3.N.*`; custom config under `common.custom["influx-v3.N"]`; the
data directory is `iobroker-data/influx-v3.N/`. Nothing overlaps with `influxdb.N`.
UI product name: **InfluxDB 3**.

**Naming decision.** While running the repository checker (v5.23.2, `lib/M4000_Repository.js`) it turned out
that for every adapter not yet in the ioBroker repository the checker raises
`[E4021] Adapter name should use "-" instead of "_"`, so the preferred working name `influx_v3` could not be
published. As required by the master prompt the finding was described first; the owner then decided on
**`influx-v3`** (npm `iobroker.influx-v3`, GitHub repository `ioBroker.influx-v3`, which also fixes
`E0002`/`E0003`). The rename was done before any public installation, so no existing instance ids or
`common.custom` keys are affected.

## 9. Risks

| Risk | Mitigation |
| --- | --- |
| Table limit with `per-state` model | `grouped` is the default; `per-state` shows an estimate and refuses new tables beyond the configured limit |
| Column limit (500) in grouped tables with many tag keys | tag key count per table is tracked; warning at 80 %, new tag keys refused beyond the limit |
| Type conflicts in shared tables | one value type per table, enforced before writing; default tables are type-separated |
| Cardinality explosion (dynamic tags, `from`) | `from` stored as field (if at all); per-key distinct-value limit; dynamic tags disabled by default |
| Secrets in tags | key/value heuristics block tokens, passwords, e-mails unless explicitly allowed per state |
| Core admin token is all-powerful | stored via `encryptedNative`/`protectedNative`; recommend Enterprise resource tokens; never logged |
| Kill -9 data loss | `disk` buffer mode writes each batch to disk before sending (at-least-once; replay is idempotent because of point merge semantics) |
| Server API changes (v3 is young) | all endpoint knowledge isolated in `InfluxV3Client`; integration tests run against `influxdb:3-core` |
| Retention docs vs. behaviour mismatch | adapter never modifies retention |

## Sources

* InfluxDB 3 Core API: <https://docs.influxdata.com/influxdb3/core/api/>
* Write data: <https://docs.influxdata.com/influxdb3/core/api/write-data/>
* Execute queries: <https://docs.influxdata.com/influxdb3/core/query-data/execute-queries/>
* JS client v3: <https://docs.influxdata.com/influxdb3/core/reference/client-libraries/v3/javascript/>, npm `@influxdata/influxdb3-client` 2.4.0
* Data retention (Core): <https://docs.influxdata.com/influxdb3/core/reference/internals/data-retention/>
* Create database (Enterprise): <https://docs.influxdata.com/influxdb3/enterprise/admin/databases/create/>
* Cloud Serverless write: <https://docs.influxdata.com/influxdb3/cloud-serverless/write-data/>
* ioBroker dev docs: <https://iobroker.github.io/dev-docs/>
* Admin jsonConfig schema/README: `@iobroker/json-config` 10.1.3 (`ConfigGeneric.getPatternAsync` exposes `customObj` to `jsonData` patterns)
* Repository checker: <https://github.com/ioBroker/ioBroker.repochecker> v5.23.2 (`lib/config.js`, `M0000_PackageJson.js`, `M1000_IOPackageJson.js`)
* Reference adapter: <https://github.com/ioBroker/ioBroker.influxdb> v5.1.6
* Project template: `@iobroker/create-adapter` 3.1.5
