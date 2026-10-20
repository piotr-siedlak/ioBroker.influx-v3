![Logo](admin/influx-v3.png)

# ioBroker.influx-v3 — InfluxDB 3 logging

Logs selected ioBroker states into **InfluxDB 3 (Core / Enterprise)** using the native v3 HTTP API, with per-state
configuration (gear icon in *Objects*), layered tags, a crash-safe buffer and history/SQL access.
It is an independent adapter: it runs **in parallel** with the classic `ioBroker.influxdb` adapter and never touches
its instances or settings.

🇵🇱 **Polska dokumentacja:** [docs/pl/README.md](docs/pl/README.md)

> Status: **0.1.0, not yet published** to npm or the ioBroker repository. Test versions:
> [docs/INSTALL-DEV.md](docs/INSTALL-DEV.md).

## Features

* Per-state activation and settings in *Objects → gear icon → influx-v3.N* (no restart needed)
* Two table models: **grouped** (shared tables, `state_id` tag, recommended) and **per-state** (one table per state)
* Tags: global, automatic (room, function, adapter, instance, device, channel, unit, role, name), per-state,
  safe templates (`{id.segment[2]}`, `{common.unit}`, `{enum.room}` …), optional dynamic tags from other states;
  protection against secrets in tags and against cardinality explosions
* Filters per state: changes only, debounce, block time, minimal delta, re-log interval, ack/quality filter,
  ignore zero/below/above, rounding, initial value, "last skipped value" for flat chart lines
* Native `/api/v3/write_lp` with millisecond precision, batching, gzip, retry with exponential backoff and jitter,
  circuit breaker, partial-write handling, 413 splitting
* Buffer in memory (saved on shutdown) or on disk (write-ahead, survives `kill -9`)
* `getHistory` for ioBroker charts (raw, average, min, max, min/max, sum, count, percentile, quantile — computed in
  InfluxDB with `date_bin`), read-only SQL and InfluxQL via `sendTo`
* Admin UI (JSON Config) in all 11 ioBroker languages (incl. English, Polish, German), connection test, mapping preview, diagnostics,
  migration of `influxdb` settings with **tag rules**, and **import of the historical data** from InfluxDB 1.x/2.x
* Multiple instances with different servers/databases

## Requirements and supported editions

| | |
| --- | --- |
| Node.js | ≥ 22 (tested with 22, 24, 26 in CI) |
| js-controller | ≥ 6.0.11 |
| Admin | ≥ 7.6.20 |

| Edition | Support in 0.1.0 |
| --- | --- |
| InfluxDB 3 **Core** | ✅ supported, integration-tested against 3.12.1 |
| InfluxDB 3 **Enterprise** | ✅ same API, expected to work, **not tested** (no Enterprise server was available for the tests) |
| InfluxDB **Cloud** Serverless / Dedicated / Clustered | ⚠️ experimental, **write only** via `/api/v2/write`; history and SQL are not available (these products offer SQL only through Flight) |
| InfluxDB 1.x / 2.x | ❌ use `ioBroker.influxdb` |

Details, measured limits and sources: [docs/RESEARCH.md](docs/RESEARCH.md).

## Installation (next to the classic adapter)

1. ioBroker Admin → *Adapters* → search for **InfluxDB 3** → install. Until the adapter is listed in the ioBroker
   repository, install it from GitHub (details: [docs/INSTALL-DEV.md](docs/INSTALL-DEV.md)):
   `iobroker url https://github.com/piotr-siedlak/ioBroker.influx-v3`, then `iobroker restart influx-v3.0`.
2. Add an instance `influx-v3.0`. An existing `influxdb.0` keeps working unchanged.
3. Configure the connection (below), define your tag keys in the **tag catalog** (tab *Tags*) and enable logging for
   states via the gear icon.

## InfluxDB 3 setup

```bash
# InfluxDB 3 Core in Docker, data persisted in the Docker volume "influxdb3-data"
docker run -d --name influxdb3 --restart unless-stopped -p 8181:8181 -v influxdb3-data:/var/lib/influxdb3 \
  influxdb:3-core influxdb3 serve --node-id iobroker --object-store file --data-dir /var/lib/influxdb3

# create an admin token (Core has admin tokens only - keep it secret)
docker exec influxdb3 influxdb3 create token --admin

# optional: create the database with a retention period (Core cannot change it later)
docker exec influxdb3 influxdb3 create database --retention-period 365d --token <TOKEN> iobroker
```

Without an explicit `create database` the database is created by the first write with **infinite** retention.
On **Enterprise**, create a resource token with read+write permission for the one database instead of an admin token.

## Instance configuration

| Tab | Content |
| --- | --- |
| Connection | edition (automatic detection via `/ping`), URL (http/https, port, optional path prefix), database, token (stored encrypted), TLS verification, CA certificate, timeouts, **Check connection** (works with unsaved values), optional explicit *Create database* |
| Writing | batched/direct, batch size (records/bytes), flush interval, gzip, `no_sync`, parallel requests, retry delays |
| Buffer | memory/disk, limits (records, bytes, disk, age), overflow policy, fsync, shutdown timeout |
| Schema | grouped/per-state, default tables per type, table rules (role/id pattern → table), value column, server limits, text/JSON handling, placement of `ack`/`q`/`from` (field/tag/none) |
| Tags | **tag catalog** (keys, allowed values, defaults), key style (`lower_snake_case` or as entered), automatic tags, native allow-list, protection, dynamic tags |
| State defaults | defaults for new per-state settings |
| History & queries | row limits, timeouts, default aggregation, user queries and allowed senders |
| Advanced | subscription threshold, log rate limits, statistics interval, statistics into InfluxDB (opt-in) |
| Diagnostics | connection, server version, written/rejected/dropped records, buffer, last error |
| Migration of settings | copy per-state settings from an `influxdb.N` instance, translating its custom tags with **tag rules** (live preview → apply; nothing is changed in `influxdb.N`) |
| Migration of data | import the history of the states logged by `influxdb.N` (also those not configured here yet) from the old InfluxDB 1.x/2.x database, in one pass over the time axis: test, preview, start/stop, point count, live progress with time left, continues by itself after a restart |

## Per-state configuration

*Objects* → gear icon of a state → section **influx-v3.0** → *enabled*. Tabs:

* **Basic:** table (empty = automatic), store as (auto/number/boolean/text), alias ID, tags of this state (key and
  value chosen from the tag catalog), changes only, debounce, re-log interval, minimal delta, **preview** of the
  resulting line protocol (not saved)
* **Filters:** block time, ack/quality filter, ignore zero/below/above, rounding, initial value, last skipped value
* **Tags & metadata:** use catalog defaults ("global tags") / automatic tags, remove tag keys, store ack/q/from,
  allow sensitive tags
* **Advanced:** value column (per-state mode), debug logs

Changes take effect immediately (no restart). Disabling stops writing immediately (values waiting in a debounce or
block window are discarded).

Example — state `heat.0.hc1.temperature` with per-state tags `Location=KOTLOWNIA`, `Device=PC`, `Sub-Type=HC1`,
table `heating`, room enum "Kotłownia", catalog default `site=home`:

```text
heating,device=PC,location=KOTLOWNIA,room=Kotłownia,site=home,state_id=heat.0.hc1.temperature,sub_type=HC1 value=38.7,ack=true,q=0i 1792418400123
```

(timestamp = `state.ts` in milliseconds, written with `precision=millisecond`; tags sorted; keys in
`lower_snake_case` by default).

## Tags

**Tag catalog** (instance settings → *Tags*): the central list of tag keys, so that every object uses the same,
standardised tags. Each entry has

| Column | Meaning |
| --- | --- |
| Tag key | e.g. `Location`, `Device`, `Sub-Type` (stored as `location`, `device`, `sub_type` with the default key style) |
| Allowed values | comma separated, e.g. `KOTLOWNIA, SALON, GARAZ` |
| Other values allowed | if set, the object dialog accepts any value for this key, including templates such as `{id.segment[2]}` |
| Default for all states | written for every state that uses global tags (replaces the former "global tags" table, which is converted automatically) |
| Description | shown in the key dropdown of the object dialog |

In the object dialog (*Objects → gear icon*) the **key is a dropdown of the catalog** – new keys cannot be created
there – and the **value** is picked from the key's allowed values (free text only where the key allows it). The
adapter enforces the same rules at runtime: tags with keys or values outside the catalog (e.g. from scripts via
`enableHistory` or from the migration) are not written, and a warning is logged.

Precedence (lowest → highest): **catalog defaults → automatic → per-state → removed keys → reserved** (`state_id`, `time`,
the value column, `ack`, `q`, `from`). Keys are normalised before merging, so `Sub-Type` and `sub_type` are the
same key; the higher layer wins and a warning is logged.

Templates (only for catalog keys that allow other values; no JavaScript, no `eval`): `{id}`, `{id.segment[N]}` (negative N from the end), `{id.adapter}`,
`{id.instance}`, `{common.unit}`, `{common.role}`, `{common.type}`, `{common.name}`, `{native.KEY}` (only keys in
the allow-list), `{enum.room}`, `{enum.function}`, `{parent.device}`, `{parent.channel}`, and – only with *dynamic
tags* enabled – `{state:<id>}` (value of another state; TTL, allowed characters and a distinct-value limit apply).

Protection: tag keys/values that look like passwords, tokens, session ids or e-mail addresses are not written
unless *allow sensitive tags* is set on that state; each variable tag key is limited to *max. distinct values*
(further values become `_other`). `from` is not stored by default; as a tag it would create one series per writer.

## Data model

See [ADR-0001](docs/adr/0001-table-model-grouped-vs-per-state.md).

* **grouped** (default): tables `numeric_states` (Float64), `boolean_states` (Boolean), `string_states` (Utf8) or
  your own tables via per-state *table* / instance *table rules*. **One value type per table** – a state whose type
  does not fit its table is not written and listed in `influx-v3.N.info.conflicts`.
* **per-state**: one table per state (name = state id or alias), like the classic adapter. InfluxDB 3 Core allows
  only **2 000 tables per server** – the adapter warns at 80 % and refuses tables beyond *max. tables*.

Numbers are always stored as floats; booleans as booleans (or 1/0 with *store as number*); objects/arrays as JSON
text (or rejected); `null`, `NaN`, `Infinity` are never written. Texts > *max. text length* are rejected (server
limit 1 MB).

**Retention** is a property of the database (Core: fixed when the database is created). There is no per-state
retention. Choose the database (each instance can use another one).

## Reliability

See [ADR-0002](docs/adr/0002-write-path-and-reliability.md). Summary:

* Retries only for transient errors (network, timeouts, 408, 429 with `Retry-After`, 5xx) with exponential backoff
  and jitter. Validation errors (400/422) are never retried; partially accepted batches are split into written and
  rejected records (`info.rejectedRecords`, log with rate limit). 401/403/404: data is kept, retry after
  *authRetryMs*.
* **memory** buffer: data in RAM, written to `iobroker-data/influx-v3.N/spool` on shutdown, replayed after start.
  **disk** buffer: every batch is written there before sending; survives `kill -9` and power loss (with fsync).
* Delivery is **at-least-once** within the limits; replay is idempotent (InfluxDB 3 merges points with identical
  table, tags and timestamp – measured). **Not exactly-once.** Records dropped by limits are counted in
  `info.droppedRecords`.

## sendTo API

```js
// history (same format as other history adapters)
sendTo('influx-v3.0', 'getHistory', {
    id: 'heat.0.hc1.temperature',
    options: { start: Date.now() - 86400000, end: Date.now(), aggregate: 'average', count: 300, ack: true, q: true },
}, res => console.log(res.result, res.step, res.error));

// read-only SQL (SELECT/WITH/SHOW/DESCRIBE/EXPLAIN, one statement, row/byte/time limits)
sendTo('influx-v3.0', 'query', {
    query: 'SELECT time, value FROM numeric_states WHERE state_id = $id ORDER BY time DESC LIMIT 10',
    params: { id: 'heat.0.hc1.temperature' },
}, res => console.log(res.result, res.truncated, res.error));

sendTo('influx-v3.0', 'queryInfluxQL', 'SELECT mean(value) FROM numeric_states WHERE time > now() - 1h GROUP BY time(5m)', cb);

sendTo('influx-v3.0', 'enableHistory', { id: 'heat.0.hc1.temperature', options: { changesOnly: true, customTags: [{ key: 'location', value: 'KOTLOWNIA' }] } }, cb);
sendTo('influx-v3.0', 'disableHistory', { id: 'heat.0.hc1.temperature' }, cb);
sendTo('influx-v3.0', 'getEnabledDPs', {}, cb);         // { id: settings + table + conflict }
sendTo('influx-v3.0', 'storeState', { id, state: [{ val: 1, ts: 1792418400000 }] }, cb); // import values (bypasses filters)
sendTo('influx-v3.0', 'flushBuffer', {}, cb);
sendTo('influx-v3.0', 'getSchema', {}, cb);              // tables, columns, retention
sendTo('influx-v3.0', 'getAdapterStats', {}, cb);        // counters, buffer, conflicts
sendTo('influx-v3.0', 'previewMapping', { id, config }, cb); // table, tags and line protocol of a (draft) config

// migration (Admin buttons): settings with tag rules, import of historical data (uses the saved dm* settings)
sendTo('influx-v3.0', 'migrationPreview', { source: 'influxdb.0', rules: [{ fromKey: 'room', toKey: 'Location' }] }, cb);
sendTo('influx-v3.0', 'migrationApply', { source: 'influxdb.0', rules, overwrite: false, ids: [/* optional */] }, cb);
// further helpers of the Admin form: migrationSuggestRules, migrationProposeStandard, getOldTagKeys, getOldTagValues,
// listInfluxdbInstances, dmFillFromInfluxdb; diagnostics and testConnection for the Diagnostics/Connection tabs
sendTo('influx-v3.0', 'dmTestSource', {}, cb);
sendTo('influx-v3.0', 'dmPreview', {}, cb);              // also stored as HTML in info.dataMigration.preview
sendTo('influx-v3.0', 'dmStart', {}, cb);                // runs in the background, progress in info.dataMigration.*
sendTo('influx-v3.0', 'dmStop', {}, cb);                 // ends it until dmStart (a restart only pauses it)
sendTo('influx-v3.0', 'dmReset', {}, cb);
```

`getHistory` aggregates: `none`/`onchange` (raw, plus one border value on each side unless `removeBorderValues`),
`average`, `min`, `max`, `minmax` (real times of the extremes), `total`, `count`, `percentile`, `quantile`
(approximate). Text states are returned raw. The returned `ts` of aggregated values is the middle of the bucket.
Not supported: `integral`, `update`, `delete*` (InfluxDB 3 SQL has no DML).

Permissions: `query`/`queryInfluxQL` can be disabled or limited to sender instances (*History & queries* tab).
The adapter opens no network port.

## Grafana

See **[docs/GRAFANA.md](docs/GRAFANA.md)** – data source setup and verified SQL for location/device/subtype
filters, `date_bin` aggregation, boolean counting, room/unit/adapter filters and the quoted `Location` /
`Sub-Type` / `Device` variant.

## Migration from ioBroker.influxdb

Two steps, both in the instance settings. The classic adapter and its database are never changed – both adapters
can run side by side during the transition. Design: [ADR-0006](docs/adr/0006-migration-tag-rules-and-history-import.md).

### 1. Migration of settings (with tag rules)

Choose the `influxdb.N` instance. Copied per state: enabled, filters, debounce/block time, rounding, alias, storage
type and custom tags. Custom tags must fit the [tag catalog](#tags), so they are translated with **tag rules**.

The old tags are taken from the existing configuration: *Old key* offers the custom tag keys of the `influxdb.N`
settings, *Old values* lists every value of that key with the number of states – select **one or several** values
per rule (none = every value). **Take over old tags** fills the table automatically: one row per old key that has no
catalog key yet (choose its new key), and for keys with a catalog key one row per catalog value suggested for the old
values (equal when case and diacritics are ignored, e.g. `Kotłownia`, `kotlownia` → `KOTLOWNIA`); values without a
suggestion get a row with an empty new value. Existing rows are never changed – press it again after choosing new
keys to get the value rows of those keys.

**Propose standard (catalog + rules)** goes one step further and matches automatically, also when the catalog is
still empty. It covers **every used tag**: the custom tags of the influxdb settings, the tags stored in the old
database (option; connection of tab *Migration of data*) and the tags of the states of this instance. Incomplete rows
(e.g. left by *Take over old tags*) are filled in, keys that allow other values get their used values listed and
their spelling variants merged too. The rule table gets a **complete mapping**, grouped by old key: one row per old
key and one row per standard value with all its old spellings – also for values that stay as they are, so every used
key and value is visible and editable (keys with more than 100 distinct values only get rows for values that change).
The message reports whether all used values are covered; the full result is shown under the buttons
(*Result of the last proposal*): keys with the same meaning are merged (`room`, `Raum`, `pomieszczenie` → `location`; built-in
synonyms in EN/DE/PL/FR/IT/ES), values that differ only in spelling or language are merged (`Kotłownia`,
`kotlownia`, `Heizungsraum` → `KOTLOWNIA`), the ioBroker rooms and functions serve as standard values, and missing
keys/values are added to the tag catalog. Numbers are compared exactly (`-1` stays different from `1`). The spelling
of new values is *automatic* (follows the catalog; empty catalog: `UPPER_CASE`), `UPPER_CASE`, `lower_case` or *as
used*. Only the form is filled – existing complete rows and your own rules are kept; check the Tags tab, the rules
and the preview, then save. Similarity is only used for this proposal, never at runtime. With the option *Apply also
standardises the tags of states already configured for this instance* (on by default), *Apply* rewrites such tags
too – only where the rules translate them into an allowed catalog tag; the preview lists these changes.

| Old key | Old values | New key | New value | Effect |
| --- | --- | --- | --- | --- |
| `room` | *(none)* | `Location` | *(empty)* | `room=Salon` → `Location=SALON` (spelling adapted to the catalog) |
| `room` | `Kotłownia`, `Boiler room` | `Location` | `KOTLOWNIA` | several old values → one catalog value |
| `source` | | `-` | | tag removed |
| `typ` | | *(empty)* | `heating` | key kept, value replaced |

A rule for specific old values wins over a rule for every value. The **live preview** (updated while you edit the
rules, before saving) lists per state the old tags, the result and every old `key=value` that still does not fit the
catalog, with the number of states – add a rule or extend the catalog until the list is empty; it starts with all old
custom tags (`room: Kotłownia (5), Salon (3), Garaż (1)`). Catalog changes are used by the preview before saving;
*Apply* needs them saved. *Apply* writes only tags allowed by the catalog. The same rules translate the tags stored in the old points during the data import.

### 2. Migration of data (import of the history)

1. Choose the `influxdb.N` instance and press *Copy connection settings* (URL, version, database/bucket,
   organisation, user). Enter the password (1.x) or a read token (2.x) – they cannot be read from another adapter.
   They are stored encrypted and never logged. **Save.**
2. Optional: limit the states (`hm-rpc.0.*, javascript.0.*`) and the period.
3. *Test connection*, then *Preview (dry run)*: the period that will be imported, how many states end at a known
   first live value, and for the first states the old measurement (alias of the `influxdb.N` settings, else the id),
   the target table and converted records from the newest hour; old measurements that are not imported are listed
   with the reason (state filter, other influxdb instance, conflict, no state). Nothing is written.
4. *Start / continue*. Progress (date reached, points of about N, points/s, estimated time left), imported and
   rejected points are shown live (`info.dataMigration.*`). After an adapter restart (update, host reboot) the import
   continues by itself; after *Stop* it waits for *Start*, which continues where it stopped. *Reset progress* starts
   the next import from the beginning. The progress file is `iobroker-data/influx-v3.N/data-migration.json`.

Which states: those configured for this instance **and** those only `influxdb.N` logs. The latter are imported with
the table and tags that *Apply* in *Migration of settings* would give them (same rules, saved catalog) – no setting
is changed. Best order: apply the settings migration first, then import – each state then ends exactly at its first
live value. Applied after the import, the time in between is filled by importing again (*Reset progress* → *Start*).

How the import runs (built for many states and years of data): it walks the time axis **once** – each time window
is read for all states with one query (adaptive: 1 minute … 30 days, about 40 000–100 000 points per window), the
next window is read while the current one is written. InfluxDB 3 is never queried during the import, so its size
does not slow the import down. Writes go out as large requests (default 50 000 points) two at a time – InfluxDB 3
answers each write only after its write-ahead log was flushed (about once per second), so request size, not count,
sets the speed. Answers that are too large or too slow shrink the window; a write timeout halves the request size.
While either server is not reachable (e.g. restarted) the import waits as long as it takes and continues by itself;
after an adapter restart it continues by itself, too (*Stop* ends it until *Start*). Before starting, the points are
counted, so the progress shows "of about N points" and the time left.

How the points are written: exactly like live values of the state today (table, catalog tags, metadata, rounding,
value type; values that cannot be converted are counted and skipped). By default each state ends at the first live
value this instance logged for it – the adapter records that time in its data directory (`live-start.json`). States
without such a record end at the start of the import; points logged by both adapters have the same timestamp, so
they simply overwrite each other. The import writes directly (not through the live buffer) and waits while the live
buffer fills up.

Limits: dynamic `{state:…}` tags are not reconstructed for the past. InfluxDB 3 Core cannot delete single points –
check the preview before *Start*; a second import with other tag rules creates additional series (to redo it, drop
the table). **InfluxDB 3 Core and long periods**: Core stores one Parquet file per table and 10 minutes and never
merges them; one query may read at most `--query-file-limit` files (default 432 ≈ 3 days). To query weeks or years
of imported history (e.g. in Grafana), start Core with a higher `--query-file-limit` (more memory per query) or use
InfluxDB 3 Enterprise, whose compaction merges old files (free licence for at-home use).

## Troubleshooting

| Symptom | Cause / action |
| --- | --- |
| `info.connection` false, log "authentication failed" | wrong/expired token; Core: `influxdb3 create token --admin` |
| "Adding a new database would exceed limit of 5 databases" | Core allows 5 databases – reuse one |
| state listed in `info.conflicts` | its value type differs from the table's type → choose another table or *store as* |
| "table limit reached" | per-state mode on Core (2 000 tables) → switch to grouped mode |
| records rejected `invalid column type` | the column was created with another type (e.g. by another tool) – use another table |
| history empty after changing table/mode | old data stays in the old table – query it directly with SQL |
| behind a reverse proxy | put the path prefix into the URL (`https://host/influx`) |
| HTTP proxy | not supported in 0.1.0 |
| "Query would scan … Parquet files, exceeding the file limit" | InfluxDB 3 Core: the query covers more than about 3 days – query a shorter period, raise `--query-file-limit` or use Enterprise (see *Migration of data* → Limits). The import itself no longer queries InfluxDB 3 |
| import stopped with "timeout after 10000 ms" (versions before the time-sliced import) | update; the import no longer asks InfluxDB 3 for the first value of each state |
| Rule table with many rows: *Old values* load slowly | the used tags are read once and shared by all cells (old database: cached 10 min, a cell waits at most 2 s for it); the first page load after a start can show values from the settings only – reopen the tab a moment later |

## Development

```bash
npm ci
npm run build            # TypeScript → build/ (committed: commit it together with src/ changes)
npm run lint && npm run check
npm run test:unit        # unit tests, no network
eval "$(scripts/influx3-test-server.sh start)"   # isolated InfluxDB 3 Core in Docker
eval "$(scripts/legacy-influx-test-server.sh start)"   # InfluxDB 1.8 + 2.7 as import sources (optional)
npm run test:influx      # tests against the real servers (incl. outage/replay, kill -9 recovery, import 1.x/2.x → 3)
npm run test:integration # js-controller harness (adapter lifecycle; end-to-end with INFLUX3_* set)
npx tsx scripts/loadtest.ts 100,1000,10000 20
npx tsx scripts/import-loadtest.ts 548 3 300   # history import, see the header of the script
scripts/influx3-test-server.sh stop && scripts/legacy-influx-test-server.sh stop
```

Architecture: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) · Decisions: [docs/adr/](docs/adr) ·
Load test: [docs/PERFORMANCE.md](docs/PERFORMANCE.md) · Publishing: [docs/PUBLISHING.md](docs/PUBLISHING.md) ·
Contributing: [CONTRIBUTING.md](CONTRIBUTING.md)

## Changelog
<!--
    Placeholder for the next version (at the beginning of the line):
    ### **WORK IN PROGRESS**
-->
### **WORK IN PROGRESS**

* (Piotr Siedlak) Migration of data redesigned for histories of several years: one pass over the time axis for all
  states (one query per time window, read-ahead), no queries against InfluxDB 3 (first live value recorded by the
  adapter), large parallel write requests (about 15× faster), waits for restarted servers and continues after an
  adapter restart by itself, point count with progress, points/s and time left. Fixes imports that stopped with
  "timeout after 10000 ms" or "Query would scan 432 Parquet files"
* (Piotr Siedlak) Migration of data: states that only the influxdb adapter logs are imported too (with the table and
  tags the settings migration would give them, no setting changed); the preview groups the old measurements that are
  not imported by reason
* (Piotr Siedlak) Tag catalog: tag keys and their allowed values are defined centrally in the instance settings; the
  object dialog only offers these keys/values (the former global tags become catalog defaults automatically)
* (Piotr Siedlak) Alias ID moved to the basic tab of the object settings
* (Piotr Siedlak) Installation from GitHub works (compiled `build/` is part of the repository)
* (Piotr Siedlak) Only adapter-managed timers during operation; shutdown never starts new timers
* (Piotr Siedlak) Fixed: a value set right after logging was enabled for a state could be lost
* (Piotr Siedlak) Settings tables are also accepted when they are stored as objects (e.g. written by scripts)
* (Piotr Siedlak) Migration of settings: tag rules translate the custom tags of influxdb into catalog tags, with a
  live preview of the result and of the tags that still do not fit
* (Piotr Siedlak) Fixed: the "influxdb instance" dropdowns stayed empty (option objects); the import preview and
  Start explain which old measurements belong to states not configured for this instance yet and what to do
* (Piotr Siedlak) Fixed: with many rule rows the old values stayed empty - the used tags are now read once and
  shared by all cells (one read of the old database instead of one per cell), dropdowns always answer with a list
* (Piotr Siedlak) "Propose standard" writes a complete mapping (every key and value, grouped by old key) and shows
  the full result in a panel; target values of rules from earlier runs are added to the catalog
* (Piotr Siedlak) "Propose standard" covers every used tag (influxdb settings, old database, this instance), fills
  incomplete rows, standardises open lists too and reports the coverage; Apply can standardise tags of states
  already configured for this instance
* (Piotr Siedlak) Migration of settings: "Propose standard" matches old keys and values automatically (synonyms,
  spelling, ioBroker rooms/functions) and proposes catalog entries and rules
* (Piotr Siedlak) Tag rules: old keys and values are offered from the influxdb settings, a rule can cover several old
  values, "Take over old tags" fills the rules automatically with suggested catalog values
* (Piotr Siedlak) New tab "Migration of data": import of the history from InfluxDB 1.x/2.x (preview, start/stop,
  resume after restart, live progress); points are written like live values, old tags pass the tag rules

### 0.1.0 (2026-10-19)

* (Piotr Siedlak) Initial version: per-state logging to InfluxDB 3 Core/Enterprise, grouped and per-state tables,
  layered tags with templates and protection, memory/disk buffer, getHistory with server-side aggregation,
  read-only SQL/InfluxQL, Admin UI in 11 languages, migration from influxdb settings

## License

MIT License

Copyright (c) 2026 Piotr Siedlak <siedlak.piotr@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
