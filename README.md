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
  migration wizard for `influxdb` settings
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
| InfluxDB 3 **Enterprise** | ✅ same API, expected to work, **not tested** (no license available) |
| InfluxDB **Cloud** Serverless / Dedicated / Clustered | ⚠️ experimental, **write only** via `/api/v2/write`; history and SQL are not available (these products offer SQL only through Flight) |
| InfluxDB 1.x / 2.x | ❌ use `ioBroker.influxdb` |

Details, measured limits and sources: [docs/RESEARCH.md](docs/RESEARCH.md).

## Installation (next to the classic adapter)

1. ioBroker Admin → *Adapters* → search for **InfluxDB 3** → install. (Until the adapter is listed in the ioBroker
   repository, test versions are installed as described in [docs/INSTALL-DEV.md](docs/INSTALL-DEV.md).)
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
| Migration | copy per-state settings from an `influxdb.N` instance (preview → apply, nothing is changed in `influxdb.N`) |

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

Tab *Migration*: choose the `influxdb.N` instance → *Preview* → *Apply*. Copied: enabled, filters, debounce/block
time, rounding, alias, storage type, custom tags. Never changed: the settings of `influxdb.N`. Historical data is
**not** migrated (export it from InfluxDB 1.x/2.x and write it with `influxdb3 write` or the `storeState` command).

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

## Development

```bash
npm ci
npm run build            # TypeScript → build/ (committed: commit it together with src/ changes)
npm run lint && npm run check
npm run test:unit        # unit tests, no network
eval "$(scripts/influx3-test-server.sh start)"   # isolated InfluxDB 3 Core in Docker
npm run test:influx      # tests against the real server (incl. outage/replay and kill -9 recovery)
npm run test:integration # js-controller harness (adapter lifecycle; end-to-end with INFLUX3_* set)
npx tsx scripts/loadtest.ts 100,1000,10000 20
scripts/influx3-test-server.sh stop
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

* (Piotr Siedlak) Tag catalog: tag keys and their allowed values are defined centrally in the instance settings; the
  object dialog only offers these keys/values (the former global tags become catalog defaults automatically)
* (Piotr Siedlak) Alias ID moved to the basic tab of the object settings
* (Piotr Siedlak) Installation from GitHub works (compiled `build/` is part of the repository)
* (Piotr Siedlak) Only adapter-managed timers during operation; shutdown never starts new timers
* (Piotr Siedlak) Fixed: a value set right after logging was enabled for a state could be lost
* (Piotr Siedlak) Settings tables are also accepted when they are stored as objects (e.g. written by scripts)

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
