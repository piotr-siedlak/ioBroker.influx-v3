# Architecture — `influx-v3`

The adapter is a TypeScript `daemon` adapter (`@iobroker/adapter-core`) without runtime dependencies
other than `adapter-core`. All logic that can be tested without ioBroker or InfluxDB lives in pure modules
under `src/lib/`; `src/main.ts` only wires them to the adapter lifecycle.

```text
               objectChange / stateChange / message                  HTTP/1.1 (keep-alive, TLS, gzip)
ioBroker ───────────────────────────────────────────▶ AdapterRuntime ─────────────────────────────▶ InfluxDB 3
                                                       (src/main.ts)
   ┌───────────────────────────────────────────────────────┼──────────────────────────────────────────┐
   │ ConfigurationService  StateRegistry  FilterPipeline  TagResolver  SchemaMapper  LineProtocol   │
   │ WriteQueue ── Spool (disk)          InfluxV3Client   HistoryService  QueryService  Observability │
   └──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Modules

| Module (file) | Responsibility | Pure? |
| --- | --- | --- |
| `AdapterRuntime` (`src/main.ts`) | lifecycle (`onReady`, `onStateChange`, `onObjectChange`, `onMessage`, `onUnload`), subscriptions, `info.*` states, wiring | no |
| `ConfigurationService` (`lib/config.ts`) | normalises and validates instance config (`native`) and per-state config (`common.custom["influx-v3.N"]`), applies instance defaults, clamps ranges, returns typed objects + validation messages | yes |
| `StateRegistry` (`lib/stateRegistry.ts`) | map *real id → registered state* (config, context, pipeline, resolved mapping); alias map; decides between individual and wildcard subscription; change detection (config hash) so unchanged objects are ignored | yes |
| `FilterPipeline` (`lib/filterPipeline.ts`) | per state: ack/q filters, coercion, ignore filters, debounce, blockTime, changesOnly, minDelta, relog timer, "last skipped value" logging, initial value. Timers are injected (testable with fake clocks) | yes |
| `TagResolver` (`lib/tagResolver.ts`) | layered tags, safe templates, key normalisation, collision detection, sanitisation, sensitive-data guard, cardinality guard, dynamic tags | yes |
| `SchemaMapper` (`lib/schemaMapper.ts`) | table selection (`grouped` / `per-state`), value type decision, value coercion, type registry (one value type per table), table/column limit accounting | yes |
| `LineProtocol` (`lib/lineProtocol.ts`) | escaping and serialisation of one record (millisecond timestamps) | yes |
| `InfluxV3Client` (`lib/influxClient.ts`) | HTTP transport (`node:http`/`https`, keep-alive agent, `ca`, `rejectUnauthorized`, timeouts, gzip), endpoints `ping`, `write_lp` / `api/v2/write`, `query_sql`, `query_influxql`, `configure/database`; error classification | I/O |
| `WriteQueue` (`lib/writeQueue.ts`) | batching (lines/bytes/interval), concurrency, retry with exponential backoff + full jitter + cap, `Retry-After`, circuit breaker, partial-write handling, 413 splitting, overflow policy, shutdown flush | I/O via injected client/spool |
| `Spool` (`lib/spool.ts`) | durable batch files (`tmp` + `fsync` + `rename`), recovery, age/size limits | fs |
| `HistoryService` (`lib/historyService.ts`) | translates ioBroker `getHistory` options into parameterised SQL (`date_bin`, `selector_min/max`, `approx_percentile_cont`), maps rows to `{val, ts, ack, q, from}` | pure SQL builder + I/O |
| `QueryService` (`lib/queryService.ts`) | read-only guard for user SQL/InfluxQL, row/size limits, sender allow-list | pure guard + I/O |
| `Observability` (`lib/observability.ts`) | counters, last success/error times, periodic (10 s) publication to `info.*`, never written to InfluxDB unless explicitly enabled | yes |
| `Migration` (`lib/migration.ts`) | explicit, previewable copy of `influxdb.N` per-state settings into `influx-v3.N` | pure mapping + I/O on user request |
| `redact` (`lib/redact.ts`) | removes tokens/authorization headers from any text that may be logged or returned | yes |

## Data flow of one state change

1. `onStateChange(id, state)` → `StateRegistry.get(id)`; unknown ids are ignored in O(1)
   (important when subscribed to `*`).
2. `FilterPipeline.process(state)` decides synchronously (or schedules a debounce/blockTime timer).
   Emitted states carry a reason (`change`, `relog`, `skipped`, `initial`, `debounced`, `blockReleased`).
3. `SchemaMapper.toRecord()` coerces the value to the table's value type (or rejects it, counted as
   `rejectedRecords` with reason).
4. Tags were pre-computed at configuration time (`TagResolver`), except dynamic tags, which are read
   from a TTL cache filled by subscriptions to their source states.
5. `LineProtocol.serialize()` → one line, timestamp = `state.ts` (ms), precision `millisecond`.
6. `WriteQueue.push(line)` → flushed by size or interval → `InfluxV3Client.write()`.

## Configuration objects

* Instance (`system.adapter.influx-v3.N` → `native`): connection, write, buffer, schema, tags,
  defaults for states, history, advanced. Token: `encryptedNative` + `protectedNative`.
* Per state (`common.custom["influx-v3.N"]`): `enabled` (provided by Admin), table/alias, store type,
  field (per-state mode only), tags, tag switches, filters, metadata switches. Admin dialog:
  `admin/jsonCustom.json`. Two instances never share configuration: every lookup uses `this.namespace`.

## Tag precedence (lowest → highest)

1. **defaults** – nothing besides the reserved `state_id` (applied last, cannot be overridden)
2. **catalog defaults** (instance tag catalog, column *default for all states*) – if `useGlobalTags`
3. **automatic tags** (room, function, adapter, instance, device, channel, unit, role, name – each switched on globally) – if `useAutoTags`
4. **per-state tags** – keys must exist in the instance **tag catalog**, values must be one of the key's allowed values
   unless the key allows other values (then also templates and dynamic placeholders); violations are ignored with a
   warning
5. **explicit overrides** – `excludeTags` removes keys coming from any lower layer
6. **reserved** – `state_id`; metadata tags (`ack`, `q`, `from`) when their placement is `tag`

Keys are normalised (`snake` style by default: `Sub-Type` → `sub_type`) **before** merging, so two
spellings of the same key collide predictably: the higher layer wins and a warning is logged once.
See ADR-0004.

## Table model

See ADR-0001. `grouped` (default): one table per value type and optional rule/explicit table; `state_id`
tag distinguishes series. `per-state`: one table per state (name = state id or alias), limited by the
server's table limit.

## Reliability

See ADR-0002. Summary: at-least-once delivery inside the documented buffer limits; replay is idempotent
because InfluxDB 3 merges points with identical table+tags+timestamp **(measured)**. No exactly-once
promise.

## Security

See ADR-0005. Summary: token only in `encryptedNative`; never logged (central redaction); diagnostics do
not echo the token; user SQL is restricted to read-only statements and limited in rows/bytes/time;
optional sender allow-list; no HTTP port is opened by the adapter.

## Directory layout

```text
admin/            jsonConfig.json, jsonCustom.json, i18n/<lang>.json, influx-v3.png
src/main.ts       AdapterRuntime
src/lib/*.ts      modules above
test/unit/        mocha unit tests (no network)
test/influx/      tests against a real InfluxDB 3 Core (Docker) — skipped when INFLUX3_URL is not set
test/integration.js, test/package.js   @iobroker/testing (real js-controller)
scripts/          influx3-test-server.sh, load test
docs/             RESEARCH, ARCHITECTURE, adr/, GRAFANA, CONFIGURATION, pl/README
```
