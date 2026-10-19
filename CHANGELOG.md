# Changelog

All notable changes of `iobroker.influx-v3`. The same entries are kept in the `## Changelog` section of
README.md (required by the ioBroker release script).

## 0.1.0 (2026-10-19)

* Initial version
* Per-state logging (`common.custom["influx-v3.N"]`) to InfluxDB 3 Core/Enterprise via `/api/v3/write_lp`,
  millisecond precision, live re-configuration without restart
* Table models `grouped` (default) and `per-state`, one value type per table, table/column limit checks
* Tag engine: global, automatic, per-state, removal, reserved keys; safe templates; dynamic tags (opt-in);
  key normalisation (`lower_snake_case`), secret detection, cardinality limits
* Filters: changes only, debounce, block time, min delta, re-log, ack/quality, ignore zero/below/above, rounding,
  initial value, last skipped value
* Write queue: batching, gzip, retry with backoff + jitter, circuit breaker, partial-write handling, 413 split,
  overflow policies; memory buffer persisted on shutdown or crash-safe disk buffer
* `getHistory` with server-side aggregation (`date_bin`, selectors, percentiles), read-only SQL/InfluxQL,
  `enableHistory`/`disableHistory`/`getEnabledDPs`, `storeState`, diagnostics, mapping preview
* Admin JSON Config (10 tabs) and per-state dialog, translations en/pl/de (other languages fall back to English)
* Migration wizard for settings of `ioBroker.influxdb` (explicit, previewable, source untouched)
* Experimental write-only support for InfluxDB Cloud via `/api/v2/write`
