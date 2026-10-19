# Load test results

Measured with `scripts/loadtest.ts` on **2026-10-19**. Only these environments were measured; other
hardware (especially Raspberry Pi / SD cards) has **not** been tested.

| Item | Value |
| --- | --- |
| Machine | Apple M1 Max, 10 cores, 64 GiB RAM, macOS (darwin arm64) |
| Node.js | v26.4.0 (load generator and write path in one process, run through `tsx`) |
| InfluxDB | 3 Core 3.12.1 in Docker (`influxdb:3-core`, `--object-store memory`), same machine |
| Settings | defaults except `flushIntervalMs = 1000`: batched, 5 000 lines / 1 MiB per batch, 1 parallel request, gzip, memory buffer |
| Write path | `FilterPipeline` (changesOnly) → tag resolution (3–4 tags + `state_id`) → line protocol → `WriteQueue` → HTTP `/api/v3/write_lp` |

Scenarios: **constant** – every state changes once every 10 s, spread evenly, 20 s; **burst** – all states change at
the same moment, 5 bursts 2 s apart. Lag = server acknowledgement time − `state.ts` of the record.

| scenario | states | generated | written | rejected | dropped | records/s | lag p50 / p95 / p99 (ms) | event loop p99 (ms) | RSS (MB) | CPU (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| constant | 100 | 199 | 199 | 0 | 0 | 10 | 1046 / 1502 / 1552 | 13 | 83 | 179 |
| burst | 100 | 500 | 500 | 0 | 0 | 50 | 1991 / 1998 / 1998 | 13 | 83 | 52 |
| constant | 1 000 | 1 990 | 1 990 | 0 | 0 | 95 | 1357 / 1824 / 1873 | 14 | 86 | 168 |
| burst | 1 000 | 5 000 | 5 000 | 0 | 0 | 497 | 1964 / 1987 / 1988 | 16 | 94 | 100 |
| constant | 10 000 | 19 800 | 19 800 | 0 | 0 | 948 | 1238 / 1697 / 1765 | 16 | 130 | 418 |
| burst | 10 000 | 50 000 | 50 000 | 0 | 0 | 4 890 | 1727 / 1921 / 1929 | 17 | 179 | 360 |

**Disk buffer rebuild:** 100 000 records in 20 spool files → `start()` (recovery scan) 2 ms, all records written
after 19.7 s.

## Interpretation

* The lag of ~1–2 s is dominated by the flush interval (1 s here) plus InfluxDB 3's write acknowledgement, which
  waits for the WAL flush (Core default `--wal-flush-interval` 1 s). `noSync` (instance option "fast
  acknowledgement") removes the WAL wait at the cost of durability on server crashes.
* With one request in flight, the throughput is bounded by one batch per WAL flush (≈ 5 000 records/s with the
  default batch size). This also bounds the replay speed after an outage (100 000 records ≈ 20 s). Larger batches
  or `maxConcurrentWrites` 2–4 increase it; ordering between batches is then not guaranteed (timestamps are explicit,
  so the stored data is the same).
* The event loop stayed below 17 ms (p99) and memory below 180 MB RSS, including the load generator and `tsx`.
* RSS includes the TypeScript runtime (`tsx`/esbuild) of the test process; an installed adapter runs the compiled
  JavaScript.

Reproduce:

```bash
eval "$(scripts/influx3-test-server.sh start)"
npx tsx scripts/loadtest.ts 100,1000,10000 20
```
