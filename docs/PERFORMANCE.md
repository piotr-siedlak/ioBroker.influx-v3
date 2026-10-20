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

## Import of historical data

Measured with `scripts/import-loadtest.ts` on **2026-10-20**, same machine; old database InfluxDB 2.7.12 in Docker,
target InfluxDB 3 Core 3.12.1 in Docker with `--wal-files-per-snapshot 10` (persists Parquet files every ~10 s, like
a server that runs for hours). The old database is seeded like the classic adapter writes (one measurement per state,
~10 % switches, two custom tags, ack/q/from fields).

**Write throughput of InfluxDB 3** (200 000 points, one client): a write is answered after the WAL flush (~1 s),
so the request size sets the speed.

| lines per request | parallel requests | points/s |
| ---: | ---: | ---: |
| 5 000 | 1 | 5 042 |
| 20 000 | 1 | 20 078 |
| 50 000 | 1 | 50 378 |
| 5 000 | 4 | 20 198 |
| 20 000 | 4 | 67 204 |
| 50 000 | 2 | 100 351 |
| 20 000 | 8 | 104 275 |

**Reading** one day of 548 states (157 824 points, 6 query groups of ≤ 100 measurements): 1.7 s (~93 000 points/s).

**Import** (defaults: 50 000 lines per request, 2 parallel requests):

| scenario | points | duration | points/s | Parquet files written by Core | 1 h / 1 day query |
| --- | ---: | ---: | ---: | --- | --- |
| state after state (versions before the time-sliced import), 548 states × 3 days, 1 point/5 min, stopped after 66 states | – | – | – | 174 files for one hour of one table (~29 per 10-minute block) – already above the 432-file limit for a 3-hour query | whole-table query: "would scan 432 Parquet files" |
| time-sliced, 548 states × 3 days, 1 point/5 min (batch 5 000, 1 request) | 473 472 | 109 s | 4 346 | 1 124 for 2 tables × 432 blocks (~1.3 per block) | ok / ok |
| time-sliced, 548 states × **2 years**, 1 point/hour | 9 600 960 | 144 s | 66 519 | 34 556 for 2 tables (one per hour that has data) | ok / ok |
| same, 548 states × 180 days, Node heap limited to 200 MB (`--max-old-space-size=200`) | 2 367 360 | 40 s | 58 961 | – | ok / ok |

Memory: the job keeps at most two time windows (≤ 100 000 points each) – the one being written and the one read
ahead. Peak RSS of the test process (including `tsx`) was 715 MB with an unlimited heap (garbage collected lazily) and
313 MB with a 200 MB heap limit, at almost the same speed.

Not measured: a Raspberry Pi or other small hardware, an old database on another machine, InfluxDB 1.x as source at
this size. A real import also competes with live logging (the import waits while the live buffer fills up).

Reproduce:

```bash
eval "$(scripts/legacy-influx-test-server.sh start)"
docker run -d --name iob-influx3-load -p 127.0.0.1:18290:8181 influxdb:3-core \
    influxdb3 serve --node-id load --object-store memory --without-auth --wal-files-per-snapshot 10
INFLUX3_LOAD_URL=http://127.0.0.1:18290 npx tsx scripts/import-loadtest.ts 548 730 3600
```
