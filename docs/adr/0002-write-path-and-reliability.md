# ADR-0002 — Write path, buffering and delivery guarantees

Status: accepted (2026-10-19)

## Decision

* Endpoint: `POST /api/v3/write_lp?db=<db>&precision=millisecond[&no_sync=true]`, default `accept_partial=true`.
  For the experimental Cloud mode: `POST /api/v2/write?bucket=<db>&precision=ms`.
* Timestamp: ioBroker `state.ts` (integer milliseconds, UTC). No conversion to nanoseconds anywhere.
* Batching: flush when `batchMaxLines` or `batchMaxBytes` (default 1 MiB, hard cap 8 MiB because the server
  limit is 10 MiB **(measured)**) is reached, or every `flushIntervalMs`. `direct` mode flushes after every
  record (still through the queue, so ordering and retries are identical).
* Concurrency: `maxConcurrentWrites` (default 1 → strict ordering).
* Error classification (`InfluxV3Client.classify`):

  | Response | Class | Action |
  | --- | --- | --- |
  | 204 | ok | — |
  | 400/422 with `data[]` (partial write) | partial | accepted lines are done; rejected lines are counted + logged (rate limited, redacted), **never retried** |
  | 400/422 without line data | invalid | whole batch rejected, counted, not retried |
  | 413 | too large | batch split in halves and re-queued; single line → rejected |
  | 401/403 | auth | circuit opens, data kept, retried every `authRetryMs` (default 5 min), `info.connection=false` |
  | 404 (database) | config | like auth |
  | 408/429/5xx, socket errors, timeouts | transient | retry with exponential backoff + full jitter, capped (`retryMaxDelayMs`), honouring `Retry-After` |

* Circuit breaker: after a transient failure no new batch is sent until the backoff expires; one probe
  batch is sent (half-open); success closes the circuit.
* Backpressure / limits: `bufferMaxRecords`, `bufferMaxBytes` (RAM), `spoolMaxBytes` (disk),
  `bufferMaxAgeHours`; overflow policy `dropOldest` (default) or `dropNewest`; every dropped record increments
  `info.droppedRecords`.

### Buffer modes

* `memory` (default): pending batches live in RAM. On shutdown they are written to the spool directory
  (`persistOnShutdown`, default on) and replayed at the next start. A crash (`kill -9`) loses the RAM content.
* `disk`: every batch is written to `iobroker-data/influx_v3.N/spool/` (`.tmp` → `fsync` → `rename`) **before**
  it is sent and deleted after the server acknowledged it. After a crash all unacknowledged batches are
  replayed. Data still in the open batch (≤ `flushIntervalMs`) can be lost.

### Guarantees

* **At-least-once** within the limits above. Not exactly-once: a batch acknowledged by the server but not yet
  deleted from the spool when the process dies is sent again.
* Re-sending is **idempotent** as long as tags did not change in between: InfluxDB 3 merges points with the
  same table, tag set and timestamp (later field values win) **(measured)**.
* Shutdown: `onUnload` stops intake, flushes for at most `shutdownFlushTimeoutMs` (default 5 s, below
  `common.stopTimeout`), persists the rest, never blocks ioBroker longer.
