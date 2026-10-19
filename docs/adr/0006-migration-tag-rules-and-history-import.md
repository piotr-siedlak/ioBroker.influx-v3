# ADR-0006: Tag rules for the migration and import of historical data

Status: accepted · Date: 2026-10-19

## Context

Users switch from the classic ioBroker.influxdb adapter (InfluxDB 1.x/2.x) to influx-v3. Two things are missing:

1. The settings migration copies the custom tags of `influxdb.N` 1:1. With the tag catalog (ADR-0004) many of those
   tags are not allowed any more (other key spelling, other values), so they are dropped instead of being translated.
2. Historical data stays in the old database. A plain `influx export` → `influxdb3 write` does not help, because the
   classic adapter stores **one measurement per state** (`value` field, `ack`/`q`/`from` as fields – or as tags with
   the 2.x option *use tags*, custom tags as tags), while influx-v3 uses grouped tables (`numeric_states`, …) with a
   `state_id` tag, catalog tags and its own metadata layout. Imported data would not be visible to the same queries.

## Decision

### 1. Tag rules (shared by both migrations)

Instance setting `migrationTagRules`: table of rules `fromKey`, `fromValue`, `toKey`, `toValue`.

| Rule field   | Meaning                                                                      |
|--------------|------------------------------------------------------------------------------|
| `fromKey`    | tag key in the old adapter (case-insensitive), required                      |
| `fromValue`  | old value (case-insensitive); empty = every value                            |
| `toKey`      | new key; empty = keep the key; `-` = remove the tag                          |
| `toValue`    | new value; empty = keep the value                                            |

Evaluation per old tag (pure function `applyTagRules`, `src/lib/tagMapping.ts`):

1. A rule with matching `fromKey` **and** `fromValue` wins over a rule with empty `fromValue`; otherwise the first
   rule in table order.
2. The result is canonicalised against the catalog: a key equal to a catalog key after normalisation and a value equal
   to an allowed value ignoring case get the catalog spelling (`kotlownia` → `KOTLOWNIA`).
3. The tag is accepted only if it passes the catalog check (same function as the runtime). Rejected tags are reported,
   never written.
4. Two old tags that end up with the same key: the first one wins, the others are reported as conflicts.

The settings migration (tab *Migration of settings*) gets the rule table and a **live preview** (`textSendTo`,
re-computed while the rules are edited, unsaved rules included): per state the old tags, the resulting tags and
the rejected ones, plus a summary of old `key=value` pairs that still do not fit the catalog (with the number of
states) – the list the user needs to write more rules or extend the catalog. *Apply* writes only accepted tags.

### 2. Import of historical data (new tab *Migration of data*)

Runs inside the adapter as a background job; it is **read-only on the source** and never deletes anything.

* **Source**: InfluxDB 1.x (InfluxQL `/query`, database, optional retention policy, user/password – Basic auth) or
  InfluxDB 2.x (Flux `/api/v2/query`, organisation, bucket, token). Password and token are stored encrypted
  (`encryptedNative` + `protectedNative`) and never logged. A button copies URL, version, database/bucket,
  organisation and user from an `influxdb.N` instance; secrets cannot be copied (they are protected for that
  adapter) and must be entered once.
* **Scope**: the states this instance logs (configured via the settings migration or by hand), optionally limited by
  an ID pattern, a start and an end date. The source measurement of a state is the alias of its `influxdb.N`
  settings, else its own alias, else its ID (the same rule the classic adapter used).
* **Conversion** uses the live code path: value coercion to the target type, rounding, target table, static tags
  (catalog defaults, automatic tags, state tags, `state_id`), metadata as fields/tags according to the instance
  settings. Tags stored in the old points pass through the tag rules and the catalog check and are added when the
  key is not set by the current configuration (option, on by default). Dynamic `{state:…}` tags are not
  reconstructed for history (their past values are unknown).
* **No overlap with live data**: by default each state is imported only up to the first value influx-v3 already
  stored for it (`SELECT MIN(time) … WHERE state_id = $id`). Points written twice are not duplicated anyway: same
  table, tags and timestamp overwrite each other in InfluxDB 3.
* **Reading in time windows** with adaptive size (start 1 day, halve when a window returns more than 50 000 rows or the
  response exceeds the size limit, double when it returns less than 10 000; 1 minute … 90 days). Constant memory,
  independent of the size of the old database.
* **Writing** directly through the client (not through the live queue, so live values never get dropped by the
  overflow policy), in batches (default 5 000 lines, ≤ 8 MiB), with retry and backoff on transient errors; partial
  writes are counted and sampled to the log. The job waits while the live queue is more than half full.
* **Checkpoint/resume**: after each written window the progress (`doneUntil` per state) is saved atomically to
  `<instance data dir>/data-migration.json`. After a restart or *Stop* the job continues where it stopped (same
  parameters) – re-written windows are idempotent. *Reset progress* starts over.
* **Status** in states `info.dataMigration.running|progress|status|points|errors` (shown live in the tab).
* **Dry run** (*Preview*): per state source measurement, first/last time in the source, time range that would be
  imported, target table and the first converted line-protocol records – nothing is written.

### Not supported (documented, not hidden)

* Deleting or re-writing data that was already imported with other tags: InfluxDB 3 Core has no point delete. A new
  import with changed rules creates new series; to redo an import the target table must be dropped manually.
* InfluxDB 1.x continuous-query results / downsampled retention policies other than the selected one.
* InfluxDB 3 Core is optimised for recent data; queries over long imported periods may hit the server's
  `--query-file-limit` – the documentation says so.

## Consequences

* New modules: `tagMapping.ts` (rules), `legacySource.ts` (1.x/2.x readers incl. annotated-CSV parser),
  `dataMigration.ts` (engine with injected source/target/checkpoint/clock – unit-testable without servers).
* `main.ts` only wires commands (`dmTestSource`, `dmFillFromInfluxdb`, `dmPreview`, `dmStart`, `dmStop`, `dmReset`,
  `migrationPreviewText`) and builds records with the same `prepare`/`toRecord` code as live logging.
* Tests: unit tests for rules, query builders/escaping, JSON and annotated-CSV parsing, window adaptation and the
  engine (resume, stop, retry, partial writes, overlap limit); real-server tests with InfluxDB 1.8 and 2.7 containers
  seeded in the exact format of the classic adapter and imported into InfluxDB 3; js-controller harness tests for the
  settings migration with rules and for a complete import via `sendTo`.

## Implementation plan

1. `tagMapping.ts` + unit tests; settings migration uses it (preview text, apply writes accepted tags only).
2. `legacySource.ts`: InfluxDB 1.x reader (`GROUP BY *` → tags), InfluxDB 2.x reader (Flux + `pivot`, annotated CSV
   with `#datatype/#group/#default`, quoted cells, error tables), first/last time, ping, measurement list.
3. `dataMigration.ts`: job planning (scope, measurement, ranges), adaptive windows, batch writer with retry,
   checkpoint store, progress, stop.
4. `main.ts` wiring + `info.dataMigration.*` objects + encrypted/protected source secrets.
5. Admin: rules table + live preview in *Migration of settings*; new tab *Migration of data*; 11 languages.
6. Tests (unit, real 1.x/2.x → 3, harness), test-server script and CI job for the legacy servers.
7. Documentation (README EN/PL, ARCHITECTURE), changelog, rebuild, commit.
