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

## Extension (2026-10-20): old tags taken from the existing configuration

Problem: the user had to type old keys and values by hand, although they are already known – they are the custom
tags in `common.custom["influxdb.N"]` of the logged states – and one tag key usually has several values
(`room` = Kotłownia, Salon, Garaż …) that often map to the same catalog value.

Decision:

1. **Inventory of old tags**: from the custom tags of all states enabled in the chosen `influxdb.N` (all
   `influxdb.*` instances if none is chosen): key (grouped ignoring case, first spelling kept) → values → number of
   states. Built on request from the `system/custom` view, cached for a few seconds.
2. **Several old values per rule**: the old-value column holds a list (empty list = every value). A rule whose list
   contains the old value wins over a rule for every value, otherwise table order. A plain string (rules saved before)
   is read as a list with one value, so existing settings keep working.
3. **Admin**: *Old key* offers the keys of the inventory with value/state counts (typing other keys stays possible,
   e.g. for tags that exist only in old points); *Old values* is a multi-select of the values of that key with state
   counts. The live preview starts with the inventory (`room: Kotłownia (5), Salon (3), Garaż (1)`).
4. **"Take over old tags"** button: appends the rule rows that are still needed, never changes existing rows:
   * an old key without catalog match and without rule → one row "key, every value" with an empty new key to choose;
   * values that the catalog would reject for a known target key → grouped by a suggested catalog value (equal when
     case, diacritics and punctuation are ignored: `Kotłownia` → `KOTLOWNIA`), one row with several old values per
     suggestion; values without suggestion get one row each with an empty new value;
   * pressing it again after new keys were chosen adds the value rows of those keys.
   Similarity is only used for these visible suggestions – automatic canonicalisation stays case-only.
5. Commands `getOldTagKeys`, `getOldTagValues`, `migrationSuggestRules`; the import of historical data uses the
   same rules unchanged.

Plan: (a) rule format with value lists + compatibility, (b) inventory and suggestions as pure functions with unit
tests, (c) commands and preview, (d) Admin columns/button + 11 languages, (e) harness test of lists, suggestions and
apply with a multi-value rule, (f) docs.

## Extension 2 (2026-10-20): automatic matching and a proposed standard

Problem: "Take over old tags" only fills rules for keys/values the catalog already knows. With an empty or partial
catalog the user still has to invent a standard (key names, value spelling) and match every old variant by hand
(`room`/`Raum`/`pomieszczenie`, `Kotłownia`/`kotlownia`/`Heizungsraum`).

Decision – button **"Propose standard (catalog + rules)"** (pure function `proposeStandard`, `src/lib/tagStandard.ts`):

1. **Keys**: an old key is matched to a catalog key when equal after folding (case, diacritics, punctuation,
   plural `s`) or when both belong to the same **synonym group** of a small built-in dictionary (EN/DE/PL/FR/IT/ES:
   location ≈ room/Raum/Zimmer/pomieszczenie/pokój/pièce …, floor ≈ Etage/piętro …, device, type, function, circuit,
   zone, building, source). Old keys of the same group share one key. Without a catalog key a new key is proposed:
   the canonical name of the group (`location`, `floor` …) or the folded old key; reserved names get a suffix.
   An existing rule for the key (user decision) always wins; `-` keeps the tag removed.
2. **Values** (closed lists): old values are grouped by folding; each group is matched to a candidate standard value –
   the catalog values of the key and, optionally, the names of the ioBroker **rooms/functions** for location/function
   keys – by folding or by a multilingual **room synonym dictionary** (boiler room ≈ Heizungsraum ≈ kotłownia,
   kitchen ≈ Küche ≈ kuchnia …). Unmatched groups become new standard values. Keys whose catalog entry allows other
   values keep their values (only the key is translated).
3. **Spelling of new values**: setting *auto* (follows the existing catalog values – all upper case → `UPPER`,
   all lower → `lower`, otherwise as used; an empty catalog → `UPPER`), `UPPER` (`GARAZ`, `BOILER_ROOM`), `lower`
   (`garaz`, `boiler_room`) or *as used* (most frequent old spelling). New keys with many distinct values (> 25)
   allow other values.
4. **Result written into the form** (nothing is saved): catalog rows extended (new keys; missing values appended to
   existing closed lists; existing rows otherwise unchanged) and the minimal rule rows appended (one "every value"
   row for a key rename, plus rows with several old values per new value only where the automatic spelling does not
   already give the result). Values with a rule of their own are left alone. The live preview shows the outcome; the
   user reviews the Tags tab and the rules and saves.
5. Matching by synonyms is only a proposal shown to the user – the runtime never maps by similarity.

Plan: (a) `tagStandard.ts` with dictionaries, folding, style and `proposeStandard` + unit tests; (b) command
`migrationProposeStandard` (inventory, room/function names, catalog/rules from the form); (c) Admin: value-style
select, "use ioBroker rooms/functions" checkbox, button, 11 languages; (d) harness test; (e) docs.

## Extension 3 (2026-10-20): the proposal covers every used tag

Problem (reported): "Propose standard" covered only part of the tags. Causes found:

1. Only the custom tags of the **influxdb settings** were looked at. Tags that exist only in the **old points**
   (configured earlier and removed later) and tags of states **already configured for influx-v3** (e.g. migrated
   before the catalog existed – ignored at runtime since then) were never proposed.
2. Keys that **allow other values** (and new keys with many values) got no values in the catalog and their spelling
   variants (`Pump 1` / `pump 1`) were not merged.
3. Rows left by *Take over old tags* with **empty new key/value** counted as user decisions, so the proposal skipped
   them and the table kept incomplete rows.

Decision:

1. **Inventory of used tags** from three sources, each value marked with its origin: custom tags of the influxdb
   settings (states), tags stored in the old database (measurements; `SHOW TAG VALUES` on 1.x, `schema.tagKeys/
   tagValues` on 2.x; connection of tab *Migration of data*, option, cached 60 s) and custom tags of the states of
   this instance (template values `{…}` excluded). The dropdowns of the rule table use the same inventory.
2. **Every key and value is standardised**: closed lists as before; open lists (catalog `allowOther`, or more than
   25 distinct values) merge spelling variants too (catalog/room/function match, else the most used spelling) and
   list their standard values in the catalog (up to 1000 per key) while still allowing other values.
3. **Placeholder rows** (an empty new key/value that still leads to a rejected tag with the current catalog) are
   completed in place; a placeholder whose old values need different standard values is split. Complete rows and
   removals (`-`) stay user decisions.
4. **Coverage check**: after the proposal every used key/value is translated with the final rules and catalog; the
   result message reports `all N used tag values covered` or lists the uncovered ones with the reason.
5. **Apply** can also standardise the tags of states **already configured for this instance** (option, on by
   default): a tag is rewritten only when the rules translate it into an allowed catalog tag; other tags stay as they
   are. The live preview lists these changes.

Plan: (a) inventory with origins + `tagInventory()` in both legacy readers (unit + real 1.8/2.7 tests); (b) proposal:
placeholders, open lists, coverage (unit tests); (c) existing-state standardisation in plan/preview/apply; (d)
commands, options, Admin, 11 languages; (e) harness test incl. old points; (f) docs.

## Extension 4 (2026-10-20): complete mapping table

Reported with a screenshot: after "Propose standard" the rule table showed rows only for some values. Cause: the
proposal wrote the *minimal* rules – no row where the automatic case-only spelling already gave the result
(`Device` → `device`, `Loxone` → `LOXONE`), so most keys and values were invisible in the table. Also, the target
value of a complete row from an earlier run was not added to the catalog when that value was missing there.

Decision: the proposal writes a **complete mapping**: per old key one "every value" row (key translation, also for
values that appear later) and per standard value one row with all its old spellings – including unchanged values.
Keys with more than 100 distinct values (identifiers) only get rows for values that change. The table is grouped by
old key (stable sort, so the rule that wins does not change). Target values of complete rules always join the
catalog. The message stays short; the full result (coverage, every key with its value mapping) is shown in the panel
*Result of the last proposal* (`info.migrationProposal`).
