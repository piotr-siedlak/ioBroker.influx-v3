# ADR-0001 — Table model: `grouped` (default) and `per-state`

Status: accepted (2026-10-19)

## Context

In InfluxDB 3 every table is a columnar table: `time`, tag columns (`Dictionary(Int32, Utf8)`) and field
columns with a fixed type. Measured on Core 3.12.1: max **2 000 tables per server**, **500 columns per
table**, a column type can never change, and a write that conflicts with an existing column type is
rejected per line. The classic `ioBroker.influxdb` writes one measurement per state, which on InfluxDB 1.x/2.x
was cheap but on Core hits the table limit at ~2 000 logged states.

## Decision

Two modes, selected per instance (`schemaMode`):

### `grouped` (default, recommended)

* Table resolution, first match wins:
  1. per-state `table` (explicit, validated);
  2. instance `tableRules` (ordered list: match `role` or `id` glob, optional value-type filter → table);
  3. default table per value type: `numeric_states` / `boolean_states` / `string_states` (configurable).
* Tag `state_id` (= alias or real id) is always written → every state is its own series.
* **Exactly one value type per table.** The `SchemaMapper` keeps a registry seeded from
  `information_schema.columns` and from the claims of enabled states (sorted by id → deterministic). A state
  whose value type conflicts with its table is **not written**; it is listed in `info.conflicts`, logged once,
  and shown in the mapping preview. Rules only apply to states whose type matches the rule's type filter
  (default filter: `number`), so automatic mapping never produces conflicts.
* Booleans can be stored as numbers (`storeAs: number` → `1/0`) to put them into numeric tables.

### `per-state`

* Table = per-state `table` or the state id (alias if configured) — same as the classic adapter's
  measurement name.
* Before activation the adapter counts enabled states + existing tables and warns at 80 % of
  `maxTables` (default 2 000 = Core limit). New tables beyond the limit are refused (state listed as conflict),
  instead of producing a flood of 422 errors.
* Optional per-state `field` name (default `value`) is only allowed in this mode.

## Consequences

* Grafana queries in `grouped` mode filter by tags (`WHERE location='KOTLOWNIA'`), not by table name.
* Changing a state's table or mode starts a new series; old data stays in the old table and is no longer
  returned by `getHistory`. This is documented in the UI help text.
* Numbers are always written as floats (`value=5`, never `5i`), because Core rejects an integer into a float
  column **(measured)**.
