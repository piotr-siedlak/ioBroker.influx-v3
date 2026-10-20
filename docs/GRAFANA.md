# Grafana with InfluxDB 3 and `influx-v3`

All queries on this page are **SQL for InfluxDB 3** (not Flux). Every query below is executed by the
automated test suite (`test/influx/influx3.test.ts`, section "Grafana scenarios") against a real
InfluxDB 3 Core 3.12.1 — with `$__timeFilter(time)` replaced by a fixed range.

## Data source

Grafana ≥ 10.3 ships the **InfluxDB** data source with the query language **SQL** (uses Flight SQL):

| Field | Value |
| --- | --- |
| Query language | `SQL` |
| URL | `http://<host>:8181` (same URL as in the adapter) |
| Database | the database configured in the adapter (default `iobroker`) |
| Token | an InfluxDB 3 token with read permission (Enterprise: a read-only resource token for this database) |
| Insecure connection | enable only for plain HTTP without TLS |

Grafana macros used below: `$__timeFilter(time)` (dashboard time range) and `$__interval`. Time is UTC in the
database; Grafana converts to the dashboard time zone.

## Data model written by the adapter (grouped mode, default)

| Table | `value` type | Typical content |
| --- | --- | --- |
| `numeric_states` | Float64 | all numeric states without a more specific rule/table |
| `boolean_states` | Boolean | switches, contacts |
| `string_states` | Utf8 | texts, JSON |
| your own tables, e.g. `temperature`, `heating`, `energy` | one type per table | via per-state *table* or instance *table rules* |

Columns: `time`, `value`, `ack` (Boolean, optional), `q` (Int64, optional), `from` (optional), tag columns
(`state_id` always; `room`, `function`, global and per-state tags such as `location`, `device`, `subtype`).

> With the default **tag key style `lower_snake_case`**, a tag entered as `Sub-Type` is stored as `sub_type`
> and `Location` as `location` — SQL then needs no quotes. With **"keep as entered"** the keys keep their case and
> every reference must be double-quoted (`"Location"`, `"Sub-Type"`), because SQL folds unquoted identifiers to
> lower case.

## Reference scenarios

The examples assume a table `temperature` (per-state setting *Table* = `temperature`, or a table rule
`role value.temperature* → temperature`) with tags `location`, `device`, `subtype`.

### 1. All temperatures for `location = 'KOTLOWNIA'`

```sql
SELECT time, state_id, value
FROM temperature
WHERE location = 'KOTLOWNIA' AND $__timeFilter(time)
ORDER BY time
```

Grafana: Format *Time series*; to get one line per sensor use the *Partition by values* transformation on
`state_id` (or add `state_id` as a column and use *Multi-frame*).

### 2. `device = 'PC'` and `subtype IN ('HC1','HC2','HC3')`

```sql
SELECT time, subtype, value
FROM temperature
WHERE device = 'PC'
  AND subtype IN ('HC1', 'HC2', 'HC3')
  AND $__timeFilter(time)
ORDER BY time
```

### 3. Temperatures aggregated to time buckets (`date_bin`)

```sql
SELECT date_bin(INTERVAL '15 minutes', time) AS time,
       subtype,
       avg(value) AS value
FROM temperature
WHERE device = 'PC' AND $__timeFilter(time)
GROUP BY 1, subtype
ORDER BY 1, subtype
```

Dynamic bucket size: `date_bin(INTERVAL '$__interval', time)` (Grafana inserts e.g. `1m`). `date_bin` requires an
`INTERVAL` literal; the bucket start is returned (not the middle as in ioBroker's `getHistory`).

### 4. History of one ioBroker `state_id`

```sql
SELECT time, value
FROM numeric_states            -- or the table of that state (see "Objects → gear → Preview")
WHERE state_id = 'heat.0.hc1.temperature' AND $__timeFilter(time)
ORDER BY time
```

### 5. Counting `boolean` states in a time window

```sql
SELECT state_id,
       count(*) AS samples,
       sum(CASE WHEN value THEN 1 ELSE 0 END) AS true_count
FROM boolean_states
WHERE $__timeFilter(time)
GROUP BY state_id
```

Share of time "on" (duty cycle, assuming regular sampling): `avg(CAST(value AS DOUBLE))`.

### 6. Filtering by `room`, `device`, `unit`, `adapter`

Enable the automatic tags *room*, *unit*, *adapter* in the instance settings (tab *Tags*); `device` is a per-state or
catalog tag here (or the automatic *device* = name of the parent device object).

```sql
SELECT time, state_id, value
FROM temperature
WHERE room = 'Kotłownia'
  AND device = 'PC'
  AND unit = '°C'
  AND adapter = 'heat'
  AND $__timeFilter(time)
```

### Tags kept as entered: `Location`, `Sub-Type`, `Device`

If the instance uses *tag key style = keep as entered*:

```sql
SELECT time, "Sub-Type", value
FROM temperature
WHERE "Location" = 'KOTLOWNIA'
  AND "Device" = 'PC'
  AND "Sub-Type" IN ('HC1', 'HC2')
  AND $__timeFilter(time)
```

Unquoted `Location` fails (`Schema error: No field named location`) — verified by the test suite.
Same query with the recommended lower-case schema:

```sql
SELECT time, sub_type, value
FROM temperature
WHERE location = 'KOTLOWNIA' AND device = 'PC' AND sub_type IN ('HC1', 'HC2') AND $__timeFilter(time)
```

### Dashboard variables

```sql
-- variable "location"
SELECT DISTINCT location FROM temperature WHERE $__timeFilter(time)
-- variable "subtype" depending on $location
SELECT DISTINCT subtype FROM temperature WHERE location = '${location}' AND $__timeFilter(time)
```

For large tables restrict variables to a time range (as above) – InfluxDB 3 scans the selected time range.

## Warnings

* **Types:** a table has exactly one `value` type. Booleans can be written as numbers (`Store as: number` → 1/0)
  to share a numeric table; otherwise they go to `boolean_states`.
* **Retention:** data older than the database retention disappears from query results first, the files are deleted
  later. Retention is a database property (InfluxDB 3 Core: set when the database is created).
* **Time:** InfluxDB stores UTC with nanosecond precision; the adapter writes ioBroker's millisecond `state.ts`.
* **Changes of tags or tables** create new series. Old data keeps the old tags/table – combine with `UNION ALL` or
  `COALESCE` if needed.
* **Per-state mode:** tables are named like the state id → quote them: `SELECT * FROM "hm-rpc.0.ABC.TEMPERATURE"`.
* **Long time ranges on InfluxDB 3 Core:** Core keeps one Parquet file per table and 10 minutes and never merges
  them; a query may read at most `--query-file-limit` files (default 432 ≈ 3 days). Dashboards over weeks or years
  (e.g. imported history) fail with `Query would scan … Parquet files, exceeding the file limit` – start Core with a
  higher `--query-file-limit` (more memory per query) or use InfluxDB 3 Enterprise, whose compaction merges old
  files.
