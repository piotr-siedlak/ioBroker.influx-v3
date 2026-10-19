# ADR-0004 — Tag engine, normalisation and cardinality protection

Status: accepted (2026-10-19)

## Decision

* **Tag catalog (added after 0.1.0, owner request):** tag keys are defined once per instance (`tagDefinitions`:
  key, allowed values, *other values allowed*, default for all states, description). The object dialog offers only
  catalog keys (`selectSendTo getTagKeys`) and suggests the allowed values (`autocompleteSendTo getTagValues`);
  validators read `instanceObj.native.tagDefinitions`, so the rules also apply when the instance is stopped. The
  runtime enforces the same rules (`checkTagAgainstCatalog`). Goal: one standard vocabulary of tags across all
  objects (and therefore stable Grafana filters). The former "global tags" table is converted once into catalog
  entries with defaults and saved in the instance settings.

* Layers and precedence: see `docs/ARCHITECTURE.md` (defaults → global → automatic → per-state → explicit
  removal → reserved).
* Key normalisation (`tagKeyStyle`): `snake` (default: lower case, every char outside `[a-z0-9_]` → `_`,
  collapsed, trimmed; `Sub-Type` → `sub_type`) or `preserve` (escaping only; SQL needs `"Sub-Type"`).
  Normalisation happens before merging so collisions are deterministic.
* Reserved keys: `time`, `state_id`, the value field name, metadata names (`ack`, `q`, `from`), keys starting
  with `_` or `iox::`, empty keys. Reserved keys in user input are dropped with a warning.
* Values: control characters → space, trimmed, trailing backslashes removed (InfluxDB rejects identifiers
  ending in `\` **(measured)**), max `maxTagValueLength` (256), empty → tag omitted.
* Templates: `{id}`, `{id.segment[N]}` (negative N from the end), `{id.adapter}`, `{id.instance}`,
  `{common.unit|role|type|name}`, `{native.<key>}` (only keys in `nativeAllowlist`), `{enum.room}`,
  `{enum.function}`, `{parent.device}`, `{parent.channel}`, `{state:<id>}` (dynamic, only with
  `dynamicTagsEnabled`). Parsed with a regular expression, resolved from a whitelist — no `eval`, no JS.
* Metadata placement is instance-wide (`metaAck`, `metaQ`, `metaFrom` ∈ `field|tag|none`) because a column
  is either a tag or a field for the whole table; per state only include/exclude switches exist.
  Defaults: `ack` boolean field, `q` integer field, `from` not stored (`from` as tag → cardinality warning).
* Sensitive data guard (default on): keys matching `pass|pwd|secret|token|apikey|api_key|auth|session|cookie|bearer`
  and values that look like e-mail addresses, `apiv3_…` tokens, JWTs or long random strings (≥ 32 chars of
  base64/hex alphabet) are dropped unless `allowSensitiveTags` is set on that state.
* Cardinality guard: for every non-constant tag key (automatic `name`, templates, dynamic) the adapter tracks
  distinct values; above `maxTagValuesPerKey` (default 1 000) new values are replaced by `_other` and a
  warning is logged once per key. Dynamic tags additionally have `dynamicTagMaxValues` (100), an optional
  allowed-values regex and a TTL after which a stale source value is replaced by the fallback (or omitted).

## Consequences

Changing tags of a state creates a new series (documented in UI help). The tag set is computed at
configuration time (cheap per write); only dynamic tags are looked up per write from an in-memory cache.
