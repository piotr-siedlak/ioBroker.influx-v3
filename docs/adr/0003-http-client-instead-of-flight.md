# ADR-0003 — Own HTTP client instead of the official Flight-based client

Status: accepted (2026-10-19)

## Context

`@influxdata/influxdb3-client` 2.4.0 writes via HTTP but queries only via Arrow Flight (gRPC/HTTP 2) and pulls
`apache-arrow`, `@grpc/grpc-js`, `grpc-web`, `@protobuf-ts/*` (≈ 12 MB). Core/Enterprise expose SQL and InfluxQL
over plain HTTP (`/api/v3/query_sql`, `/api/v3/query_influxql`) — verified.

## Decision

Implement `InfluxV3Client` on `node:http`/`node:https`:

* keep-alive `Agent` (pool size = `maxConcurrentWrites + 2`), per-request timeout, `AbortSignal` on shutdown;
* TLS: `rejectUnauthorized` switch and optional PEM CA;
* gzip request bodies above 1 KiB (configurable);
* JSON results (`format: "json"`), `$param` binding;
* exact parsing of the partial-write error body;
* no HTTP proxy support (0.1.0 and 0.2.0; documented; Node's `http` has no built-in proxy and the use case is rare
  for a LAN database).

## Consequences

Zero runtime dependencies besides `@iobroker/adapter-core`; works identically on ARM (Raspberry Pi), x86_64,
Windows and containers. Large analytical reads are better done by Grafana (which can use Flight SQL).
The client implements a small interface, so a Flight transport can be added later without touching
other modules.
