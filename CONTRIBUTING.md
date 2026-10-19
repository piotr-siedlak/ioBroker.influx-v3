# Contributing

Thank you for helping! Please:

1. Open an issue first for larger changes.
2. Keep the architecture: pure, unit-tested logic in `src/lib/`, wiring in `src/main.ts`
   (see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and the ADRs in [docs/adr](docs/adr)).
3. Never log or return secrets – use `redact()` for anything that may contain server responses.
4. Do not claim support for InfluxDB features you have not verified – add a test against
   `scripts/influx3-test-server.sh` (Docker) or document the limitation.
5. Before a pull request:

```bash
npm run lint
npm run check
npm run test:unit
eval "$(scripts/influx3-test-server.sh start)" && npm run test:influx && npm run test:integration
```

6. UI texts: add new keys to `admin/i18n/en.json`, `pl.json` and `de.json`; other languages can be generated with
   `npm run translate` (needs a translation API key).
7. Add an entry under `### **WORK IN PROGRESS**` in the changelog of README.md.

Commits follow the conventional style (`feat:`, `fix:`, `docs:`, `test:` …).
