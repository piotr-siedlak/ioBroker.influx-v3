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

6. UI texts: add every new key to all 11 files in `admin/i18n/` (`npm run translate` can fill missing languages,
   it needs a translation API key). Every Admin control needs the sizes `xs, sm, md, lg, xl` (checker `E5507`).
7. `build/` is committed (installation from GitHub needs it, and `prepare` scripts are not allowed for ioBroker
   adapters): run `npm run build` and commit the changed `build/` files together with the sources. CI fails if they
   differ.
8. Add an entry under `### **WORK IN PROGRESS**` in the changelog of README.md.

Commits follow the conventional style (`feat:`, `fix:`, `docs:`, `test:` …).
