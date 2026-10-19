# Publishing checklist (not executed)

Nothing has been published: no npm release, no GitHub push, no PR to `ioBroker.repositories`. These steps need the
owner's decision and credentials.

## 1. Adapter name and GitHub repository (blocking)

Done in code: the adapter is named **`influx-v3`** (npm `iobroker.influx-v3`); the checker rejects `_` in new
adapter names (`[E4021]`, see [RESEARCH.md §8](RESEARCH.md#8-naming-analysis)).

Still to do **before the first push**: the GitHub repository is currently `piotr-siedlak/iobroker-influx-v3`
(renamed from `iobroker-influx_v3` on 2026-10-19). The checker requires the exact form `ioBroker.<adapter>` — prefix
`ioBroker` with capital **B** followed by a **dot** (`E0002`: `/^iobroker\./i`, `E0003`: capital B) — so it has to be
renamed once more to `ioBroker.influx-v3` (*Settings → Repository name*; GitHub redirects the old URLs). `package.json` and
`io-package.json` already point to `https://github.com/piotr-siedlak/ioBroker.influx-v3`; the checker compares them
with the repository's clone URL (`E0019`) and requires the `ioBroker.` prefix (`E0002`/`E0003`).

## 2. GitHub repository

* Push `main`; set the repository *About* text and topics (`iobroker`, `influxdb`, `influxdb3`, `adapter`) — checker
  `E8001`.
* Enable Actions; the workflow `.github/workflows/test-and-release.yml` runs lint, type check, package tests, the
  adapter test matrix (Node 22/24/26 × Linux/Windows/macOS) and the InfluxDB 3 Core tests (Docker, Linux).
* Enable Dependabot (config included).

## 3. Repository checker

```bash
npx @iobroker/repochecker https://github.com/<owner>/ioBroker.influx-v3
```

Status of a local run (2026-10-19, checker 5.23.2 in `--local` mode against the still empty, not yet renamed
repository): no findings for package.json/io-package.json contents, Admin JSON Config, i18n, README, license or
Dependabot. Open items, all on the owner side: repository name/URL (`E0002`, `E0004`, `E0007`, `E0019`), external
icon reachable only after the push (`E1025`), About text and topics (`E8001`, `E8002`), first CI run (`W3039`),
npm package (`E2000`), repository listing (`W4001`). `E4031`/`E4033` appeared only because
`download.iobroker.net` was not reachable from the test environment.

## 4. Translations

Done: all 11 languages are translated (no `W5603`/`W5604`/`W5606`). Native speakers are welcome to review the
machine-independent but non-native translations of `ru, pt, nl, fr, it, es, uk, zh-cn`.

## 5. npm release (trusted publishing)

1. Create the package on npmjs.com and configure *trusted publishing* for the GitHub repository/workflow.
2. Add the release notes under `### **WORK IN PROGRESS**` in README.md.
3. `npm run release patch` (or `minor`) – the release script bumps versions, updates `io-package.json` news,
   tags and pushes; the `deploy` job publishes to npm.

## 6. ioBroker repository

1. Test thread in the ioBroker forum (beta testers, different hardware incl. Raspberry Pi).
2. PR to [`ioBroker.repositories`](https://github.com/ioBroker/ioBroker.repositories) adding the adapter to
   `sources-dist.json` (latest) — the bot runs the checker again.
3. After the review period: stable (`sources-dist-stable.json`).

## 7. Open technical items before 1.0

See "Limitations" in the final report / README: Enterprise and Cloud untested, HTTP proxy, remaining
translations, two simultaneous instances only tested at configuration level, no tests on Raspberry Pi.
