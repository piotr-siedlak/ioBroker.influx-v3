# Publishing checklist

Status 2026-10-20: version **0.2.0** is on GitHub (`piotr-siedlak/ioBroker.influx-v3`, branch `main`, installable
with `iobroker url …`, see [INSTALL-DEV.md](INSTALL-DEV.md)); `package.json`, `io-package.json` (incl. `news`) and
the changelog say 0.2.0. **Not done:** the git tag `v0.2.0` (pushing it starts the npm deploy job of the workflow),
the npm release and the PR to `ioBroker.repositories` – they need the owner's decision and credentials.

## 1. Adapter name and GitHub repository – done

The adapter is named **`influx-v3`** (npm `iobroker.influx-v3`); the checker rejects `_` in new adapter names
(`[E4021]`, see [RESEARCH.md §8](RESEARCH.md#8-naming-analysis)). The repository has the required form
`ioBroker.influx-v3` (capital B and a dot, checker `E0002`/`E0003`), matching the URLs in `package.json` and
`io-package.json` (`E0019`).

## 2. GitHub repository

* Done: `main` is pushed; GitHub Actions run `.github/workflows/test-and-release.yml` (lint, type check, package
  tests, adapter test matrix Node 22/24/26 × Linux/Windows/macOS, InfluxDB 3 Core and InfluxDB 1.8/2.7 tests in
  Docker) – green for the latest commits.
* Open (owner): repository *About* text and topics (`iobroker`, `influxdb`, `influxdb3`, `adapter`) – checker
  `E8001`/`E8002`; enable Dependabot (config included).

## 3. Repository checker

```bash
npx @iobroker/repochecker https://github.com/piotr-siedlak/ioBroker.influx-v3
```

Run on 2026-10-20 (checker 5.23.2, commit `7a2ed25`): package.json and io-package.json without findings; expected
`E2000` (not on npm yet) and `W4001` (not in the ioBroker repository yet). The remaining checks stopped with
`getaddrinfo ENOTFOUND iobroker.live` – that host was not reachable from the machine of the run; repeat the run from
a network that reaches `iobroker.live` and `download.iobroker.net`. The last complete local run (2026-10-19,
`--local`) had no findings for Admin JSON Config, i18n, README, license or Dependabot.

## 4. Translations

Done: all 11 languages are translated (no `W5603`/`W5604`/`W5606`). Native speakers are welcome to review the
machine-independent but non-native translations of `ru, pt, nl, fr, it, es, uk, zh-cn`.

## 5. npm release (trusted publishing)

1. Create the package on npmjs.com and configure *trusted publishing* for the GitHub repository/workflow.
2. For **0.2.0** (versions and news already set): push the tag `v0.2.0` (`git tag v0.2.0 && git push origin v0.2.0`);
   the `deploy` job publishes it to npm.
3. For later versions: add the release notes under `### **WORK IN PROGRESS**` in README.md, then
   `npm run release patch` (or `minor`) – the release script bumps versions, updates `io-package.json` news, tags and
   pushes; the `deploy` job publishes to npm.

## 6. ioBroker repository

1. Test thread in the ioBroker forum (beta testers, different hardware incl. Raspberry Pi).
2. PR to [`ioBroker.repositories`](https://github.com/ioBroker/ioBroker.repositories) adding the adapter to
   `sources-dist.json` (latest) — the bot runs the checker again.
3. After the review period: stable (`sources-dist-stable.json`).

## 7. Open technical items before 1.0

See the README: Enterprise and Cloud untested, no HTTP proxy, two simultaneous instances only tested at
configuration level, no tests on Raspberry Pi, history import measured only on desktop hardware
([PERFORMANCE.md](PERFORMANCE.md)).
