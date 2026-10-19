# Installing a test version from GitHub

Until `influx-v3` is published in the ioBroker repository, test versions are installed directly from this GitHub
repository. This installs the current `main` branch – use it for testing, not for production.

## Install or update

Console of the ioBroker host:

```bash
iobroker url https://github.com/piotr-siedlak/ioBroker.influx-v3
```

```bash
iobroker restart influx-v3.0
```

Or in ioBroker Admin: *Adapters* → expert mode → GitHub icon → *Custom URL* →
`https://github.com/piotr-siedlak/ioBroker.influx-v3`.

Run the same command again to update. Existing instances (`influx-v3.N`) and their settings are kept; the classic
`influxdb` adapter is not affected.

## Notes

* The compiled adapter (`build/`) is part of the repository, so no build step is needed on the ioBroker host.
* If ioBroker reports `cannot find start file`, the installed version is older than commit `0d21c36` – install again.
* After the first start, define your tag keys in the instance settings (tab *Tags* → tag catalog) before choosing tags
  in the object settings.

---

# Instalacja wersji testowej z GitHuba (PL)

Dopóki `influx-v3` nie jest opublikowany w repozytorium ioBrokera, wersje testowe instaluje się bezpośrednio z tego
repozytorium (aktualna gałąź `main` – do testów, nie do produkcji).

```bash
iobroker url https://github.com/piotr-siedlak/ioBroker.influx-v3
```

```bash
iobroker restart influx-v3.0
```

To samo polecenie służy do aktualizacji. Istniejące instancje i ich ustawienia zostają zachowane, adapter `influxdb`
nie jest zmieniany. Po pierwszym starcie zdefiniuj klucze tagów w ustawieniach instancji (zakładka *Tagi* → katalog
tagów).
