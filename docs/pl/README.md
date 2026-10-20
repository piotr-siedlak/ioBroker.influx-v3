![Logo](../../admin/influx-v3.png)

# ioBroker.influx-v3 — zapis do InfluxDB 3

Adapter zapisuje wybrane stany ioBrokera do **InfluxDB 3 (Core / Enterprise)** przez natywne API HTTP v3.
Konfiguracja odbywa się per stan (ikona zębatki w *Obiektach*), z wielowarstwowymi tagami, buforem odpornym na
awarie oraz odczytem historii i zapytaniami SQL. Jest to **niezależny** adapter: działa **równolegle** z klasycznym
`ioBroker.influxdb` i nigdy nie zmienia jego instancji ani ustawień.

🇬🇧 English: [README.md](../../README.md)

> Status: **0.1.0, jeszcze nieopublikowany** w npm ani w repozytorium ioBrokera. Wersje testowe:
> [docs/INSTALL-DEV.md](../INSTALL-DEV.md) (instalacja z GitHuba).

## Funkcje

* Włączanie i ustawienia per stan w *Obiekty → zębatka → influx-v3.N* (bez restartu adaptera)
* Dwa modele tabel: **grupowy** (wspólne tabele, tag `state_id`, zalecany) i **per-stan** (tabela na stan)
* Tagi: **katalog tagów** (klucze, dozwolone wartości, wartości domyślne), automatyczne (room, function, adapter,
  instance, device, channel, unit, role, name), per stan wybierane z katalogu,
  bezpieczne szablony (`{id.segment[2]}`, `{common.unit}`, `{enum.room}` …), opcjonalne tagi dynamiczne z innych
  stanów; ochrona przed sekretami w tagach i przed eksplozją kardynalności
* Filtry per stan: tylko zmiany, debounce, czas blokady, minimalna zmiana, ponowny zapis, filtr ack/jakości,
  ignorowanie zera / wartości poniżej / powyżej, zaokrąglanie, wartość początkowa, „ostatnia pominięta wartość”
* Natywne `/api/v3/write_lp` z precyzją milisekundową, paczki, gzip, ponawianie z wykładniczym opóźnieniem
  i losowym rozrzutem, circuit breaker, obsługa częściowego zapisu, dzielenie paczek przy 413
* Bufor w pamięci (zapisywany przy zatrzymaniu) lub na dysku (zapis przed wysłaniem, przetrwa `kill -9`)
* `getHistory` dla wykresów ioBrokera (surowe, średnia, min, max, min/max, suma, liczba, percentyl, kwantyl —
  liczone w InfluxDB przez `date_bin`), zapytania SQL i InfluxQL tylko do odczytu przez `sendTo`
* Interfejs Admin (JSON Config) we wszystkich 11 językach ioBrokera (m.in. polski, angielski, niemiecki), test połączenia, podgląd mapowania,
  diagnostyka, migracja ustawień z `influxdb` z **regułami tagów** oraz **import danych historycznych** z InfluxDB 1.x/2.x
* Wiele instancji z różnymi serwerami/bazami

## Wymagania i obsługiwane edycje

| | |
| --- | --- |
| Node.js | ≥ 22 (CI testuje 22, 24, 26) |
| js-controller | ≥ 6.0.11 |
| Admin | ≥ 7.6.20 |

| Edycja | Obsługa w 0.1.0 |
| --- | --- |
| InfluxDB 3 **Core** | ✅ obsługiwana, testy integracyjne z wersją 3.12.1 |
| InfluxDB 3 **Enterprise** | ✅ to samo API, powinna działać, **nieprzetestowana** (testy nie miały dostępu do serwera Enterprise) |
| InfluxDB **Cloud** Serverless / Dedicated / Clustered | ⚠️ eksperymentalnie, **tylko zapis** przez `/api/v2/write`; historia i SQL niedostępne (te produkty udostępniają SQL wyłącznie przez Flight) |
| InfluxDB 1.x / 2.x | ❌ użyj `ioBroker.influxdb` |

Szczegóły, zmierzone limity i źródła: [docs/RESEARCH.md](../RESEARCH.md).

## Instalacja (z GitHuba, obok klasycznego adaptera)

1. ioBroker Admin → *Adaptery* → tryb eksperta → *Instaluj z własnego adresu URL* →
   `https://github.com/piotr-siedlak/ioBroker.influx-v3` (albo w konsoli:
   `iobroker url https://github.com/piotr-siedlak/ioBroker.influx-v3`).
2. Dodaj instancję `influx-v3.0`. Istniejące `influxdb.0` działa dalej bez zmian.
3. Skonfiguruj połączenie (niżej), zdefiniuj klucze tagów w **katalogu tagów** (zakładka *Tagi*) i włącz zapis
   wybranych stanów ikoną zębatki.

## Przygotowanie InfluxDB 3

```bash
# InfluxDB 3 Core w Dockerze, dane w wolumenie "influxdb3-data"
docker run -d --name influxdb3 --restart unless-stopped -p 8181:8181 -v influxdb3-data:/var/lib/influxdb3 \
  influxdb:3-core influxdb3 serve --node-id iobroker --object-store file --data-dir /var/lib/influxdb3

# token administracyjny (Core ma wyłącznie tokeny administracyjne – trzymaj go w tajemnicy)
docker exec influxdb3 influxdb3 create token --admin

# opcjonalnie: baza z retencją (w Core nie da się jej później zmienić)
docker exec influxdb3 influxdb3 create database --retention-period 365d --token <TOKEN> iobroker
```

Bez jawnego `create database` baza zostanie utworzona przy pierwszym zapisie z **nieograniczoną** retencją.
W **Enterprise** zamiast tokenu administracyjnego utwórz token zasobu z prawem odczytu i zapisu tylko dla tej bazy.

Test połączenia z konsoli:

```bash
curl -H "Authorization: Bearer <TOKEN>" http://<host>:8181/ping
# {"product_name":"InfluxDB 3 Core","version":"3.12.1",...}
```

W ustawieniach instancji przycisk **Sprawdź połączenie** wykonuje to samo (również przed zapisaniem ustawień)
i pokazuje edycję, wersję, istnienie bazy i retencję — nigdy token.

## Konfiguracja instancji

| Zakładka | Zawartość |
| --- | --- |
| Połączenie | edycja (automatyczne wykrywanie przez `/ping`), URL (http/https, port, opcjonalny prefiks ścieżki), baza, token (zaszyfrowany), weryfikacja TLS, certyfikat CA, limity czasu, **Sprawdź połączenie**, opcjonalne ręczne *Utwórz bazę* |
| Zapis danych | paczkowy/bezpośredni, wielkość paczki (rekordy/bajty), interwał wysyłania, gzip, `no_sync`, równoległe żądania, opóźnienia ponowień |
| Bufor awaryjny | pamięć/dysk, limity (rekordy, bajty, dysk, wiek), polityka przepełnienia, fsync, czas na wysłanie przy zatrzymaniu |
| Schemat pomiarów | grupowy/per-stan, domyślne tabele per typ, reguły tabel (wzorzec roli/ID → tabela), kolumna wartości, limity serwera, teksty/JSON, umiejscowienie `ack`/`q`/`from` (pole/tag/brak) |
| Tagi | **katalog tagów** (klucze, dozwolone wartości, domyślne), styl kluczy (`lower_snake_case` lub bez zmian), tagi automatyczne, lista dozwolonych kluczy native, ochrona, tagi dynamiczne |
| Domyślne ustawienia stanów | wartości domyślne dla nowych konfiguracji stanów |
| Odczyty i historia | limity wierszy, limity czasu, domyślna agregacja, zapytania użytkownika i dozwoleni nadawcy |
| Zaawansowane | próg subskrypcji wszystkich stanów, limity logów, interwał statystyk, statystyki do InfluxDB (opcjonalnie) |
| Diagnostyka | połączenie, wersja serwera, zapisane/odrzucone/porzucone rekordy, bufor, ostatni błąd |
| Migracja ustawień | kopiowanie ustawień stanów z instancji `influxdb.N` z tłumaczeniem własnych tagów przez **reguły tagów** (podgląd na żywo → zastosuj; `influxdb.N` pozostaje bez zmian) |
| Migracja danych | import historii stanów zapisywanych przez `influxdb.N` (także jeszcze nieskonfigurowanych tutaj) ze starej bazy InfluxDB 1.x/2.x, w jednym przejściu po osi czasu: test, podgląd, start/stop, liczba punktów, postęp na żywo z pozostałym czasem, po restarcie kontynuuje sam |

## Konfiguracja per stan (zębatka)

*Obiekty* → zębatka przy stanie → sekcja **influx-v3.0** → *włączone*. Zakładki:

* **Podstawowe:** tabela (puste = automatycznie), zapisz jako (auto/liczba/logiczna/tekst), alias ID, tagi tego
  stanu (klucz i wartość wybierane z katalogu tagów), tylko zmiany, debounce, ponowny zapis, minimalna zmiana, **podgląd** wynikowego rekordu Line Protocol (bez zapisu)
* **Filtry:** czas blokady, filtr ack/jakości, ignorowanie zera / poniżej / powyżej, zaokrąglanie, wartość
  początkowa, ostatnia pominięta wartość
* **Tagi i metadane:** wartości domyślne z katalogu („tagi globalne”) / tagi automatyczne, usuwanie kluczy tagów,
  zapis ack/q/from, zgoda na „wrażliwe” tagi
* **Zaawansowane:** kolumna wartości (tryb per-stan), logi debug

Zmiany działają natychmiast (bez restartu). Wyłączenie natychmiast zatrzymuje zapis (wartości czekające w oknie
debounce lub blokady są odrzucane).

### Przykład

Stan `heat.0.hc1.temperature`, tagi stanu `Location=KOTLOWNIA`, `Device=PC`, `Sub-Type=HC1`, tabela `heating`,
pokój (enum) „Kotłownia”, tag globalny `site=home`:

```text
heating,device=PC,location=KOTLOWNIA,room=Kotłownia,site=home,state_id=heat.0.hc1.temperature,sub_type=HC1 value=38.7,ack=true,q=0i 1792418400123
```

Znacznik czasu = `state.ts` w milisekundach, zapis z `precision=millisecond`; tagi posortowane; klucze domyślnie
w stylu `lower_snake_case` (`Sub-Type` → `sub_type`).

## Tagi

**Katalog tagów** (ustawienia instancji → *Tagi*) to centralna lista kluczy tagów, dzięki której wszystkie obiekty
używają tych samych, ustandaryzowanych tagów. Każdy wpis ma: *klucz* (np. `Location`, `Device`, `Sub-Type`),
*dozwolone wartości* (po przecinku, np. `KOTLOWNIA, SALON, GARAZ`), przełącznik *inne wartości dozwolone* (wtedy
również szablony, np. `{id.segment[2]}`), *wartość domyślną dla wszystkich stanów* (zastępuje dawną tabelę „tagów
globalnych”, która jest konwertowana automatycznie) i *opis*.

W oknie obiektu (*Obiekty → zębatka*) **klucz wybiera się z listy katalogu** – nie można tam tworzyć nowych kluczy – a
**wartość** z dozwolonych wartości tego klucza (dowolny tekst tylko tam, gdzie klucz na to pozwala). Adapter stosuje
te same reguły podczas pracy: tagi z kluczami lub wartościami spoza katalogu (np. ze skryptów przez `enableHistory`
albo z migracji) nie są zapisywane, a w logu pojawia się ostrzeżenie.

Kolejność (od najsłabszej): **domyślne z katalogu → automatyczne → stanu → usunięte klucze → zastrzeżone** (`state_id`,
`time`, kolumna wartości, `ack`, `q`, `from`). Klucze są normalizowane przed scaleniem, więc `Sub-Type`
i `sub_type` to ten sam klucz; wygrywa wyższa warstwa, a w logu pojawia się ostrzeżenie.

Szablony (bez JavaScriptu i `eval`): `{id}`, `{id.segment[N]}` (ujemne N liczy od końca), `{id.adapter}`,
`{id.instance}`, `{common.unit}`, `{common.role}`, `{common.type}`, `{common.name}`, `{native.KLUCZ}` (tylko klucze
z listy dozwolonych), `{enum.room}`, `{enum.function}`, `{parent.device}`, `{parent.channel}` oraz – tylko po
włączeniu *tagów dynamicznych* – `{state:<id>}` (wartość innego stanu; obowiązuje TTL, dozwolone znaki i limit
liczby różnych wartości).

Ochrona: klucze/wartości wyglądające na hasła, tokeny, identyfikatory sesji lub adresy e-mail nie są zapisywane,
chyba że w stanie zaznaczono *zezwól na tagi wyglądające na sekrety*; każdy zmienny klucz tagu ma limit różnych
wartości (kolejne zapisywane jako `_other`). `from` domyślnie nie jest zapisywane; jako tag tworzyłoby osobną serię
dla każdego źródła.

## Model danych

Zob. [ADR-0001](../adr/0001-table-model-grouped-vs-per-state.md).

* **grupowy** (domyślny): tabele `numeric_states` (Float64), `boolean_states` (Boolean), `string_states` (Utf8)
  albo własne tabele przez *tabelę* stanu lub *reguły tabel* instancji. **Jeden typ wartości na tabelę** – stan,
  którego typ nie pasuje do tabeli, nie jest zapisywany i trafia do `influx-v3.N.info.conflicts`.
* **per-stan**: tabela na każdy stan (nazwa = ID stanu lub alias), jak w klasycznym adapterze. InfluxDB 3 Core
  pozwala na **2000 tabel na serwer** – adapter ostrzega przy 80 % i odmawia tabel ponad *maks. tabel*.

Liczby są zawsze zapisywane jako float; wartości logiczne jako boolean (lub 1/0 przy *zapisz jako liczba*);
obiekty/tablice jako tekst JSON (lub odrzucane); `null`, `NaN`, `Infinity` nigdy nie są zapisywane. Teksty dłuższe
niż *maks. długość tekstu* są odrzucane (limit serwera 1 MB).

**Retencja** jest właściwością bazy (Core: ustalana przy tworzeniu bazy). Nie ma retencji per stan. Wybierz
bazę o odpowiedniej retencji (każda instancja może używać innej).

## Niezawodność

Zob. [ADR-0002](../adr/0002-write-path-and-reliability.md). W skrócie:

* Ponawiane są tylko błędy przejściowe (sieć, timeouty, 408, 429 z `Retry-After`, 5xx) z wykładniczym opóźnieniem
  i losowym rozrzutem. Błędy walidacji (400/422) nigdy nie są ponawiane; częściowo przyjęte paczki są dzielone na
  zapisane i odrzucone rekordy (`info.rejectedRecords`, log z limitem). 401/403/404: dane są zachowywane,
  ponowienie po *authRetryMs*.
* Bufor **w pamięci**: dane w RAM, przy zatrzymaniu zapisywane do `iobroker-data/influx-v3.N/spool`, wysyłane po
  starcie. Bufor **dyskowy**: każda paczka trafia tam przed wysłaniem; przetrwa `kill -9` i zanik zasilania (z fsync).
* Dostarczanie **at-least-once** w granicach limitów; ponowne wysłanie jest idempotentne (InfluxDB 3 scala punkty
  o tej samej tabeli, tagach i czasie – zmierzone). **Nie ma gwarancji exactly-once.** Rekordy porzucone z powodu
  limitów są liczone w `info.droppedRecords`.

## API sendTo

```js
// historia (format jak w innych adapterach historii)
sendTo('influx-v3.0', 'getHistory', {
    id: 'heat.0.hc1.temperature',
    options: { start: Date.now() - 86400000, end: Date.now(), aggregate: 'average', count: 300, ack: true, q: true },
}, res => console.log(res.result, res.step, res.error));

// SQL tylko do odczytu (SELECT/WITH/SHOW/DESCRIBE/EXPLAIN, jedno polecenie, limity wierszy/bajtów/czasu)
sendTo('influx-v3.0', 'query', {
    query: 'SELECT time, value FROM numeric_states WHERE state_id = $id ORDER BY time DESC LIMIT 10',
    params: { id: 'heat.0.hc1.temperature' },
}, res => console.log(res.result, res.truncated, res.error));

sendTo('influx-v3.0', 'enableHistory', { id: 'heat.0.hc1.temperature', options: { customTags: [{ key: 'location', value: 'KOTLOWNIA' }] } }, cb);
sendTo('influx-v3.0', 'disableHistory', { id: 'heat.0.hc1.temperature' }, cb);
sendTo('influx-v3.0', 'getEnabledDPs', {}, cb);
```

Pozostałe polecenia: `queryInfluxQL`, `storeState` (import wartości z pominięciem filtrów), `flushBuffer`,
`getSchema`, `getAdapterStats`, `diagnostics`, `previewMapping`, `testConnection`, `createDatabase`; migracja
ustawień: `migrationPreview`, `migrationApply`, `migrationSuggestRules`, `migrationProposeStandard`; import danych
(korzysta z zapisanych ustawień `dm*`): `dmTestSource`, `dmPreview` (wynik także w `info.dataMigration.preview`),
`dmStart` (działa w tle, postęp w `info.dataMigration.*`), `dmStop` (kończy import do `dmStart`; restart adaptera go
tylko wstrzymuje), `dmReset`. Agregacje `getHistory`: `none`/`onchange`, `average`, `min`, `max`, `minmax`, `total`, `count`,
`percentile`, `quantile` (przybliżone). Stany tekstowe zwracane są bez agregacji; znacznik czasu agregatu to środek
przedziału. Nieobsługiwane: `integral`, `update`, `delete*` (SQL InfluxDB 3 nie ma DML).

Uprawnienia: `query`/`queryInfluxQL` można wyłączyć albo ograniczyć do wybranych instancji nadawców (zakładka
*Odczyty i historia*). Adapter nie otwiera żadnego portu sieciowego.

## Grafana

Zob. **[docs/GRAFANA.md](../GRAFANA.md)** – konfiguracja źródła danych i sprawdzone zapytania SQL, m.in.:

```sql
-- wszystkie temperatury dla location = 'KOTLOWNIA'
SELECT time, state_id, value FROM temperature
WHERE location = 'KOTLOWNIA' AND $__timeFilter(time) ORDER BY time;

-- device = 'PC' i subtype IN ('HC1','HC2','HC3')
SELECT time, subtype, value FROM temperature
WHERE device = 'PC' AND subtype IN ('HC1', 'HC2', 'HC3') AND $__timeFilter(time) ORDER BY time;

-- średnie 15-minutowe
SELECT date_bin(INTERVAL '15 minutes', time) AS time, subtype, avg(value) AS value
FROM temperature WHERE device = 'PC' AND $__timeFilter(time) GROUP BY 1, subtype ORDER BY 1, subtype;

-- klucze zapisane bez zmian (Location / Sub-Type / Device) wymagają cudzysłowów
SELECT time, "Sub-Type", value FROM temperature_tagged
WHERE "Location" = 'KOTLOWNIA' AND "Device" = 'PC' AND "Sub-Type" IN ('HC1', 'HC2') AND $__timeFilter(time);
```

## Migracja z ioBroker.influxdb

Dwa kroki, oba w ustawieniach instancji. Klasyczny adapter i jego baza nigdy nie są zmieniane – w okresie
przejściowym oba adaptery mogą działać równolegle. Projekt: [ADR-0006](../adr/0006-migration-tag-rules-and-history-import.md).

### 1. Migracja ustawień (z regułami tagów)

Wybierz instancję `influxdb.N`. Dla każdego stanu kopiowane są: włączenie, filtry, debounce/czas blokady,
zaokrąglanie, alias, typ zapisu i własne tagi. Własne tagi muszą pasować do [katalogu tagów](#tagi), dlatego są
tłumaczone **regułami tagów**.

Stare tagi są pobierane z istniejącej konfiguracji: *Stary klucz* podpowiada klucze własnych tagów z ustawień
`influxdb.N`, *Stare wartości* pokazuje wszystkie wartości tego klucza z liczbą stanów – w jednej regule można wybrać
**jedną lub kilka** wartości (brak = każda wartość). **Przejmij stare tagi** wypełnia tabelę automatycznie: jeden
wiersz dla każdego starego klucza bez odpowiednika w katalogu (wybierz nowy klucz), a dla kluczy z katalogu jeden
wiersz na każdą wartość katalogu zaproponowaną dla starych wartości (równą po pominięciu wielkości liter i znaków
diakrytycznych, np. `Kotłownia`, `kotlownia` → `KOTLOWNIA`); wartości bez propozycji dostają wiersz z pustą nową
wartością. Istniejące wiersze nigdy nie są zmieniane – po wybraniu nowych kluczy naciśnij ponownie, aby dostać wiersze
wartości tych kluczy.

**Zaproponuj standard (katalog + reguły)** idzie krok dalej i dopasowuje automatycznie, także przy pustym
katalogu. Obejmuje **każdy używany tag**: własne tagi z ustawień influxdb, tagi zapisane w starej bazie (opcja;
połączenie z zakładki *Migracja danych*) oraz tagi stanów tej instancji. Niekompletne wiersze (np. pozostawione przez
*Przejmij stare tagi*) są uzupełniane, klucze dopuszczające inne wartości dostają listę używanych wartości i również
łączone warianty pisowni. Tabela reguł dostaje **pełne mapowanie** pogrupowane według starego klucza: jeden wiersz na
każdy stary klucz i jeden wiersz na każdą wartość standardową ze wszystkimi jej starymi wariantami – także dla
wartości, które się nie zmieniają, więc każdy używany klucz i każda wartość są widoczne i edytowalne (klucze z
ponad 100 różnymi wartościami dostają wiersze tylko dla zmienianych wartości). Komunikat podaje, czy wszystkie
używane wartości są objęte; pełny wynik jest pokazany pod przyciskami (*Wynik ostatniej propozycji*): klucze o tym samym znaczeniu są łączone (`room`, `Raum`, `pomieszczenie` → `location`; wbudowane
synonimy EN/DE/PL/FR/IT/ES), wartości różniące się tylko pisownią lub językiem są łączone (`Kotłownia`, `kotlownia`,
`Heizungsraum` → `KOTLOWNIA`), pomieszczenia i funkcje ioBrokera służą jako wartości standardowe, a brakujące
klucze/wartości są dodawane do katalogu tagów. Liczby są porównywane dokładnie (`-1` pozostaje różne od `1`).
Pisownia nowych wartości: *automatycznie* (jak w katalogu; pusty katalog: `WIELKIE_LITERY`), `WIELKIE_LITERY`,
`małe_litery` albo *jak najczęściej używane*. Wypełniany jest tylko formularz – istniejące wiersze i Twoje reguły
pozostają; sprawdź zakładkę Tagi, reguły i podgląd, a potem zapisz. Podobieństwo służy tylko do tej propozycji,
nigdy w czasie działania. Z opcją *Zastosuj standaryzuje też tagi stanów już skonfigurowanych dla tej instancji*
(domyślnie włączona) *Zastosuj* przepisuje także takie tagi – tylko tam, gdzie reguły tłumaczą je na dozwolony tag z
katalogu; podgląd pokazuje te zmiany.

| Stary klucz | Stare wartości | Nowy klucz | Nowa wartość | Efekt |
| --- | --- | --- | --- | --- |
| `room` | *(brak)* | `Location` | *(puste)* | `room=Salon` → `Location=SALON` (pisownia dopasowana do katalogu) |
| `room` | `Kotłownia`, `Boiler room` | `Location` | `KOTLOWNIA` | kilka starych wartości → jedna wartość z katalogu |
| `source` | | `-` | | tag usunięty |
| `typ` | | *(puste)* | `heating` | klucz bez zmian, nowa wartość |

Reguła dla konkretnych starych wartości ma pierwszeństwo przed regułą dla każdej wartości. **Podgląd na żywo**
(aktualizowany podczas edycji reguł, jeszcze przed zapisem) pokazuje dla każdego stanu stare tagi, wynik oraz każdą
starą parę `klucz=wartość`, która nadal nie pasuje do katalogu, z liczbą stanów – dodaj regułę albo rozszerz katalog,
aż lista będzie pusta; na początku podglądu są wszystkie stare własne tagi (`room: Kotłownia (5), Salon (3), Garaż (1)`).
Zmiany katalogu podgląd uwzględnia jeszcze przed zapisem; *Zastosuj* wymaga ich zapisania. *Zastosuj* zapisuje tylko
tagi dozwolone przez katalog. Te same reguły tłumaczą tagi zapisane
w starych punktach podczas importu danych.

### 2. Migracja danych (import historii)

1. Wybierz instancję `influxdb.N` i naciśnij *Skopiuj ustawienia połączenia* (URL, wersja, baza/bucket,
   organizacja, użytkownik). Wpisz hasło (1.x) albo token do odczytu (2.x) – nie da się ich odczytać z innego
   adaptera. Są przechowywane w postaci zaszyfrowanej i nigdy nie trafiają do logów. **Zapisz.**
2. Opcjonalnie: ogranicz stany (`hm-rpc.0.*, javascript.0.*`) i okres.
3. *Testuj połączenie*, potem *Podgląd (próba bez zapisu)*: okres, który zostanie zaimportowany, ile stanów kończy
   się na znanej pierwszej wartości bieżącej, a dla pierwszych stanów stary pomiar (alias z ustawień `influxdb.N`,
   w przeciwnym razie identyfikator), tabela docelowa i przekonwertowane rekordy z ostatniej godziny; stare pomiary,
   które nie zostaną zaimportowane, są wymienione z przyczyną (filtr stanów, inna instancja influxdb, konflikt, brak
   stanu). Nic nie jest zapisywane.
4. *Start / kontynuuj*. Postęp (osiągnięta data, punkty z ok. N, punkty/s, szacowany pozostały czas), zaimportowane
   i odrzucone punkty widać na żywo (`info.dataMigration.*`). Po restarcie adaptera (aktualizacja, restart hosta)
   import kontynuuje sam; po *Stop* czeka na *Start*, który kontynuuje od miejsca zatrzymania. *Resetuj postęp*
   powoduje, że następny import zacznie się od początku. Plik postępu: `iobroker-data/influx-v3.N/data-migration.json`.

Które stany: skonfigurowane dla tej instancji **oraz** te, które zapisuje tylko `influxdb.N`. Te drugie są importowane
z tabelą i tagami, które nadałoby im *Zastosuj* w *Migracji ustawień* (te same reguły, zapisany katalog) – żadne
ustawienie nie jest zmieniane. Najlepsza kolejność: najpierw zastosuj migrację ustawień, potem importuj – wtedy każdy
stan kończy się dokładnie na swojej pierwszej wartości bieżącej. Jeśli zastosujesz ją po imporcie, okres pomiędzy
uzupełnisz ponownym importem (*Resetuj postęp* → *Start*).

Jak działa import (z myślą o wielu stanach i latach danych): przechodzi po osi czasu **raz** – każde okno czasowe
jest czytane dla wszystkich stanów jednym zapytaniem (adaptacyjnie: 1 minuta … 30 dni, ok. 40 000–100 000 punktów na
okno), a następne okno jest czytane, gdy bieżące jest zapisywane. Podczas importu InfluxDB 3 nie jest odpytywany, więc
jego rozmiar nie spowalnia importu. Zapisy idą dużymi żądaniami (domyślnie 50 000 punktów), po dwa naraz – InfluxDB 3
odpowiada na każdy zapis dopiero po zapisaniu dziennika WAL (mniej więcej raz na sekundę), więc o szybkości decyduje
rozmiar żądania, a nie ich liczba. Zbyt duże lub zbyt wolne odpowiedzi zmniejszają okno; przekroczenie czasu zapisu
zmniejsza żądania o połowę. Gdy któryś serwer jest niedostępny (np. restart), import czeka tak długo, jak trzeba,
i sam kontynuuje; po restarcie adaptera też kontynuuje sam (*Stop* kończy go do następnego *Start*). Przed startem
punkty są liczone, więc postęp pokazuje „z ok. N punktów” i pozostały czas.

Jak zapisywane są punkty: dokładnie tak jak dzisiejsze wartości bieżące stanu (tabela, tagi z katalogu, metadane,
zaokrąglanie, typ wartości; wartości, których nie da się przekonwertować, są liczone i pomijane). Domyślnie każdy stan
kończy się na pierwszej wartości bieżącej, którą ta instancja dla niego zapisała – adapter zapamiętuje ten czas
w swoim katalogu danych (`live-start.json`). Stany bez takiego wpisu kończą się w chwili startu importu; punkty
zapisane przez oba adaptery mają ten sam znacznik czasu, więc po prostu się nadpisują. Import zapisuje bezpośrednio
(nie przez bufor bieżących danych) i czeka, gdy bufor bieżących danych się zapełnia.

Ograniczenia: dynamiczne tagi `{state:…}` nie są odtwarzane dla przeszłości. InfluxDB 3 Core nie potrafi usuwać
pojedynczych punktów – sprawdź podgląd przed *Start*; drugi import z innymi regułami tagów tworzy dodatkowe serie (aby
go powtórzyć, usuń tabelę). **InfluxDB 3 Core i długie okresy**: Core zapisuje jeden plik Parquet na tabelę
i 10 minut i nigdy ich nie łączy; jedno zapytanie może odczytać najwyżej `--query-file-limit` plików (domyślnie
432 ≈ 3 dni). Aby odpytywać tygodnie lub lata zaimportowanej historii (np. w Grafanie), uruchom Core z wyższym
`--query-file-limit` (więcej pamięci na zapytanie) albo użyj InfluxDB 3 Enterprise, którego kompakcja łączy stare
pliki (darmowa licencja do użytku domowego).

## Rozwiązywanie problemów

| Objaw | Przyczyna / działanie |
| --- | --- |
| `info.connection` = false, w logu „authentication failed” | zły lub wygasły token; Core: `influxdb3 create token --admin` |
| „Adding a new database would exceed limit of 5 databases” | Core pozwala na 5 baz – użyj istniejącej |
| stan w `info.conflicts` | typ wartości różni się od typu tabeli → inna tabela albo *zapisz jako* |
| „table limit reached” | tryb per-stan w Core (2000 tabel) → przełącz na tryb grupowy |
| odrzucone rekordy `invalid column type` | kolumnę utworzono z innym typem (np. inne narzędzie) – użyj innej tabeli |
| pusta historia po zmianie tabeli/trybu | stare dane zostają w starej tabeli – odczytaj je bezpośrednio przez SQL |
| za reverse proxy | dodaj prefiks ścieżki do URL (`https://host/influx`) |
| proxy HTTP | nieobsługiwane w 0.1.0 |
| „Query would scan … Parquet files, exceeding the file limit” | InfluxDB 3 Core: zapytanie obejmuje więcej niż ok. 3 dni – zapytaj o krótszy okres, zwiększ `--query-file-limit` albo użyj Enterprise (zob. *Migracja danych* → Ograniczenia). Sam import już nie odpytuje InfluxDB 3 |
| import zatrzymał się z „timeout after 10000 ms” (wersje sprzed importu w przekrojach czasowych) | zaktualizuj; import nie pyta już InfluxDB 3 o pierwszą wartość każdego stanu |
| Tabela reguł z wieloma wierszami: *Stare wartości* ładują się wolno | używane tagi są odczytywane raz i współdzielone przez wszystkie komórki (stara baza: pamięć podręczna 10 min, komórka czeka na nią najwyżej 2 s); pierwsze otwarcie po starcie może pokazać tylko wartości z ustawień – otwórz zakładkę ponownie po chwili |

## Rozwój i testy

```bash
npm ci
npm run build                                      # build/ jest w repozytorium – commituj razem ze zmianami w src/
npm run lint && npm run check
npm run test:unit                                  # testy jednostkowe, bez sieci
eval "$(scripts/influx3-test-server.sh start)"     # izolowany InfluxDB 3 Core w Dockerze
eval "$(scripts/legacy-influx-test-server.sh start)"  # InfluxDB 1.8 + 2.7 jako źródła importu (opcjonalnie)
npm run test:influx                                # testy z prawdziwymi serwerami (awaria/replay, kill -9, import 1.x/2.x → 3)
npm run test:integration                           # js-controller (cykl życia; end-to-end przy ustawionych INFLUX3_*)
npx tsx scripts/loadtest.ts 100,1000,10000 20      # test obciążenia
npx tsx scripts/import-loadtest.ts 548 3 300       # test importu historii (opis w nagłówku skryptu)
scripts/influx3-test-server.sh stop && scripts/legacy-influx-test-server.sh stop
```

## Licencja

MIT – zob. [LICENSE](../../LICENSE).
