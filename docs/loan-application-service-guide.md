# CrediGuard Loan Application Service — Kompletny przewodnik techniczny

> **Wersja dokumentu:** 1.0
> **Zakres:** pełna analiza kodu źródłowego, architektury, maszyny stanów, Transactional Outbox, idempotencji, testów i konfiguracji serwisu `loan-application`.
> **Audytorium:** od laika (początkujący Pythonista) po seniora (architekt systemów).
> **Oryginalna specyfikacja:** `docs/SPECYFIKACJA.md` (maszyna stanów §4.3, topiki §5.1, koperta §5.2, outbox §5.3, idempotencja §5.4, modele danych §6.2–6.3, plan Etap 3 §11).
> **Serwisy siostrzane:** `docs/applicant-service-guide.md`, `docs/gateway-service-guide.md` — ten dokument zachowuje ich układ (13 sekcji), ale treść jest w 100% o Loan Application Service.

---

## Spis treści

1. [Wstęp — czym jest Loan Application Service](#1-wstęp--czym-jest-loan-application-service)
2. [Architektura warstwowa](#2-architektura-warstwowa)
3. [Struktura projektu](#3-struktura-projektu)
4. [Analiza plików źródłowych — linijka po linijce](#4-analiza-plików-źródłowych--linijka-po-linijce)
5. [Ścieżki wywołań endpointów i OutboxWorkera](#5-ścieżki-wywołań-endpointów-i-outboxworkera)
6. [Koncepcje techniczne — słowniki, dekoratory, wzorce](#6-koncepcje-techniczne--słowniki-dekoratory-wzorce)
7. [Bezpieczeństwo — dogłębna analiza](#7-bezpieczeństwo--dogłębna-analiza)
8. [Baza danych, SQLAlchemy i Transactional Outbox](#8-baza-danych-sqlalchemy-i-transactional-outbox)
9. [Testy — jednostkowe i integracyjne](#9-testy--jednostkowe-i-integracyjne)
10. [Konfiguracja i uruchamianie](#10-konfiguracja-i-uruchamianie)
11. [Kod źródłowy vs specyfikacja — luki i obszary do poprawy](#11-kod-źródłowy-vs-specyfikacja--luki-i-obszary-do-poprawy)
12. [Dodatki — przyszłe przepływy Kafka, diagramy, glosariusz, FAQ](#12-dodatki--przyszłe-przepływy-kafka-diagramy-glosariusz-faq)
13. [Podsumowanie — kluczowe decyzje architektoniczne](#13-podsumowanie--kluczowe-decyzje-architektoniczne)

---

## 1. Wstęp — czym jest Loan Application Service

### 1.1 Miejsce w systemie CrediGuard

CrediGuard to fintechowy system **MVP** zbudowany z mikroserwisów. Obsługuje proces wnioskowania o pożyczkę. W systemie występują między innymi:

- **Applicant Service** — serwis tożsamości: rejestracja, logowanie, JWT (RS256, klucz prywatny tylko u niego).
- **Loan Application Service** (omawiany w tym dokumencie) — **jedyny właściciel statusu wniosku pożyczkowego**. Tylko on zmienia statusy (maszyna stanów w `domain`). Port **8002**, baza `loan_db`.
- **Document Service** — upload i (mock-)weryfikacja dokumentu (port 8003).
- **AI Underwriting Engine** — velocity checks, hard rules, scoring ML (port 8004).
- **Disbursement Service** — wypłata przez Stripe test mode (port 8005).
- **Notification Service** — Kafka → Redis Pub/Sub dla SSE.
- **Gateway** — brama API (port 8000), weryfikuje JWT kluczem publicznym, wstrzykuje `X-User-ID`, proxyje do serwisów wewnętrznych.

> **Analogia z życia:** wyobraź sobie wydział kredytowy banku. Klient składa na okienku formularz (gateway przyjmuje pismo i sprawdza dowód). Ale **teczkę sprawy** prowadzi jeden konkretny urzędnik — Loan Application Service. Tylko on może przybić pieczątkę „zweryfikowano", „zatwierdzono", „wypłacono". Inni (weryfikator dokumentów, analityk ryzyka, kasjer) tylko **opiniują na kartkach** (zdarzenia Kafka), a urzędnik na ich podstawie przesuwa teczkę między przegródkami (stany). Gdyby każdy mógł sam przestawiać teczkę, zapanowałby chaos — stąd twarda reguła: *wszystkie statusy zmienia wyłącznie loan-application-service*.

### 1.2 Co dokładnie robi ten serwis?

Serwis implementuje **dwie operacje synchroniczne (HTTP)** i **jeden proces asynchroniczny (outbox worker)**:

| Operacja | Metoda HTTP | Ścieżka | Kod odpowiedzi | Opis |
|----------|-------------|---------|----------------|------|
| Złożenie wniosku | `POST` | `/api/v1/loans` | `202 Accepted` | Walidacja, guard „jeden aktywny wniosek", zapis + 2 zdarzenia outbox |
| Odczyt wniosku | `GET` | `/api/v1/loans/{loan_id}` | `200 OK` | Zwrot wniosku wyłącznie właścicielowi |
| Liveness | `GET` | `/health` | `200 OK` | „Serwis żyje" |
| Readiness | `GET` | `/ready` | `200 OK` | „Serwis gotowy" (obecnie stub) |

Dodatkowo w tle działa **OutboxWorker** (asyncio task): co 500 ms pobiera wiersze `PENDING` z `outbox_events` (`FOR UPDATE SKIP LOCKED`), publikuje je do Kafki (`acks=all`, `enable_idempotence=True`) i oznacza jako `SENT`.

Publikowane typy zdarzeń (klucz partycji = `aggregate_id` = `loan_id`):

| Typ zdarzenia | Topik (= typ) | Kiedy powstaje |
|---------------|---------------|----------------|
| `loan.application.submitted.v1` | `loan.application.submitted.v1` | Przy każdym `POST /loans` (fakt biznesowy) |
| `loan.status.changed.v1` | `loan.status.changed.v1` | Przy każdej zmianie statusu (dziś: `DRAFT → SUBMITTED`; w przyszłości: każda transicja) |

#### Przykłady request/response (happy path)

**Złożenie wniosku** (klientem jest gateway — nagłówki `X-User-ID` i `Idempotency-Key` przekazuje dalej):

```http
POST /api/v1/loans HTTP/1.1
X-User-ID: 550e8400-e29b-41d4-a716-446655440000
Idempotency-Key: 7c9e6679-7425-40de-944b-e07fc1f90ae7
Content-Type: application/json

{
  "amount": 15000,
  "term_months": 24,
  "monthly_income": 6000,
  "applicant_age": 30
}
```

```http
HTTP/1.1 202 Accepted
Content-Type: application/json

{
  "id": "9f8e7d6c-5b4a-4c3d-8e2f-1a2b3c4d5e6f",
  "applicant_id": "550e8400-e29b-41d4-a716-446655440000",
  "amount": "15000.00",
  "term_months": 24,
  "monthly_income": "6000.00",
  "applicant_age": 30,
  "status": "SUBMITTED",
  "decision_reasons": [],
  "created_at": "2026-09-11T10:00:00Z",
  "updated_at": "2026-09-11T10:00:00Z"
}
```

- Zwróć uwagę: `amount` wraca jako `"15000.00"` (string z groszami — Decimal przez JSON), `status` to string (StrEnum serializuje się jak tekst), `decision_reasons` puste (uzasadnienie pojawi się dopiero po Underwriting, w przyszłych transicjach). Odpowiedź nie zawiera `idempotency_key` (metadane transportu, nie atrybut sprawy — §4.4 Blok 3).

**Odczyt wniosku:**

```http
GET /api/v1/loans/9f8e7d6c-5b4a-4c3d-8e2f-1a2b3c4d5e6f HTTP/1.1
X-User-ID: 550e8400-e29b-41d4-a716-446655440000
```

```http
HTTP/1.1 200 OK
{ ...ten sam kształt co wyżej... }
```

**Błędy (przykłady ciał):**

```http
HTTP/1.1 422 Unprocessable Entity
{"detail": [{"type": "greater_than", "loc": ["body", "amount"],
             "msg": "Input should be greater than 0", "input": -500}]}
```

```http
HTTP/1.1 401 Unauthorized
{"detail": "Missing X-User-ID header"}
```

- 422 (Pydantic na granicy — use case nie powstał, baza nie dotknięta) vs 401 (strażnik tożsamości — §4.15 Blok 5) vs docelowe 404/409 (logika domenowa — dziś 500 przez brak handlerów, §11 luka #3). Trzy poziomy błędów, trzy różne miejsca wykrycia — patrz pełna tabela w §5.6.

#### Zdarzenia, które rodzi jeden POST (ślad w systemie)

Jeden `POST` zostawia po sobie **trzy trwałe ślady** (1 wiersz + 2 wiersze outbox → 2 wiadomości Kafka):

```jsonc
// outbox_events #1 → topik loan.application.submitted.v1
{"event_id": "...", "event_type": "loan.application.submitted.v1",
 "occurred_at": "...", "correlation_id": "...", "producer": "loan-application-service",
 "payload": {"loan_id": "...", "applicant_id": "...", "amount": "15000.00",
             "term_months": 24, "monthly_income": "6000.00", "applicant_age": 30}}

// outbox_events #2 → topik loan.status.changed.v1
{"event_id": "...", "event_type": "loan.status.changed.v1", ...,
 "payload": {"loan_id": "...", "applicant_id": "...",
             "old_status": "DRAFT", "new_status": "SUBMITTED", "decision_reasons": null}}
```

- Oba z tym samym kluczem partycji (`aggregate_id` = `loan.id`) — ta sama partycja, kolejność `submitted` → `status.changed` gwarantowana (§4.11 Blok 5). Konsument czytający partycję widzi najpierw fakt („wniosek złożony"), potem sygnał („status: SUBMITTED") — nigdy odwrotnie.

### 1.3 Dlaczego ten serwis jest ważny?

1. **Serce projektu (Etap 3 wg SPEC §11).** To tutaj materializuje się cała domena kredytowa: maszyna stanów, outbox, idempotencja. Bez niego nie ma o czym mówić w demo.
2. **Strażnik spójności.** Maszyna stanów egzekwowana w `domain` (nie w API, nie w bazie) — nielegalne przejście rzuca `InvalidStatusTransition`, zanim cokolwiek dotknie bazę.
3. **Lekcja niezawodności.** Transactional Outbox pokazuje różnicę między „działa na moim komputerze" a „nie gubi zdarzeń, gdy Kafka padnie": zapis wniosku i zdarzeń to **jedna transakcja SQL**; worker dowozi zdarzenia później (semantyka *at-least-once*, duplikaty łapią konsumenci).
4. **Wzorzec Hexagonal/Clean.** Porty (`LoanApplicationRepository`, `OutboxRepository`, `IdempotencyStore`) + adaptery (SQLAlchemy, Redis) + DI przez FastAPI `Depends` — ten sam szablon co Applicant Service, więc zespół kopiuje sprawdzony układ.
5. **Węzeł przyszłych przepływów.** Dziś pętla jest otwarta (konsumenci z Etapów 6–8 jeszcze nie istnieją), ale tabele `processed_events`, schematy `EVENT_SCHEMAS` i maszyna stanów są już przygotowane na ich podłączenie (patrz §12).

### 1.4 Czego ten serwis NIE robi

- **Nie weryfikuje dokumentów** — to rola Document Service (Etap 6). Loan App tylko przejdzie `SUBMITTED → DOC_VERIFICATION → UNDERWRITING` na podstawie zdarzeń.
- **Nie scoringuje** — to rola Underwriting Engine (Etap 7). Loan App tylko zapisze `decision_reasons` z eventu.
- **Nie wypłaca pieniędzy** — to rola Disbursement (Etap 8). Loan App tylko przejdzie `APPROVED → DISBURSING → DISBURSED`.
- **Nie czyta cudzych baz** (database-per-service). Nie zna haseł, nie dekoduje JWT — ufa nagłówkowi `X-User-ID` wstrzykniętemu przez gateway w zamkniętej sieci Docker.
- **Nie konsumuje jeszcze Kafki.** Tabela `processed_events` i port `ProcessedEventRepository` istnieją, ale żaden consumer ich nie używa — to celowy stan pośredni po Etapie 3.
- **Nie listuje wniosków** (`GET /loans` nie istnieje), mimo że repo ma `get_by_applicant_id` i DTO ma `LoanApplicationListResponse` — martwy kod na przyszłość.

---

## 2. Architektura warstwowa

### 2.1 Zasada zależności (ang. Dependency Rule)

Obowiązuje klasyczna **Clean / Hexagonal Architecture**, identyczna jak w Applicant Service (tam §2.1 formułuje ją tak samo — celowo kopiujemy brzmienie, by oba dokumenty mówiły jednym głosem):

> **Wewnętrzna warstwa NIE wie nic o warstwach zewnętrznych.**

W praktyce dla tego serwisu oznacza to (konkretnie, plik po pliku):

- Warstwa **domain** (`entities.py`, `value_objects.py`, `exceptions.py`) nie importuje NICZEGO spoza standardowej biblioteki Pythona (stdlib: `dataclasses`, `datetime`, `decimal`, `enum`, `typing`, `uuid`) ani własnych modułów domenowych. Nie wie, co to jest FastAPI, SQLAlchemy, Kafka, Redis, Pydantic, JWT. Sprawdź sam: `grep -r "^import\|^from" src/domain/` pokazuje tylko stdlib + `src.domain.*`. Gdyby pojawił się `sqlalchemy` — build-review pada.
- Warstwa **application** (`dto.py`, `ports/`, `use_cases/`) zależy tylko od **domain** i od **abstrakcji** (portów), które sama definiuje. Importuje Pydantic (DTO to granica walidacji — Pydantic jest tu „frameworkiem wejścia", nie domeną; kontrowersja opisana w applicant-guide: DTO w application to kompromis między czystością a ergonomią FastAPI). Nie wie, co to jest SQLAlchemy, `aiokafka`, `redis.asyncio`.
- Warstwa **infrastructure** (`database.py`, `persistence/`, `kafka/`, `redis/`) zależy od **application** (implementuje jej porty: `SQLAlchemyLoanApplicationRepository(LoanApplicationRepository)`) i od **domain** (mapuje encje: `_to_entity`). To tutaj żyje SQLAlchemy, `aiokafka`, `redis.asyncio`, `asyncpg`.
- Warstwa **api** (`routes/`, `dependencies.py`) zależy od **application** (wywołuje use case'y przez fabryki) i od **infrastructure** (konkretne repozytorium budowane w `dependencies.py` — composition root).

Kierunek zależności jest zawsze **od zewnątrz do wewnątrz**: `api → application → domain`, `infrastructure → application`. Nigdy odwrotnie! Dla laika: wyobraź cebulę — środek (domain) nie wie o skórce (frameworki), a skórka zna środek. Obierasz warstwę (np. wymieniasz Postgres na inną bazę) — środek nietknięty.

**Jedna rysa na szkle:** `src/infrastructure/kafka/producer.py` importuje modele SQLAlchemy (`OutboxEventModel`) bezpośrednio, zamiast przechodzić przez port `OutboxRepository`. Worker tworzy też własny engine w `poll_once`. To pragmatyczny skrót Etapu 3 (worker żyje poza cyklem request/response FastAPI, więc DI z `dependencies.py` go nie obejmuje), ale łamie czystość warstw: infrastructure gada z infrastructure z pominięciem portu. Uczciwie odnotowane w §11 (luka #13) z dwiema drogami naprawy (worker przez port albo jawny wyjątek w ADR). Reguła nie jest religią — wyjątek opisany i uzasadniony jest lepszy niż czystość udawana.

**Jak weryfikować regułę w CI (pomysł na przyszłość):** `import-linter` z kontraktem `domain → stdlib-only` (forbidden: `sqlalchemy`, `fastapi`, `pydantic`, `aiokafka`, `redis`). Dziś reguła jest „na słowo honoru + review"; automat ją przypilnuje, gdy zespół urośnie.

### 2.2 Diagram warstw (Mermaid)

```mermaid
flowchart TB
    subgraph API["Warstwa API (src/api)"]
        R["routes/loans.py<br/>POST /loans (202)<br/>GET /loans/{id} (200)"]
        D["dependencies.py<br/>get_current_applicant_id<br/>fabryki use case'ów<br/>init_redis / close_redis"]
    end

    subgraph APP["Warstwa Application (src/application)"]
        UC1["use_cases/create_loan_application.py"]
        UC2["use_cases/get_loan_application.py"]
        DTO["dto.py<br/>CreateLoanApplicationRequest<br/>LoanApplicationResponse"]
        P1["ports/repository.py<br/>LoanApplicationRepository<br/>OutboxRepository<br/>ProcessedEventRepository"]
        P2["ports/idempotency.py<br/>IdempotencyStore"]
    end

    subgraph DOM["Warstwa Domain (src/domain)"]
        E["entities.py<br/>LoanApplication<br/>OutboxEvent<br/>ProcessedEvent"]
        V["value_objects.py<br/>LoanStatus<br/>VALID_TRANSITIONS<br/>STATUS_PROGRESS"]
        X["exceptions.py<br/>NotFound / InvalidTransition<br/>DuplicateActive / IdempotencyUsed"]
    end

    subgraph INFRA["Warstwa Infrastructure (src/infrastructure)"]
        DB["database.py<br/>Database / get_session"]
        M["persistence/models.py<br/>LoanApplicationModel<br/>OutboxEventModel<br/>ProcessedEventModel"]
        RP["persistence/repository.py<br/>SQLAlchemy*Repository"]
        KW["kafka/producer.py<br/>OutboxWorker"]
        RS["redis/idempotency.py<br/>RedisIdempotencyStore"]
    end

    subgraph EXT["Świat zewnętrzny"]
        PG[("PostgreSQL<br/>loan_db")]
        RD[("Redis<br/>idempotency:*")]
        KF[["Kafka<br/>loan.application.submitted.v1<br/>loan.status.changed.v1"]]
        GW["Gateway<br/>X-User-ID"]
    end

    GW --> R
    R --> D
    D --> UC1
    D --> UC2
    UC1 --> DTO
    UC1 --> P1
    UC1 --> P2
    UC2 --> P1
    P1 --> E
    P1 --> V
    P1 --> X
    RP --> P1
    RS --> P2
    RP --> M
    M --> PG
    DB --> PG
    RS --> RD
    KW --> KF
    KW --> M
```

### 2.3 Dlaczego podział na warstwy? (cztery powody + jeden esej)

- **Testowalność.** Warstwę domeny i aplikacji można testować bez bazy danych, bez HTTP, bez sieci, bez Dockera. Wystarczy podmienić realne implementacje na mocki/faki: `FakeLoanRepo` (dict w pamięci) + `FakeIdempotencyStore` wystarczą, by pokryć guard aktywnego wniosku, replay idempotency i typy eventów (patrz `tests/unit/` — ~1 s, zero infrastruktury). Gdyby use case wołał SQLAlchemy wprost, każdy test wymagałby Postgresa (sekundy × setki testów = minuty; deweloper przestaje puszczać testy = testy umierają).
- **Niezależność od frameworków.** Jeśli za 2 lata zespół zamieni FastAPI na inny framework (np. Litestar), przepisuje tylko `api/` + `main.py`. Jeśli zamieni Postgres na CockroachDB — tylko `persistence/`. Jeśli Kafkę na NATS — tylko `kafka/`. Use case'y i encje nie drgną (nie importują niczego z tych technologii — kompilator tego pilnuje, nie tylko konwencja).
- **Zrozumiałość (SRP).** Każdy plik ma jedno zadanie (Single Responsibility Principle): `value_objects.py` — słownik stanów; `entities.py` — zachowania; `dto.py` — kontrakty; `create_loan_application.py` — choreografia; `repository.py` — SQL; `producer.py` — dowóz. Nowy deweloper szuka „gdzie zmienia się status" → `_transition` (jedno miejsce). Bez warstw: „gdzieś w `app.py` między routem a SQL" (polowanie).
- **Kontrola zmian (stabilność).** Zmiana w bazie (nowa kolumna) dotyka `models.py` + `repository.py` + ewentualnie DTO — nie logiki (`_transition` nie wie o kolumnach). Zmiana procesu (nowe przejście) dotyka `value_objects.py` + metodę encji + testy — nie SQL (tabela bez zmian). Mały blast radius = małe ryzyko = odważniejsze refaktory.
- **Wzorzec do kopiowania (skalowanie zespołu).** Każdy kolejny serwis (document, underwriting, disbursement) powiela ten szkielet: encja z transicjami → porty → use case → adaptery → DI → faki → testcontainers. Trzeci serwis pisze się 2× szybciej niż pierwszy (szablon w głowie), a review jest mechaniczne („czy port? czy fake? czy transicja przez `_transition`?"). To jest zwrot z inwestycji w architekturę: płacisz raz (Etap 1–3), zbierasz przy każdym kolejnym serwisie.

### 2.4 Porównanie z Applicant Service — tabela

| Aspekt | Applicant Service | Loan Application Service |
|--------|-------------------|--------------------------|
| Warstwy | domain / application / infrastructure / api | identyczne |
| Encje | `Applicant`, `RefreshToken` | `LoanApplication`, `OutboxEvent`, `ProcessedEvent` |
| Value objects | brak | `LoanStatus` (StrEnum) + `VALID_TRANSITIONS` + `STATUS_PROGRESS` |
| Use case'y | register / login / refresh / get_me | create_loan_application / get_loan_application |
| Porty repo | Applicant / RefreshToken | LoanApplication / Outbox / ProcessedEvent |
| Porty pomocnicze | PasswordHasher / TokenService | IdempotencyStore |
| Baza | `applicant_db` (2 tabele) | `loan_db` (3 tabele) |
| Redis | brak | tak (idempotencja HTTP) |
| Kafka | brak (tylko planowane `applicant.registered.v1`) | **producent** (outbox worker); konsumentów brak |
| JWT | podpisuje (prywatny) / dekoduje | brak — ufa `X-User-ID` |
| `POST` zwraca | `200` + tokeny | **`202 Accepted`** (async-first!) |
| Password hashing | Argon2id | brak |

Komentarz do kluczowych wierszy (reszta to oczywiste mapowanie 1:1):

- **Value objects „brak → są":** Applicant nie ma value objects (encje wystarczą: e-mail to string, daty to daty). Loan App ma całą maszynę stanów — bez `LoanStatus`/`VALID_TRANSITIONS` status byłby gołym stringiem obsługiwanym `if`-ami (rozjazd gwarantowany). Wniosek: value objects pojawiają się tam, gdzie domena ma **słownik pojęć** (stany, typy, kategorie), nie tylko dane.
- **Porty pomocnicze „kryptografia → idempotencja":** każdy serwis ma porty repo (dane), a drugi port odzwierciedla jego **specjalizację techniczną**: Applicant szyfruje i podpisuje (PasswordHasher, TokenService), Loan App gwarantuje „raz" (IdempotencyStore) i dowozi (OutboxRepository). Czytając porty, czytasz charakter serwisu.
- **Kafka „brak → producent":** Applicant nie dotyka brokera (zdarzenie `registered.v1` planowane, nie zaimplementowane). Loan App jest pierwszym serwisem z żywym kontaktem z Kafką (producent) — to tutaj zespół uczy się `acks`, kluczy partycji i kopert. Konsumenci przyjdą z Etapami 6–8 (i będą trudniejsi: retry, DLQ, `processed_events`).
- **JWT „podpisuje → ufa":** asymetria zaufania w pigułce (Applicant wie wszystko o tożsamości, Loan App nic — tylko `X-User-ID`). Gdyby Loan App dekodował JWT, potrzebowałby klucza publicznego + biblioteki + rotacji (duplikacja odpowiedzialności gateway). Jedno miejsce weryfikacji = jedno miejsce ataku = jedna obrona.
- **`POST` „200 → 202":** najważniejszy wiersz tabeli. Applicant odpowiada synchronicznie (rejestracja kończy się w requeście), Loan App — asynchronicznie (request to dopiero początek sprawy). Kod statusu uczy klienta protokołu: 200 = „skończone", 202 = „śledź przez SSE". Pomyłka (200 zamiast 202) to kłamstwo protokołowe, nie kosmetyka.

---

## 3. Struktura projektu

### 3.1 Pełne drzewo katalogów

```
services/loan-application/
├── Dockerfile                        # python:3.12-slim, EXPOSE 8002, uvicorn src.main:app
├── pyproject.toml                    # metadane, zależności, ruff/mypy/pytest config
├── alembic.ini                       # konfiguracja Alembic
├── alembic/
│   ├── env.py                        # async env (async_engine_from_config, NullPool)
│   └── versions/
│       └── 0001_initial.py           # 3 tabele + ix_outbox_pending
├── src/
│   ├── __init__.py
│   ├── main.py                       # FastAPI app, lifespan (DB+Redis+OutboxWorker), /health, /ready
│   ├── api/
│   │   ├── __init__.py
│   │   ├── dependencies.py           # X-User-ID, fabryki repo/use case, init/close Redis
│   │   └── routes/
│   │       ├── __init__.py
│   │       └── loans.py              # POST /loans (202), GET /loans/{id}
│   ├── domain/
│   │   ├── __init__.py
│   │   ├── entities.py               # LoanApplication + _transition, OutboxEvent, ProcessedEvent
│   │   ├── value_objects.py          # LoanStatus, VALID_TRANSITIONS, STATUS_PROGRESS
│   │   └── exceptions.py             # 4 wyjątki domenowe
│   ├── application/
│   │   ├── __init__.py
│   │   ├── dto.py                    # CreateLoanApplicationRequest, *Response, ListResponse
│   │   ├── ports/
│   │   │   ├── __init__.py
│   │   │   ├── repository.py         # 3 porty repozytoriów
│   │   │   └── idempotency.py        # port IdempotencyStore
│   │   └── use_cases/
│   │       ├── __init__.py
│   │       ├── create_loan_application.py
│   │       └── get_loan_application.py
│   └── infrastructure/
│       ├── __init__.py
│       ├── database.py               # Database, get_session, init/close_database
│       ├── persistence/
│       │   ├── __init__.py
│       │   ├── models.py             # 3 modele ORM
│       │   └── repository.py         # 3 adaptery SQLAlchemy
│       ├── kafka/
│       │   ├── __init__.py
│       │   └── producer.py           # schematy eventów + OutboxWorker
│       └── redis/
│           ├── __init__.py
│           └── idempotency.py        # RedisIdempotencyStore (SET NX EX)
└── tests/
    ├── __init__.py
    ├── unit/
    │   ├── __init__.py
    │   ├── domain/
    │   │   ├── __init__.py
    │   │   └── test_entities.py      # maszyna stanów, is_active, monthly_payment
    │   └── application/
    │       ├── __init__.py
    │       ├── test_create_loan.py   # sukces, duplikat, idempotencja, typy eventów
    │       └── test_get_loan.py      # sukces, not found, cudzy wniosek
    └── integration/
        ├── __init__.py
        ├── conftest.py               # testcontainers: Postgres 16 + Redis 7
        └── test_loan_flow.py         # create+get, outbox, idempotencja, guard, 404
```

### 3.2 Po co podział na `src/`? (układ src-layout)

Katalog `src/` (skrót od ang. *source*) odizolowuje kod wykonywalny od reszty projektu (testy, konfiguracja, migracje, Dockerfile). Konkretnie:

- **Instalacja editable:** `pip install -e .` (Dockerfile: `RUN pip install -e ".[dev]"`) instaluje pakiet tak, że `import src...` działa z dowolnego katalogu (testy wołane z root serwisu widzą `src` bez grzebania w `sys.path`). Bez `src/` (flat-layout: kod luzem w root) importy działałyby „przez przypadek" (katalog roboczy w `sys.path`) — test puszczony z innego katalogu padałby z `ModuleNotFoundError`. `src/` zamienia przypadek w gwarancję.
- **Jednoznaczna baza dla narzędzi:** `src = ["src"]` w `[tool.ruff]` (ruff wie, gdzie szukać importów pierwszego rzędu) + `explicit_package_bases = true` w mypy (jawne korzenie pakietów — krytyczne w monorepo, gdzie **siedem** serwisów ma własne `src/`! Bez jawnych baz mypy mógłby rozwiązać `src.domain` do cudzego serwisu).
- **Brak kolizji i importów-widm:** testy leżą poza `src/` (`tests/` obok, nie w środku) — `pytest` nie zbiera plików produkcyjnych jako testów, a `from tests...` nie działa w produkcji (dobrze — testy nie są zależnością runtime). Migracje (`alembic/`) i config (`alembic.ini`, `pyproject.toml`) też poza `src/` — obraz Dockera kopiuje je jawnie (`COPY src`, `COPY alembic`), nic „przy okazji".
- **Konwencja całego repo:** Applicant, gateway i ten serwis mają identyczny szkielet (`src/` + `tests/` + `alembic/` + `Dockerfile` + `pyproject.toml`, modyfikowany per rola — patrz §3.4). Nowy serwis = kopia szkieletu + usunięcie zbędnego. Narzędzia (pre-commit, CI) zakładają ten układ bez wyjątków.

### 3.3 Po co `__init__.py` w każdym katalogu?

Pliki `__init__.py` oznaczają, że dany katalog jest **pakietem Pythona** (jawne pakiety, nie namespace-packages). Dają hierarchiczne importy:

```python
from src.domain.entities import LoanApplication
from src.application.ports.repository import LoanApplicationRepository
```

Bez nich (przed Pythonem 3.3) importy by nie działały wcale; po 3.3 działałyby jako namespace-packages, ale projekt celowo trzyma jawne `__init__.py` (puste lub re-eksportujące) — struktura jest widoczna w drzewie plików, a mypy z `namespace_packages = true` + `explicit_package_bases = true` wymaga jawności (niejawne pakiety + monorepo z wieloma `src/` = ryzyko kolizji nazw między serwisami!).

### 3.4 Czego tu nie ma (i dlaczego) — porównanie z sąsiadami

| Element | Applicant | Gateway | Loan App | Dlaczego tak |
|---------|-----------|---------|----------|--------------|
| `keys/` (RSA) | ✅ (prywatny+publiczny) | ❌ (tylko mount publicznego) | ❌ | Ten serwis nie dotyka kryptografii (ufa `X-User-ID`); brak kluczy = brak sekretów do wycieku |
| `alembic/` | ✅ | ❌ (brak bazy!) | ✅ | Jest baza (`loan_db`, 3 tabele) → są migracje; gateway bazy nie ma |
| warstwa `domain/` | ✅ | ❌ (brak logiki biznesowej) | ✅ | Jest maszyna stanów → jest domena; gateway to „grube proxy" bez domeny |
| `src/services/` (transport) | ❌ | ✅ (proxy+token) | ❌ | Logika transportowa mieszka w adapterach (`kafka/`, `redis/`), nie w osobnej warstwie |
| `README.md` serwisu | ✅ | ❌ (luka!) | ❌ (luka — `pyproject` deklaruje `readme`, pliku brak) | Dług dokumentacyjny; ten przewodnik go spłaca z nawiązką |
| `src/application/security/` | ✅ (Argon2, JWT) | ❌ | ❌ | Brak sekretów do hashowania/podpisywania (patrz §7.6) |

- **Wniosek z tabeli:** kształt katalogu wynika z roli serwisu (baza? → alembic; kryptografia? → keys/security; domena? → domain). Szablon nie jest kopiowany ślepo — każdy serwis ma podzbiór szkieletu adekwatny do odpowiedzialności. Review-pytanie przy nowym serwisie: „które katalogi z szablonu są Ci potrzebne i dlaczego?" (odpowiedź „wszystkie na zapas" = over-engineering).

---

## 4. Analiza plików źródłowych — linijka po linijce

> Kolejność: od najbardziej wewnętrznej warstwy (domain) po zewnętrzną (api, main, migracje). Dla każdego pliku: cel, analiza blok po bloku, przykłady, konsekwencje usunięcia, alternatywy.

---

### 4.1 Plik: `src/domain/value_objects.py` (49 linii)

**Cel:** słownik maszyny stanów — enum statusów, tabela legalnych przejść, mapa status→krok SSE. Zero logiki, same dane. To ten plik analityk biznesowy czyta jako pierwszy. Plik nie importuje niczego poza `enum` ze standardowej biblioteki — spełnia twardą regułę projektu (domain → tylko stdlib).

#### Blok 1: docstring domyślny i importy

W odróżnieniu od Applicant Service ten plik **nie ma docstringa modułowego** (pierwsza linia to od razu import). To drobna niespójność stylistyczna — każdy inny moduł domenowy w repo ma `"""..."""` na górze. Przy `help()` czy w IDE brakuje jednozdaniowego opisu. Kandydat do dopisania: `"""Value objects: loan status machine and SSE progress map."""`.

```python
from __future__ import annotations
```

- **Linia 1:** `from __future__ import annotations` — włącza leniwe (opóźnione) ewaluowanie adnotacji typów (PEP 563). Wszystkie adnotacje w pliku (`dict[LoanStatus, set[LoanStatus]]`, `tuple[int, str]`) traktowane są jak ciągi znaków do momentu, gdy ktoś je faktycznie odczyta (mypy, `typing.get_type_hints`).
- **Po co tu konkretnie?** Adnotacja `VALID_TRANSITIONS: dict[LoanStatus, set[LoanStatus]]` odwołuje się do `LoanStatus` zdefiniowanego **poniżej w tym samym pliku** przy samej zmiennej — bez leniwych adnotacji taki „forward reference" w adnotacji zmiennej modułowej mógłby sprawiać problemy na starszych interpreterach. Z tą linią kolejność definicji przestaje mieć znaczenie.
- **Co by było, gdyby jej zabrakło?** W Pythonie 3.12 ten konkretny plik i tak by się wczytał (adnotacje zmiennych nie są ewaluowane w runtime poza `get_type_hints`), ale projekt trzyma tę linię w **każdym** pliku dla jednolitości i kompatybilności wstecz. Usunięcie z jednego pliku nie psuje nic dziś — psuje zasadę „wszędzie tak samo", a zasady całościowe są tańsze niż wyjątki.
- **Alternatywy:** (1) brak linii + cudzysłowowe adnotacje `"dict[LoanStatus, ...]"` — działa, ale brzydkie; (2) `from typing import Dict, Set` — styl pre-3.9, w tym repo zabroniony przez ruff (`UP` — pyupgrade wymaga natywnych generyków).

```python
from enum import StrEnum
```

- **Linia 3:** import `StrEnum` z modułu `enum` (stdlib). `StrEnum` istnieje od Pythona 3.11; projekt wymaga `>=3.12` (patrz `pyproject.toml`), więc użycie jest legalne i gwarantowane w każdym środowisku (dev, CI, Docker `python:3.12-slim`).
- **Czym `StrEnum` różni się od zwykłego `Enum`?** Członek `StrEnum` **jest** stringiem: `isinstance(LoanStatus.SUBMITTED, str)` → `True`, `LoanStatus.SUBMITTED == "SUBMITTED"` → `True`, `f"{LoanStatus.SUBMITTED}"` → `"SUBMITTED"` (nie `"LoanStatus.SUBMITTED"`!). Dla laika: to „etykieta, która zachowuje się jak zwykły tekst, ale edytor i mypy pilnują, byś nie literował jej z palca".
- **Przykład praktyczny:**
  ```python
  >>> LoanStatus.SUBMITTED == "SUBMITTED"
  True
  >>> f"status={LoanStatus.APPROVED}"
  'status=APPROVED'
  >>> ", ".join([LoanStatus.DRAFT, LoanStatus.SUBMITTED])
  'DRAFT, SUBMITTED'
  ```
  Trzeci przykład pokazuje siłę: lista statusów łączy się w string bez żadnej konwersji — bo to stringi.
- **Co by było bez `StrEnum` (zwykły `Enum`)?** `f"{Status.SUBMITTED}"` dałoby `"Status.SUBMITTED"`, JSON dostałby obiekt nieserializowalny, a SQLAlchemy nie zapisałoby wartości do `String(50)` bez jawnego `.value` w każdym miejscu. Każde zapomnienie `.value` = błąd w innym miejscu systemu. `StrEnum` usuwa całą klasę błędów.
- **Alternatywy:** (1) `class LoanStatus(str, Enum)` (mixin) — działa nawet na 3.9, ale `__str__` drukuje `LoanStatus.SUBMITTED`, a `__format__` (używany przez f-stringi!) też; trzeba by nadpisywać `__str__`, czyli dopisywać kod dla efektu, który `StrEnum` daje za darmo. (2) Luźne stałe `SUBMITTED = "SUBMITTED"` — zero kontroli typów, literówka `"SUBMITED"` przechodzi wszędzie. (3) `Literal["DRAFT", "SUBMITTED", ...]` — dobre dla mypy, ale bez iterowalności (`for s in LoanStatus` w `exists_active_for_applicant` byłoby niemożliwe) i bez `.value`.

#### Blok 2: enum `LoanStatus`

```python
class LoanStatus(StrEnum):
    DRAFT = "DRAFT"
```

- **Linia `DRAFT = "DRAFT"`:** stan roboczy encji tuż po konstruktorze. Konwencja „nazwa = identyczna wartość" (nie `DRAFT = "draft"` ani `"Draft"): wartość leci do bazy, eventów i JSON — ma być KRZYKLIWA i stabilna. Gdyby wartość była `"draft"`, każde porównanie z bazą (`status == "DRAFT"` w starym kodzie) by się rozjechało.
- `DRAFT` nigdy nie ląduje w bazie w obecnym flow (encja od razu woła `submit()`), ale istnieje jako jawny punkt startu: test `test_draft_to_submitted` i event `old_status: DRAFT` dokumentują pełną historię („był szkic, jest złożony").
- **Co gdyby usunąć DRAFT i startować od SUBMITTED?** Konstruktor tworzyłby od razu status „publiczny", a różnica między „obiekt w pamięci" a „wniosek złożony" zniknęłaby. Transicja DRAFT→SUBMITTED to granica „decyzji o złożeniu" — dziś trywialna, jutro może nieść walidację kompletności formularza.

```python
    SUBMITTED = "SUBMITTED"
    DOC_VERIFICATION = "DOC_VERIFICATION"
    UNDERWRITING = "UNDERWRITING"
```

- Trzy stany „w locie": wniosek przyjęty (1), dokument sprawdzany (2), ryzyko liczone (3). `SUBMITTED` to jedyny status, który serwis produkuje dziś sam; dwa następne czekają na konsumentów z Etapów 6–7 (patrz §12.2).
- Nazwy z podkreślnikami (nie myślnikami/spacjami), bo wartości lądują w nazwach topików pośrednio i w kluczach JSON — `DOC_VERIFICATION` jest bezpieczne wszędzie (URL, JSON, SQL, logi).

```python
    APPROVED = "APPROVED"
    DISBURSING = "DISBURSING"
    DISBURSED = "DISBURSED"
```

- Ścieżka sukcesu: decyzja (4) → wypłata w toku (5) → wypłacono (5-zamknięty). Rozróżnienie `DISBURSING` (stan przejściowy, „pieniądze w drodze") od `DISBURSED` (terminalny, „dotarły") jest krytyczne: retry webhooka Stripe w stanie DISBURSING jest legalny, w DISBURSED — to już redelivery do odrzucenia.
- **Uwaga nazewnicza:** `DISBURSING`, nie `DISBURSEMENT_IN_PROGRESS` — krótsze, spójne z `DISBURSED`/`DISBURSEMENT_FAILED`. Zmiana nazwy wartości = zmiana kontraktu (baza, eventy, frontend) — stąd enum zamiast luźnych stringów: zmiana w jednym miejscu + mypy wskaże wszystkie użycia.

```python
    DOC_REJECTED = "DOC_REJECTED"
    REJECTED = "REJECTED"
    DISBURSEMENT_FAILED = "DISBURSEMENT_FAILED"
```

- Trzy stany terminalne-porażki, każdy z innego etapu (dokument / ryzyko / wypłata). Rozróżnienie ma znaczenie biznesowe: `DOC_REJECTED` → „donieś lepszy skan", `REJECTED` → „uzasadnienie z ML", `DISBURSEMENT_FAILED` → „spróbuj ponownie / kontakt". Jeden wspólny `FAILED` zatarłby te ścieżki.
- Razem: **10 stanów = dokładnie maszyna ze SPEC §4.3.** Ani jednego więcej, ani mniej. Nadmiarowy stan (np. `CANCELLED`, którego nie ma w specu) kusiłby do „użycia przy okazji" i rozjechał kontrakt z frontendem.

#### Blok 3: `VALID_TRANSITIONS` — serce maszyny

```python
VALID_TRANSITIONS: dict[LoanStatus, set[LoanStatus]] = {
```

- Adnotacja mówi wszystko: klucz to status-źródło, wartość to **zbiór** dozwolonych celów. `set` (nie `list`), bo: (a) przynależność `in` na secie to O(1), (b) zbiór z definicji nie ma duplikatów — nie da się wpisać dwa razy tego samego przejścia, (c) semantycznie „dozwolone cele" to zbiór, nie kolejność.
- Wielkie litery (konwencja stałych modułowych): ten słownik budowany jest raz przy imporcie i **nigdy nie powinien być mutowany** w runtime. (Python tego nie wymusza — gdyby ktoś zrobił `VALID_TRANSITIONS[X].add(Y)`, maszyna by się „rozluźniła" globalnie. Obrona: code review + test `test_invalid_transition_raises`.)
- **Przykład odczytu:** `VALID_TRANSITIONS[LoanStatus.SUBMITTED]` → `{DOC_VERIFICATION, REJECTED}`. Dwa wyjścia: naprzód albo szybkie odrzucenie.

```python
    LoanStatus.DRAFT: {LoanStatus.SUBMITTED},
```

- Jedno wyjście ze szkicu: złożenie. Brak DRAFT→DRAFT (ponowne „złożenie szkicu" nie ma sensu) i brak DRAFT→czegokolwiek innego (nie da się zatwierdzić niezłożonego wniosku — test `test_invalid_transition_raises` sprawdza DRAFT→APPROVED).

```python
    LoanStatus.SUBMITTED: {LoanStatus.DOC_VERIFICATION, LoanStatus.REJECTED},
```

- Dwa wyjścia: standard (weryfikacja dokumentu) i **szybka ścieżka odrzucenia**. Po co odrzucenie już tu? Np. wykryty duplikat, wniosek testowy, wycofanie przez klienta — bez sensu pchać sprawę przez kosztowną weryfikację i scoring, skoro decyzja już zapadła. To jest „fail fast" na poziomie procesu.

```python
    LoanStatus.DOC_VERIFICATION: {
        LoanStatus.UNDERWRITING,
        LoanStatus.DOC_REJECTED,
    },
```

- Zapis wielolinijkowy (każdy cel w osobnej linii z przecinkiem na końcu) — gdy dojdzie trzecie wyjście (np. `DOC_RESUBMIT`), diff w gicie to jedna dodana linia. Formatowanie to też decyzja architektoniczna: ruff by to przepisał tak samo.
- Dwa wyjścia lustrzane wobec UNDERWRITING (dalej vs odrzucenie) — symetria celowa: każdy etap weryfikacyjny kończy się „idziemy dalej" albo „stop z powodem".

```python
    LoanStatus.UNDERWRITING: {LoanStatus.APPROVED, LoanStatus.REJECTED},
    LoanStatus.APPROVED: {LoanStatus.DISBURSING},
    LoanStatus.DISBURSING: {LoanStatus.DISBURSED, LoanStatus.DISBURSEMENT_FAILED},
```

- `APPROVED` ma **jedno** wyjście: wypłata. Brak APPROVED→REJECTED (decyzja jest ostateczna — „rozmyślenie się" po akceptacji to w MVP osobny proces, nie transicja; w banku też nie „odrzucasz" zaakceptowanego kredytu, tylko go nie wypłacasz / anulujesz umową).
- `DISBURSING` ma dwa wyjścia, bo świat zewnętrzny (Stripe, bank) może odmówić — `DISBURSEMENT_FAILED` to jedyny stan porażki, który nie wynika z decyzji merytorycznej, tylko technicznej.

```python
    LoanStatus.DISBURSED: set(),
    LoanStatus.DOC_REJECTED: set(),
    LoanStatus.REJECTED: set(),
    LoanStatus.DISBURSEMENT_FAILED: set(),
}
```

- Cztery terminale z pustym zbiorem. **Dlaczego jawne `set()`, a nie brak klucza?** Bo `_transition` robi `VALID_TRANSITIONS.get(self.status, set())`: przy braku klucza dostaje pusty zbiór (odmowa), przy kluczu z pustym zbiorem — też odmowę. Różnica jest w czytelności: jawny wpis mówi „ten stan istnieje i celowo nie ma wyjść", brak wpisu mówiłby „zapomnieliśmy?". Test `test_cannot_transition_from_terminal_status` iteruje po tych czterech i próbuje `submit()` — gdyby ktoś usunął wpis, test dalej przejdzie (dzięki `.get`), ale diagram (§12.1) rozjedzie się z kodem. Jawność wygrywa.
- **Co gdyby usunąć ten słownik i zahardkodować `if`-y w encji?** Dziewięć metod z rozproszonymi warunkami (`if self.status != ...: raise`), każda zmiana maszyny to polowanie po pliku + ryzyko, że dwie metody rozjadą się w definicji „terminalności". Tutaj diagram maszyny = dosłownie ten słownik; analityk biznesowy czyta go bez znajomości Pythona.

#### Blok 4: `STATUS_PROGRESS` i `TOTAL_STEPS` — język frontendu

```python
STATUS_PROGRESS: dict[LoanStatus, tuple[int, str]] = {
    LoanStatus.SUBMITTED: (1, "Wniosek przyjęty..."),
    LoanStatus.DOC_VERIFICATION: (2, "Weryfikacja dokumentu..."),
    LoanStatus.UNDERWRITING: (3, "Analiza ryzyka kredytowego..."),
```

- Krotka `(krok, komunikat)`: `tuple[int, str]` — kolejność ma znaczenie (najpierw numer, potem tekst), więc krotka, nie dict. Numer kroku dla timeline (kropki 1–5), komunikat jako `message` w SSE (kontrakt SPEC §5.6).
- Wielokropki w komunikatach „..." sygnalizują „trwa" (stany przejściowe), ich brak — „zakończone" (`APPROVED: "Wniosek zaakceptowany"`). Drobna konwencja UX zakodowana w domenie.
- Komunikaty po polsku już tutaj (nie na froncie): decyzja MVP — jeden język, zero warstwy i18n. Gdy dojdzie angielski, ta mapa stanie się `dict[LoanStatus, dict[str, ...]]` albo wyprowadzi się do frontendu; dziś prostota wygrywa.

```python
    LoanStatus.APPROVED: (4, "Wniosek zaakceptowany"),
    LoanStatus.DISBURSING: (5, "Wypłata środków..."),
    LoanStatus.DISBURSED: (5, "Wypłacono"),
    LoanStatus.DOC_REJECTED: (2, "Dokument zweryfikowany negatywnie"),
    LoanStatus.REJECTED: (3, "Wniosek odrzucony"),
    LoanStatus.DISBURSEMENT_FAILED: (5, "Błąd wypłaty"),
}
```

- Krok **dzielony** dla par sukces/porażka na tym samym etapie: odrzucenie dokumentu to wciąż krok 2, odrzucenie wniosku — krok 3, błąd wypłaty — krok 5. Timeline na froncie nie „cofa się" do zera przy porażce; pokazuje „doszedłeś do kroku N i tam padło". Gdyby porażki miały krok 0, pasek postępu by zniknął — gorszy UX.
- **Brak `DRAFT` — celowo.** DRAFT nigdy nie opuszcza procesu (brak eventu, brak SSE). Gdyby jakiś przyszły kod spróbował `STATUS_PROGRESS[LoanStatus.DRAFT]`, dostanie `KeyError` — głośny sygnał błędu programisty („nie wysyłaj szkicu do SSE"), nie cichą wadę danych. Alternatywa (wpis `DRAFT: (0, ...)`) zamaskowałaby błąd logiczny.

```python
TOTAL_STEPS = 5
```

- Stała modułowa (WIELKIE_LITERY), nie enum i nie część `LoanStatus` — bo to parametr **prezentacji**, nie domeny kredytowej. Frontend liczy `step/TOTAL_STEPS` do paska. Gdy dojdzie etap KYC, zmieniasz jedną liczbę (i tak — musisz też dodać statusy; stała nie zastępuje maszyny, tylko mianownik ułamka).
- Bez adnotacji typu (`: int`) — wartość literalna `5` jest samodokumentująca dla mypy (wywnioskuje `int`). Applicant-guide opisywałby tu różnicę między stałą modułową a polem klasy: modułowa żyje od importu, jest jedna na proces, importowana jako `from ... import TOTAL_STEPS`.

**Podsumowanie pliku:** 49 linii, zero logiki, trzy struktury danych. Mimo to to najważniejszy plik do review biznesowego — każda zmiana tutaj to zmiana procesu kredytowego. Reguła: PR dotykający `value_objects.py` wymaga akceptacji „biznesowej", nie tylko technicznej.

---

### 4.2 Plik: `src/domain/entities.py` (100 linii)

**Cel:** definiuje **czyste encje domenowe** (`LoanApplication`, `OutboxEvent`, `ProcessedEvent`). Encje przenoszą dane i zachowania (metody) domenowe. Ten plik NIE importuje niczego spoza standardowej biblioteki ani własnych modułów domenowych — spełnia twardą regułę projektu (domain → tylko stdlib + domain).

#### Blok 1: docstring domyślny i importy

Podobnie jak `value_objects.py`, plik nie ma docstringa modułowego — dwie pierwsze linie to importy. (Applicant-guide poświęca tu akapit każdemu importowi; robimy tak samo.)

```python
from __future__ import annotations
```

- **Linia 1:** leniwe adnotacje (PEP 563) — patrz §4.1, Blok 1. Tutaj konkretnie umożliwia zapis `str | None` i `datetime | None` w polach dataclassa oraz forward-referencje bez cudzysłowów. W Pythonie 3.12 zapis `X | None` działa i bez tej linii, ale jednolitość repo (każdy plik ją ma) jest warta więcej niż jedna linia.

```python
from dataclasses import dataclass, field
```

- **Linia 3:** import dekoratora `@dataclass` i funkcji `field()`. `@dataclass` każe Pythonowi wygenerować `__init__`, `__repr__`, `__eq__` na podstawie pól klasy — bez niego każda encja wymagałaby ~20 linii ręcznego konstruktora.
- `field()` służy do dwóch rzeczy: (a) `default_factory` — wartość liczona **przy każdym** tworzeniu instancji (UUID, listy, `now()`); (b) dokumentowanie, że default nie jest stały. Dla laika: `field(default_factory=uuid4)` znaczy „wywołaj `uuid4()` dla każdego nowego obiektu", a samo `= uuid4()` znaczyłoby „wywołaj raz przy definicji klasy i wklej wynik wszystkim" — klasyczny błąd początkujących.
- **Przykład błędu, którego unikamy:**
  ```python
  @dataclass
  class Zla:
      id: UUID = uuid4()          # JEDNO uuid dla wszystkich instancji!
  a, b = Zla(), Zla()
  assert a.id == b.id            # niestety True — katastrofa
  ```
- **Alternatywy:** (1) ręczny `__init__` — szum, łatwo zapomnieć pola w `__eq__`; (2) `NamedTuple` — niemutowalny, transicje wymagałyby `loan._replace(status=...)` i gubiłyby „zachowania" (metody transicji musiałyby żyć poza encją); (3) Pydantic `BaseModel` — walidacja za darmo, ale zależność zewnętrzna w domain (zakazana) + semantyka kopii przy walidacji.

```python
from datetime import UTC, datetime
```

- **Linia 4:** `datetime` (znaczniki czasu) i stała `UTC` (Python 3.11+). Applicant Service używał `timezone.utc` dla kompatybilności z 3.9 (`requires-python = ">=3.9"`); tutaj `requires-python = ">=3.12"`, więc nowocześniejsze `UTC` — krótsze, czytelniejsze, bez łańcucha atrybutów.
- **Aware vs naive:** `datetime.now(UTC)` zwraca datetime ze strefą (aware). Naiwne `datetime.now()` (bez strefy) mieszane z kolumnami `timestamptz` w Postgresie daje błędy porównań i ciche przesunięcia o strefę serwera. Reguła repo: **zawsze aware, zawsze UTC**.

```python
from decimal import Decimal
```

- **Linia 5:** `Decimal` — pieniądze. Dla laika: `float` przechowuje liczby w systemie binarnym i nie potrafi dokładnie zapisać np. `0.1` (klasyczne `0.1 + 0.2 == 0.30000000000000004`). W ratach kredytowych grosz różnicy × tysiące klientów = pozew. `Decimal("10000.50")` trzyma dokładnie to, co widać. Kolumny `Numeric(12, 2)` mapują się na `Decimal` 1:1 (SQLAlchemy robi konwersję automatycznie).
- **Co gdyby `float`?** Test `payment * n > amount` mógłby mrugać na granicach zaokrągleń; JSON serializowałby `15000.0` zamiast `"15000.00"`; eventy straciłyby precyzję. Całe repo (SPEC §6.2: `numeric(12,2)`) jest zaprojektowane pod Decimal.

```python
from typing import Any
```

- **Linia 6:** `Any` = „dowolny typ, nie sprawdzaj". Użyte tylko w `payload: dict[str, Any]` — payload outbox to dowolny JSON (stringi, liczby, listy, zagnieżdżenia). Sztywne typowanie (`dict[str, str]`) zabiłoby elastyczność eventów: `term_months` to int, `decision_reasons` to lista. mypy strict akceptuje `Any` tylko jawnie — tu jest jawnie i uzasadnione.
- **Alternatywa:** `payload: dict[str, str | int | list[str] | None]` — precyzyjniejsze, ale każdy nowy kształt eventu wymagałby edycji encji. Union byłaby wiecznie nieaktualna. `Any` wygrywa.

```python
from uuid import UUID, uuid4
```

- **Linia 7:** `UUID` (typ identyfikatorów) i `uuid4()` (losowy generator v4). `uuid4` jako `default_factory` — patrz wyżej. Wersja 4 (losowa), nie 1 (z MAC + czasem) — brak wycieku adresu serwera i czasu w ID; kolejność i tak zapewnia `created_at`.

```python
from src.domain.exceptions import InvalidStatusTransition
from src.domain.value_objects import VALID_TRANSITIONS, LoanStatus
```

- **Linie 9–10:** importy wewnątrz warstwy (domain → domain) — dozwolone. Encja potrzebuje tabeli przejść i wyjątku odmowy. Gdyby `VALID_TRANSITIONS` mieszkało w `entities.py`, powstałby cykl przy imporcie z testów value objects — rozdzielenie plików to też higiena importów.

#### Blok 2: klasa `LoanApplication` — pola

```python
@dataclass
class LoanApplication:
    applicant_id: UUID
    amount: Decimal
    term_months: int
    monthly_income: Decimal
    applicant_age: int
```

- **Pięć pól wymaganych** (bez defaultów — dataclass wymaga, by pola bez defaultów stały **przed** polami z defaultami; przestawienie da `TypeError: non-default argument follows default argument` już przy imporcie).
- **Kolejność nieprzypadkowa:** `applicant_id` pierwsze — tożsamość właściciela to najważniejszy atrybut agregatu (autoryzacja w `GET` porównuje właśnie to pole). Potem parametry wniosku: kwota, okres, dochód, wiek — w tej samej kolejności co DTO i payload eventu (łatwe mapowanie wzrokowe).
- **Brak walidacji w konstruktorze** (`amount` ujemne przejdzie!): waliduje Pydantic na granicy (DTO). Domena ufa, że dostała czyste dane — to świadomy podział (patrz Applicant: encja też nie waliduje e-maila). Gdyby ktoś skonstruował encję ręcznie z `amount=-5`, baza przyjmie (brak CHECK), a `monthly_payment` policzy ujemną ratę. Obrona istnieje tylko na API — luka odnotowana w §11 jako krawędź (testy jednostkowe też konstruują wprost, więc zła wartość w teście przejdzie cicho).

```python
    id: UUID = field(default_factory=uuid4)
    status: LoanStatus = LoanStatus.DRAFT
    decision_reasons: list[str] = field(default_factory=list)
    idempotency_key: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
```

- **Linia `id`:** losowane w Pythonie (nie `server_default` w bazie), bo ID musi być znane **przed** INSERT-em — eventy outbox w tej samej transakcji potrzebują `aggregate_id`. Gdyby ID losowała baza (`gen_random_uuid()`), use case musiałby najpierw `flush`, odczytać ID i dopiero budować eventy — dwa round-tripy więcej i trudniejszy test jednostkowy.
- **Linia `status = DRAFT`:** każda encja rodzi się szkicem; dopiero `submit()` ją „publikuje". Nie da się skonstruować encji od razu w SUBMITTED (trzeba by podmienić pole po konstrukcji — jawne, widoczne w review). To jest „pit of success": domyślna ścieżka jest poprawna.
- **Linia `decision_reasons`:** `field(default_factory=list)` — pusta lista, nie `None`. Różnica semantyczna: `[]` = „uzasadnienia jeszcze brak", kolumna JSON dostanie `[]` (a nie NULL). `_to_entity` przy odczycie robi `model.decision_reasons or []`, więc NULL ze starych wierszy też staje się `[]` — spójnie.
- **Pułapka mutowalnego defaultu (dla laika):** zapis `decision_reasons: list[str] = []` współdzieliłby JEDNĄ listę między wszystkimi wnioskami (default liczony raz przy definicji klasy). `reject(reasons=[...])` na jednym wniosku „zatrułoby" wszystkie nowe. `default_factory=list` woła `list()` na nowo dla każdej instancji. Ten sam motyw co `uuid4` wyżej.
- **Linia `idempotency_key = None`:** klucz HTTP (opcjonalny — wniosek bez nagłówka też legalny). Typ `str | None` (nie `Optional[str]` jak w Applicant — nowocześniej, bo py312). Trzymany w encji i w bazie (UNIQUE) jako druga linia obrony obok Redisa.
- **Linie `created_at` / `updated_at`:** `lambda: datetime.now(UTC)` — lambda, bo `default_factory` wymaga **wywoływalnego bezargumentowego**, a `datetime.now` wymaga argumentu strefy. `default_factory=datetime.now` dałoby czas naiwny (bez strefy) — stąd opakowanie. Gdyby ktoś napisał `= datetime.now(UTC)` (bez factory), wszystkie encje miałyby czas **importu modułu**, nie utworzenia — testy tworzenia dwóch wniosków „po kolei" miałyby identyczne znaczniki.

#### Blok 3: `_transition` — jedyne miejsce mutacji statusu

```python
    def _transition(self, new_status: LoanStatus) -> None:
        allowed = VALID_TRANSITIONS.get(self.status, set())
        if new_status not in allowed:
            raise InvalidStatusTransition(self.status.value, new_status.value)
        self.status = new_status
        self.updated_at = datetime.now(UTC)
```

- **Linia `def _transition`:** podkreślnik = „prywatne, nie wołaj z zewnątrz" (konwencja, nie wymuszenie — Python nie ma prawdziwej prywatności). Dziewięć publicznych metod to cienkie wrappery. Niezmiennik systemu: **status zmienia się wyłącznie przez `_transition`**. Grep po `\.status =` w repo powinien znaleźć tylko tę jedną linię — jeśli znajdziesz drugą, to błąd.
- **Linia `allowed = ...get(self.status, set())`:** `.get` z defaultem zamiast `[...]` — nieznany status (np. uszkodzony wiersz w bazie, przyszły stan bez wpisu) daje pusty zbiór = odmowa, nie `KeyError`. Obrona w głąb: błąd danych → czytelny `InvalidStatusTransition`, nie techniczny `KeyError`.
- **Linia `if new_status not in allowed`:** test przynależności do setu O(1). `new_status` to enum — porównanie enumów to porównanie tożsamości wartości, bez pułapek wielkości liter.
- **Linia `raise InvalidStatusTransition(self.status.value, ...)`:** `.value` (stringi `"DRAFT"`, `"APPROVED"`), nie enumy — komunikat jest serializowalny do JSON-logów i czytelny: `Invalid status transition: DRAFT -> APPROVED`. Gdyby przekazać enumy, `str()` dałby to samo (StrEnum!), ale `.value` jest jawne i niezależne od typu enuma.
- **Linia `self.updated_at = datetime.now(UTC)`:** każda transicja stempluje czas. Bez tego nie wiesz, czy wniosek „wisi w UNDERWRITING 5 minut czy 5 dni" — a to podstawa alertów (outbox lag, utknięte sprawy) i przyszłego SLA. Uwaga: `created_at` nigdy nie jest dotykane po konstrukcji (niezmiennik narodzin).
- **Przykład:**
  ```python
  loan = LoanApplication(applicant_id=..., amount=Decimal("10000"), term_months=12,
                         monthly_income=Decimal("5000"), applicant_age=30)
  loan.status            # LoanStatus.DRAFT
  loan.submit()          # OK
  loan.status            # LoanStatus.SUBMITTED
  loan.approve()         # InvalidStatusTransition: SUBMITTED -> APPROVED
  ```
- **Co gdyby usunąć sprawdzanie i przypisywać wprost?** Maszyna stanów istniałaby tylko na papierze (SPEC) — każdy bug w consumerze (np. `approve()` na DRAFT) zapisywałby nielegalny stan do bazy, a frontend pokazywałby „zaakceptowano" dla wniosku bez scoringu. Ten `if` to najtańsze ubezpieczenie w systemie.

#### Blok 4: dziewięć metod transicji

```python
    def submit(self) -> None:
        self._transition(LoanStatus.SUBMITTED)

    def start_doc_verification(self) -> None:
        self._transition(LoanStatus.DOC_VERIFICATION)

    def reject_doc(self) -> None:
        self._transition(LoanStatus.DOC_REJECTED)

    def start_underwriting(self) -> None:
        self._transition(LoanStatus.UNDERWRITING)

    def approve(self) -> None:
        self._transition(LoanStatus.APPROVED)
```

- Pięć pierwszych: jednolinijkowe delegacje. Nazwy czasownikowe w języku biznesu: `start_doc_verification` (nie `to_doc_verification`), `reject_doc` vs `reject` — rozróżnienie odrzucenia **dokumentu** od odrzucenia **wniosku** (inne stany docelowe: `DOC_REJECTED` vs `REJECTED`!). Gdyby istniała jedna metoda `reject()`, consumer z Document musiałby wiedzieć, który stan docelowy wybrać — logika procesu wyciekłaby z domeny do infrastruktury.
- Zwracają `None` (mutują `self`), nie kopię — encja jest mutowalna (dataclass bez `frozen=True`). Alternatywa immutable (`@dataclass(frozen=True)` + zwracanie nowej instancji) wymusiłaby przepisanie repo na merge i skomplikowała testy (`loan = loan.submit()`). Mutowalność to świadomy wybór MVP.

```python
    def reject(self, reasons: list[str] | None = None) -> None:
        self._transition(LoanStatus.REJECTED)
        if reasons:
            self.decision_reasons = reasons
```

- Jedyna metoda z argumentem. **Kolejność ma znaczenie:** najpierw transicja (może rzucić!), potem zapis powodów. Gdyby odwrócić, nieudana transicja zostawiłaby podmienione `decision_reasons` na obiekcie w pamięci — brudny stan mimo wyjątku.
- `if reasons:` (nie `if reasons is not None:`) — pusta lista też nie nadpisuje. Odrzucenie bez uzasadnienia nie czyści starego (obrona przed `reject([])` po wcześniejszym `reject(["DTI 62%"])`). Subtelne, ale przemyślane: powody to historia decyzji, nie pole do czyszczenia.
- **Przykład:** `loan.reject(reasons=["Wysoki DTI"])` → status REJECTED + powody; `loan.reject()` → status REJECTED, powody bez zmian.

```python
    def start_disbursing(self) -> None:
        self._transition(LoanStatus.DISBURSING)

    def complete_disbursement(self) -> None:
        self._transition(LoanStatus.DISBURSED)

    def fail_disbursement(self) -> None:
        self._transition(LoanStatus.DISBURSEMENT_FAILED)
```

- Trójka wypłatowa (Etap 8). `start_disbursing` oddzielone od `complete_disbursement`, bo między nimi żyje świat zewnętrzny (Stripe, webhook) — stan przejściowy DISBURSING to „czekam na bank". Bez niego nie dałoby się odróżnić „wypłata wysłana, czekam" od „wypłacona".
- **Dlaczego nie jedna metoda `transition_to(status)`?** Bo publiczne API encji ma mówić językiem biznesu. Literówkę w `aprove()` wyłapie linter/mypy (brak takiej metody); literówkę w `transition_to("APROVED")` — dopiero produkcja o 3 nad ranem. To jest esencja DDD: język wszechobecny (ubiquitous language) w kodzie.

#### Blok 5: properties `is_active` i `monthly_payment`

```python
    @property
    def is_active(self) -> bool:
        return self.status not in {
            LoanStatus.REJECTED,
            LoanStatus.DISBURSED,
            LoanStatus.DOC_REJECTED,
            LoanStatus.DISBURSEMENT_FAILED,
        }
```

- `@property` — dostęp bez nawiasów: `loan.is_active`, nie `loan.is_active()`. Dla laika: property udaje pole, ale liczy się przy każdym dostępie. Sygnalizuje „to cecha obiektu" (jak `full_name` w Applicant), nie „akcja".
- Zbiór terminali **zduplikowany** względem `VALID_TRANSITIONS` (puste sety) — świadoma denormalizacja dla czytelności. Alternatywa bez duplikacji: `not VALID_TRANSITIONS[self.status]` — krótsze, ale mniej jawne (pusty zbiór = terminalny to wiedza pośrednia). Cena duplikacji: dodając nowy stan terminalny musisz pamiętać o dwóch miejscach (test `test_is_active_*` pilnuje).
- Literał setu `{...}` budowany przy każdym dostępie — koszt pomijalny (4 elementy). Mikro-optymalizacja (stała modułowa) nie warta zachodu.

```python
    @property
    def monthly_payment(self) -> Decimal:
        if self.term_months == 0:
            return Decimal("0")
        monthly_rate = Decimal("0.01")  # 12% annual / 12 months
        if monthly_rate == 0:
            return self.amount / self.term_months
        factor = (1 + monthly_rate) ** self.term_months
        return self.amount * (monthly_rate * factor) / (factor - 1)
```

- **Linia guarda `term_months == 0`:** DTO wymaga ≥3, ale domena broni się sama (encja może powstać w teście z 0). Bez guarda: dzielenie przez zero w ostatniej linii. Zwraca `Decimal("0")` (nie `0` ani `0.0`) — typ wyniku zawsze Decimal.
- **Linia `monthly_rate = Decimal("0.01")`:** literał **stringowy** `"0.01"`, nie `Decimal(0.01)`! Dla laika: `0.01` jako float to w binarnym przybliżeniu `0.010000000000000000208...`; `Decimal(0.01)` zamroziłby ten błąd na zawsze. `Decimal("0.01")` parsuje tekst dziesiętnie — dokładnie jeden grosz na złotówkę. Komentarz `# 12% annual / 12 months` dokumentuje pochodzenie (SPEC §7.2).
- **Linia `if monthly_rate == 0`:** martwa przy stałej 0.01 — zostawiona na wypadek promocji 0% (wtedy wzór annuitetowy ma dzielenie przez zero: `factor - 1 = 0`, a rata to po prostu kapitał/okres). To jest „kod na przyszłość" z komentarzem-wyjaśnieniem w cenie jednego `if`.
- **Linie wzoru:** rata annuitetowa `R = K · (r(1+r)ⁿ / ((1+r)ⁿ − 1))`. `(1 + Decimal) ** int` — potęgowanie Decimal przez int jest dokładne (nie float!). Całe wyrażenie w Decimal — wynik dokładny co do grosza (z nadmiarem miejsc; zaokrąglenie do 2 miejsc robi warstwa prezentacji/Underwriting, nie domena).
- **Przykład:** `amount=12000, n=12, r=1%` → rata ≈ 1066.19, suma ≈ 12794 > 12000 (odsetki istnieją — to asercjonuje test).
- **Uwaga:** dziś nie wołane nigdzie w serwisie — zalążek pod DTI w Underwritingu (rata/dochód). Test jednostkowy liczy `payment * n > amount`. Ktoś kiedyś zapyta „po co to tu" — odpowiedź: §11.

#### Blok 6: `OutboxEvent`

```python
@dataclass
class OutboxEvent:
    aggregate_id: UUID
    event_type: str
    payload: dict[str, Any]
    id: UUID = field(default_factory=uuid4)
    status: str = "PENDING"
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    sent_at: datetime | None = None
```

- `aggregate_id` pierwsze — to oś całego outbox: ID agregatu = klucz partycji Kafka (porządek) + korelacja w logach. W tym serwisie zawsze `loan.id`.
- `event_type: str` (nie enum!) — typy eventów przyrastają z rozwojem (dziś 2, docelowo 5+); enum zamroziłby rejestr i wymuszał edycję domeny przy każdym nowym evencie. String + rejestr `EVENT_SCHEMAS` w infrastrukturze to luźniejsze sprzężenie. Cena: literówka w typie wyjdzie dopiero w workerze (warning + skip), nie w mypy.
- `status: str = "PENDING"` — string, nie enum: to stan **workera** (kolejka), nie domeny kredytowej. Dwie wartości: PENDING → SENT. Brak FAILED — nieudana publikacja po prostu zostaje PENDING (retry w następnym ticku).
- `sent_at = None` do publikacji — rozróżnienie „kiedy powstało" (`created_at`) od „kiedy dowiezione" (`sent_at`) pozwala mierzyć lag outbox (metryka `outbox_lag` z SPEC §9.3).
- **Brak metody `mark_sent()`** — status zmienia worker SQL-em (`UPDATE ... SET SENT`), nie przez encję. Niespójność stylistyczna: encja jest „głupim pojemnikiem" dla workera, a „mądrą" dla statusów wniosku. Uzasadnienie: worker operuje na modelach ORM, nie encjach (szybciej, batchowo). Odnotowane w §11.

#### Blok 7: `ProcessedEvent`

```python
@dataclass
class ProcessedEvent:
    event_id: UUID
    event_type: str
    aggregate_id: UUID
    id: UUID = field(default_factory=uuid4)
    processed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
```

- Rekord idempotencji konsumenta: „zdarzenie `event_id` już przetworzyliśmy — redelivery to no-op". **Kluczowe rozróżnienie dla laika:** `id` to PK wiersza (losowe, techniczne), `event_id` to ID zdarzenia z koperty (biznesowe, z UNIQUE w bazie). To `event_id` chroni przed duplikatem, nie `id`.
- Dziś nieużywana (brak konsumentów) — gotowa na Etapy 6–8. Jej istnienie już teraz wymusza kształt tabeli `processed_events` w migracji, więc przyszły consumer nie będzie wymagał migracji bazy — tylko kodu. To jest projektowanie „pod przyszłość" bez over-engineeringu: schemat gotowy, logika nie.
- `processed_at` bez parametru (zawsze „teraz") — w odróżnieniu od `is_valid(now)` w Applicant, tu nie ma potrzeby wstrzykiwania czasu (brak logiki „czy wygasło").

**Podsumowanie pliku:** 100 linii, trzy encje, cała mądrość domenowa serwisu. Testy jednostkowe (`test_entities.py`, 140 linii) są dłuższe niż implementacja — tak ma być: logika ma być trywialnie czytelna, a pewność mają dawać testy.

---

### 4.3 Plik: `src/domain/exceptions.py` (36 linii)

**Cel:** definiuje hierarchię **wyjątków domenowych**. Dzięki nim logika biznesowa sygnalizuje błędy w sposób niezależny od HTTP — warstwa application nie wie, co to `HTTPException` ani kody 404/409 (tak samo jak w Applicant Service: tam `ApplicantDomainError`, tu `LoanApplicationDomainError`). Mapowanie na HTTP należy do warstwy API — dziś go brak (patrz §11), więc wyjątki lecą do FastAPI jako 500.

#### Blok 1: nagłówek i klasa bazowa

```python
from __future__ import annotations

from uuid import UUID
```

- **Linia `from __future__ import annotations`:** jak w całym repo — leniwe adnotacje. Tutaj konkretnie adnotacje `loan_id: UUID`, `applicant_id: UUID` w konstruktorach. Bez linii też by działało (UUID zaimportowane wprost), ale jednolitość.
- **Linia `from uuid import UUID`:** tylko typ (nie `uuid4` — wyjątki niczego nie losują). `UUID` służy do typowania pól kontekstowych (`self.loan_id`, `self.applicant_id`). Stringowa wersja (`str`) byłaby słabsza: UUID gwarantuje format już na wejściu wyjątku.

```python
class LoanApplicationDomainError(Exception):
    """Base exception for Loan Application domain errors."""
```

- Dziedziczy po wbudowanym `Exception` (nie `ValueError`, nie `RuntimeError`) — własna gałąź hierarchii, by `except LoanApplicationDomainError` łapało **tylko** błędy tej domeny, nigdy np. cudzy `ValueError` z parsowania. Dla laika: to jak osobna szufladka na „nasze problemy" — sięgasz do niej bez ryzyka, że wyciągniesz coś obcego.
- Sam docstring, zero metod — klasa-znacznik. W przyszłości jeden handler w `main.py`:
  ```python
  @app.exception_handler(LoanApplicationDomainError)
  async def domain_error_handler(request, exc): ...
  ```
  zamieni całą hierarchię na HTTP (404/409/422). Dziś handler nie istnieje — FastAPI zwraca 500 z generycznym JSON. Testy (`pytest.raises`) i tak przechodzą, bo testują wyjątki wprost, nie przez HTTP.
- **Co gdyby każdy wyjątek dziedziczył wprost po `Exception`?** Brak wspólnego przodka = brak jednego handlera = cztery osobne `exception_handler` albo `except` z krotką. Jeden przodek to jeden punkt mapowania.

#### Blok 2: `LoanApplicationNotFound`

```python
class LoanApplicationNotFound(LoanApplicationDomainError):
    """Raised when loan application is not found."""

    def __init__(self, loan_id: UUID) -> None:
        self.loan_id = loan_id
        super().__init__(f"Loan application not found: {loan_id}")
```

- **Linia `def __init__(self, loan_id: UUID)`:** własny konstruktor przyjmuje `loan_id` (typ UUID — wywołujący musi dać poprawny identyfikator, nie dowolny string). Dla laika: `__init__` to „przepis na zbudowanie obiektu" — wołany automatycznie przy `LoanApplicationNotFound(loan_id)`.
- **Linia `self.loan_id = loan_id`:** zapis kontekstu jako atrybutu. Po co? Testy asercjonują `exc.value.loan_id == ...`, logi strukturalne mogą wysłać `loan_id` jako osobne pole JSON (wyszukiwanie po polu, nie parsowanie stringa). Bez atrybutu kontekst ginie w zdaniu.
- **Linia `super().__init__(f"...")`:** wywołanie konstruktora `Exception` z gotowym komunikatem. `super()` = „klasa nadrzędna". f-string wstawia UUID: `"Loan application not found: 550e8400-..."`.
- **Używany w DWÓCH sytuacjach:** (a) brak wiersza w bazie, (b) wniosek istnieje, ale należy do innego klienta (maskowanie — §7.2). Komunikat celowo nie zdradza, który przypadek zaszedł („not found" w obu). Gdyby komunikaty się różniły („not found" vs „forbidden"), atakujący enumerujący UUID odróżniłby cudze wnioski od nieistniejących.
- **Przykład:**
  ```python
  with pytest.raises(LoanApplicationNotFound) as exc_info:
      await use_case.execute(uuid4(), uuid4())
  ```
- **Alternatywa:** dwa wyjątki (`NotFound` + `AccessDenied`) — lepsze logi wewnętrzne, gorsze bezpieczeństwo (rozróżnienie wycieka przez kody HTTP, chyba że oba mapujesz na 404 — a wtedy po co dwa?).

#### Blok 3: `InvalidStatusTransition`

```python
class InvalidStatusTransition(LoanApplicationDomainError):
    """Raised when status transition is not allowed."""

    def __init__(self, from_status: str, to_status: str) -> None:
        self.from_status = from_status
        self.to_status = to_status
        super().__init__(
            f"Invalid status transition: {from_status} -> {to_status}"
        )
```

- Niesie **oba** statusy — jako `str` (nie `LoanStatus`), bo wyjątek ma być serializowalny i niezależny (log JSON, ewentualny transport). `_transition` przekazuje `.value` (stringi) — spójnie.
- Dwa atrybuty (`from_status`, `to_status`) zamiast jednego zdania — test asercjonuje je osobno:
  ```python
  assert exc_info.value.from_status == "DRAFT"
  assert exc_info.value.to_status == "APPROVED"
  ```
  Bez atrybutów test musiałby parsować string (`"DRAFT -> APPROVED" in str(exc)`) — kruche (zmiana formatowania zdania psuje test).
- Format `A -> B` (strzałka ASCII, nie `→` unicode) — bezpieczny w logach o dowolnym kodowaniu, grep-po-`->` działa wszędzie.
- **Gdzie łapany?** Nigdzie w serwisie (fail-fast: nielegalna transicja to błąd programisty/konsumenta, nie stan do naprawy). W przyszłości: mapowanie na 422 + monitoring (licznik `invalid_transitions_total` — każda taka to sygnał, że consumer woła złą metodę).

#### Blok 4: `DuplicateActiveApplication`

```python
class DuplicateActiveApplication(LoanApplicationDomainError):
    """Raised when applicant already has an active loan application."""

    def __init__(self, applicant_id: UUID) -> None:
        self.applicant_id = applicant_id
        super().__init__(
            f"Applicant {applicant_id} already has an active loan application"
        )
```

- Guard biznesowy „jeden aktywny wniosek na klienta" — polityka MVP przeciw spamowi, wyścigom o scoring i wielokrotnym wypłatom. Komunikat mówi wprost o przyczynie (to nie jest maskowanie jak przy 404 — klient widzi własne wnioski, ukrywanie nie ma sensu).
- Docelowo HTTP **409 Conflict** (konflikt stanu: „masz już aktywną sprawę"), nie 400 (to nie jest zły format) ani 422 (dane poprawne, stan systemu nie pozwala). Dziś: 500 przez brak handlera (§11, luka #3).
- **Dlaczego wyjątek, a nie zwrot `None`/`False`?** Bo use case ma sygnaturę `-> LoanApplicationResponse` — nie ma „pustej odpowiedzi" w kontrakcie. Wyjątek wymusza obsługę (nie da się go zignorować przez przypadek, w przeciwieństwie do `None`, które ktoś kiedyś odeśle jako 200 z pustym ciałem).

```python
class IdempotencyKeyAlreadyUsed(LoanApplicationDomainError):
    """Raised when idempotency key was already used."""

    def __init__(self, idempotency_key: str) -> None:
        self.idempotency_key = idempotency_key
        super().__init__(f"Idempotency key already used: {idempotency_key}")
```

- **Martwy wyjątek** — zdefiniowany, nigdzie nie rzucany. Use case zamiast rzucać robi **replay** (zwraca istniejący wniosek). Trzy hipotezy, dlaczego istnieje: (a) dokumentacja intencji („kiedyś rozważaliśmy twardy błąd"), (b) haczyk na przyszłą strategię „payload mismatch → 422" (ten sam klucz, inna treść = błąd klienta, nie replay), (c) kopia szablonu z innego serwisu. Tak czy inaczej: dziś nieużywany, testy go nie dotyką, mypy/ruff milczą (martwy kod publiczny nie jest flagowany).
- **Co z nim zrobić?** Albo usunąć (YAGNI), albo ożywić przy strategii „surowego" klucza (§11, luka #8). Zostawienie bez komentarza to najgorsza opcja — ktoś za pół roku będzie zgadywał jak Ty teraz. Ten akapit jest tym komentarzem.

**Podsumowanie pliku:** 36 linii, 5 klas, zero logiki. Hierarchia jest „słownikiem błędów" domeny — tak jak `value_objects.py` jest słownikiem stanów. Oba pliki czyta się bez uruchamiania czegokolwiek.

---

### 4.4 Plik: `src/application/dto.py` (35 linii)

**Cel:** definiuje **DTO** (Data Transfer Objects — obiekty transferu danych) wejścia i wyjścia use case'ów. DTO to „pudełka na dane", które podróżują między warstwami: request HTTP → DTO (walidacja Pydantic) → encja domenowa → odpowiedź DTO → JSON. Tu używamy Pydantic `BaseModel`, bo daje walidację, serializację i dokumentację OpenAPI w jednym (tak samo jak `src/application/dto.py` w Applicant Service — tamten plik opisany jest w applicant-guide §4.6 z identyczną filozofią).

> **Uwaga architektoniczna:** w Applicant istnieje też `src/api/schemas.py` z niemal identycznymi klasami (duplikacja DTO między warstwami). Tutaj tego problemu **nie ma** — jest tylko jeden plik DTO w `application`, a `routes/loans.py` importuje wprost z niego (`from src.application.dto import ...`). Czyściej: jeden kontrakt, zero dryfu między dwoma kopiami.

#### Blok 1: importy

```python
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, Field

from src.domain.value_objects import LoanStatus
```

- **Linia po linii:** `datetime` (znaczniki w odpowiedzi), `Decimal` (pieniądze — ten sam typ co w encji, brak konwersji), `UUID` (identyfikatory), `BaseModel` (baza Pydantic v2), `Field` (ograniczenia + opisy).
- **Import `LoanStatus` z domain do application** — dozwolony kierunek zależności (application → domain). Dzięki temu pole `status` w odpowiedzi ma typ enum, nie goły string: Pydantic zwaliduje i zserializuje.
- **Czego brak:** `EmailStr` (to Applicant; tu nie ma e-maili), `ConfigDict` (użyty dict `model_config`, patrz niżej).

#### Blok 2: `CreateLoanApplicationRequest` — kontrakt wejścia

```python
class CreateLoanApplicationRequest(BaseModel):
    """Request to create a new loan application."""

    amount: Decimal = Field(gt=0, le=200_000, description="Loan amount in PLN")
    term_months: int = Field(ge=3, le=60, description="Loan term in months")
    monthly_income: Decimal = Field(gt=0, description="Monthly income in PLN")
    applicant_age: int = Field(ge=18, le=75, description="Applicant age")
```

- **Linia `amount`:** `Decimal`, nie `float` — Pydantic sparsuje `"10000"`, `"10000.50"`, `10000` i `10000.0` do `Decimal` (ostatnie z uwagą: float wejściowy niesie błąd binarny, więc frontend powinien słać liczby/stringi, nie floaty). `gt=0` (ściśle większe — pożyczka 0 zł nie ma sensu; `ge=0` przepuściłoby zero), `le=200_000` (górny limit z SPEC §6.2).
- **Podkreślniki w `200_000`:** Python ignoruje `_` w literałach liczbowych (`200_000 == 200000`) — zapis dla oczu, nie dla maszyny. Różnica między `200000` a `200_000` to różnica między „literówką o zero" a „widzę od razu".
- **Luka dolnego limitu:** SPEC §6.2 mówi „1 000–200 000 PLN", a tu `gt=0` — pożyczka 1 zł przejdzie walidację. Świadome czy przeoczone? Underwriting i tak odrzuci absurdalny wniosek (hard rules), ale czysta walidacja powinna mieć `ge=1000`. Odnotowane w §11 (luka #9). Przykład odpowiedzi przy naruszeniu:
  ```json
  {"detail": [{"type": "greater_than", "loc": ["body", "amount"],
               "msg": "Input should be greater than 0", "input": -500}]}
  ```
  FastAPI zwraca **422 Unprocessable Entity** zanim use case w ogóle powstanie (dependencja `request: CreateLoanApplicationRequest` waliduje przy wejściu).
- **Linia `term_months` (`ge=3, le=60`):** lustro hard rules Underwritingu (SPEC §4.5: „okres poza 3–60 mies. → REJECTED"). Walidacja „wcześnie i tanio": wniosek z okresem 120 mies. odpada na API w milisekundy, nie po 5 sekundach pipeline'u ML. `ge` (większe-równe) zamiast `gt`, bo 3 i 60 to wartości legalne (granice inkluzywne).
- **Linia `monthly_income` (`gt=0`, bez górnego limitu):** słusznie bez sufitu — nie ma górnego limitu dochodu w biznesie (milioner też może wziąć pożyczkę). Zero i wartości ujemne odpadają (dochód 0 = brak zdolności z definicji).
- **Linia `applicant_age` (`ge=18, le=75`):** pełnoletność (18 — wymóg prawny zawierania umów) i 75 (polityka ryzyka, SPEC §4.5). Wniosek z wiekiem 15 lat odpada z 422 — nie trzeba angażować scoringu, by odmówić dziecku kredytu.
- **Linia `description=...`:** trafia do OpenAPI — Swagger UI (`/docs`) pokazuje opisy pól za darmo, a generowani klienci (frontend Zod!) mogą je przepisać 1:1. To jest most kontraktowy frontend↔backend ze SPEC §11 (Etap 5: „formularz RHF+Zod zmapowany 1:1 z Pydantic").
- **Co gdyby usunąć `Field` i zostawić gołe typy?** Walidacja biznesowa przeniosłaby się do use case'a (ręczne `if amount <= 0: raise ...`) albo — gorzej — do bazy (brak CHECK constraintów w migracji!). Każde nowe wejście (przyszły endpoint) musiałoby powtarzać sprawdzenia. `Field` centralizuje reguły w jednym miejscu.
- **Alternatywy:** (1) walidatory `@field_validator` — potrzebne dopiero przy regułach relacyjnych (np. „kwota ≤ 24× dochód" — dziś to hard rule Underwritingu, nie API); (2) `Annotated[Decimal, ...]` — równoważne stylistycznie, projekt wybrał `= Field(...)`.

#### Blok 3: `LoanApplicationResponse` — kontrakt wyjścia

```python
class LoanApplicationResponse(BaseModel):
    """Loan application response."""

    id: UUID
    applicant_id: UUID
    amount: Decimal
    term_months: int
    monthly_income: Decimal
    applicant_age: int
    status: LoanStatus
    decision_reasons: list[str] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
```

- **Linia po linii:** odpowiedź to lustro encji (te same 10 pól, minus `idempotency_key`). Klient dostaje pełny obraz sprawy: kto, ile, na jak długo, przy jakim dochodzie/wieku, w jakim stanie, z jakim uzasadnieniem, kiedy ruszona.
- **Linia `status: LoanStatus`:** Pydantic v2 rozumie `StrEnum` natywnie — serializuje do `"SUBMITTED"` (string), deserializuje z powrotem. Gdyby pole było `str`, literówka w bazie przeszłaby do klienta cicho; z enumem — błąd walidacji (fail-fast).
- **Linia `decision_reasons`:** `Field(default_factory=list)` (nie `= []` — ten sam motyw mutowalnego defaultu co w encji, §4.2). Pydantic v2 wymaga `Field(default_factory=...)` dla fabryk; zwykłe `= []` jest współdzielone (Pydantic robi kopię przy walidacji, ale konwencja factory jest bezpieczniejsza i jednolita z dataclass).
- **Brak `idempotency_key` w odpowiedzi — celowo.** Klucz to metadane transportu (jak koperta listu), nie atrybut wniosku (jak treść). Klient go wysłał, nie musi go dostawać z powrotem. Włączenie go do odpowiedzi sugerowałoby, że można go „edytować" — a klucz jest write-once.
- **Linia `model_config = {"from_attributes": True}`:** składnia Pydantic v2 (Applicant miał starą `class Config: from_attributes = True` ze składni v1!). Pozwala budować model **wprost z obiektu** (atrybuty), nie tylko z dict: `LoanApplicationResponse.model_validate(loan)` czyta `loan.id`, `loan.status`, ... Use case'y korzystają w obu miejscach (`create` i `get`). Bez tego trzeba by ręcznie przepisywać pole-po-polu (`id=loan.id, amount=loan.amount, ...` — 10 linii szumu na każde użycie + ryzyko pominięcia pola przy dodaniu nowego).
- **Przykład serializacji:**
  ```python
  >>> LoanApplicationResponse.model_validate(loan).model_dump_json()
  '{"id":"...","applicant_id":"...","amount":"15000.00","status":"SUBMITTED",...}'
  ```
  `Decimal` serializuje się do JSON jako string/liczba zależnie od konfiguracji — konsument (frontend) parsuje na swoją precyzję.

#### Blok 4: `LoanApplicationListResponse` — wrapper na przyszłość

```python
class LoanApplicationListResponse(BaseModel):
    """List of loan applications."""

    applications: list[LoanApplicationResponse]
```

- Wrapper `{applications: [...]}` zamiast gołej tablicy — konwencja API: jutro dodasz `total`, `page`, `limit` bez łamania kontraktu (goła tablica by to uniemożliwiła). Trzy linijki dziś oszczędzają wersjonowanie API jutro.
- **Martwy kod:** żaden endpoint go nie zwraca (brak `GET /loans`). Towarzyszy martwemu `get_by_applicant_id` w repo — razem czekają na endpoint listowania (dashboard „moje wnioski"). Para „port + DTO gotowe, endpoint nie" to typowy stan pośredni po Etapie 3.

**Podsumowanie pliku:** 35 linii, zero logiki, cała granica walidacji serwisu. Reguła: zmiana limitów biznesowych (np. 200 000 → 300 000) to jedna linia tutaj + aktualizacja SPEC — nie grzebanie w use case'ach.

---

### 4.5 Plik: `src/application/ports/repository.py` (42 linie)

**Cel:** definiuje **porty** (interfejsy) dla repozytoriów. Port to abstrakcyjny kontrakt mówiący: „cokolwiek będzie przechowywać dane, musi umieć te rzeczy". Konkretną implementację (adapter) dostarcza warstwa infrastruktury (`SQLAlchemy*Repository`). Dzięki temu use case'y zależą od **abstrakcji**, a nie od SQLAlchemy — dokładnie jak `ApplicantRepository` w applicant-guide §4.3 (tamten rozdział tłumaczy ABC i `@abstractmethod` od zera; tu skupiamy się na różnicach domenowych).

#### Blok 1: importy

```python
"""Repository ports for Loan Application Service."""

from __future__ import annotations

from abc import ABC, abstractmethod
from uuid import UUID

from src.domain.entities import LoanApplication, OutboxEvent, ProcessedEvent
```

- **Linia `from abc import ABC, abstractmethod`:** `ABC` (Abstract Base Class) — klasa bazowa typów abstrakcyjnych; `@abstractmethod` oznacza metodę bez implementacji. Klasy z metodami abstrakcyjnymi **nie da się instancjonować** (`TypeError: Can't instantiate abstract class...`) — trzeba ją zaimplementować w klasie pochodnej. Dla laika: to „formularz z pustymi polami" — adapter musi je wypełnić, inaczej Python nie pozwoli go użyć.
- **Import encji z domain** — dozwolony kierunek (application → domain). Porty mówią językiem encji (`LoanApplication`), nigdy modeli ORM — use case nie wie, że Postgres istnieje.
- **Brak `datetime`, `Optional`, `datetime`:** porty są minimalne — tylko to, co potrzebne w sygnaturach. Porównaj z Applicant (`get_all_valid(now: datetime)` tam; tu outbox nie filtruje po czasie, tylko po statusie).

#### Blok 2: port `LoanApplicationRepository`

```python
class LoanApplicationRepository(ABC):
    """Port for loan application persistence."""

    @abstractmethod
    async def save(self, loan: LoanApplication) -> None:
        """Save loan application."""

    @abstractmethod
    async def get_by_id(self, loan_id: UUID) -> LoanApplication | None:
        """Get loan application by ID."""

    @abstractmethod
    async def get_by_applicant_id(self, applicant_id: UUID) -> list[LoanApplication]:
        """Get all loan applications for applicant."""

    @abstractmethod
    async def exists_active_for_applicant(self, applicant_id: UUID) -> bool:
        """Check if applicant has an active loan application."""

    @abstractmethod
    async def get_by_idempotency_key(self, key: str) -> LoanApplication | None:
        """Get loan application by idempotency key."""
```

Metoda po metodzie:

1. **`save(loan) -> None`** — zapis (INSERT dziś; upsert jutro — §11 luka #2). Zwraca `None`, bo to operacja efektowa (side effect), nie zapytanie. `async`, bo I/O bazy. W adapterze: `session.add + flush` (bez commit — commit należy do `get_session`!).
2. **`get_by_id(loan_id) -> LoanApplication | None`** — `None` gdy brak (nie wyjątek!). Decyzję „brak = błąd" podejmuje use case (`raise LoanApplicationNotFound`), nie repo — repo jest głupie i szczere: „nie mam". Zapis `X | None` (nie `Optional[X]` jak w Applicant) — nowocześniej, py312.
3. **`get_by_applicant_id -> list[...]`** — **martwa dziś** (brak `GET /loans`), gotowa na dashboard. Pusta lista (nie `None`) gdy brak wniosków — konwencja „kolekcja zawsze istnieje, bywa pusta" (łatwiejsza dla klienta: `for` po pustej liście działa, po `None` — nie).
4. **`exists_active_for_applicant -> bool`** — guard „jeden aktywny wniosek". Zwraca **tylko bool**, nie encje: baza liczy po indeksie (`SELECT id ... WHERE applicant + status IN (...)`), nie ciągnie wierszy. Tańsze niż `get_by_applicant_id + filtrowanie w Pythonie` (które ściągnęłoby całą historię wniosków klienta dla odpowiedzi tak/nie). To jest portowa wersja zasady „licz w bazie, nie w pamięci".
5. **`get_by_idempotency_key -> ... | None`** — **nigdy nie wołane** (use case idzie przez Redis). Po co w porcie? Trzy hipotezy: (a) tryb awaryjny „bez Redisa" (replay z bazy po kolumnie UNIQUE), (b) audyt („który wniosek kryje się za tym kluczem?"), (c) przyszła strategia „twardy błąd przy mismatch payload". Implementacja istnieje w adapterze — port nie kłamie.
- **Dlaczego pięć metod, a nie generyczny CRUD?** Bo port opisuje **potrzeby use case'ów**, nie możliwości bazy. Każda metoda ma dokładnie jednego konsumenta (lub przyszłego konsumenta). Generyczne `find(criteria)` byłoby elastyczne, ale rozmyłoby kontrakt i utrudniło faki w testach.

#### Blok 3: porty outbox i processed events

```python
class OutboxRepository(ABC):
    """Port for outbox event persistence."""

    @abstractmethod
    async def save(self, event: OutboxEvent) -> None:
        """Save outbox event."""

    @abstractmethod
    async def get_pending(self, limit: int = 10) -> list[OutboxEvent]:
        """Get pending outbox events (for publishing)."""

    @abstractmethod
    async def mark_sent(self, event_id: UUID) -> None:
        """Mark outbox event as sent."""
```

- `save` — use case dokłada eventy w tej samej transakcji co wniosek (dwa wołania w `create`). `get_pending(limit=10)` — batch workerowi: limit chroni pamięć przy zaległościach (10, nie 10 000 — worker i tak tickuje co 500 ms; większy batch = dłuższa transakcja = dłuższe trzymanie locków `FOR UPDATE`). Default `= 10` w porcie, więc wołający może nie podawać.
- `mark_sent(event_id)` — tylko ID, nie cała encja (worker wie tylko „to wysłałem"). Niespójność z workerem: worker robi UPDATE z `sent_at`, a adapterowe `mark_sent` ustawia sam status (§11 luka #7) — port nie wspomina o `sent_at`, więc adapter jest „zgodny z portem, niezgodny z workerem".

```python
class ProcessedEventRepository(ABC):
    """Port for processed event tracking (consumer idempotency)."""

    @abstractmethod
    async def exists(self, event_id: UUID) -> bool:
        """Check if event was already processed."""

    @abstractmethod
    async def save(self, event: ProcessedEvent) -> None:
        """Save processed event record."""
```

- Lustro idempotencji konsumenckiej: `exists` (redelivery? skip) + `save` (zapamiętaj). Dziś bez konsumentów — port + adapter + tabela czekają. Gdy powstanie pierwszy consumer (Etap 6), jego use case dostanie ten port w konstruktorze i nie dotknie SQL — cała bepaaldność idempotencji będzie w teście z fakiem (dict), nie w Dockrze.
- **Pułapka check-then-act (dla seniora):** `exists → save` w dwóch zapytaniach ma wyścig (dwa consumery równolegle: oba `exists=False`, oba `save` → jeden dostaje `IntegrityError` na UNIQUE). Poprawka docelowa: polegać na UNIQUE + łapać `IntegrityError` jako „już przetworzone" (patrz §12.2 checklist).

**Analiza wzorca Repository (po polsku, jak w applicant-guide):** repozytorium udaje „kolekcję encji w pamięci" (jak lista), a naprawdę trzyma dane w bazie. Analogia: kartoteka w bibliotece — mówisz „znajdź mi książkę o tym tytule", nie interesuje Cię regał. Use case mówi `repo.get_by_id(...)` i nie wie, czy dane leżą w Postgresie, SQLite czy diccie w teście.

---

### 4.6 Plik: `src/application/ports/idempotency.py` (11 linii)

**Cel:** port dla magazynu kluczy idempotencji. Najmniejszy plik z logiką kontraktową w systemie — dwie metody, zero implementacji. Cała idempotencja HTTP (SPEC §5.4) w pigułce.

```python
"""Idempotency store port for Loan Application Service."""

from __future__ import annotations

from abc import ABC, abstractmethod


class IdempotencyStore(ABC):
    """Port for idempotency key storage."""

    @abstractmethod
    async def acquire(self, key: str, value: str, ttl_seconds: int = 86400) -> bool:
        """Acquire idempotency key. Returns True if acquired, False if already exists."""

    @abstractmethod
    async def get(self, key: str) -> str | None:
        """Get value stored under idempotency key, or None."""
```

- **Linia `acquire`:** „zajmij klucz, jeśli wolny". Semantyka jak zamek: `True` = zająłem (klucz był wolny), `False` = zajęty (ktoś był pierwszy). `key: str` (nagłówek HTTP), `value: str` (`loan.id` jako string — Redis trzyma stringi, nie UUID), `ttl_seconds: int = 86400` (24 h — formularz otwarty wczoraj nie blokuje dziś; default w porcie, więc use case woła `acquire(key, value)` bez TTL).
- **Linia `get`:** odczyt do replay (`str | None` — `None` = „pierwszy raz widzimy ten klucz"). Wartość to `loan.id` (string); use case parsuje `UUID(...)` i dociąga wiersz.
- **Dlaczego dwie metody zamiast jednej `get_or_create`?** Bo use case robi: `get` (replay? zwróć) → ...praca... → `acquire` (zapamiętaj). Jedna metoda „daj-albo-stwórz" wymusiłaby tworzenie wartości przed pracą (klucz-widmo przy crashu) albo skomplikowany callback. Rozdzielenie pozwala wpiąć retry i różne strategie po obu stronach.
- **Dlaczego `bool` z `acquire`, skoro use case ignoruje wynik?** Bo port jest uczciwy (mówi, co się stało), a use case dziś nie potrzebuje (guard bazy rozstrzyga wyścigi). Jutro ktoś może użyć `False` do metryki `idempotency_conflicts_total` — port już gotowy.
- **Implementacje:** Redis (`SET NX EX` — §4.12), dict w testach (trzy klasy Fake w trzech plikach testowych — każda minimalna, każda lokalna; współdzielony Fake byłby DRY, ale lokalny jest czytelniejszy w małym teście).
- **Czemu port, a nie konkretny Redis w use case?** Ta sama odpowiedź co `PasswordHasher` w Applicant: wymiana magazynu (Redis → Postgres `idempotency_keys` → Dynamo) nie dotyka logiki. Use case testujesz bez Dockera. To jest Odwrócenie Zależności (Dependency Inversion) w najczystszej postaci: moduł wysokopoziomowy (use case) definiuje kontrakt, niskopoziomowy (Redis) go spełnia.

---

### 4.7 Plik: `src/application/use_cases/create_loan_application.py` (90 linii)

**Cel:** implementacja **use case'a** „Złożenie wniosku" — serce serwisu. Use case to pojedyncza operacja biznesowa wykonywana przez użytkownika. Tutaj: replay idempotencji → guard aktywnego wniosku → encja + `submit()` → zapis + 2 eventy outbox → `acquire` → odpowiedź 202. Kolejność kroków jest kontraktem (zamiana replay z guardem łamie idempotencję — patrz Krok 2).

#### Blok 1: importy

```python
"""Create loan application use case."""

from __future__ import annotations

from uuid import UUID

from src.application.dto import CreateLoanApplicationRequest, LoanApplicationResponse
from src.application.ports.idempotency import IdempotencyStore
from src.application.ports.repository import (
    LoanApplicationRepository,
    OutboxRepository,
)
from src.domain.entities import LoanApplication, OutboxEvent
from src.domain.exceptions import DuplicateActiveApplication
from src.domain.value_objects import LoanStatus
```

- Use case zależy od **DTO**, **portów** (abstrakcji!) i **encji domenowych**. Nie importuje niczego z `infrastructure` ani `api` — logika testowalna bez bazy, Redisa i frameworka. To jest granica, której pilnuje mypy i review: import `sqlalchemy` w tym pliku = błąd architektoniczny.
- Import `LoanStatus` tylko dla `LoanStatus.DRAFT.value` w payloadzie status-eventu (jedno użycie). `UUID` tylko do parsowania wartości z Redisa w replay.
- **Czego brak:** `ProcessedEventRepository` (to use case zapisu, nie konsument), `IdempotencyKeyAlreadyUsed` (strategia replay zamiast błędu — §4.3).

#### Blok 2: konstruktor (wstrzykiwanie zależności — DI)

```python
class CreateLoanApplicationUseCase:
    """Use case for creating a loan application."""

    def __init__(
        self,
        loan_repo: LoanApplicationRepository,
        outbox_repo: OutboxRepository,
        idempotency_store: IdempotencyStore,
    ) -> None:
        self._loan_repo = loan_repo
        self._outbox_repo = outbox_repo
        self._idempotency_store = idempotency_store
```

- **Constructor Injection** (wstrzykiwanie przez konstruktor) — najczystsza forma DI, identyczna jak `RegisterUseCase` w Applicant (tam cztery zależności, tu trzy — brak hashera/tokenów, bo ten serwis nie zna haseł ani JWT).
- Atrybuty prywatne (`self._...`) — „proszę nie dotykać z zewnątrz". Testy i tak dotykają (`loan_repo.loans`, `outbox_repo.events`, `idempotency.store` w fakach — ale to atrybuty faków, nie use case'a).
- **Co gdyby use case sam tworzył zależności** (`SQLAlchemyLoanApplicationRepository(session)` w środku)? Silne sprzężenie z infrastrukturą: każdy test łączyłby się z bazą, wymiana Redisa wymagałaby edycji logiki, a dwa use case'y mogłyby dostać różne instancje store'a (rozjazd replay). Dlatego zależności przychodzą z zewnątrz (z `dependencies.py`).

#### Blok 3: `execute` — sygnatura i Krok 1 (replay)

```python
    async def execute(
        self,
        applicant_id: UUID,
        request: CreateLoanApplicationRequest,
        idempotency_key: str | None = None,
    ) -> LoanApplicationResponse:
        """Execute loan application creation."""
        if idempotency_key:
            existing_id = await self._idempotency_store.get(idempotency_key)
            if existing_id is not None:
                existing_loan = await self._loan_repo.get_by_id(UUID(existing_id))
                if existing_loan is not None:
                    return LoanApplicationResponse.model_validate(existing_loan)
```

- **Sygnatura:** `applicant_id` (z `X-User-ID`, nie z body — klient nie deklaruje, kim jest!), `request` (zwalidowane DTO), `idempotency_key` (opcjonalny nagłówek; `None` = „nie chcę idempotencji, zawsze nowy wniosek"). Zwraca `LoanApplicationResponse` (nie encję! — granica warstw: na zewnątrz tylko DTO).
- **Linia `if idempotency_key:`** — pusty string i `None` traktowane jak brak (falsy). Bez klucza cały blok pomijany: wniosek zawsze nowy (nawet duplikat treści — toScattergun bez klucza to wola klienta; guard aktywnego i tak złapie drugi wniosek tego samego klienta).
- **Linia `existing_id = await ...get(...)`:** punkt asynchroniczny (I/O Redis). `existing_id` to string (`loan.id`) albo `None`.
- **Linia `if existing_id is not None:`** — `is not None`, nie truthiness: pusty string z Redisa (patologia) poszedłby dalej i wysypał się na `UUID("")` z `ValueError`. Świadoma ostrożność? Połowa: `""` przejdzie (`"" is not None` → True) i rzuci `ValueError` → 500. Krawędź opisana w §11 (własny Redis = zaufane źródło; walidacja wartości kosztowałaby gałąź na coś, co nie występuje).
- **Linia `existing_loan = await ...get_by_id(UUID(existing_id))`:** parsowanie stringa z Redisa do UUID + dociągnięcie wiersza. Dwa poziomy magazynu (Redis: klucz→ID; baza: ID→wniosek) — Redis nie trzyma całego wniosku (oszczędność pamięci, jedno źródło prawdy w bazie).
- **Podwójny `if` (ID, potem wiersz):** chroni przed „Redis ma klucz, bazy już nie" (odtworzona baza, ręczny DELETE, inny environment z tym samym Redisem). Zamiast zwracać widmo (crash na `None`), spada do normalnego flow i tworzy wniosek na nowo. To jest graceful degradation w czterech linijkach.
- **Return w środku (early return):** znaleziony wniosek → natychmiastowy zwrot TEJ SAMEJ odpowiedzi (ten sam `id`, ten sam `status`). Uwaga: zwracany jest **aktualny** stan wiersza (np. wniosek mógł już przejść do UNDERWRITING przez consumera!), nie migawka z momentu utworzenia. To poprawne: klient pyta „jaki jest stan mojej sprawy", nie „co odpowiedziałeś 5 minut temu".
- **Przykład:**
  ```python
  r1 = await uc.execute(aid, req, "klucz-1")   # tworzy, id=X
  r2 = await uc.execute(aid, req, "klucz-1")   # replay, id=X, brak INSERT-ów
  assert r1.id == r2.id
  ```

#### Blok 4: Krok 2 — guard aktywnego wniosku

```python
        if await self._loan_repo.exists_active_for_applicant(applicant_id):
            raise DuplicateActiveApplication(applicant_id)
```

- Jedna linia, cała polityka „jeden aktywny wniosek". `await` (I/O bazy), bool, wyjątek z kontekstem (`applicant_id` w środku — §4.3).
- **Kolejność PO replay — krytyczna.** Gdyby guard był przed replay, powtórzone żądanie z kluczem (wniosek już istnieje = aktywny!) dostałoby `DuplicateActiveApplication` zamiast swojej odpowiedzi. To jest najczęstszy błąd implementacji idempotencji („najpierw sprawdź, potem odtwórz") — tutaj poprawnie: „najpierw odtwórz, potem sprawdź".
- **Co gdyby usunąć guarda?** Klient mógłby mieć 5 równoległych wniosków: 5 scoringów ML, 5 wypłat, chaos w `GET /loans/{id}` („który jest mój?"). Guard to nie fanaberia — to warunek, by maszyna stanów per klient była w ogóle sensowna.

#### Blok 5: Krok 3 — encja i `submit()`

```python
        loan = LoanApplication(
            applicant_id=applicant_id,
            amount=request.amount,
            term_months=request.term_months,
            monthly_income=request.monthly_income,
            applicant_age=request.applicant_age,
            idempotency_key=idempotency_key,
        )
        loan.submit()
```

- **Mapowanie DTO → encja pole-po-polu.** `Decimal` przechodzą 1:1 (ten sam typ po obu stronach — brak konwersji, brak utraty precyzji). `id`, `status`, `created_at` generują się same (default_factory). `idempotency_key` ląduje w encji (i w bazie jako UNIQUE — druga linia obrony).
- **Linia `loan.submit()`:** jawny akt złożenia — DRAFT → SUBMITTED przez `_transition` (może rzucić tylko przy błędzie programisty, bo DRAFT ma jedno legalne wyjście). Wniosek **nigdy nie czeka** w DRAFT (brak „szkiców roboczych" w MVP — formularz frontendu trzyma szkic lokalnie, backend widzi tylko złożone).
- **Dlaczego encja powstaje przed zapisem (a nie model ORM od razu)?** DTO to walidowane wejście (granica), encja to obiekt z regułami (`submit()` egzekwuje maszynę), model ORM to szczegół infrastruktury. Konwersja DTO→encja→model rozdziela poziomy: błąd maszyny wychodzi przed dotknięciem bazy.

#### Blok 6: Krok 4 — zapis + dwa eventy (jedna transakcja)

```python
        await self._loan_repo.save(loan)

        event_payload = {
            "loan_id": str(loan.id),
            "applicant_id": str(loan.applicant_id),
            "amount": str(loan.amount),
            "term_months": loan.term_months,
            "monthly_income": str(loan.monthly_income),
            "applicant_age": loan.applicant_age,
        }

        outbox_event = OutboxEvent(
            aggregate_id=loan.id,
            event_type="loan.application.submitted.v1",
            payload=event_payload,
        )
        await self._outbox_repo.save(outbox_event)
```

- **Linia `save(loan)`:** `INSERT` + `flush` (bez commit — commit robi `get_session` na końcu requestu). Od tego momentu `loan.id` istnieje w transakcji i eventy mogą się do niego odwoływać.
- **Payload `submitted.v1`:** pełne dane wniosku (konsument Document nie musi pytać bazy Loan App — event jest samowystarczalny; to jest „fat event", nie „thin notification with callback"). UUID i Decimal jako `str` (JSON nie zna ani UUID, ani Decimal — string jest dokładny; float straciłby grosze). `term_months`/`applicant_age` jako int (JSON zna inty).
- **Typ eventu jako literał stringowy** `"loan.application.submitted.v1"` (nie stała!). Dwa użycia w kodzie (tu i w `EVENT_SCHEMAS` workera) — literówka rozjechałaby produkcję z konsumpcją (worker: „unknown event type, skipping"). Kandydat na współdzieloną stałą w `libs/events` (patrz §11). Sufiks `.v1` = jawne wersjonowanie kontraktu (SPEC §5.1).

```python
        status_event_payload = {
            "loan_id": str(loan.id),
            "applicant_id": str(loan.applicant_id),
            "old_status": LoanStatus.DRAFT.value,
            "new_status": loan.status.value,
            "decision_reasons": None,
        }
        status_outbox_event = OutboxEvent(
            aggregate_id=loan.id,
            event_type="loan.status.changed.v1",
            payload=status_event_payload,
        )
        await self._outbox_repo.save(status_outbox_event)
```

- **Drugi event: `status.changed.v1`.** `old_status: DRAFT` (jawnie, nie „poprzedni z bazy" — encja wie, skąd przyszła), `new_status` z encji (po `submit()` = SUBMITTED), `decision_reasons: None` (brak uzasadnienia na starcie — klucz obecny z `None`, by schemat był stabilny: konsument nie musi obsługiwać „brak klucza" vs „null").
- **Dlaczego dwa eventy, nie jeden?** `submitted` to fakt biznesowy (konsument: Document Service), `status.changed` to sygnał obserwowalności (konsument: Notification/SSE). Rozdzielenie pozwala subskrybować tylko to, co potrzebne, i rozszerzać sygnały (każda przyszła transicja dokłada `status.changed`) bez zmiany faktów. Cena: 2× INSERT (grosze).
- **`aggregate_id=loan.id` w obu:** ten sam UUID = ta sama partycja Kafka = kolejność `submitted` przed `status.changed` gwarantowana per partycja. Gdyby klucze się różniły, SSE mogłoby dostać „status SUBMITTED" przed „wniosek złożony" — nonsens na timeline.
- **Wszystko w jednej transakcji** (commit w `get_session`): albo wniosek + oba eventy, albo nic. Crash po `save(loan)`, przed eventami = rollback = brak wniosku-widma bez eventów. To jest cały Transactional Outbox w pigułce (szerzej: §8.3).

#### Blok 7: Krok 5 — `acquire` i odpowiedź

```python
        if idempotency_key:
            await self._idempotency_store.acquire(
                idempotency_key, str(loan.id)
            )

        return LoanApplicationResponse.model_validate(loan)
```

- **`acquire PO zapisie — krytyczne.** Klucz w Redis pojawia się dopiero, gdy wniosek na pewno istnieje (commit jeszcze przed nami, ale `flush` przeszedł, constrainty sprawdzone). Gdyby `acquire` był przed `save`, crash pomiędzy zostawiłby klucz-widmo → replay znajdowałby ID bez wiersza → spadałby do tworzenia (drugi `if` ratuje), ale przy pechu (wiersz z innego wniosku?) — bałagan. Porządek „najpierw prawda (baza), potem cache (Redis)" to reguła systemów rozproszonych.
- **Wynik `acquire` ignorowany:** wyścig dwóch równoległych requestów z tym samym kluczem (oba przeszły `get` → None, oba tworzą) rozstrzyga baza: UNIQUE na `idempotency_key` (drugi INSERT rzuci `IntegrityError` → rollback → klient retryuje → replay znajduje pierwszy) albo guard aktywnego wniosku. At-least-once na poziomie HTTP zbiega do jednego wniosku — dokładnie tak jak at-least-once na Kafce zbiega przez `processed_events`.
- **Return `model_validate(loan)`:** encja → DTO wprost z atrybutów (`from_attributes`). Status 202 dopina warstwa API, nie use case (use case nie zna HTTP — czysty).
- **Co gdyby `acquire` rzucił (Redis padł)?** Wyjątek → rollback transakcji → wniosek nie powstaje, klient dostaje 500 i powtarza. Bezpieczne (brak wniosku-widma), ale kruche dostępnościowo: padnięty Redis = brak nowych wniosków mimo zdrowej bazy. Opcja: try/except wokół `acquire` z logiem (degradacja do „baza-only", replay przez `get_by_idempotency_key` — który dziś jest martwy, ale właśnie do tego!). Odnotowane w §11.

**Podsumowanie use case'a:** 90 linii, 5 kroków, zero `if`-ów biznesowych poza guardem i replay. Cała „mądrość" mieszka w encji (transicje) i DTO (limity) — use case jest choreografem, nie decydentem. Taki ma być: orkiestracja, nie logika.

---

### 4.8 Plik: `src/application/use_cases/get_loan_application.py` (22 linie)

**Cel:** use case „Odczyt wniosku" — weryfikacja właściciela i zwrot sprawy. Najkrótszy use case w systemie (porównaj z `get_me.py` w Applicant: tam też ~20 linii, też jedna zależność, też maskowanie niepowodzeń). Dwie reguły bezpieczeństwa w cenie kilkunastu linii.

#### Blok 1: importy i konstruktor

```python
"""Get loan application use case."""

from __future__ import annotations

from uuid import UUID

from src.application.dto import LoanApplicationResponse
from src.application.ports.repository import LoanApplicationRepository
from src.domain.exceptions import LoanApplicationNotFound
```

- Jedyny import domenowy to wyjątek (nie encja! — use case nie konstruuje encji, tylko czyta). DTO tylko wyjściowe (brak request-DTO — parametry to dwa UUID z URL i nagłówka).
- Konstruktor z **jedną** zależnością (`loan_repo`) — odczyt nie potrzebuje outbox (nic nie zmienia) ani idempotencji (GET jest idempotentny z natury: N odczytów = 1 odczyt). Minimalizm zależności to też dokumentacja: „ten use case niczego nie mutuje".

```python
class GetLoanApplicationUseCase:
    """Use case for retrieving a loan application."""

    def __init__(self, loan_repo: LoanApplicationRepository) -> None:
        self._loan_repo = loan_repo
```

#### Blok 2: `execute` — trzy linie logiki

```python
    async def execute(
        self, loan_id: UUID, applicant_id: UUID
    ) -> LoanApplicationResponse:
        """Execute loan retrieval."""
        loan = await self._loan_repo.get_by_id(loan_id)
        if loan is None:
            raise LoanApplicationNotFound(loan_id)
        if loan.applicant_id != applicant_id:
            raise LoanApplicationNotFound(loan_id)
        return LoanApplicationResponse.model_validate(loan)
```

- **Linia `loan = await ...get_by_id(loan_id)`:** jedyny punkt I/O. `None` = brak wiersza (repo nie rzuca — patrz §4.5).
- **Linia `if loan is None: raise ...`:** brak → 404 (docelowo; dziś 500 przez brak handlera). Standard.
- **Linia `if loan.applicant_id != applicant_id: raise ... (ten sam!)`:** maskowanie właścicielskie (anti-enumeration, §7.2). Cudzy wniosek → identyczny `LoanApplicationNotFound(loan_id)` co brak. Atakujący z listą UUID nie odróżni „nie istnieje" od „nie twoje" — ani po kodzie (docelowo 404 w obu), ani po komunikacie (ten sam string), ani po czasie (oba to jeden SELECT + porównanie; różnica nanosekundowa, niemierzalna przez sieć).
- **Porównanie `UUID != UUID`:** typy po obu stronach (obie parsowane: `loan_id` przez FastAPI z URL, `applicant_id` przez `get_current_applicant_id` z nagłówka). Brak pułapek formatowania (myślniki/wielkość liter w stringach nie grają roli, bo porównujemy obiekty, nie tekst).
- **Dlaczego nie 403 dla cudzego?** 403 („istnieje, ale nie wolno") zdradza istnienie. W banku analogia: urzędnik na pytanie o cudzą teczkę mówi „nie ma takiej sprawy", nie „jest, ale panu nie pokażę". (Applicant robi identycznie w login: jeden komunikat na dwa powody.)
- **Alternatywy:** (1) filtrowanie w SQL (`WHERE id AND applicant_id`) — jedno zapytanie zamiast pobrania + porównania w Pythonie; równoważne bezpieczeństwa (brak wiersza = ten sam wyjątek), minimalnie szybsze. Obecny wariant czyta cały wiersz i porównuje w pamięci — prostsze w teście z fakiem (fake implementuje tylko `get_by_id`). (2) Osobny wyjątek `AccessDenied` → 403 — odrzucone świadomie (patrz wyżej).
- **Przykład (test `test_get_loan_wrong_applicant`):**
  ```python
  with pytest.raises(LoanApplicationNotFound):
      await use_case.execute(loan.id, uuid4())   # losowy obcy applicant
  ```

**Podsumowanie pliku:** 22 linie, w tym ~8 logiki. Małe pliki też zasługują na analizę: to tutaj stoi całe bezpieczeństwo odczytu (jedna zła linia — np. brak drugiego `if` — i każdy zalogowany czyta cudze wnioski z dochodami).

---

### 4.9 Plik: `src/infrastructure/persistence/models.py` (79 linii)

**Cel:** trzy modele ORM — lustro SPEC §6.2–6.3 w SQLAlchemy 2.0 (nowoczesny styl `Mapped[]` + `mapped_column`, nie stare `Column`). Modele to „tłumaczenie" encji na język bazy: te same pola, inne typy (UUID→`PG_UUID`, Decimal→`Numeric`, enum→`String`, daty→`timestamptz`). Ten plik importuje SQLAlchemy — dlatego mieszka w `infrastructure`, nie w `domain` (Applicant-guide §4.12 opisuje identyczny podział na przykładzie `ApplicantModel`).

#### Blok 1: importy i baza

```python
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID, uuid4

from sqlalchemy import JSON, Index, Numeric, String, text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import DateTime
```

- **Linia po linii:** `datetime`/`Decimal`/`UUID` — te same typy co w encji (mapowanie 1:1, zero konwersji w głowie). `Any`/`Optional` — dla kolumn JSON i nullable. `uuid4` — default ID po stronie Pythona.
- **`PG_UUID` z aliasem:** `from sqlalchemy.dialects.postgresql import UUID as PG_UUID` — „UUID, ale ten postgresowy". Alias, bo nazwa `UUID` jest już zajęta przez `uuid.UUID` z linijki wyżej. Bez aliasu mielibyśmy kolizję nazw (drugi import nadpisałby pierwszy i `Mapped[UUID]` znaczyłoby co innego w różnych linijkach — koszmar).
- **`Mapped` + `mapped_column` (styl 2.0):** `id: Mapped[UUID] = mapped_column(...)` — adnotacja mówi Pythonowi/mypy „to UUID", a `mapped_column` mówi SQLAlchemy „taka kolumna". Stary styl (`id = Column(PG_UUID, primary_key=True)`) nie niósł informacji typowej — mypy widział `Column`, nie `UUID`, i każde `model.id` wymagało castów. Nowy styl = typy działają w obie strony. Ruff/mypy strict tego wymagają.
- **`text`:** surowe fragmenty SQL (`text("now()")`, `text("status = 'PENDING'")`) — używane tam, gdzie ORM nie ma wyrażenia (defaulty serwerowe, WHERE indeksu częściowego).

```python
class Base(DeclarativeBase):
    pass
```

- Wspólna baza metadanych dla wszystkich modeli. `Base.metadata` to rejestr „jakie tabele istnieją": `alembic/env.py` (`target_metadata`) generuje z niego migracje, `init_database`/`conftest` (`Base.metadata.create_all`) tworzą schemat w dev/testach. Jedna baza = jeden rejestr = brak tabel-widm (model niezarejestrowany = tabela niepowstająca nigdzie).
- `pass` (nie docstring) — minimalizm; klasa-znacznik bez zachowania.

#### Blok 2: `LoanApplicationModel` — kolumna po kolumnie

```python
class LoanApplicationModel(Base):
    """Loan application database model."""

    __tablename__ = "loan_applications"

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid4
    )
    applicant_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), index=True, nullable=False
    )
```

- **Linia `__tablename__`:** liczba mnoga, snake_case (`loan_applications`, nie `LoanApplication`) — konwencja Postgresa. Nazwa trafia do migracji, logów i zapytań — zmiana po starcie produkcji wymaga migracji rename (dlatego ustala się ją raz, na początku).
- **Linia `id`:** `PG_UUID(as_uuid=True)` — natywne UUID w Postgresie (16 bajtów, nie 36-znakowy string!). `as_uuid=True` znaczy „konwertuj do `uuid.UUID` w Pythonie" (bez tego SQLAlchemy zwracałby stringi i `_to_entity` musiałby parsować). `primary_key=True` (klastrowany indeks B-tree za darmo). `default=uuid4` — **funkcja**, nie wynik (patrz §4.2: `uuid4`, nie `uuid4()`!). Default po stronie Pythona (nie `server_default`), bo ID musi być znane przed INSERT-em (eventy w tej samej transakcji potrzebują `aggregate_id`).
- **Linia `applicant_id`:** `index=True` (osobny indeks B-tree — guard `exists_active_for_applicant` i przyszłe listowanie filtrują po tej kolumnie; bez indeksu każdy POST robiłby seq-scan całej tabeli). `nullable=False` (każdy wniosek ma właściciela — brak NULL-i).
- **Bez Foreign Key — celowo!** (database-per-service, SPEC §6.2: „brak FK między bazami"). Dla laika: klucz obcy kazałby bazie `loan_db` sprawdzać istnienie wiersza w bazie `applicant_db` — a to inna baza, inny user, brak grantów. Spójność między serwisami zapewniają zdarzenia (`applicant.registered.v1`), nie constrainty. Cena: osierocone `applicant_id` po usunięciu klienta (akceptowane w MVP; produkcja: soft-delete + event `applicant.deleted`).

```python
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    term_months: Mapped[int] = mapped_column(nullable=False)
    monthly_income: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    applicant_age: Mapped[int] = mapped_column(nullable=False)
```

- **`Numeric(12, 2)`:** 12 cyfr łącznie, 2 po przecinku — do 99 999 999,99... dokładnie: max `9999999999.99` (10 cyfr całkowitych). Limit biznesowy 200 000 mieści się z 50× zapasem; zapas jest na przyszłe produkty (hipoteka), nie na obecne potrzeby. `Decimal` mapuje się 1:1 (SQLAlchemy konwertuje automatycznie w obie strony — `model.amount` to już `Decimal`, nie string).
- **Dlaczego nie `Float`?** Float w bazie = błąd binarny utrwalony na dysku (0.1 zapisane jako 0.1000000000000000055). `Numeric` trzyma dziesiętnie dokładnie. Fintech bez `Numeric` nie istnieje.
- **`term_months`/`applicant_age` jako zwykłe `Integer`:** `mapped_column(nullable=False)` bez typu jawnego — SQLAlchemy wnioskuje `Integer` z adnotacji `Mapped[int]`. Zwięzłość stylu 2.0: typ piszesz raz (w adnotacji), nie dwa razy.
- **Brak CHECK constraintów** (`CHECK (amount > 0)`, `CHECK (term_months BETWEEN 3 AND 60)`): waliduje Pydantic, nie baza. Gdyby ktoś pominął API (ręczny INSERT, skrypt migracyjny), baza przyjmie ujemną kwotę. Obrona w głąb sugerowałaby CHECK-e w migracji (tanie, jednorazowe) — odnotowane w §11 jako hardening.

```python
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="DRAFT")
    decision_reasons: Mapped[Optional[list[Any]]] = mapped_column(JSON, nullable=True)
    idempotency_key: Mapped[Optional[str]] = mapped_column(
        String(255), unique=True, nullable=True
    )
```

- **Linia `status`:** `String(50)` (nie enum bazy!) — dodanie stanu to zmiana w `value_objects.py`, nie migracja typu `CREATE TYPE`. Długość 50 z zapasem (`DISBURSEMENT_FAILED` = 19 znaków; 50 wytrzyma nawet `DISBURSEMENT_PARTIALLY_FAILED`). `default="DRAFT"` po stronie Pythona (wstawiany przy INSERT z ORM; surowe SQL-e muszą podać jawnie — spójnie z encją, która też startuje od DRAFT).
- **Linia `decision_reasons`:** `JSON` (jsonb w Postgresie — indeksowalny, queryowalny operatorami `->`). `Optional` + `nullable=True`: `None` = „uzasadnienia jeszcze brak" (odróżnialne od `[]`). Typ `list[Any]` (nie `list[str]`), bo JSON z bazy może przynieść cokolwiek przy uszkodzonych danych — `_to_entity` i tak robi `or []`.
- **Linia `idempotency_key`:** `String(255)` (nagłówki HTTP bywają długie; UUID to 36 znaków, ale klient może słać własne formaty) + `unique=True` (druga linia obrony — Redis pierwsza). `nullable=True` + UNIQUE: w Postgresie wiele NULL-i **nie łamie** UNIQUE (NULL ≠ NULL), więc wnioski bez klucza przechodzą swobodnie. W MySQL/SQLite semantyka NULL-w-UNIQUE też pozwala — ale projekt jest na Postgresie, więc liczy się Postgres.
- **Co gdyby `unique=False`?** Wyścig dwóch równoległych requestów z tym samym kluczem (oba przeszły Redis-`get` → None) utworzyłby dwa wnioski z tym samym kluczem — replay wskazywałby losowy. UNIQUE zamienia ten wyścig w `IntegrityError` drugiego (rollback → retry klienta → replay znajduje pierwszy). Constraint jako synchronizacja — darmowa, atomowa.

```python
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
```

- **`DateTime(timezone=True)`** = `timestamptz` (znacznik ze strefą). `server_default=text("now()")` — default po stronie **bazy** (`now()` transakcyjny: wszystkie wiersze w jednej transakcji dostają ten sam czas!). Różnica wobec encji (`datetime.now(UTC)` w Pythonie): encja stempluje przed wysyłką, baza — przy zapisie. Normalnie różnią się o milisekundy; przy długiej transakcji (locki) — więcej. Który wierzyć? Bazowy (to on jest w wierszu; encyjny ginie, bo model dostaje `created_at` z encji jawnie... uwaga: `save()` przekazuje `created_at=loan.created_at`, więc server_default działa tylko przy surowych INSERT-ach! Niespójność: dwa źródła czasu, wygrywa Python. Do ujednolicenia — §11).
- **Brak `onupdate` dla `updated_at`:** SQLAlchemy nie podbija automatycznie (trzeba by `onupdate=func.now()`). Podbija encja (`_transition`) i worker jawnie. Surowy UPDATE omijający encję zostawi stary `updated_at` — akceptowane (wszystkie zapisy idą przez encję/repo).

#### Blok 3: `OutboxEventModel` — kolejka w tabeli

```python
class OutboxEventModel(Base):
    """Outbox event database model."""

    __tablename__ = "outbox_events"

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid4
    )
    aggregate_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="PENDING")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    sent_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        Index("ix_outbox_pending", "status", postgresql_where=text("status = 'PENDING'")),
    )
```

- Kolumna po kolumnie: `id` (PK, losowe), `aggregate_id` (bez indeksu! — worker czyta po `status`, nie po agregacie; konsument SSE będzie czytał po agregacie... brak indeksu to przyszły seq-scan przy `WHERE aggregate_id` — kandydat do indeksu w Etapie 4, odnotowane w §11), `event_type` (255 — nazwy z wersją, np. `loan.application.submitted.v1` = 29 znaków, zapas na dłuższe), `payload` (JSON NOT NULL — event bez treści nie istnieje), `status` (20 znaków wystarczy na PENDING/SENT/FAILED), `created_at` (FIFO workera: `ORDER BY created_at`), `sent_at` (NULL do publikacji — miara laga).
- **`__table_args__` z partial index:** `ix_outbox_pending` tylko nad wierszami `WHERE status = 'PENDING'`. Dla laika: zwykły indeks to spis treści całej książki; partial to spis tylko „rozdziałów do przeczytania" — mały (PENDING to promil wierszy po rozruchu), szybki, nie rośnie z historią SENT. Worker (`WHERE status='PENDING' ORDER BY created_at LIMIT 10`) trafia w indeks idealnie.
- **Pułapka nazw:** `postgresql_where=` (nie `postgres_where`) — parametr specyficzny dla dialektu jako kwargs z prefiksem. Literówka = `TypeError` przy imporcie modeli (fail-fast, na szczęście wcześnie).
- **Co gdyby brak indeksu?** Przy 100 wierszach nic. Przy 10 mln SENT (rok produkcji) każdy tick workera skanowałby miliony wierszy co 500 ms — samobójstwo wydajnościowe. Indeks to nie optymalizacja, to warunek działania outbox w skali.

#### Blok 4: `ProcessedEventModel` — pamięć konsumenta

```python
class ProcessedEventModel(Base):
    """Processed event database model (consumer idempotency)."""

    __tablename__ = "processed_events"

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid4
    )
    event_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), unique=True, nullable=False
    )
    event_type: Mapped[str] = mapped_column(String(255), nullable=False)
    aggregate_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
```

- `event_id` UNIQUE — serce idempotencji konsumenckiej: drugi INSERT tego samego `event_id` rzuci `IntegrityError` (fizyczna niemożliwość podwójnego przetworzenia, niezależna od logiki aplikacji). `event_type` + `aggregate_id` bez indeksów (audyt/inspekcja DLQ przeczyta po `event_id`; wyszukiwanie „wszystkie eventy agregatu" to przyszłość).
- Tabela pusta dziś (brak konsumentów), ale schemat gotowy — przyszły consumer nie wymaga migracji, tylko kodu (§12.2 checklist).

**Podsumowanie pliku:** 79 linii, trzy tabele, zero zapytań (zapytania mieszkają w `repository.py` i workerze). Rozdział „co" (kształt danych) od „jak" (dostęp) — modele nie wiedzą, kto je czyta.

---

### 4.10 Plik: `src/infrastructure/persistence/repository.py` (171 linii)

**Cel:** trzy adaptery SQLAlchemy implementujące porty z §4.5. Adapter to „konkret za abstrakcją": use case woła `repo.save(loan)`, a tutaj to zamienia się w `session.add + flush`. Mapowanie encja↔model w obie strony (`_to_entity`). Najdłuższy plik infrastruktury — bo SQL jest rozwlekły, nie dlatego że skomplikowany.

#### Blok 1: importy i konstruktor

```python
"""SQLAlchemy repositories for Loan Application Service."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.ports.repository import (
    LoanApplicationRepository,
    OutboxRepository,
    ProcessedEventRepository,
)
from src.domain.entities import LoanApplication, OutboxEvent, ProcessedEvent
from src.domain.value_objects import LoanStatus
from src.infrastructure.persistence.models import (
    LoanApplicationModel,
    OutboxEventModel,
    ProcessedEventModel,
)
```

- Importy ze **wszystkich** warstw (application-porty, domain-encje, infrastructure-modele) — to jest dozwolone: infrastructure zależy od wszystkiego (jest na zewnątrz, patrz §2.1). Odwrotny kierunek (domain importujące stąd) byłby błędem.
- `select` + `update` (konstrukty zapytań 2.0 — nie surowe stringi SQL, nie Query 1.x). `AsyncSession` — sesja asynchroniczna (sterownik `asyncpg`).
- **Dziedziczenie po portach** (`class SQLAlchemyLoanApplicationRepository(LoanApplicationRepository)`) — Python wymusi implementację wszystkich metod abstrakcyjnych przy instancjonowaniu. Zapomniana metoda = `TypeError` przy starcie (fail-fast), nie `AttributeError` w runtime. mypy strict dodatkowo sprawdzi sygnatury (parametry i zwroty muszą się zgadzać z portem).

```python
class SQLAlchemyLoanApplicationRepository(LoanApplicationRepository):
    """SQLAlchemy implementation of LoanApplicationRepository."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
```

- **Konstruktor synchroniczny** (`def`, nie `async def`) — wstrzyknięcie sesji to nie I/O, nie ma czego awaitować. Sesja przychodzi z request-scopa (`get_session` w `dependencies.py`) — ta sama sesja dla loan-repo i outbox-repo w ramach requestu = ta sama transakcja (warunek outbox!).
- **Repozytorium nigdy nie commituje.** `commit` należy do `get_session` (po `yield`). Gdyby repo commitowało po każdym `save`, wniosek i eventy lądowałyby w trzech osobnych transakcjach (okno na niespójność między nimi). Brak commitów w tym pliku to nie przeoczenie — to projekt.

#### Blok 2: `save` — encja → wiersz (tylko INSERT)

```python
    async def save(self, loan: LoanApplication) -> None:
        """Save loan application (insert)."""
        model = LoanApplicationModel(
            id=loan.id,
            applicant_id=loan.applicant_id,
            amount=loan.amount,
            term_months=loan.term_months,
            monthly_income=loan.monthly_income,
            applicant_age=loan.applicant_age,
            status=loan.status.value,
            decision_reasons=loan.decision_reasons,
            idempotency_key=loan.idempotency_key,
            created_at=loan.created_at,
            updated_at=loan.updated_at,
        )
        self._session.add(model)
        await self._session.flush()
```

- **Linia po linii:** konstrukcja modelu to przepisanie 11 pól 1:1. Jedyna konwersja: `status=loan.status.value` (enum → string; jedyne takie miejsce przy zapisie). `Decimal`/`UUID`/`datetime` przechodzą bez konwersji (SQLAlchemy + asyncpg rozumieją natywnie). `decision_reasons` (lista) → kolumna JSON (serializacja automatyczna).
- **Linia `add`:** rejestracja w sesji (unit of work — sesja zbiera obiekty do wysłania). Synchroniczna (bez I/O — nic nie jedzie do bazy).
- **Linia `flush`:** wysłanie INSERT do bazy **w ramach transakcji** (wiersz widoczny dla kolejnych zapytań w tej transakcji, constrainty — PK, UNIQUE — sprawdzone już teraz). Różnica `flush` vs `commit`: flush = „wyślij, ale nie zatwierdzaj" (można jeszcze wycofać), commit = „zatwierdź nieodwołalnie". Błąd UNIQUE na `idempotency_key` wychodzi tu (w requeście, z rollbackiem), nie w tle workera.
- **Tylko INSERT — luka #2 (§11).** Drugie `save` tej samej encji (po przyszłej transicji `start_underwriting`) zrobi INSERT z tym samym PK → `IntegrityError`. Przyszłe consumery potrzebują upsert (`merge`) albo osobnej metody `update`. Dziś flow (jeden zapis per wniosek) tego nie potrzebuje — ale pułapka czeka na Etap 6.
- **Alternatywy:** (1) `session.merge(model)` (upsert: INSERT lub UPDATE po PK) — naprawia lukę jednym słowem, ale maskuje różnicę insert/update (przypadkowy `save` nadpisze wiersz); (2) osobne `insert()`/`update()` w porcie — jawne, wymaga edycji portu + faków w testach. Wybór do podjęcia w Etapie 6.

#### Blok 3: odczyty `get_by_id` i `get_by_applicant_id`

```python
    async def get_by_id(self, loan_id: UUID) -> LoanApplication | None:
        """Get loan application by ID."""
        stmt = select(LoanApplicationModel).where(LoanApplicationModel.id == loan_id)
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        if model is None:
            return None
        return self._to_entity(model)
```

- **Linia `stmt = select(...).where(...)`:** budowa zapytania (bez I/O — `select` to konstruktor obiektu, nie podróż do bazy). Dla laika: `select(Model)` = „daj wiersze tej tabeli", `.where(Model.id == loan_id)` = „gdzie id równe". Porównanie `==` na kolumnie to nie bool — to wyrażenie SQL (operator overload w SQLAlchemy).
- **Linia `await execute`:** jedyny punkt I/O (podróż do Postgresa). `result` to kursor-wynik, nie dane.
- **Linia `scalar_one_or_none()`:** „daj at most jeden obiekt, albo None; jak więcej — rzuć". Po PK więcej być nie może (baza gwarantuje), ale metoda dokumentuje oczekiwanie („tu ma być 0..1, nie lista"). Alternatywy: `scalar_one()` (rzuciłby przy braku — a brak to legalny stan, nie błąd), `scalars().first()` (cichy pierwszy przy wielu — maskowanie).
- **Linie `if None: return None` + `_to_entity`:** brak propagowany w górę (wyjątek rzuca use case, nie repo — §4.5).

```python
    async def get_by_applicant_id(self, applicant_id: UUID) -> list[LoanApplication]:
        """Get all loan applications for applicant (newest first)."""
        stmt = (
            select(LoanApplicationModel)
            .where(LoanApplicationModel.applicant_id == applicant_id)
            .order_by(LoanApplicationModel.created_at.desc())
        )
        result = await self._session.execute(stmt)
        return [self._to_entity(m) for m in result.scalars().all()]
```

- `order_by(created_at.desc())` — najnowsze pierwsze (dashboard: aktualna sprawa na górze). `.desc()` na kolumnie (nie string `"DESC"` — typowane, odporne na literówki). List comprehension mapuje każdy wiersz przez `_to_entity`.
- **Martwa dziś** (brak `GET /loans`), gotowa na jutro. Indeks na `applicant_id` (§4.9) już na nią czeka.

#### Blok 4: `exists_active_for_applicant` — guard w SQL

```python
    async def exists_active_for_applicant(self, applicant_id: UUID) -> bool:
        """Check for active application (lightweight existence query)."""
        active_statuses = [
            s.value for s in LoanStatus if s not in {
                LoanStatus.REJECTED,
                LoanStatus.DISBURSED,
                LoanStatus.DOC_REJECTED,
                LoanStatus.DISBURSEMENT_FAILED,
            }
        ]
        stmt = select(LoanApplicationModel.id).where(
            LoanApplicationModel.applicant_id == applicant_id,
            LoanApplicationModel.status.in_(active_statuses),
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None
```

- **Linia `active_statuses`:** „wszystkie statusy minus terminalne" — lustro `is_active` z encji (ta sama czwórka). Obliczone w Pythonie (iteracja po 10 członach enuma — nanosekundy), wysłane do SQL jako lista stringów (`.value` — SQL nie zna enumów Pythona). **Dwa `where` po przecinku** = AND (konwencja SQLAlchemy: przecinek w `where` to koniunkcja, nie trzeba `and_()`).
- **Linia `select(...id)`:** tylko kolumna ID (nie cały wiersz!) — lekkie zapytanie po indeksie `applicant_id`, bez deserializacji JSON/czasów. Różnica wobec `get_by_applicant_id`: tam pełne wiersze (lista), tu jedno ID (tak/nie). To jest portowa wersja zasady „licz w bazie, nie w pamięci".
- **Linia `is not None`:** `scalar_one_or_none()` daje ID albo None → bool. `is not None` (nie truthiness) — poprawne dla None-semantyki.
- **Wyścig (dla seniora):** check-then-act bez locka — dwa równoległe POST-y mogą oba dostać `False` i oba utworzyć wniosek. Łata: UNIQUE-częściowy w bazie (`UNIQUE(applicant_id) WHERE status IN (aktywne)`) albo advisory lock. W MVP akceptowane (okno milisekundowe, gateway rate-limit 3/10 min je zwęża); produkcja wymaga constraintu — §11.

```python
    async def get_by_idempotency_key(self, key: str) -> LoanApplication | None:
        """Get loan application by idempotency key (fallback lookup)."""
        stmt = select(LoanApplicationModel).where(
            LoanApplicationModel.idempotency_key == key
        )
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        if model is None:
            return None
        return self._to_entity(model)
```

- Lustro `get_by_id` po kolumnie UNIQUE. Martwe dziś (use case idzie przez Redis) — tryb awaryjny „bez Redisa" / audyt (§4.5).

#### Blok 5: `_to_entity` — wiersz → encja

```python
    def _to_entity(self, model: LoanApplicationModel) -> LoanApplication:
        """Map ORM model back to domain entity."""
        return LoanApplication(
            id=model.id,
            applicant_id=model.applicant_id,
            amount=model.amount,
            term_months=model.term_months,
            monthly_income=model.monthly_income,
            applicant_age=model.applicant_age,
            status=LoanStatus(model.status),
            decision_reasons=model.decision_reasons or [],
            idempotency_key=model.idempotency_key,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )
```

- **Synchroniczna** (`def` — czyste mapowanie, zero I/O). Prywatna (podkreślnik — szczegół adaptera).
- **Linia `status=LoanStatus(model.status)`:** string → enum. Uszkodzony string w bazie (np. ręczny UPDATE na `"APROVED"`) rzuci `ValueError` tutaj — fail-fast przy odczycie (głośny błąd zamiast wniosku-widma z nieznanym stanem). Cena: jeden zły wiersz psuje odczyt (ale nie zapis innych).
- **Linia `decision_reasons=model.decision_reasons or []`:** NULL z bazy → `[]` (encja nigdy nie widzi None — jej typ to `list[str]`, nie `Optional`). `or` łapie też `[]` (pusta lista jest falsy, ale wynik ten sam — `[]`).
- **Symetria z `save`:** pola w tej samej kolejności, te same konwersje w drugą stronę. Review-reguła: dodając pole do encji, musisz dotknąć `save` i `_to_entity` (i model, i DTO...) — kompilator tego nie sprawdzi (dlatego testy integracyjne czytają to, co zapisały).

#### Blok 6: `SQLAlchemyOutboxRepository`

```python
class SQLAlchemyOutboxRepository(OutboxRepository):
    """SQLAlchemy implementation of OutboxRepository."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, event: OutboxEvent) -> None:
        """Save outbox event (same transaction as the loan)."""
        model = OutboxEventModel(
            id=event.id,
            aggregate_id=event.aggregate_id,
            event_type=event.event_type,
            payload=event.payload,
            status=event.status,
            created_at=event.created_at,
            sent_at=event.sent_at,
        )
        self._session.add(model)
        await self._session.flush()
```

- Identyczny schemat jak `save` wniosku (konstrukcja + add + flush, bez commit). Status/payload przechodzą 1:1 (string, JSON). To ten `save` woła use case dwukrotnie (dwa eventy, jedna transakcja).

```python
    async def get_pending(self, limit: int = 10) -> list[OutboxEvent]:
        """Get oldest pending events (worker batch)."""
        stmt = (
            select(OutboxEventModel)
            .where(OutboxEventModel.status == "PENDING")
            .order_by(OutboxEventModel.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(stmt)
        return [self._to_entity(m) for m in result.scalars().all()]
```

- **Łańcuch klauzul:** filtr PENDING → FIFO po `created_at` (starsze pierwsze — porządek publikacji = porządek powstania) → `LIMIT` (bounded batch) → `FOR UPDATE SKIP LOCKED` (locki bez czekania — §6.8). Kolejność w łańcuchu nie ma znaczenia semantycznego (SQLAlchemy buduje jedno zapytanie), ale czytelność: od filtrowania do blokowania.
- **`with_for_update(skip_locked=True)`:** wiersze zablokowane przez inny proces workera są **pomijane**, nie czekają. Dwie repliki serwisu nie depczą sobie po piętach (każda dostaje rozłączny podzbiór). Bez `skip_locked`: druga replika wisiałaby na lockach pierwszej (serializacja workerów — po co dwie repliki?).
- **Uwaga:** ten `get_pending` wołany jest dziś tylko w testach (`test_outbox_events_created`) — produkcyjny worker ma własną kopię zapytania w `poll_once` (duplikacja logiki, §11 luka #13). Docelowo worker powinien wołać repozytorium, nie modele.

```python
    async def mark_sent(self, event_id: UUID) -> None:
        """Mark event SENT (no SELECT round-trip)."""
        stmt = (
            update(OutboxEventModel)
            .where(OutboxEventModel.id == event_id)
            .values(status="SENT")
        )
        await self._session.execute(stmt)
        await self._session.flush()
```

- `UPDATE ... WHERE id` bez uprzedniego SELECT — jedno zapytanie zamiast dwóch (worker wie ID, nie potrzebuje wiersza). `values(status="SENT")` — tylko status; **`sent_at` nietknięte** (niespójność z workerem, który ustawia oba — §11 luka #7). `flush` (nie commit — transakcja należy do wołającego).

```python
    def _to_entity(self, model: OutboxEventModel) -> OutboxEvent:
        """Map ORM model back to domain entity."""
        return OutboxEvent(
            id=model.id,
            aggregate_id=model.aggregate_id,
            event_type=model.event_type,
            payload=model.payload,
            status=model.status,
            created_at=model.created_at,
            sent_at=model.sent_at,
        )
```

#### Blok 7: `SQLAlchemyProcessedEventRepository`

```python
class SQLAlchemyProcessedEventRepository(ProcessedEventRepository):
    """SQLAlchemy implementation of ProcessedEventRepository."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def exists(self, event_id: UUID) -> bool:
        """Check if event was already processed."""
        stmt = select(ProcessedEventModel.id).where(
            ProcessedEventModel.event_id == event_id
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None

    async def save(self, event: ProcessedEvent) -> None:
        """Record processed event."""
        model = ProcessedEventModel(
            id=event.id,
            event_id=event.event_id,
            event_type=event.event_type,
            aggregate_id=event.aggregate_id,
            processed_at=event.processed_at,
        )
        self._session.add(model)
        await self._session.flush()
```

- Nieużywane dziś (brak konsumentów); gotowe na Etapy 6–8. `exists` po `event_id` (UNIQUE) — lekkie `SELECT id`. Wyścig check-then-act opisany w §4.5 (docelowo UNIQUE + `IntegrityError`).

**Podsumowanie pliku:** 171 linii, trzy adaptery, jedna zasada: „repo nie commituje, repo nie zna HTTP, repo mapuje w obie strony". Najdłuższy, ale i najbardziej mechaniczny plik — SQLAlchemy robi tu za „drogi sterownik z typami".

---

### 4.11 Plik: `src/infrastructure/kafka/producer.py` (173 linie)

**Cel:** schematy payloadów eventów + `OutboxWorker` — jedyny kontakt serwisu z Kafką, i to tylko jako **producent** (konsumentów brak; będą w Etapach 6–8). Plik łączy dwie role: rejestr kontraktów (`EVENT_SCHEMAS`) i mechanikę dowozu (worker). Importuje modele SQLAlchemy wprost (`OutboxEventModel`) — pragmatyczne obejście portu `OutboxRepository` (patrz §2.1 i §11 luka #13).

#### Blok 1: importy

```python
"""Kafka producer with Transactional Outbox worker."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

from aiokafka import AIOKafkaProducer
from crediguard_events import EventEnvelope
from crediguard_observability import get_logger
from pydantic import BaseModel

from src.infrastructure.persistence.models import OutboxEventModel
```

- **Linia `from aiokafka import AIOKafkaProducer`:** asynchroniczny producent Kafki (SPEC §3: „aiokafka — spójny z event loopem FastAPI"). Dla laika: zwykły `kafka-python` blokowałby wątek na czas wysyłki; `AIOKafkaProducer` zawiesza coroutine (`await send_and_wait`) i oddaje loop innym requestom. Alternatywa `confluent-kafka` (szybsza, librdkafka w C) jest synchroniczna — wymagałaby `run_in_executor` i komplikowała lifespan.
- **Linia `from crediguard_events import EventEnvelope`:** koperta ze współdzielonego `libs/events` (jedno źródło prawdy kontraktu w monorepo — zmiana koperty to jeden PR we wszystkich serwisach, SPEC §10: uzasadnienie monorepo).
- **Linia `from crediguard_observability import get_logger`:** structlog-JSON z `libs/observability` (każdy wpis ma `service`, `timestamp`; `correlation_id` dokładane w wywołaniach).
- **Linia `from pydantic import BaseModel`:** schematy payloadów jako modele Pydantic (walidacja przed wysyłką — zły payload nie opuszcza procesu).
- **Linia `from ...models import OutboxEventModel`:** worker czyta wprost modele, nie port — skrót Etapu 3 (worker żyje poza cyklem request/response, więc DI z `dependencies.py` go nie obejmuje). Cena: duplikacja zapytania `get_pending` (worker ma własną kopię) i zależność infrastructure→infrastructure zamiast infrastructure→application.

#### Blok 2: schematy payloadów i rejestr

```python
class LoanApplicationSubmittedV1(BaseModel):
    """Payload for loan.application.submitted.v1."""

    loan_id: UUID
    applicant_id: UUID
    amount: str
    term_months: int
    monthly_income: str
    applicant_age: int


class LoanStatusChangedV1(BaseModel):
    """Payload for loan.status.changed.v1."""

    loan_id: UUID
    applicant_id: UUID
    old_status: str
    new_status: str
    decision_reasons: list[str] | None = None


EVENT_SCHEMAS: dict[str, type[BaseModel]] = {
    "loan.application.submitted.v1": LoanApplicationSubmittedV1,
    "loan.status.changed.v1": LoanStatusChangedV1,
}
```

- **Linia po linii `SubmittedV1`:** pełne dane wniosku (fat event — konsument Document nie pyta bazy Loan App). `amount`/`monthly_income` jako `str` (nie Decimal!): JSON nie zna Decimal, a string jest dokładny (`"15000.00"` parsuje się bezstratnie; float `15000.0` już niekoniecznie). UUID jako `UUID` (Pydantic zserializuje do stringa w kopercie). Sufiks `V1` w nazwie klasy = wersja kontraktu (V2 kiedyś = nowa klasa + nowy wpis, stara żyje dla starych konsumentów).
- **Linia `decision_reasons ... = None`:** default None (event bez uzasadnienia to legalny stan początkowy). Klucz zawsze obecny (stabilny schemat — konsument nie rozgałęzia „brak klucza" vs „null").
- **Linia `EVENT_SCHEMAS`:** rejestr „typ → walidator". Adnotacja `dict[str, type[BaseModel]]` (wartości to klasy, nie instancje — `type[...]`). Nieznany typ w bazie → `None` z `.get` → warning + skip (nie crash workera — jeden zły wiersz nie zatrzymuje kolejki; patrz `_publish_event`). To jest fail-open dla robusności kolejki (przeciwieństwo fail-fast w domenie — różne miejsca, różne filozofie).
- **Luka spójności:** literały `"loan.application.submitted.v1"` występują tu i w use case (dwa miejsca, zero wspólnej stałej). Literówka w jednym = worker skipuje eventy use case'a na zawsze (cicha śmierć — tylko warning w logach). Kandydat na stałe w `libs/events` (§11).

#### Blok 3: `OutboxWorker.__init__`, `start`, `stop`

```python
class OutboxWorker:
    """Background worker draining outbox_events to Kafka."""

    def __init__(
        self,
        bootstrap_servers: str,
        database_url: str,
        poll_interval_ms: int = 500,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._database_url = database_url
        self._poll_interval_ms = poll_interval_ms
        self._producer: AIOKafkaProducer | None = None
        self._running = False
```

- Trzy parametry konfiguracyjne (brokery, baza, interwał) + dwa stany (`_producer`, `_running`). `poll_interval_ms=500` (nie sekundy!): jednostka w nazwie eliminuje pomyłkę 500 s vs 500 ms. 500 ms to kompromis: świeżość SSE (opóźnienie publikacji ≤0.5 s + czas Kafki) vs obciążenie bazy (2 zapytania/s na pustej kolejce — szum).
- `_producer: ... | None = None` — „jeszcze nie wystartował". Każde użycie sprawdza (`_publish_event` rzuca `RuntimeError` gdy None — fail-fast zamiast `AttributeError: 'NoneType'`).

```python
    async def start(self) -> None:
        """Start Kafka producer (connect to brokers)."""
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            acks="all",
            enable_idempotence=True,
        )
        await self._producer.start()
        self._running = True
        logger.info(
            "OutboxWorker started",
            bootstrap_servers=self._bootstrap_servers,
            poll_interval_ms=self._poll_interval_ms,
        )
```

- **Linia `acks="all"`:** lider partycji czeka na potwierdzenie **wszystkich** synchronicznych replik (ISR), zanim uzna zapis. Trwałość > latencja (dodatkowe milisekundy na event). Alternatywy: `acks=1` (tylko lider — szybsze, ryzyko utraty przy padzie lidera przed replikacją), `acks=0` (ogień i zapomnij — loteria). Dla eventów kredytowych tylko `all` jest defensywne.
- **Linia `enable_idempotence=True`:** idempotentny producent (Kafka: deduplikacja retry po PID+sequence). Nie mylić z idempotencją biznesową! To chroni przed duplikatem sieciowym (producent wysłał, nie dostał ACK, wysłał znowu — broker odrzuci drugi), nie przed duplikatem logicznym (crash po send, przed commit — tu duplikat powstanie i filtruje go konsument po `event_id`). Dwa poziomy, dwie nazwy, częste zamieszanie — stąd to zdanie.
- **Linia `await self._producer.start()`:** połączenie z brokerami, pobranie metadanych (partycje, liderzy). Bez tego pierwsze `send_and_wait` wisiałoby na bootstrapie. Błąd tutaj (Kafka nieosiągalna) wychodzi przy starcie serwisu (fail-fast w lifespan), nie w pierwszym ticku.
- **Linia `_running = True`:** flaga pętli `run()`. Ustawiana po udanym starcie (nieudany start = flaga False = `run()` kończy natychmiast, gdyby ktoś je wystartował).

```python
    async def stop(self) -> None:
        """Stop producer (flush + disconnect)."""
        self._running = False
        if self._producer:
            await self._producer.stop()
        logger.info("OutboxWorker stopped")
```

- Kolejność: najpierw flaga (pętla kończy po bieżącym ticku), potem `stop()` producenta (flush buforów — wysłane, ale niepotwierdzone, dostają szansę). `if self._producer` (worker mógł nie wystartować — stop bez startu nie crashuje). `main.py` dodatkowo `cancel()` taska (gdyby spał w `sleep`).

#### Blok 4: `poll_once` — serce dowozu

```python
    async def poll_once(self) -> int:
        """Poll PENDING events, publish to Kafka, mark SENT. Returns count."""
        from sqlalchemy import select, update
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
        from sqlalchemy.pool import NullPool

        engine = create_async_engine(self._database_url, poolclass=NullPool)
        session_factory = async_sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )

        published_count = 0
        try:
            async with session_factory() as session:
                stmt = (
                    select(OutboxEventModel)
                    .where(OutboxEventModel.status == "PENDING")
                    .order_by(OutboxEventModel.created_at)
                    .limit(10)
                    .with_for_update(skip_locked=True)
                )
                result = await session.execute(stmt)
                events = result.scalars().all()

                for event_model in events:
                    try:
                        await self._publish_event(event_model)

                        update_stmt = (
                            update(OutboxEventModel)
                            .where(OutboxEventModel.id == event_model.id)
                            .values(
                                status="SENT",
                                sent_at=datetime.now(UTC),
                            )
                        )
                        await session.execute(update_stmt)
                        published_count += 1
                    except Exception:
                        logger.exception(
                            "Failed to publish outbox event",
                            event_id=str(event_model.id),
                            event_type=event_model.event_type,
                        )

                await session.commit()
        finally:
            await engine.dispose()

        return published_count
```

- **Linie importów wewnątrz metody (defer):** `sqlalchemy` importowane lokalnie, nie na górze pliku. Po co? By import modułu `producer` nie ciągnął silnika (testy schematów bez DB; szybszy import w `main`). Konwencja dyskusyjna (ruff tego nie flaguje; niektórzy uważają za antypattern) — ale celowa: worker to „osobny program w programie".
- **Linia `engine = create_async_engine(...)`:** osobny silnik **na każdy tick** (co 500 ms: connect → praca → dispose). Izolacja od puli requestów (zalanie outbox nie głodzi API; wyciek w workerze nie dotyka requestów). Koszt: handshake TCP + negocjacja asyncpg co tick (milisekundy na pustej kolejce — mierzalne, ale małe). Docelowo: długowieczny engine (§11 luka #6). `NullPool` (brak poolingu — silnik i tak żyje jeden tick).
- **Linia `stmt = ...`:** kopia zapytania z `SQLAlchemyOutboxRepository.get_pending` (duplikacja — §11 luka #13): PENDING → FIFO → LIMIT 10 → `FOR UPDATE SKIP LOCKED`. Locki trzymane do `commit` na końcu (wersje niewysłane blokują się nawzajem między replikami tylko w oknie ticka).
- **Linia `events = ...scalars().all()`:** materializacja batcha do listy (max 10 — bounded memory). Pętla po liście, nie po kursorze (kursor + `send_and_wait` w środku = długi otwarty kursor + locki przez cały czas sieci).
- **Linie pętli:** per event: publikuj → oznacz SENT (+ `sent_at` — jedyne miejsce je ustawiające!) → licznik. **Błąd jednego eventu** → `logger.exception` (stack trace w logach!) + kontynuacja (nie `mark_sent`, wiersz zostaje PENDING → retry w następnym ticku; trujący wiersz (zły payload) będzie próbować w nieskończoność co 500 ms — brak licznika prób/DLQ dla outbox (DLQ jest dla konsumentów, nie producenta); w praktyce zły payload nie przechodzi walidacji Pydantic już w use case, więc wiersz-trucizna nie powstaje — chyba że schemat zmieni się po zapisie (wdrożenie V2 bez migracji wierszy V1... krawędź na przyszłość).
- **Linia `await session.commit()`:** jeden commit na batch (publikacje + oznaczenia). Crash między `send_and_wait` a `commit` = event w Kafce, wiersz dalej PENDING = **duplikat** przy następnym ticku (at-least-once; konsument filtruje po `event_id`). Nie da się „dokładnie raz" przy dwóch systemach (baza + Kafka) bez 2PC/XA — outbox świadomie wybiera at-least-once + deduplikację (§6.7).
- **Linia `finally: await engine.dispose()`:** silnik zamykany zawsze (sukces, błąd, crash) — brak wycieku połączeń. Bez `finally` wyjątek z `poll_once` zostawiłby wiszące sockety (wyczerpanie puli Postgresa po godzinie ticków).
- **Return `published_count`:** metryka dla `run()` (log tylko gdy >0 — cisza na pustej kolejce, nie spam co 500 ms).

#### Blok 5: `_publish_event` — jeden wiersz do Kafki

```python
    async def _publish_event(self, event_model: OutboxEventModel) -> None:
        """Validate, wrap in envelope, and send one event."""
        if self._producer is None:
            raise RuntimeError("OutboxWorker not started")

        schema = EVENT_SCHEMAS.get(event_model.event_type)
        if schema is None:
            logger.warning(
                "Unknown event type, skipping",
                event_type=event_model.event_type,
            )
            return

        payload_data = event_model.payload
        if isinstance(payload_data, dict):
            payload = schema.model_validate(payload_data)
        else:
            payload = schema.model_validate_json(str(payload_data))

        envelope = EventEnvelope(
            event_type=event_model.event_type,
            correlation_id=uuid4(),
            producer="loan-application-service",
            payload=payload,
        )

        topic = event_model.event_type
        key = str(event_model.aggregate_id).encode()

        await self._producer.send_and_wait(
            topic=topic,
            key=key,
            value=envelope.model_dump_json().encode(),
        )

        logger.info(
            "Published outbox event",
            event_id=str(event_model.id),
            event_type=event_model.event_type,
            topic=topic,
        )
```

- **Linia guarda:** `RuntimeError` (nie `AssertionError` — to błąd programisty-konsumenta API workera, nie niezmiennik wewnętrzny). Fail-fast z czytelnym komunikatem zamiast `AttributeError: 'NoneType' object has no attribute 'send_and_wait'`.
- **Linie rejestru:** `.get` (nie `[...]`) + warning + `return` (nie `raise`!). Nieznany typ = skip wiersza... który **zostaje PENDING na zawsze** (brak `mark_sent`!) i będzie skipowany co tick z warningiem (log-spam, ale kolejka płynie dalej). Alternatywa: oznaczać SKIP jako SENT z adnotacją (gubi historię) albo osobny status SKIPPED (czystsze, wymaga migracji). Dziś: wieczny warning — akceptowane, bo nieznane typy nie występują (rejestr pokrywa wszystkie typy, które use case produkuje).
- **Linie walidacji payloadu:** dwie ścieżki (dict z JSONB vs string z innych sterowników) — defensywa na różnicę asyncpg/psycopg. `model_validate` rzuci `ValidationError` przy uszkodzonym payloadzie → łapie `except` w `poll_once` → retry w kółko (patrz wyżej: wiersz-trucizna).
- **Linie koperty:** `event_id` i `occurred_at` z default_factory (generowane **w momencie publikacji**, nie utworzenia wniosku!). Rozjazd `created_at` (baza, moment POST) vs `occurred_at` (moment wysyłki) to celowy ślad opóźnienia outbox (metryka laga bez dodatkowych kolumn). `producer="loan-application-service"` (literał — nazwa serwisowa do routingu logów/alertów).
- **Linia `correlation_id=uuid4()`:** świeży UUID zamiast propagacji z requestu (worker nie ma dostępu do nagłówka HTTP — wiersz outbox nie przechowuje correlation_id!). Łamie trace end-to-end (Jaeger: request i event to dwa osobne drzewa). Poprawka: kolumna `correlation_id` w outbox + przepisanie z `X-Correlation-ID` przy POST (§11 luka #5). Najbardziej „widoczna" luka dla demo obserwowalności (SPEC §9: „jeden trace od POST przez Kafkę" — dziś urwany).
- **Linia `topic = event_model.event_type`:** konwencja SPEC §5.1 (topik nazywa się jak typ: `loan.application.submitted.v1`). Zero mapowania = zero rozjazdu (nie da się wysłać na zły topik przez literówkę w mapie, bo mapy nie ma).
- **Linia `key = str(aggregate_id).encode()`:** klucz partycji = ID agregatu (string→bytes dla aiokafka). Ta sama partycja dla obu eventów wniosku = kolejność `submitted`→`status.changed` gwarantowana. Bez klucza (None) Kafka rozrzucałaby round-robin (kolejność losowa — katastrofa dla konsumentów).
- **Linia `send_and_wait`:** wysyłka synchroniczna z perspektywy workera (czeka na ACK `all`). `_and_wait` (nie fire-and-forget `send`): worker musi wiedzieć, że wysłano, zanim oznaczy SENT. Cena: przepustowość = 1 event na round-trip (batch 10 = 10 round-tripów na tick — przy 500 ms to sufit ~20 eventów/s; wystarczy na MVP, do skalowania: transakcyjny `send_batch` — przyszłość).
- **Linia `value=envelope.model_dump_json().encode()`:** koperta → JSON-string → bytes (aiokafka wymaga bytes). `model_dump_json` (nie `json.dumps(model_dump())`): szybsze (Rust-core w Pydantic v2) i poprawne dla UUID/datetime (serializacja natywna).
- **Linia loga:** `event_id` + `event_type` + `topic` jako pola strukturalne (grep po `event_id` łączy log serwisu z logiem konsumenta — korelacja bez correlation_id, na piechotę).

#### Blok 6: `run` — pętla życia

```python
    async def run(self) -> None:
        """Main loop: poll, log, sleep. Never raises."""
        import asyncio

        while self._running:
            try:
                count = await self.poll_once()
                if count > 0:
                    logger.info("OutboxWorker published events", count=count)
            except Exception:
                logger.exception("OutboxWorker poll error")
            await asyncio.sleep(self._poll_interval_ms / 1000)
```

- **`import asyncio` wewnątrz** (jak sqlalchemy w `poll_once` — defer importów w workerze; `asyncio` i tak zawsze dostępne, tu chodzi o konwencję „worker samowystarczalny").
- **`while self._running`:** flaga, nie `while True` (zatrzymywalne grzecznie przez `stop()`; `while True` wymagałoby anulowania taska jako jedynej drogi).
- **`try` wokół `poll_once`:** wyjątek z ticka (pad bazy, pad Kafki) → `logger.exception` + kolejna iteracja. Worker **nigdy nie umiera** (nie ma `raise` na tej ścieżce). Bez try: pierwszy błąd bazy zabijałby taska na zawsze (cicha śmierć — API działa, eventy stoją, nikt nie zauważa, bo healthcheck zielony). To jest najważniejszy `try` w pliku.
- **`if count > 0`:** log tylko przy pracy (cisza na pustej kolejce — log co 500 ms „nic nie zrobiłem" to 172 800 linii dziennie śmieci).
- **`sleep(ms/1000)`:** konwersja ms→s (bo `asyncio.sleep` bierze sekundy). Stały odstęp (nie adaptacyjny: brak backoff przy pustej kolejce ani przyspieszenia przy pełnej — proste, przewidywalne; adaptacja to przyszła optymalizacja).

**Podsumowanie pliku:** 173 linie, jedyny most do świata asynchronicznego. Trzy odpowiedzialności (schematy, publikacja, pętla) w jednym module — spójne, bo wszystkie służą „dowieź wiersz do Kafki". Rozdzielenie na `schemas.py` + `worker.py` miałoby sens przy 5+ typach eventów (dziś 2 — nie warto).

---

### 4.12 Plik: `src/infrastructure/redis/idempotency.py` (24 linie)

**Cel:** adapter `IdempotencyStore` na Redis — implementacja portu z §4.6 w dwóch wywołaniach. Najkrótszy plik infrastruktury (porównaj z gateway `rate_limit.py` — tam cała maszyneria sliding window; tutaj Redis robi robotę jednym `SET`).

#### Blok 1: importy i konstruktor

```python
"""Redis-backed idempotency store."""

from __future__ import annotations

import redis.asyncio as redis

from src.application.ports.idempotency import IdempotencyStore
```

- `import redis.asyncio as redis` — asynchroniczny klient (biblioteka `redis>=5.0.0` ma `asyncio` wbudowane; nie trzeba `aioredis` osobno). Alias `as redis`, bo pełna ścieżka `redis.asyncio.Redis` w adnotacjach byłaby rozwlekła.
- Import portu z application (infrastructure → application — jedyny dozwolony kierunek w górę).

```python
class RedisIdempotencyStore(IdempotencyStore):
    """Idempotency store backed by Redis SET NX EX."""

    def __init__(self, redis_client: redis.Redis) -> None:
        self._redis = redis_client
```

- Konstruktor synchroniczny (wstrzyknięcie klienta, nie połączenie — `from_url` nie łączy, połączenie jest leniwe przy pierwszym wywołaniu). Klient przychodzi z `init_redis` (singleton w `dependencies.py`).

#### Blok 2: `acquire` — atomowe „zajmij"

```python
    async def acquire(self, key: str, value: str, ttl_seconds: int = 86400) -> bool:
        """Atomically acquire key (SET NX EX)."""
        result = await self._redis.set(
            f"idempotency:{key}", value, nx=True, ex=ttl_seconds
        )
        return result is not None
```

- **Linia `f"idempotency:{key}"`:** prefix przestrzeni kluczy. Redis to współdzielony flat-namespace (jedna baza `0` dla serwisu!) — bez prefixu klucz `abc-123` zderzyłby się z rate-limitem gateway (`ratelimit:...` ma własny prefix), przyszłym pub/sub i czymkolwiek innym. Prefix to konwencja całego repo (gateway: `ratelimit:`, tutaj: `idempotency:`). Dla laika: jak nazwisko w książce telefonicznej — samo imię nie wystarcza.
- **Linia `nx=True`:** `NX` = „set only if Not eXists" (zajmij, jeśli wolne). Atomowe na poziomie serwera Redis (jednowątkowy — brak wyścigu między „sprawdź" a „ustaw", w przeciwieństwie do GET+SET w dwóch wywołaniach). To jest cała synchronizacja rozproszona serwisu w jednym flagu.
- **Linia `ex=ttl_seconds`:** `EX` = wygaśnięcie w sekundach (TTL 24 h). Bez TTL klucz żyłby wiecznie (wyciek pamięci + wieczna blokada klucza — formularz sprzed miesiąca blokowałby ten sam UUID... w praktyce klucze są losowe, ale higiena). `SET ... NX EX` w **jednym** wywołaniu (nie `SETNX` + osobne `EXPIRE` — między nimi crash zostawiłby klucz bez TTL = wieczny).
- **Linia `return result is not None`:** `redis-py` zwraca `True`/`None` (nie bool False!) przy `nx=True`: `True` = ustawiono (klucz był wolny), `None` = nie ustawiono (zajęty). `is not None` mapuje na kontrakt portu (`bool`). Pułapka: `bool(None) == False`, więc `return bool(result)` też by działało — ale `is not None` jest jawne i niezależne od „truthiness" przyszłych typów zwrotów.
- **Przykład:**
  ```python
  await store.acquire("k1", "loan-X")   # True  (wolny → zajęty)
  await store.acquire("k1", "loan-Y")   # False (zajęty → loan-Y odrzucone)
  await store.get("k1")                 # "loan-X" (pierwszy wygrywa)
  ```

#### Blok 3: `get` — odczyt z tolerancją typów

```python
    async def get(self, key: str) -> str | None:
        """Get stored loan ID or None."""
        result = await self._redis.get(f"idempotency:{key}")
        if result is None:
            return None
        if isinstance(result, bytes):
            return result.decode()
        return str(result)
```

- **Linia `if result is None: return None`:** brak klucza = pierwszy raz (use case idzie dalej, nie replay). Rozróżnienie None-vs-wartość to cała logika replay.
- **Linie `isinstance(result, bytes)`:** klient utworzony z `decode_responses=True` zwraca `str`, bez — `bytes`. Adapter toleruje oba (defensywa na wypadek zmiany `init_redis` lub innego klienta w testach). `.decode()` (UTF-8 default — loan.id to ASCII, więc bezpieczne). Ostatni `str(result)` łapie wszystko inne (np. `memoryview` z niektórych sterowników).
- **Brak TTL-refresh przy `get`:** odczyt nie przedłuża życia klucza (klucz wygasa 24 h po `acquire`, niezależnie od replay). Alternatywa (sliding expiration) przedłużałaby przy każdym replay — niepotrzebne (replaye dzieją się w sekundach po utworzeniu, nie po dniach).

**Podsumowanie pliku:** 24 linie, zero logiki biznesowej, cała mądrość w semantyce `SET NX EX`. Gdy Redis padnie, `acquire` rzuci (`ConnectionError`) → rollback wniosku (§4.7 Blok 7) — bezpieczne, ale niedostępne; hardening w §11.

---

### 4.13 Plik: `src/infrastructure/database.py` (62 linie)

**Cel:** singleton silnika SQLAlchemy + fabryka sesji request-scoped z semantyką transakcyjną (commit/rollback). To tutaj materializuje się „jedna transakcja na request" — fundament outbox. (Applicant ma niemal identyczny plik; różnice: brak `AsyncSession` scope'ów per use case, ten sam `NullPool`.)

#### Blok 1: klasa `Database`

```python
"""Database engine and session management."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool


class Database:
    """Holds engine + session factory (one per process)."""

    def __init__(self, url: str) -> None:
        self._engine = create_async_engine(url, poolclass=NullPool)
        self._session_factory = async_sessionmaker(
            self._engine, class_=AsyncSession, expire_on_commit=False
        )
```

- **Linia `create_async_engine(url, poolclass=NullPool)`:** silnik asynchroniczny na sterowniku `asyncpg` (URL `postgresql+asyncpg://...`). `NullPool` = brak poolingu po stronie aplikacji (każde połączenie otwierane/zamykane na żądanie; pula po stronie PgBouncer/serwera w produkcji). Dlaczego nie domyślny `QueuePool`? Trzy powody: (a) worker tworzy własne silniki per tick (pooling i tak nie współdzielony), (b) testy z testcontainers: pool trzymałby połączenia między testami (wycieki, „database is being accessed by other users" przy drop), (c) obciążenie MVP jest małe (requesty krótkie, handshake lokalny ~1 ms). W produkcji z setkami RPS: `QueuePool`/`AsyncAdaptedQueuePool` + PgBouncer — odnotowane jako已知 trade-off.
- **Linia `async_sessionmaker(..., expire_on_commit=False)`:** fabryka sesji. `expire_on_commit=False` — po commicie atrybuty encji/modeli **nie wygasają** (bez tego pierwszy dostęp do `loan.status` po commicie robiłby lazy-refresh na (już zwróconej do puli) sesji → `DetachedInstanceError` albo ciche zapytanie w tle). Z `False`: obiekty używalne po commicie (use case robi `model_validate(loan)` po pracy — encje, nie modele, więc i tak bezpieczne; flaga to pas bezpieczeństwa).
- **`AsyncGenerator` + `Optional` w importach:** typy dla `get_session` (generator sesji) i `_db` (singleton, None przed init).

```python
    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        return self._session_factory

    async def create_all(self) -> None:
        from src.infrastructure.persistence.models import Base

        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def close(self) -> None:
        await self._engine.dispose()
```

- **Linia `session_factory` (property):** read-only dostęp do fabryki (enkapsulacja: nikt z zewnątrz nie podmieni fabryki na polu).
- **Linie `create_all`:** tworzenie schematu z metadanych (bez Alembic!). Import `Base` **wewnątrz metody** (defer — unika cyklu importów przy starcie: `database` ← `models`? nie, `models` nie importuje `database`; defer tu to ostrożność/konwencja). `engine.begin()` (transakcja DDL) + `run_sync` (most async→sync: `create_all` jest synchroniczne, więc wykonane w wątku połączenia). Wołane w `init_database` (dev) i `conftest` (testy). Produkcja: Alembic (§11 luka #11).
- **Linia `close`:** `dispose` zamyka wszystkie połączenia silnika (zwolnienie socketów przy shutdown). Bez tego proces wisiałby na otwartych gniazdach (Docker `stop` z timeoutem → SIGKILL → „brudne" rozłączenia w logach Postgresa).

#### Blok 2: singleton modułowy i `get_session`

```python
_db: Optional[Database] = None


def get_database() -> Database:
    assert _db is not None, "Database not initialized"
    return _db


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    db = get_database()
    async with db.session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
```

- **Linia `_db = None`:** singleton modułowy (moduł Pythona importowany raz = naturalny singleton, jak `_redis` w gateway). `None` = „nie wystartowano".
- **Linie `get_database`:** `assert` (nie `if/raise`!) — niezmiennik programisty, nie błąd runtime („baza MUSI być zainicjalizowana przed pierwszym requestem; jeśli nie, to bug w lifespan, nie sytuacja do obsługi"). `assert` ginie przy `python -O` (optymalizacja wyłącza asserty!) — w produkcji kontenerowej `-O` się nie używa, więc akceptowane; purysta napisałby `if _db is None: raise RuntimeError`.
- **Linia `async with ... session_factory() as session`:** sesja na request (otwarcie przy wejściu, zamknięcie przy wyjściu — context manager).
- **Linia `yield session`:** generatorowa dependencja FastAPI — wszystko między `yield` a końcem endpointu (endpoint → use case → repozytoria z `flush`) dzieje się **przed** commitem. Dla laika: `yield` to „pauza": FastAPI dostaje sesję, robi swoje, wraca tu po odpowiedź.
- **Linia `await session.commit()`:** JEDEN commit na request (wniosek + 2 eventy — atomowość outbox!). Sukces = wszystko trwale.
- **Linie `except → rollback → raise`:** dowolny wyjątek (walidacja, UNIQUE, pad bazy) = wycofanie całości + propagacja (FastAPI zamieni na 500/dedykowany kod). Brak połowicznych zapisów („wniosek bez eventów" nie istnieje).
- **Linia `finally → close`:** sesja zamykana zawsze (sukces, błąd, anulowanie). Bez tego wyciek sesji = wyciek połączenia z puli = po N requestach brak połączeń (klasyczna awaria „działało godzinę i stanęło").

```python
async def init_database(url: str) -> None:
    global _db
    _db = Database(url)
    await _db.create_all()


async def close_database() -> None:
    global _db
    if _db:
        await _db.close()
        _db = None
```

- `init_database`: konstrukcja + `create_all` (dev-wygoda). `global _db` (modyfikacja zmiennej modułowej — bez `global` powstałaby lokalna i singleton by nie zadziałał; ten sam motyw co `_redis` w gateway).
- `close_database`: strażnik `if _db` (podwójny stop nie crashuje) + czyszczenie referencji (GC, brak wiszącego zamkniętego silnika).

**Podsumowanie pliku:** 62 linie, cała semantyka transakcyjna serwisu. Kto rozumie `get_session`, rozumie outbox: „flush w use case, commit na wyjściu, rollback przy błędzie".

---

### 4.14 Plik: `src/api/routes/loans.py` (44 linie)

**Cel:** dwa endpointy HTTP — cienka warstwa nad use case'ami. Zero logiki biznesowej, zero SQL, zero wyjątków domenowych (te lecą w górę do przyszłych handlerów). Plik importuje DTO i fabryki, nie implementacje (routes znają porty przez typy use case'ów, nie klasy SQLAlchemy). (Applicant ma `routes/auth.py` + `routes/me.py` z tą samą filozofią; tutaj jeden plik, bo dwa endpointy.)

#### Blok 1: importy i router

```python
"""Loan application HTTP routes."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Header

from src.api.dependencies import (
    get_create_loan_application_use_case,
    get_current_applicant_id,
    get_get_loan_application_use_case,
)
from src.application.dto import CreateLoanApplicationRequest, LoanApplicationResponse
from src.application.use_cases.create_loan_application import CreateLoanApplicationUseCase
from src.application.use_cases.get_loan_application import GetLoanApplicationUseCase
```

- Importy z trzech warstw (api-dependencies, application-dto/use-cases) — routes to klej między HTTP a use case'ami, więc zależą w obie „dozwolone" strony. Nie importują `sqlalchemy`, `redis`, `aiokafka` (infrastruktura schowana za fabrykami).
- `Header` (nie `Query`/`Body`/`Path`) — `Idempotency-Key` to nagłówek (metadane transportu, nie treść). Dla laika: `Body` = „co", `Path`/`Query` = „które", `Header` = „jak/kontekst".

```python
router: APIRouter = APIRouter(prefix="/loans", tags=["loans"])
```

- Adnotowany `router: APIRouter` (nie gołe `router = ...`) — mypy strict wymaga adnotacji zmiennych modułowych? Nie wymaga, ale projekt adnotuje dla czytelności (jak `producer: str` w kopercie). `prefix="/loans"` + `include_router(..., prefix="/api/v1")` w `main.py` = pełna ścieżka `/api/v1/loans`. Prefix na routerze (nie w dekoratorach) — zmiana wersji API (`/api/v2`) to jedna linia w `main`, nie edycja każdego endpointu. `tags=["loans"]` — grupowanie w Swagger UI (wszystkie endpointy wniosków pod jednym nagłówkiem).

#### Blok 2: `POST /loans` — 202, nie 200

```python
@router.post(
    "",
    response_model=LoanApplicationResponse,
    status_code=202,
    summary="Submit a new loan application",
)
async def create_loan_application(
    request: CreateLoanApplicationRequest,
    applicant_id: UUID = Depends(get_current_applicant_id),
    use_case: CreateLoanApplicationUseCase = Depends(get_create_loan_application_use_case),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> LoanApplicationResponse:
    """Submit a new loan application (async processing)."""
    return await use_case.execute(applicant_id, request, idempotency_key)
```

- **Linia `@router.post("", ...)`:** pusty string (nie `"/"`) — przy prefixie daje dokładnie `/api/v1/loans` (bez końcowego slasha). `"/"` dałoby `/api/v1/loans/` (inny URL! FastAPI przekierowałby 307, ale klienci z `Idempotency-Key` i przekierowaniami miewają problemy — POST + redirect = ryzyko zgubienia body w niektórych klientach). Pusty string eliminuje klasę problemów.
- **Linia `response_model=LoanApplicationResponse`:** FastAPI filtruje i waliduje odpowiedź (gdyby use case zwrócił coś z extra polem, klient go nie zobaczy; gdyby brakowało pola — 500 z czytelnym błędem serializacji, nie cichy brak). Podwójna walidacja (Pydantic w use case + Pydantic w response) to celowa redundancja na granicy.
- **Linia `status_code=202`:** **jedyny 202 w systemie** (Applicant zwraca 200 wszędzie). Semantyka HTTP: 200 = „zrobione", 201 = „utworzone i gotowe", 202 = „przyjęte do przetworzenia" (asynchronicznie). Frontend po 202 **nie czeka** — otwiera SSE na `/events` (kontrakt §5.6). Gdyby było 200, klient myślałby, że decyzja zapadła (a scoring nawet nie ruszył — wniosek dopiero czeka w outbox!).
- **Linia `summary=...`:** tytuł w Swagger UI (krótki, ludzki; `description` — długi, opcjonalny — tu pominięty, bo DTO niosą opisy pól).
- **Linia `request: CreateLoanApplicationRequest`:** body JSON → walidacja Pydantic **przed** wejściem do funkcji (zły `amount` = 422, use case nie powstaje, baza nie dotknięta). Kolejność parametrów: ciało pierwsze (konwencja FastAPI — ciało to „główny" argument).
- **Linia `applicant_id = Depends(get_current_applicant_id)`:** tożsamość z nagłówka (nie z body! — klient nie deklaruje, kim jest; mówi mu to gateway). `Depends` bez nawiasów z funkcją (nie `Depends(...)` z wywołaniem — FastAPI woła samo, per request).
- **Linia `use_case = Depends(...)`:** fabryka buduje use case z repozytoriami na tej sesji (transakcja!). Typ adnotacji to konkretna klasa (nie port — routes znają use case'y, use case'y znają porty; każdy poziom zależy od konkretu poziomu niżej i abstrakcji jeszcze niżej).
- **Linia `idempotency_key = Header(default=None, alias="Idempotency-Key")`:** `alias` (nazwa w HTTP z myślnikami i wielkością), `default=None` (nagłówek opcjonalny — brak = `None` = brak idempotencji). Nazwa parametru w Pythonie snake_case (`idempotency_key`), w HTTP kebab-case (`Idempotency-Key`) — alias je spina. Bez aliasu FastAPI szukałby nagłówka `idempotency-key` (małe litery działają w HTTP/2, ale konwencja SPEC §5.4 to `Idempotency-Key`).
- **Linia `return await use_case.execute(...)`:** cała funkcja to jedna linia delegacji (plus sygnatura). Zero `try/except` (wyjątki lecą do handlerów — dziś ich brak, więc 500; §11 luka #3). Cienka warstwa: endpoint tłumaczy HTTP→Python i Python→HTTP, niczego nie decyduje.
- **Przykład request/response:**
  ```http
  POST /api/v1/loans HTTP/1.1
  X-User-ID: 550e8400-e29b-41d4-a716-446655440000
  Idempotency-Key: 7c9e6679-7425-40de-944b-e07fc1f90ae7
  Content-Type: application/json

  {"amount": 15000, "term_months": 24, "monthly_income": 6000, "applicant_age": 30}
  ```
  ```http
  HTTP/1.1 202 Accepted
  {"id":"...","applicant_id":"550e...","amount":"15000.00","status":"SUBMITTED",...}
  ```

#### Blok 3: `GET /loans/{loan_id}` — odczyt właścicielski

```python
@router.get(
    "/{loan_id}",
    response_model=LoanApplicationResponse,
    summary="Get a loan application by ID",
)
async def get_loan_application(
    loan_id: UUID,
    applicant_id: UUID = Depends(get_current_applicant_id),
    use_case: GetLoanApplicationUseCase = Depends(get_get_loan_application_use_case),
) -> LoanApplicationResponse:
    """Get a loan application by ID (owner only)."""
    return await use_case.execute(loan_id, applicant_id)
```

- **Linia `"/{loan_id}"`:** parametr ścieżki (klamry). FastAPI parsuje segment URL do `UUID` (**przed** wejściem do funkcji): zły format (`/loans/nie-uuid`) = 422 z czytelnym błędem, use case nie rusza. Typ `UUID` (nie `str`!) — parsowanie na granicy, nie w logice.
- **Brak `idempotency_key`:** GET jest idempotentny z natury (N odczytów = 1 odczyt) — klucz niepotrzebny. SPEC §5.4 wymaga klucza tylko dla POST.
- **Brak `GET /loans` (lista):** dashboard „moje wnioski" nie ma endpointu (repo i DTO gotowe — §4.5/§4.4; luka #4 w §11). Kolejność w pliku (POST przed GET) odzwierciedla chronologię życia wniosku (najpierw utwórz, potem czytaj).
- **Co gdyby endpoint sprawdzał właściciela sam (bez use case)?** Logika autoryzacji w dwóch miejscach (routes + use case) albo tylko w routes (use case bezbronny przy wywołaniu z innego miejsca — np. przyszłego CLI admina). Obecnie: routes dowożą tożsamość, use case egzekwuje — jedno miejsce decyzji.

**Podsumowanie pliku:** 44 linie, dwie funkcje-jednolinijkowce. Cienkość to zaleta: każda linia logiki w routes to linia, której nie da się przetestować bez HTTP (testy use case'ów omijają routes — szybkie, bez TestClienta).

---

### 4.15 Plik: `src/api/dependencies.py` (91 linii)

**Cel:** korzeń kompozycji (composition root) — jedyne miejsce, które wie, jak złożyć system z klocków: singleton Redis, fabryki repozytoriów (na sesji requestu), fabryki use case'ów, strażnik tożsamości `X-User-ID`. (Applicant ma analogiczny plik z fabrykami hashera/JWT; tutaj zamiast kryptografii — Redis i outbox. Gateway używa singletonów modułowych zamiast `Depends` — tutaj `Depends`, bo sesja jest per-request.)

#### Blok 1: importy

```python
"""FastAPI dependencies (composition root)."""

from __future__ import annotations

from typing import Optional
from uuid import UUID

import redis.asyncio as aioredis
from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.ports.idempotency import IdempotencyStore
from src.application.use_cases.create_loan_application import CreateLoanApplicationUseCase
from src.application.use_cases.get_loan_application import GetLoanApplicationUseCase
from src.infrastructure.database import get_session
from src.infrastructure.persistence.repository import (
    SQLAlchemyLoanApplicationRepository,
    SQLAlchemyOutboxRepository,
)
from src.infrastructure.redis.idempotency import RedisIdempotencyStore
```

- Importy ze wszystkich czterech warstw (api je spina — to jest jej praca). `status` z FastAPI (stałe `HTTP_401_UNAUTHORIZED` zamiast literału 401 — czytelność + odporność). `Optional` (stary styl — plik używa `Optional[str]` w dwóch miejscach, choć reszta repo to `X | None`; drobna niespójność stylistyczna, ruff `UP045` zignorowany w configu właśnie po to).
- **Brak importu `ProcessedEventRepository`:** żaden use case go nie potrzebuje (brak konsumentów) — fabryki nie kłamią o zależnościach.

#### Blok 2: singleton Redis + store

```python
_redis_client: Optional[aioredis.Redis] = None
_idempotency_store: Optional[IdempotencyStore] = None


def get_redis_client() -> aioredis.Redis:
    if _redis_client is None:
        raise RuntimeError("Redis not initialized")
    return _redis_client


def get_idempotency_store() -> IdempotencyStore:
    global _idempotency_store
    if _idempotency_store is None:
        _idempotency_store = RedisIdempotencyStore(get_redis_client())
    return _idempotency_store


def init_redis(redis_url: str) -> None:
    global _redis_client
    _redis_client = aioredis.from_url(redis_url, decode_responses=True)


async def close_redis() -> None:
    global _redis_client, _idempotency_store
    if _redis_client:
        await _redis_client.close()
        _redis_client = None
        _idempotency_store = None
```

- **Linie zmiennych modułowych:** dwa singletony (`_redis_client`, `_idempotency_store`), oba `None` przed startem. Moduł jako singleton (import raz na proces — ten sam motyw co `_redis` w gateway i `_db` w `database.py`).
- **Linie `get_redis_client`:** strażnik z `RuntimeError` (nie `assert` jak w `get_database` — tu jawny wyjątek, bo wołane też spoza requestu; niejednolitość `assert` vs `raise` między plikami to drobny dług stylistyczny). Fail-fast: zła kolejność startu (request przed `init_redis`) = czytelny błąd, nie `AttributeError: 'NoneType'`.
- **Linie `get_idempotency_store`:** lazy singleton (tworzony przy pierwszym użyciu, nie w `init_redis`). Dlaczego lazy? Store zależy od klienta (klient od URL) — tworzenie w `init_redis` też by działało; lazy rozdziela „połączenie" od „opakowania" (opakowanie tanie, bezstanowe). `global` (modyfikacja modułowej — bez niego powstałaby lokalna).
- **Linia `init_redis`:** `from_url(redis_url, decode_responses=True)` — klient ze stringami (nie bytes; adapter toleruje oba — §4.12). Konstrukcja synchroniczna (nie łączy! — połączenie leniwe przy pierwszym `SET`/`GET`). Wołana w lifespan (eager — fail-fast na złym URL? Nie do końca: zły host wyjdzie dopiero przy pierwszym użyciu... `from_url` nie waliduje połączenia. Prawdziwy fail-fast wymagałby `await client.ping()` w lifespan — hardening na przyszłość).
- **Linie `close_redis`:** `await close()` (zwolnienie socketów) + wyzerowanie obu zmiennych (store trzyma starego klienta — musi zniknąć razem z nim, inaczej lazy-singleton zwróciłby store z zamkniętym klientem po restarcie!). `if _redis_client` (podwójny stop bezpieczny). Asynchroniczna (I/O zamknięcia), w przeciwieństwie do synchronicznego `init_redis` (konstrukcja bez I/O) — asymetria uzasadniona naturą operacji.

#### Blok 3: fabryki repozytoriów — ta sama sesja!

```python
def get_loan_application_repo(
    session: AsyncSession = Depends(get_session),
) -> SQLAlchemyLoanApplicationRepository:
    return SQLAlchemyLoanApplicationRepository(session)


def get_outbox_repo(
    session: AsyncSession = Depends(get_session),
) -> SQLAlchemyOutboxRepository:
    return SQLAlchemyOutboxRepository(session)
```

- **Kluczowy mechanizm:** obie fabryki deklarują `Depends(get_session)` — FastAPI **współdzieli** wynik dependencji w ramach jednego requestu (cache per-request). Oba repozytoria dostają **tę samą sesję** = tę samą transakcję = atomowość outbox (wniosek + 2 eventy, jeden commit). Gdyby każde tworzyło własną sesję (dwa `get_session()` wołane wprost), miałyby dwie transakcje (okno niespójności!). Ten cache to techniczny warunek całego §8.3.
- Funkcje synchroniczne (konstrukcja, nie I/O). Zwracają konkrety (nie porty) — bo to composition root (jedyne miejsce, które zna konkrety; use case'y dostają je jako porty przez adnotacje w konstruktorach... konkretnie: konstruktor przyjmuje `LoanApplicationRepository` (port), a dostaje `SQLAlchemy...` (konkret) — podstawienie Liskov w akcji.

#### Blok 4: fabryki use case'ów

```python
def get_create_loan_application_use_case(
    loan_repo: SQLAlchemyLoanApplicationRepository = Depends(get_loan_application_repo),
    outbox_repo: SQLAlchemyOutboxRepository = Depends(get_outbox_repo),
    idempotency_store: IdempotencyStore = Depends(get_idempotency_store),
) -> CreateLoanApplicationUseCase:
    return CreateLoanApplicationUseCase(loan_repo, outbox_repo, idempotency_store)


def get_get_loan_application_use_case(
    loan_repo: SQLAlchemyLoanApplicationRepository = Depends(get_loan_application_repo),
) -> GetLoanApplicationUseCase:
    return GetLoanApplicationUseCase(loan_repo)
```

- Czyste fabryki (nowy use case na request — bezstanowe, tanie; brak sensu singletonizować). Adnotacje parametrów to konkrety (bo `Depends` zwraca konkrety), konstruktor przyjmuje je jako porty (podstawienie). Zamiana adaptera w testach API (`app.dependency_overrides[get_loan_application_repo] = fake`) nie dotyka routes ani use case'ów — klin testowy w jednym miejscu.
- Trzy vs jedna zależność — asymetria dokumentuje różnicę operacji (zapis z efektami ubocznymi vs czysty odczyt).

#### Blok 5: `get_current_applicant_id` — strażnik bramy wewnętrznej

```python
async def get_current_applicant_id(
    x_user_id: Optional[str] = Header(None, alias="X-User-ID"),
) -> UUID:
    if x_user_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-User-ID header",
        )
    try:
        return UUID(x_user_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid X-User-ID header",
        )
```

- **Linia `Header(None, alias="X-User-ID")`:** nagłówek opcjonalny (`None` gdy brak — to funkcja decyduje, nie FastAPI). Alias (kebab-case w HTTP, snake_case w Pythonie — jak `Idempotency-Key` w routes).
- **Linia `if x_user_id is None: raise 401`:** brak nagłówka = nieuwierzytelniony (401, nie 400/500 — to kwestia tożsamości). Komunikat rozróżnia „brak" od „zły format" (dwa różne `detail`) — pomocne w logach gateway-vs-serwis (rozróżnienie „gateway nie wstrzyknął" od „wstrzyknął śmieci"). Klientem jest gateway (sieć wewnętrzna), nie atakujący — tu jawność pomaga, nie szkodzi (odwrotnie niż przy maskowaniu 404 dla ludzi).
- **Linie `try UUID / except ValueError → 401`:** walidacja formatu (UUID z myślnikami, 36 znaków, hex). Śmieci (`"admin"`, `"1"`, SQL-injection w nagłówku) = 401, nie 422: klientem jest gateway, nie człowiek — 401 mówi „nie przeszedłeś przez bramę", 422 sugerowałoby „popraw formularz" (bez sensu dla maszyny). `UUID()` rzuca `ValueError` (nie `TypeError` — input zawsze string lub None, a None obsłużone wyżej).
- **`async def` mimo braku `await`:** wymóg konwencji (dependencje FastAPI mogą być async; spójność z resztą łańcucha). Puste `async` kosztuje jedno utworzenie coroutine na request (nanosekundy).
- **Brak weryfikacji JWT:** zaufanie do sieci wewnętrznej (gateway sprawdził podpis RS256, wstrzyknął ID; serwis ufa — §7.1). Serwis nie ma klucza publicznego ani biblioteki JWT w zależnościach (`pyproject` nie zawiera `python-jose` — celowo!).

**Podsumowanie pliku:** 91 linii, zero logiki biznesowej, cała „elektryka" systemu. Kto czyta ten plik, widzi graf zależności bez wchodzenia w endpointy.

---

### 4.16 Plik: `src/main.py` (73 linie)

**Cel:** aplikacja FastAPI + cykl życia (lifespan): DB → Redis → OutboxWorker → `yield` (serwis żyje) → stop w odwrotnej kolejności. Wejście procesu (`uvicorn src.main:app`). Healthchecki `/health` + `/ready`.

#### Blok 1: logging i lifespan — start

```python
"""FastAPI application for Loan Application Service."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress

from crediguard_observability import configure_logging, get_logger
from fastapi import FastAPI

from src.api.routes.loans import router as loans_router

configure_logging("loan-application-service")
logger = get_logger()
```

- **Linie `configure_logging("loan-application-service")`:** JSON-logi (structlog) z nazwą serwisu w każdym wpisie (pole `service` — grep po usłudze w `docker compose logs`). Wołane na imporcie (przed lifespan — by logi startu też były strukturalne). Import `loans_router` z aliasem (`as loans_router` — bo `router` to zbyt ogólna nazwa w tej przestrzeni; jawny alias eliminuje kolizję z przyszłymi routerami).
- **Importy `asyncio`/`suppress` na górze** (nie defer jak w workerze — `main` to entrypoint, wszystko jawnie; inna konwencja niż `producer.py`, spójna z gateway `main.py`).

```python
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    import os

    from src.api.dependencies import close_redis, init_redis
    from src.infrastructure.database import close_database, init_database
    from src.infrastructure.kafka.producer import OutboxWorker

    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://loan_svc:loan_dev_pw@localhost:5433/loan_db",
    )
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6380/0")
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")

    await init_database(database_url)
    init_redis(redis_url)
    logger.info("Database and Redis initialized")

    outbox_worker = OutboxWorker(
        bootstrap_servers=kafka_bootstrap,
        database_url=database_url,
    )
    await outbox_worker.start()
    worker_task = asyncio.create_task(outbox_worker.run())
    logger.info("Outbox worker started")

    logger.info("Loan Application Service started")
    yield
```

- **Linia `@asynccontextmanager`:** dekorator zamieniający generator async w menedżer kontekstu (FastAPI `lifespan=` wymaga właśnie tego kształtu: kod przed `yield` = start, po = stop). Dla laika: wszystko przed `yield` dzieje się przy starcie serwera, `yield` to „serwis pracuje", wszystko po — przy zamykaniu.
- **Linia `import os` wewnątrz (defer):** stdlib też deferowany (konsekwencja stylu „ciężkie importy późno"? `os` jest lekkie — tu defer to raczej nawyk autora niż potrzeba; nieszkodliwe). Importy infrastruktury wewnątrz lifespan: `app` importuje się bez żywych zależności (testy unit importują `src.main`? nie muszą startować DB — lifespan nie wykonuje się przy imporcie).
- **Linie `os.getenv(...)`:** konfiguracja z env z defaultami dev (konwencja CrediGuard: Postgres **5433** (nie 5432 — by nie kolidować z lokalnym serwerem dewelopera), Redis **6380** (nie 6379), Kafka **9094** (zewnętrzny listener KRaft; wewnętrzny w sieci Docker to zwykle 9092 — default celuje w dev-lokalny, compose nadpisze). Kredencjale dev w defaulcie (`loan_svc:loan_dev_pw`) — wygoda lokalna; produkcja nadpisuje env (sekrety nigdy w kodzie — reguła CLAUDE.md; default to nie sekret, tylko dev-śmieć).
- **Linia `await init_database`:** silnik + `create_all` (schemat gotowy przed pierwszym requestem). `await` (I/O: połączenie + DDL).
- **Linia `init_redis`:** synchroniczna (konstrukcja klienta bez I/O — patrz §4.15). Nie-awaitowana celowo (nie coroutine).
- **Linie workera:** konstrukcja (parametry, bez I/O) → `await start()` (połączenie z brokerami — pad Kafki = błąd startu = serwis nie startuje... czy to dobrze? Kontrowersja: API mogłoby działać bez Kafki (outbox czeka PENDING!), ale lifespan nie rozróżnia — start wymaga brokerów. DoD Etapu 3 („ubicie Kafki nie gubi") dotyczy pracy, nie startu. Hardening: start workera z retry / lazy-connect — przyszłość).
- **Linia `asyncio.create_task(outbox_worker.run())`:** worker jako background-task na **tym samym** event loop co API (I/O-bound polling — GIL nie grozi, w przeciwieństwie do Underwriting z ProcessPool). Task trzymany w zmiennej (by go anulować przy stop).
- **Linie logów:** trzy kamienie milowe startu (DB+Redis, worker, serwis) — przy diagnozie „serwis nie wstał" widać, na którym kroku stanął.

#### Blok 2: lifespan — stop (kolejność odwrotna!)

```python
    await outbox_worker.stop()
    worker_task.cancel()
    with suppress(asyncio.CancelledError):
        await worker_task
    await close_redis()
    await close_database()
    logger.info("Loan Application Service stopped")
```

- **Linia `stop()`:** flaga False + flush producenta (wysłane dostają szansę na ACK).
- **Linie `cancel()` + `suppress(CancelledError)`:** anulowanie taska (gdyby spał w `sleep(0.5)`) + połknięcie oczekiwanego `CancelledError` (anulowanie to planowane zakończenie, nie błąd — bez `suppress` shutdown logowałby traceback przy każdym wdrożeniu). Dla laika: `cancel()` to „przestań spać i wyjdź", a `CancelledError` to „wychodzę" — nie błąd, więc tłumimy.
- **Kolejność:** worker → Redis → baza (odwrotna do startu: baza → Redis → worker). Zasada: zamykaj w odwrotnej kolejności otwierania (to, co powstało ostatnie, znika pierwsze — worker mógłby jeszcze chcieć bazy/Redisa przy domknięciu ticka; baza zamykana ostatnia, bo wszyscy jej potrzebują).
- **Brak obsługi SIGTERM/SIGINT wprost:** uvicorn tłumaczy sygnały na zamknięcie lifespan (standard — nie trzeba pisać handlerów).

#### Blok 3: aplikacja i healthchecki

```python
app = FastAPI(
    title="CrediGuard Loan Application Service",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(loans_router, prefix="/api/v1")


@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready", tags=["health"])
async def ready() -> dict[str, str]:
    return {"status": "ready"}
```

- **Linie `FastAPI(...)`:** `title` (Swagger UI), `version` (nagłówek docs + `/openapi.json`), `lifespan` (cykl życia). Brak `exception_handlers` (luka #3 — tu powinny trafić mapowania domenowe!), brak middleware (correlation ID dokleja gateway; serwis nie ma własnego — krawędź obserwowalności).
- **Linia `include_router(..., prefix="/api/v1")`:** wersjonowanie API w jednym miejscu (router niesie `/loans`, main dokleja `/api/v1`; V2 = drugi include, stara żyje).
- **Linie `/health`:** liveness („proces żyje") — zawsze `{"status": "ok"}`, zero zależności (nawet przy padłej bazie odpowiada 200 — celowo: orchestrator restartuje tylko przy martwym procesie, nie przy chorej bazie).
- **Linie `/ready`:** readiness („mogę przyjmować ruch") — **STUB** (zawsze `ready`, nie pinguje DB/Redis/Kafka!). Konsekwencja: orchestrator (`depends_on: service_healthy` w compose) uzna serwis za gotowy, choć baza jeszcze wstaje → pierwsze requesty dostaną 500 (krótka niedostępność po deploy). Poprawka: `SELECT 1` + `PING` + metadata Kafki (§11 luka #10). Liveness vs readiness (dla laika): liveness = „żyje" (restartuj, gdy nie), readiness = „zdrowy" (nie ślij ruchu, gdy nie) — dwa różne pytania, dwa endpointy.
- **Typ `-> dict[str, str]`:** adnotacja zwrotu (mypy strict; FastAPI serializuje do JSON automatycznie).

**Podsumowanie pliku:** 73 linie, cały cykl życia procesu. Kto rozumie lifespan, rozumie kolejność startu (baza→Redis→Kafka) i stopu (odwrotnie) — reguła do kopiowania w każdym kolejnym serwisie.

---

### 4.17 Plik: `alembic/versions/0001_initial.py` (87 linii) i `alembic/env.py` (59 linii)

**Cel:** wersjonowana migracja początkowa (produkcja) + asynchroniczne środowisko Alembic. Dwa tryby schematu w repo: `create_all` (dev/test, §4.13) i migracje (prod). Ten plik to tryb prod.

#### Blok 1: nagłówek rewizji

```python
"""Initial migration: create loan_applications, outbox_events, processed_events tables.

Revision ID: 0001
Revises:
Create Date: 2026-01-01 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None
```

- **Docstring z metadanymi** (konwencja Alembic: opis + rewizja + data). `revision = "0001"` (prosty, czytelny — nie hash jak z `alembic revision --autogenerate`; ręczna numeracja działa przy jednej linii migracji). `down_revision = None` (pierwsza migracja — brak rodzica). `branch_labels`/`depends_on = None` (bez gałęzi — single-head, najprostszy graf).
- **Importy:** `op` (operacje DDL: create_table/drop_table/create_index), `sa` (typy generyczne), `postgresql` (UUID postgresowy). Dwa dialekty typów (tu `sa.func.now()`/`postgresql.UUID`, w modelach `text("now()")`/`PG_UUID`) — równoważne semantycznie, różne w zapisie (migracja pisana „językiem Alembic", modele „językiem ORM").

#### Blok 2: `upgrade()` — trzy tabele

```python
def upgrade() -> None:
    op.create_table(
        "loan_applications",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("applicant_id", postgresql.UUID(as_uuid=True), nullable=False, index=True),
        sa.Column("amount", sa.Numeric(12, 2), nullable=False),
        sa.Column("term_months", sa.Integer(), nullable=False),
        sa.Column("monthly_income", sa.Numeric(12, 2), nullable=False),
        sa.Column("applicant_age", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(50), nullable=False, server_default="DRAFT"),
        sa.Column("decision_reasons", sa.JSON(), nullable=True),
        sa.Column("idempotency_key", sa.String(255), unique=True, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
    )
```

- Lustro `LoanApplicationModel` kolumna-po-kolumnie (te same typy, te same constrainty). `index=True` wprost w kolumnie (Alembic utworzy indeks przy tabeli). `server_default="DRAFT"` (string literalny jako default DDL — surowe INSERT-y bez statusu dostaną DRAFT z bazy, spójnie z encją). `sa.func.now()` (funkcja bazy, nie `text()` — w migracji oba działają; `func` jest bardziej „ORM-owo").
- **Reguła spójności:** każda zmiana `models.py` wymaga nowej migracji (ręcznej lub `--autogenerate`) — inaczej prod (migracje) i dev (`create_all`) rozjadą się (dryf schematu, §11 luka #11). Review-reguła: PR dotykający modeli bez migracji = odrzuć (chyba że zmiana kosmetyczna).

```python
    op.create_table(
        "outbox_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("aggregate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(255), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="PENDING"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_outbox_pending",
        "outbox_events",
        ["status"],
        postgresql_where=sa.text("status = 'PENDING'"),
    )

    op.create_table(
        "processed_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), unique=True, nullable=False),
        sa.Column("event_type", sa.String(255), nullable=False),
        sa.Column("aggregate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
    )
```

- Outbox + partial index (osobne `create_index` po tabeli — partial nie da się wyrazić wprost w kolumnie; stąd dwa wywołania). `processed_events` z UNIQUE na `event_id` (idempotencja konsumencka na poziomie bazy od dnia zero).

#### Blok 3: `downgrade()` — odwrotność

```python
def downgrade() -> None:
    op.drop_table("processed_events")
    op.drop_index("ix_outbox_pending", table_name="outbox_events")
    op.drop_table("outbox_events")
    op.drop_table("loan_applications")
```

- Odwrotna kolejność (LIFO): to, co powstało ostatnie, znika pierwsze. Indeks przed tabelą (drop tabeli z indeksem i tak go usunie, ale jawność + kompatybilność z bazami, które wymagają kolejności). Downgrade w prod to ostateczność (utrata danych!), ale musi istnieć (Alembic wymaga pary; testy migracji w CI: upgrade→downgrade→upgrade).

#### Blok 4: `alembic/env.py` — async bridge

```python
from __future__ import annotations

from logging.config import fileConfig

from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context

from src.infrastructure.persistence.models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata
```

- `target_metadata = Base.metadata` — serce autogeneracji: Alembic porównuje metadane (modele) z bazą i generuje diff. Bez tego `--autogenerate` widziałby pustkę.
- `fileConfig` (logowanie Alembic z `alembic.ini`, nie structlog — migracje logują po swojemu, to tooling, nie serwis).

```python
def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(url=url, target_metadata=target_metadata,
        literal_binds=True, dialect_opts={"paramstyle": "named"})
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    import asyncio
    asyncio.run(run_async_migrations())
```

- **Dwa tryby:** offline (generowanie SQL bez bazy: `alembic upgrade head --sql` — do review migracji!) i online (prawdziwe DDL). Offline używa URL z configu (sync sterownik!), online — async engine.
- **`run_sync(do_run_migrations)`:** most async→sync (Alembic jest synchroniczny; `run_sync` wykonuje synchroniczną funkcję na asynchronicznym połączeniu). Standardowy przepis SQLAlchemy 2.0 — kopiowany między projektami 1:1.
- **`asyncio.run(...)` na dole:** entrypoint skryptowy (Alembic woła `env.py` jako skrypt, nie import — stąd tworzenie własnej pętli, nie użycie istniejącej).
- **`pool.NullPool`:** migracja to jednorazowy proces (połączenie na chwilę) — pooling bez sensu.

**Podsumowanie plików:** 146 linii razem, fundament wdrożeniowy. Dev ich nie widzi (`create_all`), prod bez nich nie istnieje (`alembic upgrade head` w pipeline).

---

## 5. Ścieżki wywołań endpointów i OutboxWorkera

### 5.1 `POST /api/v1/loans` — złożenie wniosku (happy path)

```
1.  Klient → Gateway (JWT ✓, X-User-ID wstrzyknięte, Idempotency-Key przekazany)
2.  → POST /api/v1/loans {"amount":15000,"term_months":24,...} + X-User-ID + Idempotency-Key
3.  routes.loans.create_loan_application
4.  → Depends(get_current_applicant_id): "550e..." → UUID(...) ✓
5.  → Depends(get_create_loan_application_use_case)
6.      → Depends(get_session): otwarcie transakcji
7.      → Depends(get_loan_application_repo / get_outbox_repo): ta sama sesja!
8.      → Depends(get_idempotency_store): Redis singleton
9.  → use_case.execute(applicant_id, request, key)
10.     a. Redis GET idempotency:{key} → None (pierwszy raz)
11.     b. SELECT id ... WHERE applicant_id AND status IN (aktywne) → brak → OK
12.     c. LoanApplication(...) + submit() → status SUBMITTED
13.     d. INSERT loan_applications (flush)
14.     e. INSERT outbox_events [submitted.v1] (flush)
15.     f. INSERT outbox_events [status.changed.v1] (flush)
16.     g. Redis SET idempotency:{key}={loan.id} NX EX 86400
17.     h. return LoanApplicationResponse (status SUBMITTED)
18. ← get_session: COMMIT (1 transakcja: wniosek + 2 eventy)
19. ← 202 Accepted {"id":"...","status":"SUBMITTED",...}
20. (tło, ≤500 ms później) OutboxWorker.poll_once:
     SELECT ... WHERE PENDING ORDER BY created_at LIMIT 10 FOR UPDATE SKIP LOCKED
     → send_and_wait × 2 (klucz = loan.id) → UPDATE SENT × 2 → COMMIT
```

### 5.2 Powtórzony `POST` z tym samym `Idempotency-Key`

```
10'. a. Redis GET → loan.id → SELECT ... → znaleziony → return TEJ SAMEJ odpowiedzi
     (kroki b–g pominięte; brak nowego wiersza, brak nowych eventów, brak 409)
```

### 5.3 Drugi wniosek innego klienta / duplikat tego samego klienta

- Inny `applicant_id` → guard przechodzi (sprawdzany per klient) → nowy wniosek.
- Ten sam klient, drugi wniosek bez klucza (lub z nowym) → `exists_active → True` → `DuplicateActiveApplication` → dziś 500 (brak handlera; docelowo 409 — §11).

### 5.4 `GET /api/v1/loans/{id}`

```
→ Depends(get_current_applicant_id) → GetLoanApplicationUseCase.execute(loan_id, applicant_id)
→ SELECT ... WHERE id → None → 404 (dziś 500); cudzy → 404-maskowanie; własny → 200
```

### 5.5 Cykl życia OutboxWorkera (oś czasu procesu)

```
start serwisu → init_database → init_redis → OutboxWorker.start()
  (połączenie z brokerami, _running=True)
  → create_task(run()) → pętla: poll_once → sleep(0.5) → poll_once → ...
  → SIGTERM (docker stop) → stop() (_running=False, flush producenta)
  → cancel(task) + suppress(CancelledError) → close_redis → close_database
```

- Worker nie ma własnych endpointów (niewidoczny z zewnątrz — stan widać tylko w logach `published events` i w tabeli `outbox_events`). Restart workerowi nie szkodzi (stan w bazie, nie w pamięci — §8.3).
- Tick na pustej kolejce: SELECT (ms) → `count=0` (cisza w logach) → sleep. Koszt pustego ticka: 1 zapytanie + handshake engine (luka #6) co 500 ms — ~2 QPS szumu na bazę (pomijalne).

### 5.6 Ścieżki błędów (co się psuje, gdzie i jakim kodem)

| Błąd | Gdzie wykryty | Dziś | Docelowo |
|------|---------------|------|----------|
| Zły JSON / zły typ w body | Pydantic (granica) | 422 | 422 ✓ (działa) |
| Zły `loan_id` w URL | FastAPI (parsowanie UUID) | 422 | 422 ✓ |
| Brak/zły `X-User-ID` | `get_current_applicant_id` | 401 | 401 ✓ |
| Powtórzony klucz | replay w use case | 202 (ta sama odpowiedź) | 202 + `Idempotent-Replay: true` |
| Drugi wniosek (aktywny) | guard w use case | 500 ✗ | 409 |
| Brak wniosku / cudzy | use case | 500 ✗ | 404 (maskowanie) |
| Nielegalna transicja (consumer) | encja `_transition` | 500 ✗ | 422 + alert |
| Pad bazy w trakcie | `get_session` (rollback) | 500 | 500 + retry klienta |
| Pad Redisa w trakcie | `acquire`/`get` | 500 ✗ | degradacja do baza-only |
| Pad Kafki | worker (log, retry w kółko) | 202 + PENDING (poprawnie!) | tak samo ✓ |

---

## 6. Koncepcje techniczne — słowniki, dekoratory, wzorce

> Ta sekcja to mini-kurs (jak §6 w applicant-guide): od zera wyjaśniamy konstrukcje Pythona użyte w serwisie, a potem wzorce architektoniczne. Senior może przeskoczyć do §6.7.

### 6.0 Słowniki, listy, sety, krotki — cztery pojemniki

Cała maszyna stanów to trzy struktury (§4.1). Dla laika — czym się różnią:

```python
lista = ["DRAFT", "SUBMITTED"]          # list: kolejność tak, duplikaty tak
krotka = (1, "Wniosek przyjęty...")     # tuple: jak lista, ale niemutowalna
zbior = {"APPROVED", "REJECTED"}        # set: bez kolejności, bez duplikatów
slownik = {"DRAFT": {"SUBMITTED"}}      # dict: klucz → wartość
```

- `list` (nawiasy `[]`): gdy liczy się kolejność (kolejność pól, batch eventów). Duplikaty dozwolone.
- `tuple` (nawiasy `()`): „lista na zawsze" — `STATUS_PROGRESS` trzyma `(krok, komunikat)`, bo para ma stały kształt (najpierw int, potem str). Niemutowalność = nikt nie podmieni kroku na tekst przez przypadek.
- `set` (nawiasy `{}`): test przynależności `x in set` to O(1) (hash), a na liście O(n) (przeglądanie). `VALID_TRANSITIONS[status]` to sety, bo pytanie brzmi „czy wolno?", nie „które z kolei?". Bonus: set nie przyjmie duplikatu (drugie `add` tego samego = no-op).
- `dict` (klucz: wartość): lookup `VALID_TRANSITIONS[status]` to O(1) po hashu klucza. Gdyby przejścia trzymać jako listę par `(z, do)`, każde sprawdzenie to przeglądanie całości.
- **Przykład z serwisu:** `new_status not in allowed` (set, §4.2) + `EVENT_SCHEMAS.get(event_type)` (dict z defaultem, §4.11) — dwa pojemniki, dwa pytania („wolno?" i „czym walidować?").

### 6.1 `async`/`await` — współbieżność bez wątków

Dla laika: zwykła funkcja (`def`) robi wszystko naraz i blokuje wątek na czas czekania (baza, sieć). Funkcja `async def` to coroutine — na `await` **oddaje sterowanie** event loop, który w tym czasie obsługuje inne requesty. Gdy baza odpowie, coroutine wraca.

```python
result = await self._session.execute(stmt)  # czekam na bazę → loop robi inne rzeczy
model = LoanApplicationModel(...)           # CPU, bez await → natychmiast
self._session.add(model)                    # CPU, bez await
await self._session.flush()                 # znowu I/O → znowu pauza
```

- Reguła: `await` tam, gdzie I/O (baza, Redis, Kafka, sieć); brak `await` tam, gdzie czyste CPU (konstrukcja encji, `Decimal`, `UUID()`, `_transition`). Błąd początkującego #1: `await` na funkcji synchronicznej (`await uuid4()` — TypeError). Błąd #2: brak `await` na coroutine (zwraca coroutine, nie wynik — use case dostałby „obietnicę wniosku", nie wniosek; mypy to łapie).
- Tutaj cała praca to I/O (POST = baza + Redis; worker = baza + Kafka), więc model async jest idealny. CPU jest trywialne (walidacja Pydantic, budowa koperty — mikrosekundy). Dlatego ten serwis nie potrzebuje `ProcessPool` (w przeciwieństwie do Underwriting z ML — tam predykcja blokowałaby loop na setki ms i dusiła heartbeaty Kafki; SPEC §4.5 wyjaśnia różnicę `to_thread` vs procesy).
- `asyncio.create_task(worker.run())` (§4.16): planowanie coroutine w tle (nie czeka na wynik — „leć obok"). `asyncio.sleep(0.5)` (§4.11): pauza bez blokowania wątku (w przeciwieństwie do `time.sleep(0.5)`, które zamroziłoby cały serwer na pół sekundy!).

### 6.2 Dekoratory — `@dataclass`, `@property`, `@asynccontextmanager`, `@abstractmethod`

Dla laika: dekorator (`@coś` nad `def`/`class`) to „opakowanie" — bierze funkcję/klasę i dodaje jej zachowanie bez zmiany kodu w środku. Składnia:

```python
@dekorator
def f(...): ...
# znaczy: f = dekorator(f)
```

- `@dataclass` (§4.2): generuje `__init__`/`__repr__`/`__eq__` z pól klasy + obsługuje `field(default_factory=...)`. Bez niego każda encja = ~20 linii ręcznego konstruktora i porównań.
- `@property` (§4.2: `is_active`, `monthly_payment`): metoda udająca pole (`loan.is_active`, nie `loan.is_active()`). Sygnalizuje „cecha, nie akcja". Property liczy się przy każdym dostępie (bez cache — tu tanie, bo pola niemutowalne po konstrukcji... poza `status`; `is_active` po transicji liczy się na nowo — poprawnie!).
- `@asynccontextmanager` (§4.16: `lifespan`): zamienia generator async (`yield` w środku) w menedżer kontekstu dla FastAPI. Kod przed `yield` = start, po = stop. Bez niego trzeba by pisać klasę z `__aenter__`/`__aexit__` ( ceremoniał dla tego samego efektu).
- `@abstractmethod` (§4.5/§4.6): metoda bez ciała w klasie `ABC` — „wypełnij mnie w podklasie". Klasy z metodą abstrakcyjną nie da się instancjonować (TypeError przy próbie — fail-fast na brak implementacji).
- `@router.post/get` (§4.14): rejestracja endpointu („ta funkcja obsługuje POST pod tym URL"). Dekorator z argumentami (`status_code=202`) — fabryka dekoratorów, ale mechanika ta sama.

### 6.3 `@dataclass` — encje bez boilerplate (przypomnienie z przykładami)

Patrz §4.2 Blok 1 (pułapka `= uuid4()` vs `field(default_factory=uuid4)`, pułapka `= []`). Dodajmy trzeci przykład — `__repr__` za darmo:

```python
>>> loan = LoanApplication(applicant_id=..., amount=Decimal("10000"), ...)
>>> loan
LoanApplication(applicant_id=UUID('...'), amount=Decimal('10000'), ..., status=<LoanStatus.DRAFT: 'DRAFT'>, ...)
```

Bez `@dataclass` debugowanie to `print(loan.__dict__)` (brzydkie) albo ręczny `__repr__` (szum). Z dekoratorem — czytelny zrzut za darmo, a `__eq__` porównuje po polach (testy: `assert fetched.id == result.id` działają na wartościach, nie tożsamościach obiektów).

### 6.4 `StrEnum` — stringi z dyscypliną

Patrz §4.1 Blok 1 (przykłady `==`, f-string, `join`). Dodajmy: iterowalność enuma napędza guard SQL:

```python
>>> [s.value for s in LoanStatus if s not in {LoanStatus.REJECTED, ...}]
['DRAFT', 'SUBMITTED', 'DOC_VERIFICATION', 'UNDERWRITING', 'APPROVED', 'DISBURSING', 'DISBURSED']
```

Enum to „zamknięty słownik" (iterujesz, filtrujesz, nikt nie dopisze z palca). `Literal[...]` by tego nie dał (brak iteracji), luźne stałe też nie (rozproszone po pliku).

### 6.5 UUID — tożsamość bez centrali

`uuid4()` losuje 128 bitów (122 losowe) — kolizja praktycznie niemożliwa (prawdopodobieństwo jak wygrana w lotto × wygrana w lotto). Dlatego ID losuje Python, nie baza (sekwencja wymagałaby centralnego licznika — bottleneck + zdradza liczbę wniosków konkurencji!). UUID w URL-u jest niezgadywalne (w przeciwieństwie do `/loans/123` — enumeracja `124, 125, ...`). To + maskowanie 404 (§7.2) = prywatność „z matematyki".
- Format: `550e8400-e29b-41d4-a716-446655440000` (8-4-4-4-12 hex). `UUID(str)` parsuje i waliduje (zły format = `ValueError` — §4.15 zamienia na 401, §4.14 na 422).

### 6.6 `X | None` i `Optional` — brak jako wartość

Pola opcjonalne (`idempotency_key`, `sent_at`, `decision_reasons` w evencie): `str | None` znaczy „string albo brak". Nowy zapis (3.10+) zamiast `Optional[str]` (Applicant używa starego — tu nowocześniej, bo py312). Różnica None-vs-pusto: `None` = „nie dotyczy/nie wiadomo" (klucz niepodany, event niewysłany), `[]`/`""` = „puste, ale istniejące". Mieszanie ich to klasyczny bug (np. `if not reasons` łapie oba — w `reject()` celowo, bo oba znaczą „nic nie zapisuj").

### 6.7 Generatory i `yield` — pauza w środku funkcji

`get_session` (§4.13) to generator async: kod przed `yield` (otwórz sesję), `yield session` („pauza — FastAPI, rób swoje"), kod po (commit/rollback/close). Dla laika: `yield` to jak zakładka w książce — funkcja wraca dokładnie tam, gdzie przerwała, z zachowaniem zmiennych lokalnych (`session` żyje przez pauzę). FastAPI woła kod-po-`yield` po zakończeniu endpointu (nawet przy wyjątku — stąd `try/except/finally` wokół). Ten sam mechanizm napędza `lifespan` (start/`yield`/stop).

### 6.8 Context managery — `async with` i sprzątanie

`async with session_factory() as session` (§4.11), `async with engine.begin()`, `with suppress(CancelledError)` (§4.16): blok `with` gwarantuje „posprzątaj po wyjściu" (zamknij sesję, dispose silnika, zignoruj oczekiwany wyjątek) — także przy błędzie w środku. Bez `with`: każde `return`/`raise` wymagałoby ręcznego `close()` (zapomnisz raz — wyciek). Reguła: zasób (sesja, silnik, task) zawsze przez menedżer kontekstu.

### 6.9 Repository — kolekcja udająca pamięć

Patrz §4.5 (analogia z kartoteką). Dodajmy, jak to wygląda w teście — fake to 5 linii:

```python
class FakeLoanRepo(LoanApplicationRepository):
    def __init__(self): self.loans = {}
    async def save(self, loan): self.loans[str(loan.id)] = loan
    async def get_by_id(self, loan_id): return self.loans.get(str(loan_id))
    ...
```

Use case nie widzi różnicy między fakiem a Postgresem (ten sam port!). To jest cała Hexagonal Architecture w pigułce: logika gada z interfejsem, testy podkładają pamięć, produkcja — bazę.

### 6.10 Dependency Injection przez `Depends`

Patrz §4.15 Blok 3 (cache per-request = ta sama sesja = ta sama transakcja). Dla laika: `Depends(funkcja)` znaczy „najpierw zawołaj tę funkcję i daj mi wynik". FastAPI buduje drzewo (liście: `get_session`, `get_redis_client`; gałęzie: repozytoria; korzeń: endpoint) i pamięta wyniki w ramach requestu (liść wołany raz, choć dwie gałęzie go potrzebują). Bez DI: każdy endpoint ręcznie otwierałby sesję, budował repozytoria i use case (10 linii ceremonii × endpoint + ryzyko dwóch sesji zamiast jednej).

### 6.11 Transactional Outbox — „najpierw zapisz, potem donieś"

Patrz §8.3 (oś czasu, DoD Etapu 3). Istota w jednym zdaniu: **baza jest kolejką** (tabela PENDING), a Kafka — tylko transportem (worker przepisuje). Alternatywy i ich wady: (a) zapis do Kafki wprost z use case („dual write": crash między INSERT a send = wniosek bez eventu LUB event bez wniosku — niespójność nie do naprawienia automatycznie); (b) 2PC/XA przez bazę i brokera (ciężkie, Kafka tego nie wspiera natywnie, latencja); (c) event sourcing (cały stan z eventów — przebudowa systemu, nie MVP). Outbox to złoty środek: jedna transakcja + at-least-once + deduplikacja.

### 6.12 `FOR UPDATE SKIP LOCKED` — kolejka w SQL

Patrz §8.4. Dodajmy mechanikę locków (dla laika): `FOR UPDATE` = „te wiersze są moje do końca transakcji (nikt inny ich nie ruszy)". Bez `SKIP LOCKED` drugi worker **czekałby** na locki pierwszego (kolejka!). Ze `SKIP LOCKED` — **pomija** zajęte i bierze wolne (równoległość). Po commicie pierwszego locki znikają (wiersze już SENT — i tak nie wrócą do puli). Efekt: N replik workera bez koordynacji (koordynacją jest baza).

### 6.13 Idempotencja dwupoziomowa

HTTP: `Idempotency-Key` → Redis `SET NX EX` (szybki replay, §4.12) + kolumna UNIQUE (twarda granica, §4.9). Kafka: `event_id` → `processed_events` (redelivery = no-op, §4.10 Blok 7). Oba poziomy ze SPEC §5.4; pierwszy działa w produkcji, drugi czeka na konsumentów (schemat gotowy). Różnica poziomów: HTTP chroni przed **klientem** (retry), Kafka — przed **brokerem** (redelivery). Dwa różne źródła duplikatów, dwa różne filtry.

### 6.14 Koperta zdarzeń (`libs/events`)

`EventEnvelope[event_id, event_type, occurred_at, correlation_id, producer, payload]` — jeden kontrakt dla wszystkich serwisów (SPEC §5.2). Pola generowane (ID, czas) vs deklarowane (typ, producent) vs biznesowe (payload). Payload walidowany rejestrem `EVENT_SCHEMAS` przed wysyłką (§4.11). Wersjonowanie sufiksem (`.v1`) + klasy `*V1` (przyszłe V2 obok, nie zamiast — starzy konsumenci nie pękają).

### 6.15 `Decimal` dla pieniędzy

Patrz §4.2 (`Decimal("0.01")`, nie `Decimal(0.01)`). Dodajmy demonstrację dla laika:

```python
>>> 0.1 + 0.2
0.30000000000000004      # float kłamie (binarnie nie da się zapisać 0.1)
>>> Decimal("0.1") + Decimal("0.2")
Decimal('0.3')           # Decimal mówi prawdę (dziesiętnie)
```

W ratach: błąd float × 12 rat × tysiące klientów = grosze, które gdzieś muszą się podziać (zwykle na niekorzyść banku albo klienta — obie opcje złe). Stąd `Numeric(12,2)` w bazie + `Decimal` w kodzie + `str` w eventach (JSON nie zna Decimal — `"15000.00"`).

### 6.16 Partial index — indeks nad podzbiorem

Patrz §4.9 Blok 3. Dodajmy liczby: przy 10 mln wierszy SENT i 100 PENDING pełny indeks ma ~10 mln wpisów (setki MB, wolniejszy zapis każdego INSERT — indeks trzeba aktualizować!), partial — 100 wpisów (kilobajty). Worker czyta wyłącznie PENDING, więc partial to indeks „skrojony na miarę zapytania". Reguła: indeksuj to, co czytasz (warunek WHERE), nie całą tabelę.

---

## 7. Bezpieczeństwo — dogłębna analiza

> Applicant-guide poświęca bezpieczeństwu cały esej (Argon2, JWT, anty-enumeracja). Tutaj stawka jest inna: nie hasła, lecz **cudze pieniądze i cudze dane** (dochody, decyzje). Atakujący nie łamie kryptografii — enumeruje UUID, podrabia nagłówki, zalewa spamem.

### 7.1 Model zaufania: sieć zamknięta + gateway (granica systemu)

Serwis **nie widzi JWT** (w `pyproject.toml` nie ma `python-jose` — celowo!). Ufa nagłówkowi `X-User-ID` wyłącznie dlatego, że spełnione są **dwa warunki naraz**:

(a) **Gateway czyści nagłówek z ruchu zewnętrznego** (`_STRIPPED_HEADERS` w `proxy.py` gateway: `x-user-id`, `x-user-roles`, `authorization` usuwane z requestu klienta, potem wstrzykiwane zaufane po weryfikacji RS256). Klient nie może „przyjść z własnym X-User-ID" — gateway je wyrzuca do kosza przed forwardingiem.

(b) **Kontener słucha tylko w sieci `internal`** (compose: brak port-forwardingu 8002 na hosta poza dev). Pakiet z `X-User-ID` może dotrzeć do serwisu tylko od członka sieci wewnętrznej (gateway, inny serwis, operator z dostępem do Dockera).

Złamanie dowolnego warunku (bug w stripowaniu gateway LUB wystawienie portu na świat) = pełne podszycie („jestem dowolnym klientem"). To jest **obrona w głąb z jednym murkiem** — szczerze cienka, ale zgodna z modelem MVP („sieć = granica", SPEC §8: „serwisy ufają tym nagłówkom (sieć zamknięta)"). Bezpośredni `curl -H "X-User-ID: <cudzy-uuid>" :8002/...` zadziała — akceptowane ryzyko dev, nie luka nieznana.
- **Droga produkcyjna:** mTLS między serwisami (kontener bez certyfikatu nie dzwoni) albo weryfikacja JWT w każdym serwisie (klucz publiczny wszędzie — gateway traci monopol, zyskuje odporność). Oba rozwiązania eliminują „zaufanie do kabla". Wpisz do ADR przy pierwszym incydencie (nie wcześniej — MVP ma dowozić, nie fortyfikować).

### 7.2 Anti-enumeration w `GET` — milczenie zamiast odmowy

Cudzy `loan_id` → `LoanApplicationNotFound` (docelowo 404), nie 403. Scenariusz ataku bez maskowania: atakujący (zalogowany, więc ma własne JWT) iteruje po UUID (`GET /loans/{zgadnięty}`); 403 = „trafiłem w cudzy wniosek" (istnienie potwierdzone — połowa sukcesu), 404 = „nie ma" (szum). Z maskowaniem: każde pudło i każde trafienie wyglądają identycznie (404, ten sam string, ten sam czas — jeden SELECT + porównanie w obu ścieżkach; różnica nanosekundowa, niemierzalna przez sieć z jitterem).
- Ten sam motyw co `InvalidCredentials` w Applicant („Invalid email or password" bez rozróżnienia „nie ma usera" vs „złe hasło") — repo stosuje wzorzec konsekwentnie w obu serwisach.
- **Granica maskowania:** `POST` nie maskuje (`DuplicateActiveApplication` mówi wprost — klient widzi własne sprawy, ukrywanie nie ma sensu), `GET` maskuje (klient nie powinien wiedzieć o cudzych). Reguła: maskuj to, czego klient nie powinien poznać; nie maskuj jego własnych błędów.
- **Alternatywa odrzucona:** osobny `AccessDenied → 403` (lepsze logi wewnętrzne — „ktoś grzebie w cudzych", gorsze bezpieczeństwo — wyciek istnienia). Gdybyś chciał oba (log + bezpieczeństwo): loguj ostrzeżenie **wewnętrznie** (`logger.warning("cross-applicant access", ...)`) i zwracaj 404 na zewnątrz. Dziś nawet loga nie ma — hardening na przyszłość.

### 7.3 Guard „jeden aktywny wniosek" — polityka anty-spamowa

Chroni przed trzema rzeczami naraz: (a) **spamem** (złośliwy klient zasypuje kolejkę scoringu — ML to drogi CPU, każde zapytanie kosztuje), (b) **wyścigiem o wypłatę** (dwa równoległe wnioski → dwie wypłaty na ten sam dochód), (c) **chaosem UX** (5 aktywnych spraw — „która jest moja?").
- Egzekucja dziś: SELECT w use case (okno wyścigu milisekundowe — dwa równoległe POST-y przejdą oba; §4.10 Blok 4). Gateway zwęża okno (rate-limit 3/10 min), ale go nie zamyka. Pełne zamknięcie: UNIQUE-częściowy `UNIQUE(applicant_id) WHERE status IN (aktywne)` — baza jako ostateczny arbiter (constraint nie ma wyścigów). Wpisane w §11 jako hardening produkcyjny.
- **Dlaczego „aktywny", a nie „jakikolwiek"?** Klient po `REJECTED`/`DISBURSED` może wnioskować znowu (nowa sprawa, nowa ocena) — blokada wieczna byłaby karą za historię. Terminalne = „sprawa zamknięta, wolno od nowa".

### 7.4 Idempotency-Key: replay, nie błąd — anatomia retry klienta

Scenariusz: aplikacja mobilna wysyła POST (wniosek utworzony, `acquire` zapisany, commit OK), ale odpowiedź ginie (tunel, timeout). Bez klucza: retry = drugi wniosek = guard krzyczy 409 (klient w panice: „czy pierwszy przeszedł?!"). Z kluczem: retry = ta sama odpowiedź, ten sam `id` (klient spokojny: „przeszedł, mam potwierdzenie").
- **Strategia „pierwszy wygrywa":** klucz wiąże się z `loan.id`, nie z treścią. Wysłanie **innej** treści z tym samym kluczem zwróci stary wniosek (nie błąd!). Uzasadnienie: klucz = „to samo żądanie, powtórzone" (klient generuje UUID przy otwarciu formularza — SPEC §5.4). Alternatywa „porównaj payload, przy rozjeździe 422" jest surowsza (łapie bugi klienta), ale łamie prosty kontrakt („powtórz = to samo"). MVP wybiera prostotę; surowość to przyszły `IdempotencyKeyAlreadyUsed` (§4.3 Blok 4).
- **TTL 24 h:** klucz żyje dobę (formularz wczorajszy nie blokuje dzisiejszego). Po wygaśnięciu ten sam klucz = nowy wniosek (akceptowane: klient nie trzyma kluczy tygodniami).
- **Atak:** atakujący nie zna cudzych kluczy (losowe UUID) i nic nie zyskuje, znając własne (replay własnego wniosku). Klucz nie jest sekretem — jest nonce'em. Brak ryzyka enumeracji (klucze nie są listowalne — Redis nie ma „pokaż wszystkie").

### 7.5 Walidacja na granicy (Pydantic) — odrzucaj tanio

`amount ≤ 200 000`, `term 3–60`, `age 18–75` — złośliwe/bezsensowne dane odpadają z **422 przed dotknięciem bazy** (nawet use case nie powstaje). Ekonomia: 422 kosztuje mikrosekundy CPU; przepchnięcie absurdu do Underwriting kosztuje sekundy ML + wiersze w bazie + eventy w Kafce. Limity lustrzane wobec hard rules (SPEC §4.5): atakujący nie zapcha kolejki wnioskami z wiekiem 5 lat ani kwotą 10^18 (przepełnienie `Numeric(12,2)` i tak by odmówiło, ale później i drożej).
- **Głębokość walidacji:** składnia (typy, zakresy) na API; semantyka (DTI, scoring) w Underwriting; spójność (guard, unikalność) w bazie. Trzy poziomy, każdy na swoim miejscu — nie wszystko w jednym (Applicant robi identycznie: format e-maila w DTO, istnienie w use case, UNIQUE w bazie).

### 7.6 Czego brak (świadomie w MVP) — rejestr akceptowanych ryzyk

- **Rate limiting w serwisie** — robi gateway (3 POST / 10 min / user, SPEC §8). Serwis ufa (jak z `X-User-ID`: granica = sieć). Bezpośrednie zalanie `:8002` (omijając gateway) nie jest limitowane — akceptowane, bo port nie jest wystawiony.
- **Autoryzacja ról** — tylko właściciel (`applicant_id == X-User-ID`); brak ról `admin`/`operator` (panel operatora poza MVP, SPEC §1.3). Każde zapytanie „pokaż mi wszystkie wnioski" (support, audyt) wymaga dziś dostępu do bazy — do naprawy przy pierwszym tickecie supportu.
- **Szyfrowanie PII** — `applicant_id` to UUID (nie PESEL — niekradzione tożsamościowo), ale `monthly_income`/`amount` leżą w plain (jak w Applicant hasła nigdy w plain — tu nie ma sekretów do hashowania, są dane do szyfrowania-at-rest: pgcrypto/Vault w produkcji, RODO „pseudonimizacja" w dokumentacji).
- **`/ready` nie sprawdza zależności** (stub — §4.16 Blok 3): orchestrator nie wykryje niesprawnej bazy (pierwsze requesty po deploy dostaną 500). Liveness OK (proces), readiness do naprawy (§11 luka #10).
- **Brak audytu dostępu** (kto czytał czyj wniosek, kiedy): `GET` nie loguje (nawet `info`). Przy skardze „ktoś widział moje dane" nie ma śladu. Produkcja: `logger.info("loan accessed", loan_id, applicant_id)` + retencja — grosze, a robi różnicę w postępowaniu.

> Zasada repo (z applicant-guide §7): bezpieczeństwo to nie funkcja, tylko **lista decyzji z uzasadnieniami**. Powyższe sześć punktów to rejestr „wiemy i akceptujemy" — lepszy niż milczenie, bo review może każdy punkt zakwestionować.

---

## 8. Baza danych, SQLAlchemy i Transactional Outbox

> Applicant-guide poświęca bazie cały esej (§8: sesje, `flush` vs `commit`, brak FK, `exists_by_email`). Tutaj idziemy dalej: trzy tabele to nie tylko „dane", ale **maszyna niezawodności** (outbox jako kolejka, constrainty jako synchronizacja).

### 8.1 Trzy tabele `loan_db` — mapa

| Tabela | Klucz | Unikalne | Indeksy | Rola |
|--------|-------|----------|---------|------|
| `loan_applications` | `id` UUID PK | `idempotency_key` | `applicant_id` | agregaty + guard + replay |
| `outbox_events` | `id` UUID PK | — | partial `ix_outbox_pending` | kolejka publikacji (worker czyta) |
| `processed_events` | `id` UUID PK | `event_id` | — | idempotencja konsumenta (przyszłość) |

- Typy: `PG_UUID(as_uuid=True)` (16 bajtów, nie stringi), `Numeric(12,2)` (pieniądze dokładnie), `String(50/255/20)` (statusy/typy/stany workera), `JSON`/`JSONB` (payload, powody), `DateTime(timezone=True)` = `timestamptz` (zawsze ze strefą) z `now()`.
- Jedna instancja Postgresa, trzy logiczne bazy (`applicant_db`, `loan_db`, ...) + osobni userzy bez wzajemnych grantów (SPEC §3: „izolacja jak w produkcji, bez 5 kontenerów"). `loan_svc` nie widzi tabel applicanta (nawet gdyby chciał — brak grantów; defense in depth na poziomie bazy, nie tylko kodu).

### 8.2 Sesja i transakcja (`database.py`) — esej o granicach

`NullPool` + `async_sessionmaker(expire_on_commit=False)` (szczegóły: §4.13 Blok 1). Istota w trzech zdaniach:
- **Sesja = jednostka pracy** (unit of work): zbiera `add`/`execute`, wysyła przy `flush`, zatwierdza przy `commit`, wycofuje przy `rollback`, zamyka przy `close`. Żyje jeden request (request-scope) — nie dłużej (wyciek), nie krócej (rozpad transakcji).
- **`get_session` jako generator kontekstu:** `yield` → praca (endpoint → use case → repozytoria, same `flush`) → `commit`; wyjątek → `rollback`; zawsze → `close`. Cały `POST /loans` (wniosek + 2 eventy) to **jeden commit** (atomowość outbox!). Worker ma własny krótki engine per tick (izolacja kosztem handshake — §11 luka #6; requestów nie dotyczy).
- **`flush` vs `commit` (dla laika, jak w applicant-guide):** `flush` = „wyślij do bazy, ale nie zatwierdzaj" (baza sprawdza constrainty, wiersz widoczny w transakcji, można wycofać). `commit` = „zatwierdź nieodwołalnie" (koniec transakcji, locki zwolnione, dane dla wszystkich). Use case robi trzy `flush` (wniosek, ev1, ev2) i zero `commit`; `commit` robi `get_session` raz. Gdyby repozytoria commitowały same, mielibyśmy trzy transakcje (okno: wniosek bez eventów — dokładnie to, czemu outbox zapobiega).

### 8.3 Outbox end-to-end — oś czasu z awariami

```
POST → INSERT loan + INSERT ev1 + INSERT ev2 → COMMIT (transakcja T1)
  → tick workera (transakcja T2): SELECT PENDING ... SKIP LOCKED
  → send_and_wait × N (Kafka) → UPDATE SENT × N → COMMIT
  → następny tick (co 500 ms)
```

Scenariusze (to jest DoD Etapu 3 — „dowód, nie deklaracja"):
- **Kafka pada na godzinę:** ticki logują `poll error` (worker żyje — §4.11 Blok 6), wiersze czekają PENDING (baza rośnie o 2 wiersze/wniosek — przy 100 wnioskach/godz. to 200 wierszy, szum). Kafka wraca → worker nadrabia w batchach po 10 (kolejność FIFO zachowana — SSE nie zobaczy „status przed faktem"). **Utracone zdarzenia: zero.**
- **Serwis pada między COMMIT T1 a tickiem:** restart → lifespan startuje workera → nadrabia PENDING (worker nie pamięta stanu w pamięci — stan jest w bazie; restart to po prostu „długi tick").
- **Crash między `send_and_wait` a COMMIT T2:** event w Kafce, wiersz dalej PENDING → następny tick wyśle **duplikat** (at-least-once!). Konsument filtruje po `event_id` (`processed_events` — §4.10 Blok 7). Bez deduplikacji: Document zweryfikowałby dwa razy (nieszkodliwe), Disbursement wypłaciłby dwa razy (katastrofa — dlatego wypłata ma podwójną idempotencję: Stripe-Key + `processed_events`, SPEC §4.6).
- **Dlaczego nie „dokładnie raz" (exactly-once)?** Bo wymagałoby transakcji rozproszonej (2PC/XA) między Postgresem a Kafką — Kafka tego nie wspiera natywnie, a latencja zabiłaby API. Branżowy standard to at-least-once + idempotentni konsumenci (tak robią Allegro, Uber, Stripe-webhooki). Outbox to implementacja tego standardu w 30 linijkach.

### 8.4 `SKIP LOCKED` w praktyce — dwie repliki, zero koordynacji

Dwa procesy workera (2 repliki serwisu w compose/k8s) robią co 500 ms `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 10`. Replika A lockuje wiersze 1–10 (locki do COMMIT); replika B w tym czasie **pomija** 1–10 (SKIP!) i bierze 11–20. Bez `SKIP LOCKED`: B wisiałaby na lockach A (serializacja — po co dwie repliki?). Bez `FOR UPDATE`: obie brałyby te same wiersze (podwójna publikacja — duplikaty z premedytacją).
- **Locki są krótkie:** tick = SELECT + N×(sieć do Kafki) + UPDATE + COMMIT. Przy pustej kolejce: milisekundy. Przy pełnej: sekundy (10× round-trip do Kafki) — locki trzymane przez cały batch (inne repliki omijają — nie czekają; nikt nie głoduje).
- **Alternatywy:** (a) partycjonowanie aplikacyjne („replika 1 bierze parzyste ID") — sztywne, nierówne obciążenie; (b) leader election (ZooKeeper/etcd) — jeden aktywny worker, reszta czeka (marnotrawstwo + złożoność); (c) kolejka w Redis (Streams) — drugi system do utrzymania, utrata trwałości SQL. `SKIP LOCKED` wygrywa prostotą: koordynacją jest baza, którą i tak mamy.

### 8.5 Brak FK — database-per-service jako decyzja (nie lenistwo)

`applicant_id` to zwykła kolumna UUID, nie klucz obcy (SPEC §6.2: „brak FK między bazami — celowo"). Dla laika: klucz obcy kazałby bazie `loan_db` sprawdzać istnienie wiersza w `applicant_db` przy każdym INSERT — a to inna baza, inny użytkownik, brak grantów (i brak połączenia sieciowego między bazami!). Spójność między serwisami zapewniają **zdarzenia** (`applicant.registered.v1` → kiedyś; dziś: zaufanie, że gateway nie wstrzyknie losowego UUID), nie constrainty.
- **Cena:** osierocone `applicant_id` (usunięcie klienta nie kaskaduje — nie może). W MVP akceptowane (klienci się nie usuwają — brak endpointu delete w Applicant!). Produkcja: soft-delete (`deleted_at`) + event `applicant.deleted.v1` + consumer oznaczający wnioski (nie kasujący — historia kredytowa jest regulowana!).
- **Zysk:** niezależne wdrożenia (migracja applicanta nie blokuje loan-app), niezależne backupy/restore (odtworzenie `loan_db` nie wymaga `applicant_db`), brak rozproszonych deadlocków między bazami. To jest istota mikroserwisów na poziomie danych — „luźne sprzężenie" to nie slogan, tylko brak FK.

### 8.6 Mapa zapytań — co, gdzie i po czym

| Zapytanie | Gdzie | Po czym idzie | Indeks |
|-----------|-------|---------------|--------|
| `get_by_id` | repo + replay | PK `id` | PK (B-tree) |
| `exists_active_for_applicant` | guard | `applicant_id` + `status IN` | `applicant_id` |
| `get_by_idempotency_key` | (martwe) | `idempotency_key` | UNIQUE |
| `get_pending` | worker/testy | `status='PENDING'` + `created_at` | partial `ix_outbox_pending` |
| `exists(event_id)` | (przyszłość) | `event_id` | UNIQUE |
| `get_by_applicant_id` | (przyszłość) | `applicant_id` + `created_at DESC` | `applicant_id` |

- Każde zapytanie w serwisie ma indeks (żadnego seq-scanu w ścieżce requestu). Review-reguła: nowe zapytanie bez indeksu = pytanie „a przy milionie wierszy?".

---

## 9. Testy — jednostkowe i integracyjne

> Piramida testów (jak w applicant-guide §9): szeroka podstawa testów unit (szybkie, bez Dockera) + wąski czubek integracyjnych (wolne, prawdziwy Postgres). Outbox-workera (Kafka) nie testuje nikt — luka w pokryciu (§11) z planem naprawy poniżej.

### 9.0 Taksonomia — co gdzie i po co

| Warstwa | Plik | Zależności | Czas | Co udowadnia |
|---------|------|------------|------|--------------|
| domain unit | `test_entities.py` | brak (czysty Python) | ms | maszyna stanów, `is_active`, rata |
| application unit | `test_create_loan.py`, `test_get_loan.py` | faki (dict/list) | ms | replay, guard, typy eventów, maskowanie |
| integration | `test_loan_flow.py` + `conftest.py` | testcontainers (PG 16) | sekundy | SQL naprawdę działa (UNIQUE, SKIP LOCKED, transakcje) |

- Reguła: logika (transicje, kolejność kroków) w unit; baza (constrainty, zapytania) w integracji. Use case testowany na fakach nie wie, że istnieje SQL — a test integracyjny nie wie, że istnieją faki. Oba patrzą na ten sam kontrakt (porty).

### 9.1 `tests/unit/domain/test_entities.py` (140 linii) — test po teście

```python
from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest
from src.domain.entities import LoanApplication, OutboxEvent, ProcessedEvent
from src.domain.exceptions import InvalidStatusTransition
from src.domain.value_objects import LoanStatus


class TestLoanApplicationStateMachine:
    def _make_loan(self, status: LoanStatus = LoanStatus.DRAFT) -> LoanApplication:
        loan = LoanApplication(
            applicant_id=uuid4(),
            amount=Decimal("10000"),
            term_months=12,
            monthly_income=Decimal("5000"),
            applicant_age=30,
        )
        loan.status = status
        return loan
```

- **Helper `_make_loan(status)`:** fabryka z domyślnymi poprawnymi danymi + **bezpośrednie ustawienie statusu** (`loan.status = status` — omija `_transition` celowo!). Dlaczego nie przez transicje? Bo testuje **pojedynczą** transicję, nie historię („z DRAFT do X przez 5 kroków" testowałoby 5 rzeczy naraz — przy failu nie wiadomo, która padła). Bezpośredni zapis to „przygotowanie sceny", nie testowany kod (pole mutowalne — dataclass nie broni; testy mają przywileje, których nie ma produkcja).
- **Dane:** `amount=10000` (string w Decimal — dokładnie), sensowny środek zakresów (nie granice — granice testuje DTO, nie encja).

```python
    def test_draft_to_submitted(self) -> None:
        loan = self._make_loan(LoanStatus.DRAFT)
        loan.submit()
        assert loan.status == LoanStatus.SUBMITTED

    def test_submitted_to_doc_verification(self) -> None:
        loan = self._make_loan(LoanStatus.SUBMITTED)
        loan.start_doc_verification()
        assert loan.status == LoanStatus.DOC_VERIFICATION
    # ... analogicznie: submitted→rejected, doc_verification→underwriting,
    # doc_verification→doc_rejected, underwriting→approved (+reasons),
    # approved→disbursing, disbursing→disbursed, disbursing→disbursement_failed
```

- Dziewięć testów happy-path (jeden na metodę transicji). Każdy: scena → akt → asercja (Arrange-Act-Assert). `test_underwriting_to_rejected_with_reasons` asercjonuje dodatkowo `decision_reasons == ["Wysoki DTI"]` (jedyny test powodów — polski string w teście dokumentuje język komunikatów).
- **Testy synchroniczne** (`def`, nie `async def` — domena nie ma I/O; event loop niepotrzebny). Adnotacja `-> None` (mypy strict wymaga; pytest ignoruje zwrot).

```python
    def test_invalid_transition_raises(self) -> None:
        loan = self._make_loan(LoanStatus.DRAFT)
        with pytest.raises(InvalidStatusTransition) as exc_info:
            loan.approve()
        assert exc_info.value.from_status == "DRAFT"
        assert exc_info.value.to_status == "APPROVED"
```

- `pytest.raises(...) as exc_info` (context manager łapiący wyjątek + dostęp do obiektu). Asercje na **atrybutach** (nie na stringu komunikatu!) — zmiana sformułowania zdania nie psuje testu (stabilność), a atrybuty to kontrakt (§4.3).
- DRAFT→APPROVED (przeskok przez pół maszyny) — reprezentant wszystkich nielegalnych (pełne pokrycie „każdy×każdy" to 90 kombinacji — przesada; jeden jawny + pętla terminali poniżej).

```python
    def test_cannot_transition_from_terminal_status(self) -> None:
        for terminal in [LoanStatus.DISBURSED, LoanStatus.REJECTED,
                         LoanStatus.DOC_REJECTED, LoanStatus.DISBURSEMENT_FAILED]:
            loan = self._make_loan(terminal)
            with pytest.raises(InvalidStatusTransition):
                loan.submit()
```

- Pętla po terminalach (parametryzacja „ręczna" — `pytest.mark.parametrize` byłby elegantszy: osobny test per stan w raporcie; tutaj jeden test na cztery — przy failu widać który? Nie! Pętla przerywa na pierwszym failu. Dług: przepisać na parametrize — §11).
- `submit()` jako próba (najbardziej „niewinna" transicja — nawet ona odmówiona ze stanu terminalnego).

```python
    def test_is_active_true_for_draft(self) -> None: ...
    def test_is_active_true_for_submitted(self) -> None: ...
    def test_is_active_false_for_rejected(self) -> None: ...
    def test_is_active_false_for_disbursed(self) -> None: ...
```

- Cztery testy property (dwa aktywne, dwa terminalne — reprezentanci, nie pełne pokrycie 10 stanów; pełne byłoby lepsze — pętla „wszystkie aktywne True, wszystkie terminalne False" w 6 linijkach).

```python
    def test_monthly_payment_calculation(self) -> None:
        loan = LoanApplication(applicant_id=uuid4(), amount=Decimal("12000"),
            term_months=12, monthly_income=Decimal("5000"), applicant_age=30)
        payment = loan.monthly_payment
        assert payment > Decimal("0")
        assert payment * loan.term_months > loan.amount  # includes interest
```

- Dwie asercje własnościowe (nie dokładna wartość!): rata dodatnia + suma rat > kapitał (odsetki istnieją). Dlaczego nie `== Decimal("1066.19")`? Bo dokładna wartość przywiązuje test do stałej 12% (zmiana oprocentowania = zmiana testu, choć logika poprawna). Asercje własnościowe przeżyją zmianę stopy (dopóki >0). Komentarz `# includes interest` tłumaczy drugą asercję (bez niego wygląda na magię).

```python
class TestOutboxEvent:
    def test_default_status_is_pending(self) -> None:
        event = OutboxEvent(aggregate_id=uuid4(),
            event_type="loan.application.submitted.v1", payload={"loan_id": "test"})
        assert event.status == "PENDING"
        assert event.sent_at is None


class TestProcessedEvent:
    def test_creation(self) -> None:
        event = ProcessedEvent(event_id=uuid4(),
            event_type="loan.application.submitted.v1", aggregate_id=uuid4())
        assert event.processed_at is not None
```

- Trzy testy „konstruktory działają" (defaulty: PENDING, None, now). Skromne, ale łapią regresję defaultów (ktoś zmienia `status = "SENT"`? test krzyczy). Payload `{"loan_id": "test"}` (niepełny, ale `Any` przyjmuje — test dokumentuje luźność typu).

### 9.2 `tests/unit/application/test_create_loan.py` (156 linii) — faki i cztery scenariusze

```python
class FakeLoanRepo(LoanApplicationRepository):
    def __init__(self) -> None:
        self.loans: dict[str, LoanApplication] = {}
        self._active_for_applicant = False

    async def save(self, loan: LoanApplication) -> None:
        self.loans[str(loan.id)] = loan

    async def get_by_id(self, loan_id: Any) -> LoanApplication | None:
        return self.loans.get(str(loan_id))

    async def get_by_applicant_id(self, applicant_id: Any) -> list[LoanApplication]:
        return [l for l in self.loans.values() if l.applicant_id == applicant_id]

    async def exists_active_for_applicant(self, applicant_id: Any) -> bool:
        return self._active_for_applicant

    async def get_by_idempotency_key(self, key: str) -> LoanApplication | None:
        return None
```

- **Fake dziedziczy po porcie** (`LoanApplicationRepository` — ABC wymusza wszystkie metody; zapomniana = TypeError przy konstrukcji fake'a, nie w teście). `dict[str, LoanApplication]` (klucz: string ID — jak Redis!). `_active_for_applicant` jako flaga (test ustawia `True` dla scenariusza duplikatu — sterowanie guardem bez wierszy w bazie).
- **`Any` w sygnaturach** (`loan_id: Any` zamiast `UUID`): fake toleruje stringi i UUID (luźniej niż port — celowo, by testy mogły wołać z czymkolwiek; mypy strict... `Any` przechodzi strict (jawne Any jest dozwolone), choć purysta napisałby UUID i konwertował w teście.
- **`get_by_idempotency_key → None` zawsze:** fake nie implementuje ścieżki bazowej (use case jej nie używa — fake jest szczery co do pokrycia).

```python
class FakeOutboxRepo(OutboxRepository):
    def __init__(self) -> None:
        self.events: list[Any] = []

    async def save(self, event: Any) -> None:
        self.events.append(event)

    async def get_pending(self, limit: int = 10) -> list[Any]:
        return []

    async def mark_sent(self, event_id: Any) -> None:
        pass


class FakeIdempotencyStore(IdempotencyStore):
    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    async def acquire(self, key: str, value: str, ttl_seconds: int = 86400) -> bool:
        if key in self.store:
            return False
        self.store[key] = value
        return True

    async def get(self, key: str) -> str | None:
        return self.store.get(key)
```

- `FakeOutboxRepo.events` (lista — test liczy `len(events) == 2` i czyta typy!). `get_pending → []` (use case nie czyta pending — fake pusty). `FakeIdempotencyStore` implementuje **prawdziwą** semantykę NX (pierwszy `acquire` True, drugi False — jak Redis; test replay polega na tym!).
- Trzy faki w jednym pliku (lokalne, nie współdzielone między plikami — duplikacja `FakeIdempotencyStore` w trzech plikach! Dług DRY: wspólny `tests/fakes.py` — §11).

```python
@pytest.mark.asyncio
async def test_create_loan_success() -> None:
    loan_repo, outbox_repo, idempotency = FakeLoanRepo(), FakeOutboxRepo(), FakeIdempotencyStore()
    use_case = CreateLoanApplicationUseCase(loan_repo, outbox_repo, idempotency)
    request = CreateLoanApplicationRequest(amount=Decimal("10000"), term_months=12,
        monthly_income=Decimal("5000"), applicant_age=30)
    result = await use_case.execute(uuid4(), request)
    assert result.applicant_id == applicant_id ...
    assert result.status == LoanStatus.SUBMITTED
    assert len(loan_repo.loans) == 1
    assert len(outbox_repo.events) == 2  # submitted + status changed
```

- Kluczowa asercja: **2 eventy** (nie 1, nie 0!). Test pinuje kontrakt „dwa eventy per utworzenie" — ktoś usuwający `status.changed` dowie się z testu, nie z produkcji. `result.applicant_id == applicant_id` (wniosek należy do wołającego — bezpieczeństwo w teście sukcesu!).
- `@pytest.mark.asyncio` mimo `asyncio_mode = "auto"` w pyproject (redundancja celowa: testy działają też bez configu — np. wołane z innym `rootdir`).

```python
@pytest.mark.asyncio
async def test_create_loan_duplicate_active_raises() -> None:
    loan_repo._active_for_applicant = True   # scena: klient ma aktywny wniosek
    ...
    with pytest.raises(DuplicateActiveApplication):
        await use_case.execute(uuid4(), request)
```

- Sterowanie flagą fake'a (bez wierszy — guard mockowany, nie liczony). Testuje reakcję use case'a na guarda, nie guarda samego (guard testuje integracja na prawdziwym SQL!).

```python
async def test_create_loan_with_idempotency_key() -> None:
    result = await use_case.execute(uuid4(), request, "test-key-123")
    assert result.status == LoanStatus.SUBMITTED
    assert await idempotency.get("test-key-123") is not None   # klucz zapisany!
```

- Asercja na **skutek uboczny** (klucz w storze), nie tylko odpowiedź. Bez niej `acquire` mógłby zniknąć z use case'a, a testy milczeć (replay testuje integracja `test_idempotency_key_prevents_duplicate`).

```python
async def test_outbox_events_have_correct_types() -> None:
    ...
    event_types = [e.event_type for e in outbox_repo.events]
    assert "loan.application.submitted.v1" in event_types
    assert "loan.status.changed.v1" in event_types
```

- Pin typów (literówka w typie = fail). Razem z `len == 2` daje pełny kontrakt outbox w dwóch testach.

### 9.3 `tests/unit/application/test_get_loan.py` (64 linie) — duck typing w akcji

```python
class FakeLoanRepo:
    def __init__(self) -> None:
        self.loans: dict[str, LoanApplication] = {}

    async def get_by_id(self, loan_id: Any) -> LoanApplication | None:
        return self.loans.get(str(loan_id))
```

- **Nie dziedziczy po porcie** (tylko `get_by_id` — use case nie potrzebuje reszty!). `GetLoanApplicationUseCase(repo)  # type: ignore[arg-type]` — mypy protestuje (fake nie jest `LoanApplicationRepository`), test ucisza (świadome: minimalny fake > pełny fake dla jednej metody). Dyskusyjne (pełny fake byłby „poprawny typowo"), ale pragmatyczne: 8 linii zamiast 30.
- Trzy testy: sukces (`result.id == loan.id` — ten sam wniosek), brak (`pytest.raises(LoanApplicationNotFound)`), **cudzy** (wniosek istnieje, applicant losowy → ten sam wyjątek — maskowanie w teście!).

### 9.4 `tests/integration/conftest.py` (49 linii) — kontenery jako fixture'y

```python
@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres

@pytest.fixture(scope="session")
def redis_container():
    with RedisContainer("redis:7-alpine") as redis:
        yield redis

@pytest.fixture(scope="session")
def database_url(postgres_container):
    return postgres_container.get_connection_url(driver="asyncpg")

@pytest.fixture(scope="session")
def redis_url(redis_container):
    return redis_container.get_connection_url()

@pytest_asyncio.fixture(scope="session")
async def engine(database_url):
    engine = create_async_engine(database_url, poolclass=NullPool)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()

@pytest_asyncio.fixture
async def session(engine):
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session
```

- **Linia po linii:** kontenery session-scope (start raz na sesję pytest — nie per test! start Postgresa to sekundy; per-test byłoby minutami). `postgres:16-alpine` (ta sama wersja co prod — test na 15 przy prod 16 to fikcja). `get_connection_url(driver="asyncpg")` (URL dla async sterownika — kontener daje sync URL, driver parametryzuje).
- **Scope'y:** `database_url`/`engine` session (silnik + `create_all` raz — schemat współdzielony), `session` function (świeża sesja per test — izolacja transakcji... ale uwaga: dane **nie są czyszczone** między testami! `create_all` raz, brak truncate/rollback-scope. Testy używają losowych UUID (`uuid4()` per test), więc nie kolidują — izolacja „przez unikalność", nie „przez czyszczenie". Działa, dopóki testy nie liczą wierszy globalnie (`SELECT count(*)` padłby). Dług: fixture czyszczący (TRUNCATE po teście) — §11).
- **`redis_url` nieużywany** (żaden test go nie bierze — Redis stoi pusty!). `FakeIdempotencyStore` w integracji = prawdziwy Postgres + udawany Redis (połowa integracji). Luka #12 w §11: test `RedisIdempotencyStore` na prawdziwym Redisie (SET NX EX naprawdę!).
- `NullPool` (jak w serwisie — brak wiszących połączeń między testami).

### 9.5 `tests/integration/test_loan_flow.py` (166 linii) — sześć dowodów

- `test_create_and_get_loan` (prawdziwe repozytoria, `FakeIdempotencyStore`): POST-logika + GET-logika end-to-end na SQL (asercje pól + status SUBMITTED + `fetched.id == result.id`).
- `test_outbox_events_created`: `get_pending(limit=100)` po utworzeniu → oba typy dla tego `aggregate_id` (filtr po agregacie — inne testy też piszą PENDING do tej samej tabeli! bez filtra test byłby flaky).
- `test_idempotency_key_prevents_duplicate`: dwa `execute` z tym samym kluczem → `result2.id == result1.id` (replay na prawdziwym SQL + fakowym storze) + `idempotency.get(key) == str(result1.id)` (klucz wskazuje na wniosek).
- `test_duplicate_active_application_rejected`: dwa `execute` bez klucza, ten sam applicant → drugi rzuca (guard na prawdziwym SQL — `exists_active...` liczy naprawdę!).
- `test_get_nonexistent_loan` + `test_get_loan_wrong_applicant`: 404-logika na prawdziwym repo (maskowanie działa i na SQL).
- **Czego brak:** test workera (`poll_once` na prawdziwej bazie + mock producenta — worker nie ma testów wcale!), test `SKIP LOCKED` (dwa workery równolegle), test `UNIQUE` (dwa INSERT z tym samym kluczem → `IntegrityError`), test `RedisIdempotencyStore` (patrz wyżej). Plan: §11.

### 9.6 Jak uruchamiać (i co oznaczają wyniki)

```bash
cd services/loan-application && pytest          # wszystko (wymaga Dockera dla integracji)
pytest tests/unit                               # tylko szybkie, bez Dockera (~1 s)
pytest tests/integration -x -q                  # tylko przepływ bazodanowy (Docker!)
pytest tests/unit/domain -q                     # sama maszyna stanów
```

- `-x` (stop na pierwszym failu — do debugowania), `-q` (cisza — do CI). Brak `coverage` w configu (do dodania: `--cov=src --cov-fail-under=80` — dziś nie wiadomo, ile pokrycia mamy; applicant-guide też to wypomina).
- **Złota zasada:** unit zielone zawsze i wszędzie (pre-commit); integracja w CI (Docker dostępny). Commit z czerwonym unitem = odrzuć bez dyskusji.

---

## 10. Konfiguracja i uruchamianie

### 10.1 Zmienne środowiskowe

| Zmienna | Default w kodzie | Znaczenie |
|---------|------------------|-----------|
| `DATABASE_URL` | `postgresql+asyncpg://loan_svc:loan_dev_pw@localhost:5433/loan_db` | baza `loan_db` (5433 = konwencja repo) |
| `REDIS_URL` | `redis://localhost:6380/0` | idempotencja (6380 = konwencja repo) |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | broker (zewnętrzny listener KRaft) |

Nowa zmienna → dopisz do `.env.example` w root (reguła CLAUDE.md).

### 10.2 `pyproject.toml` — zależności i jakość, linia po linii

```toml
[project]
name = "crediguard-loan-application"
version = "0.1.0"
description = "Loan Application Service - Application intake, Transactional Outbox, State Machine"
readme = "README.md"
requires-python = ">=3.12"
```

- `name` z prefixem `crediguard-` (konwencja monorepo — każdy serwis instalowalny osobno, `pip install -e services/loan-application`). `version 0.1.0` (pre-1.0 = API niestabilne — wolno łamać). `readme = "README.md"` (deklaracja, ale pliku brak w katalogu serwisu! — `pip install` ostrzeże; luka dokumentacyjna jak w gateway).
- `requires-python = ">=3.12"` (nie `==3.12` — dopuszczone nowsze; stąd `StrEnum`, `datetime.UTC`, `X | None`, `tomllib` gdyby potrzebne). Applicant miał `>=3.9` (stąd `timezone.utc`, `Optional`, `str, Enum`-rozważania) — tutaj nowocześniej, bo serwis młodszy.

```toml
dependencies = [
    "fastapi>=0.115.0", "uvicorn[standard]>=0.32.0",
    "sqlalchemy[asyncio]>=2.0.36", "asyncpg>=0.29.0", "alembic>=1.13.1",
    "pydantic>=2.9.2", "pydantic-settings>=2.5.2",
    "structlog>=24.4.0", "httpx>=0.27.2", "python-multipart>=0.0.9",
    "aiokafka>=0.10.0", "redis>=5.0.0",
]
```

- Grupa po grupie: HTTP (`fastapi`, `uvicorn[standard]` — standard = uvloop+httptools, szybszy loop), baza (`sqlalchemy[asyncio]` — extra asyncIO, `asyncpg` — sterownik, `alembic` — migracje), dane (`pydantic`, `pydantic-settings` — choć serwis nie ma `Settings`! martwa zależność? `pydantic-settings` nieużywane w kodzie (URL-e z `os.getenv`, nie `BaseSettings`) — kandydat do wywalenia lub do wprowadzenia `Settings` jak w gateway), obserwowalność (`structlog` — JSON-logi; OTel instrumentacji brak w deps! — auto-instrumentacja z SPEC §9 to przyszłość), reszta (`httpx` — nieużywane w kodzie! pozostałość po szablonie? grep nie znajduje importu httpx w serwisie — martwa zależność do wywalenia; `python-multipart` — multipart dla przyszłego... nic? brak uploadu w tym serwisie — też martwe; `aiokafka` + `redis` — używane, zasadne).
- `>=` (dolne granice, bez górnych — `pip` bierze najnowsze; ryzyko: breaking-change w nowej wersji psuje build; praktyka: lockfile (`uv.lock`/`poetry.lock`) — tu `.venv/.lock` istnieje, ale niecommitowany? do sprawdzenia w CI).
- **Czego brak:** `python-jose` (serwis nie dotyka JWT — celowo!), `pytest-*` w głównych (tylko dev), `libs` (`crediguard_events`, `crediguard_observability` — instalowane jak? `pip install -e ".[dev]"` nie dociąga lokalnych `libs/`! Działają przez `mypy_path` + `PYTHONPATH`/mount w compose? Krawędź instalacyjna — §11).

```toml
[project.optional-dependencies]
dev = ["pytest>=8.3.3", "pytest-asyncio>=0.23.8", "pytest-httpx>=0.27.0",
       "testcontainers>=4.8.0", "ruff>=0.6.9", "mypy>=1.13.0"]
```

- `pytest-asyncio` (testy `async def` — z `asyncio_mode = "auto"` markery i tak stawiane defensywnie). `pytest-httpx` (mock HTTP — nieużywany, bo serwis nie woła HTTP! kolejna martwa zależność z szablonu). `testcontainers` (PG+Redis w testach). `ruff` + `mypy` (lint + typy w dev, nie w obrazie prod? `pip install -e ".[dev]"` w Dockerfile instaluje dev w obrazie — obraz puchnie o testowe pakiety; prod powinien instalować bez `[dev]` — hardening Dockerfile).

```toml
[tool.ruff]
target-version = "py312"
line-length = 100
src = ["src"]

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM", "C4"]
ignore = ["B008", "B904", "UP045"]
```

- `target-version = "py312"` (ruff wie, co wolno: `X | None`, `StrEnum`...). `line-length = 100` (nie 88-blackowe — dłuższe linie SQL/Pydantic czytelniejsze). `src = ["src"]` (baza importów — `from src...` działa).
- Selekty: `E` (błędy), `F` (pyflakes — nieużywane importy! wyłapałyby martwe `httpx`? nie — ruff sprawdza użycie w kodzie, a zależność w `pyproject` to nie import; martwe deps wykrywa `pipdeptree`/`deptry`, nie ruff), `I` (sort importów — isort), `UP` (pyupgrade — nowoczesna składnia), `B` (bugbear — pułapki jak mutowalny default!), `SIM` (uproszczenia), `C4` (comprehensions).
- `ignore`: `B008` (wywołania w defaultach — FastAPI `Depends()` w defaultach parametrów! bugbear by krzyczał, projekt ucisza świadomie), `B904` (`raise ... from` w except — projekt pomija... choć `token.py` w gateway używa `from e`; tu niekonsekwencja), `UP045` (`Optional` vs `X | None` — uciszone, bo `dependencies.py` używa starego `Optional`).

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"

[tool.mypy]
python_version = "3.12"
strict = true
warn_unused_configs = true
namespace_packages = true
explicit_package_bases = true
mypy_path = ["../../../libs/events/src", "../../../libs/observability/src"]

[[tool.mypy.overrides]]
module = ["aiokafka.*", "crediguard_events.*", "crediguard_observability.*"]
ignore_missing_imports = true
```

- `asyncio_mode = "auto"` (testy async bez markerów — markery i tak stawiane, podwójne zabezpieczenie). `strict = true` (sygnał seniorski: adnotacje wszędzie, `disallow_untyped_defs`, `warn_return_any`... — `Any` w payloadach jawnie, więc przechodzi). `mypy_path` do `libs/*` (typy koperty i loggera widoczne — bez tego `crediguard_events` byłoby `Any`). `ignore_missing_imports` dla `aiokafka.*` (brak stubów — mypy nie sprawdza wnętrza biblioteki, tylko nasze użycie... a raczej: nie sprawdza wcale; `AIOKafkaProducer` to dla mypy `Any` — ostrożnie z literówkami w argumentach!).

### 10.3 Dockerfile — linia po linii

```dockerfile
FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
RUN pip install --no-cache-dir -e ".[dev]"

COPY src ./src
COPY alembic.ini ./
COPY alembic ./alembic

EXPOSE 8002

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8002"]
```

- `python:3.12-slim` (nie alpine — `asyncpg` kompiluje się przeciw glibc; alpine-musl = problemy z kołami binarnymi; slim = Debian bez śmieci, kompromis rozmiaru i kompatybilności). `AS base` (nazwany stage — pod przyszłe multi-stage: `base` → `prod` bez dev-deps).
- `PYTHONDONTWRITEBYTECODE=1` (bez `.pyc` w obrazie — mniejszy layer, brak problemów ze starymi bytecode przy mountach), `PYTHONUNBUFFERED=1` (logi natychmiast do stdout — bez tego `docker logs` opóźnione o bufor 4KB! krytyczne dla obserwowalności), `PIP_NO_CACHE_DIR` (mniejszy obraz), `PIP_DISABLE_PIP_VERSION_CHECK` (szybszy build, mniej szumu).
- `gcc + libpq-dev` (kompilacja `asyncpg` ze źródeł, gdy brak wheela; `rm -rf /var/lib/apt/lists` — czyszczenie cache apt w tej samej warstwie! gdyby w osobnym RUN, warstwa z cache zostałaby w obrazie na zawsze — klasyczny błąd dockerowy).
- Kolejność COPY (cache warstw!): najpierw `pyproject.toml` + install (warstwa zależności — przebudowana tylko przy zmianie deps), potem `src` + `alembic` (kod — przebudowany przy każdym commicie). Gdyby skopiować wszystko naraz, każda zmiana kodu reinstalowałaby zależności (minuty zamiast sekund).
- `pip install -e ".[dev]"` (editable + dev — wygoda dev (testy w kontenerze), balast prod (pytest/ruff/mypy w obrazie runtime!); prod-stage powinien robić `pip install .` bez `[dev]` — hardening).
- `EXPOSE 8002` (dokumentacja, nie publikacja! port wystawia compose, nie Dockerfile — częste nieporozumienie laika: samo EXPOSE niczego nie otwiera).
- `CMD uvicorn src.main:app --host 0.0.0.0` (`0.0.0.0`, nie `127.0.0.1` — w kontenerze localhost to wnętrze kontenera; gateway z innego kontenera musi wejść z zewnątrz!). Brak `--reload` (watchdog tylko lokalnie — w obrazie `--reload` = marnowanie CPU + ryzyko).
- **Czego brak:** `HEALTHCHECK` (Docker-native check — compose używa `service_healthy` po `/health`; jawny HEALTHCHECK w obrazie uniezależniłby od compose), `USER` (proces jako root w kontenerze! — `USER appuser` to standard prod-hardening; root w kontenerze + escape = root na hoście), `.dockerignore` (czy `tests/` i `.venv` nie lądują w kontekście? `COPY src` bierze tylko src — OK, ale kontekst wysyłany do demona zawiera wszystko bez `.dockerignore` — wolny build).

### 10.4 Komendy

```bash
make infra-up              # Postgres + Redis + Kafka + MinIO + Jaeger
cd services/loan-application && pytest
uvicorn src.main:app --port 8002 --reload   # lokalnie (wymaga infra)
alembic upgrade head       # migracje na produkcji
curl -X POST :8002/api/v1/loans -H "X-User-ID: <uuid>" -H "Idempotency-Key: <uuid>" \
  -H "Content-Type: application/json" -d '{"amount":15000,"term_months":24,"monthly_income":6000,"applicant_age":30}'
```

---

## 11. Kod źródłowy vs specyfikacja — luki i obszary do poprawy

| # | Luka | Gdzie | Skutek dziś | Poprawka |
|---|------|-------|-------------|----------|
| 1 | **Brak konsumentów** `document.verified / underwriting.completed / disbursement.completed.v1` | brak modułu `consumers/` | Pętla otwarta: wniosek zostaje w SUBMITTED na zawsze | §12 — 3 handlery + `ProcessedEventRepository.exists/save` w jednej transakcji ze zmianą statusu |
| 2 | **`save()` tylko INSERT** | `repository.py:26` | Przyszłe transicje nie mają jak utrwalić zmiany (INSERT duplikatu PK → `IntegrityError`) | `save` jako upsert (`merge`) lub osobne `update`; UPDATE musi iść w transakcji z INSERT-em `status.changed.v1` |
| 3 | **Brak handlera wyjątków domenowych** | `routes/loans.py`, brak `exception_handlers` | `NotFound/Duplicate` → 500 zamiast 404/409 | `app.exception_handler(LoanApplicationDomainError)` + mapowanie: NotFound→404, Duplicate→409, InvalidTransition→422 |
| 4 | **Brak `GET /loans` (lista)** | brak endpointu; `get_by_applicant_id` + `ListResponse` martwe | Dashboard nie pokaże historii wniosków | `GET /loans` → `get_by_applicant_id` → `LoanApplicationListResponse` |
| 5 | **Świeży `correlation_id`** | `producer.py:143` | Trace urwany: event nie linkuje do requestu | Zapisać `correlation_id` (z `X-Correlation-ID` gateway) w payloadzie outbox przy POST; worker przepisuje do koperty |
| 6 | **Engine per tick** | `poll_once:77` | Handshake DB co 500 ms | Długowieczny engine w `__init__`/`start`, `dispose` w `stop` |
| 7 | **`mark_sent` bez `sent_at`** | `repository.py:130` | Niespójność z workerem (worker ustawia, repo nie) | Dodać `sent_at` do UPDATE w repo albo usunąć metodę na rzecz logiki workera |
| 8 | **Martwy `IdempotencyKeyAlreadyUsed`** | `exceptions.py:33` | Nigdy nie rzucany (replay zamiast błędu) | Zostawić jako dokumentację albo zaimplementować strategię „payload mismatch → 422" |
| 9 | **Brak dolnego limitu 1 000 PLN** | `dto.py:13` (`gt=0` zamiast `ge=1000`) | SPEC §6.2 mówi 1 000–200 000; pożyczka 1 zł przechodzi | `Field(ge=1000, le=200_000)` |
| 10 | **`/ready` to stub** | `main.py:72` | Orchestrator uzna za gotowy serwis bez bazy | Ping DB (`SELECT 1`), Redis (`PING`), Kafka (metadata) |
| 11 | **`create_all` obok Alembic** | `database.py:55` | Dryf schematu: kod może stworzyć inny schemat niż migracje | Na prod tylko `alembic upgrade head`; `create_all` wyłącznie w testach |
| 12 | **Integracja nie używa Redisa** | `test_loan_flow.py` (Fake store) | Prawdziwy `RedisIdempotencyStore` nieprzetestowany | Fixture `redis_client` z `redis_url` + test SET NX EX |
| 13 | **Worker importuje modele ORM** | `producer.py:11` | Obejście portu `OutboxRepository` (warstwy) | Worker przez port (`get_pending`/`mark_sent`) albo jawny wyjątek architektoniczny w ADR |

Żadna luka nie łamie DoD Etapu 3 (zapis → outbox → Kafka → replay → guard działają). Pozycje 1–3 to must-have przed Etapem 6.

### Priorytety napraw (proponowana kolejność)

- **P0 (blokuje Etap 6):** #1 (konsumenci), #2 (upsert — bez niego transicje nie utrwalą się), #3 (handlery — bez nich consumer wołający transicje dostanie 500 zamiast sygnału).
- **P1 (demo/observability):** #5 (`correlation_id` — bez niego trace z SPEC §9 nie istnieje), #10 (`/ready` — compose `service_healthy` kłamie), #9 (dolny limit — wstyd na demo: „pożyczka 1 zł przeszła").
- **P2 (higiena):** #6 (engine), #7 (`sent_at`), #12 (testy Redis/workera), #13 (worker przez port), #4 (`GET /loans` — potrzebne dopiero w Etapie 5!), #8 (martwy wyjątek — decyzja usunąć/ożywić), #11 (`create_all` vs Alembic — ważne przy pierwszym wdrożeniu prod-like).

### Definition of Done Etapu 3 — checklista dowodowa (SPEC §11)

- [x] `POST /loans` → rekord w bazie (test: `test_create_and_get_loan`).
- [x] Zdarzenie widoczne w topiku (worker + `send_and_wait`; ręcznie: kcat/skrypt na `loan.application.submitted.v1`).
- [ ] Ubicie Kafki nie gubi zdarzeń — **do wykonania ręcznie:** `docker stop kafka` → POST → `SELECT count(*) FROM outbox_events WHERE status='PENDING'` (=2) → `docker start kafka` → po chwili statusy SENT + eventy w topiku. Test automatyczny nie istnieje (worker bez testów!) — dopisać przed hailowaniem DoD.
- [x] Duplikat `Idempotency-Key` → ta sama odpowiedź (test: `test_idempotency_key_prevents_duplicate`).
- [x] `pytest` zielone + `mypy --strict` przechodzi (do potwierdzenia w CI).

---

## 12. Dodatki — przyszłe przepływy Kafka, diagramy, glosariusz, FAQ

### 12.1 Maszyna stanów — diagram

```mermaid
stateDiagram-v2
    [*] --> DRAFT: konstruktor encji
    DRAFT --> SUBMITTED: submit() [POST /loans, dziś]
    SUBMITTED --> DOC_VERIFICATION: start_doc_verification() [Etap 6]
    SUBMITTED --> REJECTED: reject() [szybka ścieżka]
    DOC_VERIFICATION --> UNDERWRITING: start_underwriting() [document.verified.v1]
    DOC_VERIFICATION --> DOC_REJECTED: reject_doc() [document.rejected.v1]
    UNDERWRITING --> APPROVED: approve() [underwriting.completed APPROVED]
    UNDERWRITING --> REJECTED: reject(reasons) [underwriting.completed REJECTED]
    APPROVED --> DISBURSING: start_disbursing() [Etap 8]
    DISBURSING --> DISBURSED: complete_disbursement() [disbursement.completed.v1]
    DISBURSING --> DISBURSEMENT_FAILED: fail_disbursement() [disbursement.failed.v1]
    DISBURSED --> [*]
    DOC_REJECTED --> [*]
    REJECTED --> [*]
    DISBURSEMENT_FAILED --> [*]
```

### 12.2 Przyszły przepływ end-to-end (Etapy 6–8 + 4) — wyjaśnienie

> Kod konsumentów **nie istnieje** (stan po Etapie 3). Poniżej kontrakt ze SPEC §5.1–5.2 i dokładny przepis podłączenia — tak, by implementacja była mechaniczna.

```mermaid
flowchart LR
    subgraph LOAN["Loan App (dziś → przyszłość)"]
        POST["POST /loans<br/>SUBMITTED"]
        C1["consumer: document.*<br/>→ DOC_VERIFICATION<br/>→ UNDERWRITING / DOC_REJECTED"]
        C2["consumer: underwriting.completed<br/>→ APPROVED / REJECTED + reasons"]
        C3["consumer: disbursement.*<br/>→ DISBURSING → DISBURSED / FAILED"]
    end
    POST -->|loan.application.submitted.v1<br/>key=applicant_id| DOC["Document Service<br/>(Etap 6)"]
    DOC -->|document.verified.v1| UW["Underwriting (Etap 7)"]
    DOC -->|document.verified.v1| C1
    DOC -->|document.rejected.v1| C1
    UW -->|underwriting.completed.v1<br/>{decision, reasons, score}| C2
    UW -->|underwriting.completed APPROVED| DISB["Disbursement (Etap 8)"]
    DISB -->|disbursement.completed/failed.v1| C3
    C1 & C2 & C3 -->|loan.status.changed.v1| NOTIF["Notification → Redis Pub/Sub<br/>→ Gateway SSE (Etap 4)"]
```

**Krok po kroku, co się stanie z wnioskiem:**

1. **Dziś:** `POST` → `SUBMITTED` + 2 eventy. `loan.application.submitted.v1` leży w topiku; nikt go nie czyta.
2. **Etap 6 (Document):** Document Service konsumuje `submitted.v1`, robi mock-weryfikację (sleep 2–5 s + reguły) i publikuje `document.verified.v1` albo `document.rejected.v1` (klucz: `applicant_id` — ta sama partycja, kolejność zachowana). Nowy consumer w Loan App: `exists(event_id)`? skip : `start_doc_verification()` + `save(update)` + INSERT `status.changed.v1` + `save(ProcessedEvent)` — **wszystko w jednej transakcji**; potem `start_underwriting()` albo `reject_doc()` + drugi `status.changed.v1`.
3. **Etap 7 (Underwriting):** konsumuje `document.verified.v1`, robi velocity check → hard rules → ML, publikuje `underwriting.completed.v1` z `{decision: APPROVED|REJECTED, reasons: [top-3 PL], probability_of_default, model_version}`. Consumer w Loan App: `approve()` albo `reject(reasons)` — `decision_reasons` z eventu lądują w kolumnie JSON i w odpowiedzi `GET`.
4. **Etap 8 (Disbursement):** konsumpcja `underwriting.completed` (tylko APPROVED) → Stripe PaymentIntent z `Idempotency-Key = loan_id` → webhook → `disbursement.completed.v1`. Consumer w Loan App: `start_disbursing()` przy wysyłce do Stripe (event wewnętrzny lub optymistycznie), `complete_disbursement()` / `fail_disbursement()` po webhooku.
5. **Etap 4 (SSE):** KAŻDA transicja powyżej dokłada `loan.status.changed.v1` (ten sam kształt co dziś: `{loan_id, applicant_id, old_status, new_status, decision_reasons}`). Notification Service tłumaczy je na Redis Pub/Sub `loan-status:{applicant_id}`; gateway strumieniuje `event: status|decision` z `STATUS_PROGRESS[status]` jako `(step, message)`.

**Checklist implementacyjna konsumenta** (wspólna dla C1/C2/C3):
- [ ] `libs/kafka` BaseConsumer: retry 3× (1 s, 4 s, 16 s + jitter) → DLQ `{topic}.dlq` z nagłówkami `x-error`, `x-original-topic`, `x-retry-count`.
- [ ] Transakcja: `INSERT processed_events` + `UPDATE loan_applications` + `INSERT outbox_events(status.changed)` — jeden commit (redelivery = `exists → skip`).
- [ ] Naprawić lukę #2 (upsert w `save`) i #5 (`correlation_id` z koperty wejściowej przepisany do outbox).
- [ ] Test chaos: `SIMULATE_FAILURE` → event ląduje w DLQ, inne wnioski płyną (DoD Etapu 9).

### 12.3 Tabela eventów (stan docelowy)

| Topik | Klucz | Producent | Konsumenci | Status |
|-------|-------|-----------|------------|--------|
| `loan.application.submitted.v1` | applicant_id | Loan App ✅ | Document ⏳ | działa produkcja, brak konsumenta |
| `document.verified.v1` / `rejected.v1` | applicant_id | Document ⏳ | Underwriting, Loan App ⏳ | przyszłość (Etap 6) |
| `underwriting.completed.v1` | applicant_id | Underwriting ⏳ | Disbursement, Loan App ⏳ | przyszłość (Etap 7) |
| `disbursement.completed.v1` / `failed.v1` | applicant_id | Disbursement ⏳ | Loan App ⏳ | przyszłość (Etap 8) |
| `loan.status.changed.v1` | applicant_id | Loan App ✅ (częściowo) | Notification ⏳ | dziś tylko DRAFT→SUBMITTED |

### 12.4 Glosariusz (rozszerzony)

- **Agregat** — encja-korzeń spójności (tu: `LoanApplication` + jej eventy). Jeden commit = jeden agregat. Termin z DDD (Domain-Driven Design Erica Evansa): agregat to granica transakcji — wszystko w środku zmienia się razem albo wcale.
- **Transactional Outbox** — wzorzec: zdarzenia jako wiersze w tej samej transakcji co zmiana stanu; osobny worker publikuje do brokera. Rozwiązuje „dual write problem" (dwa systemy, brak wspólnej transakcji). Alternatywy: 2PC (ciężkie), CDC/Debezium (log binarny bazy jako źródło eventów — eleganckie, ale drugi system do utrzymania), event sourcing (stan z eventów — przebudowa).
- **At-least-once** — gwarancja „co najmniej raz" (duplikaty możliwe) w zamian za „nigdy nie zgub". Przeciwieństwo at-most-once („co najwyżej raz" — może zgubić, nigdy nie zduplikuje; dobre dla metryk, złe dla pieniędzy). Exactly-once istnieje tylko w zamkniętych systemach (Kafka Streams transakcyjnie) — nie między bazą a brokerem.
- **At-most-once** — patrz wyżej. W CrediGuard nieużywane (żadne zdarzenie nie może zginąć).
- **Idempotencja** — `f(f(x)) = f(x)`: powtórka bez efektu. HTTP: replay po kluczu (Redis + UNIQUE). Kafka: `processed_events` (redelivery = no-op). Idempotentny jest też producent Kafki (`enable_idempotence` — inny poziom: deduplikacja sieciowa).
- **SKIP LOCKED** — `SELECT ... FOR UPDATE SKIP LOCKED`: bierz wiersze, pomijając zablokowane przez innych (kolejka bez czekania). Bez `SKIP`: czekanie na locki (serializacja). Bez `FOR UPDATE`: podwójne branie (duplikaty).
- **Klucz partycji** — `applicant_id`/`aggregate_id`: wszystkie eventy agregatu na jednej partycji = kolejność (Kafka gwarantuje porządek tylko w partycji!) + równoległość między agregatami (różne partycje = różni konsumenci naraz).
- **Partycja** — podzielony dziennik topiku (tu: 3 partycje per topik, SPEC §5.1). Więcej partycji = więcej równoległości, ale porządek tylko w obrębie jednej (stąd klucz!).
- **DLQ** — Dead Letter Queue: topik `{nazwa}.dlq` na wiadomości po wyczerpaniu retry (inspekcja ręczna via `scripts/dlq_inspect.py`, re-publikacja; nie blokada partycji — zatruty event nie zatrzymuje zdrowych).
- **Retry z backoff** — ponowienia z rosnącym odstępem (1 s, 4 s, 16 s) + jitter (losowość ±, by N konsumentów nie retryowało synchronicznie — „thundering herd"). Etap 9, `libs/kafka`.
- **SSE** — Server-Sent Events: jednokierunkowy strumień serwer→przeglądarka (`text/event-stream`, heartbeat `: ping` co 15 s, `Last-Event-ID` do wznawiania). Lżejsze od WebSocket (nie trzeba dwukierunkowości — klient tylko słucha statusu).
- **Replay** — zwrot zapisanej odpowiedzi dla powtórzonego `Idempotency-Key` (SPEC przewiduje nagłówek `Idempotent-Replay: true`; dziś bez nagłówka — §11). Nie mylić z replay eventów w Kafce (ponowne czytanie od offsetu)!
- **Guard** — reguła odrzucająca operację przed mutacją (tu: `exists_active_for_applicant`). Różnica od walidacji: walidacja sprawdza kształt danych (DTO), guard — stan systemu (baza).
- **Port / Adapter (Hexagonal)** — port: interfejs w `application` („czego potrzebuję"); adapter: implementacja w `infrastructure` („czym to robię"). Use case zna tylko port; testy podkładają fake, produkcja — SQL/Redis.
- **DTO** — Data Transfer Object: pudełko na dane między warstwami (tu: Pydantic — walidacja + serializacja + OpenAPI w jednym).
- **Value Object** — obiekt bez tożsamości, równy przez wartość (tu: `LoanStatus` — dwa `SUBMITTED` to to samo; w przeciwieństwie do encji, gdzie dwa wnioski z tymi samymi polami to dwa wnioski).
- **Koperta (envelope)** — opakowanie eventu (ID, typ, czas, correlation, producent + payload). Konsumenci routują po kopercie (typ!), czytają payload (treść).
- **Correlation ID** — UUID wiążące request z eventami i logami (od gateway przez HTTP i Kafkę do Jaegera). Dziś urwany między POST a eventem (luka #5) — do naprawy przed demem obserwowalności.
- **Liveness vs Readiness** — `/health`: „proces żyje" (restartuj, gdy nie); `/ready`: „mogę przyjmować ruch" (nie ślij, gdy nie). Dwa pytania, dwa endpointy (dziś ready to stub).
- **Timestamptz** — `timestamp with time zone`: moment absolutny (nie „godzina na zegarze w Warszawie"). Zawsze aware, zawsze UTC — reguła repo.
- **Faki (test doubles)** — `FakeLoanRepo` & co.: implementacje portów na dict/list do testów unit (szybkie, bez Dockera). Różnica od mocków: fake ma prawdziwą (uproszczoną) logikę (NX w storze!), mock tylko nagrywa wywołania.

### 12.5 FAQ (rozszerzone)

**Czemu `POST` zwraca 202, a nie 200?**
Bo reszta (weryfikacja, scoring, wypłata) jest asynchroniczna — potrwa sekundy/minuty. 202 mówi klientowi: „przyjęte, śledź postęp przez SSE". 200 sugerowałoby zakończenie sprawy (kłamstwo), 201 — gotowy zasób (prawda tylko technicznie: wiersz istnieje, ale sprawa nie). 202 to najuczciwszy kod.

**Czemu dwa eventy przy utworzeniu, nie jeden?**
`submitted` to fakt dla konsumentów domenowych (Document: „zacznij weryfikować"), `status.changed` to sygnał dla obserwatorów (Notification/SSE: „pokaż krok 1"). Rozdzielenie pozwala subskrybować tylko to, co potrzebne (Underwriting nie obchodzą zmiany statusu wypłaty), i dokładać sygnały (każda przyszła transicja = kolejny `status.changed`) bez zmiany faktów. Cena: jeden INSERT więcej.

**Czemu Redis i kolumna UNIQUE — nie wystarczy jedno?**
Redis = szybkość (replay bez SQL, mikrosekundy) + TTL (samoczyszczenie); kolumna = twardość (przeżyje flush Redisa, łapie wyścigi CHECK-owo w bazie). Sam Redis: flush = amnezja (duplikaty). Sama kolumna: replay wymagałby SELECT per request (wolniej) i nie miałby TTL (klucze wieczne). Dwa poziomy, dwie różne awarie pokryte.

**Czemu worker tworzy engine na każdy tick?**
Izolacja od puli requestów (prosty kod, brak dzielonego stanu, wyciek w workerze nie dotyka API). Koszt to handshake TCP co 500 ms (milisekundy na pustej kolejce — mierzalne, akceptowalne w MVP). Do refaktoryzacji na długowieczny engine (luka #6) przy pierwszym profilowaniu — nie wcześniej (przedwczesna optymalizacja to root of all evil).

**Skąd `old_status: DRAFT` w pierwszym `status.changed`?**
Encja rodzi się jako DRAFT i od razu przechodzi `submit()`. Event dokumentuje pełną prawdę: „był DRAFT, jest SUBMITTED" — konsument SSE wie, że to początek historii (krok 1), nie środek. Gdyby `old_status` był pusty, timeline musiałby zgadywać („skąd przyszedł?").

**Czemu `monthly_payment` liczy przy stałej 12%?**
SPEC §7.2: „rata annuitetowa przy stałym oprocentowaniu 12% (stała biznesowa w configu)". Domena powiela stałą, by policzyć DTI (rata/dochód) bez wołania Underwritingu. Gdy stopa stanie się konfigurowalna (konstruktor/property z parametrem), stała wyprowadzi się do configu — dziś YAGNI.

**Co się stanie, gdy Kafka padnie na godzinę?**
Nic strasznego: requesty działają (zapis + PENDING — baza rośnie o 2 wiersze/wniosek), worker loguje `poll error` co 500 ms (żyje, nie umiera — §4.11 Blok 6), po powrocie Kafki nadrabia kolejkę w batchach po 10 (FIFO — SSE zobaczy historię w porządku). Utracone zdarzenia: zero. To jest cały sens outbox i DoD Etapu 3.

**Co się stanie, gdy padnie Redis?**
`acquire`/`get` rzucą `ConnectionError` → rollback wniosku → klient dostaje 500 i powtarza (bezpieczne: brak wniosku-widma; niedostępne: brak nowych wniosków mimo zdrowej bazy). Redis to SPOF ścieżki zapisu (nie odczytu — `GET /loans/{id}` nie dotyka Redisa). Hardening: try/except z degradacją do „baza-only" (replay przez `get_by_idempotency_key` — dziś martwe, właśnie do tego!) — §11.

**Co się stanie, gdy padnie Postgres?**
Wszystko staje (zapis, odczyt, worker — jedna baza na wszystko w serwisie). `GET /health` dalej zielone (liveness nie sprawdza bazy — celowo!), `/ready` też (stub — do naprawy, luka #10). Klienci dostają 500; eventy PENDING czekają (baza nie przyjmuje, ale i nie traci — pad bazy ≠ utrata danych przy trwałym storage). Recovery: baza wstaje → serwis działa bez restartu (silnik łączy na nowo; worker tickuje dalej).

**Dlaczego nie ma `GET /loans` (listy)?**
Bo Etap 3 kończy się na minimum (POST + GET po ID + outbox). Repo (`get_by_applicant_id`) i DTO (`LoanApplicationListResponse`) już czekają — endpoint to ~10 linii (routes + fabryka istnieje). Luka #4 w §11. Dashboard „moje wnioski" bez tego nie powstanie (Etap 5 będzie go potrzebował!).

**Dlaczego statusy zmienia tylko ten serwis?**
Bo rozproszona maszyna stanów (każdy serwis przestawia sobie) kończy się wyścigami i stanami-widmami („Document ustawił UNDERWRITING, Underwriting w tym czasie REJECTED" — kto wygrał?). Jeden właściciel + zdarzenia-opinie (nie rozkazy!) = determinizm: konsumenci proponują (`document.verified`), właściciel dysponuje (transicja + event). To jest najważniejsza reguła architektoniczna repo (CLAUDE.md, punkt 5).

**Jak debugować „wniosek utknął w SUBMITTED"?**
Kolejność: (1) `GET /loans/{id}` — jaki status naprawdę? (2) `SELECT * FROM outbox_events WHERE aggregate_id = ...` — czy eventy SENT (worker dowozi?) czy PENDING (Kafka padła?) — rozjazd `created_at` vs `sent_at` to lag. (3) Logi workera po `event_id` (grep łączy serwis z konsumentami). (4) Topik w Kafce (kcat/skrypt — czy event fizycznie jest?). (5) DLQ konsumenta (Etap 9 — czy nie wylądował w trupiarni?). Ta checklista to przyszły runbook — dopisz wyniki do README po pierwszym incydencie.

---

## 13. Podsumowanie — kluczowe decyzje architektoniczne

1. **Maszyna stanów w domain, nie w bazie.** `VALID_TRANSITIONS` + `_transition` — reguły czytelne, testowalne bez Dockera, niemożliwe do obejścia (prywatna mutacja statusu).
2. **Outbox w tej samej transakcji co wniosek.** `get_session` commituje raz: albo wszystko (wniosek + 2 eventy), albo nic. Nigdy „wniosek bez eventu".
3. **Dwa eventy per utworzenie.** Fakt (`submitted`) + sygnał (`status.changed`) — rozdzielenie konsumentów domenowych od obserwujących od pierwszego dnia.
4. **Podwójna idempotencja HTTP.** Redis (szybkość) + UNIQUE (twardość). Replay zamiast błędu — klienty mobilne z niestabilną siecią nie tworzą duplikatów.
5. **Maskowanie w odczycie.** Cudzy wniosek = 404, nie 403 — enumeracja UUID nic nie daje.
6. **Klucz Kafka = `aggregate_id`.** Porządek per agregat za darmo; `submitted` zawsze przed `status.changed` na partycji.
7. **`acks=all` + idempotent producer.** Trwałość i brak duplikatów sieciowych po stronie producenta; resztę duplikatów łapią konsumenci (`event_id`).
8. **`202 Accepted` jako kontrakt async-first.** API nie udaje synchroniczności tam, gdzie praca trwać będzie sekundy/minuty.
9. **Przygotowanie pod przyszłość bez over-engineeringu.** `processed_events`, `EVENT_SCHEMAS`, `STATUS_PROGRESS`, `get_by_applicant_id` istnieją, choć nieużywane — to jawne haki na Etapy 4/6–8, nie martwy kod z przypadku (każdy opisany w §11/§12).
10. **Ten sam szablon co Applicant.** Porty → adaptery → DI → testy z fakami → testcontainers. Zespół nie uczy się nowego układu na każdy serwis.

### Ścieżka czytania dla ról

- **Laik (pierwszy kontakt z repo):** §1 (analogia z teczką) → §6.0–6.8 (kurs Pythona) → §4.1–4.3 (słowniki domeny) → §5.1 (jeden przepływ end-to-end) → §12.5 FAQ.
- **Developer (implementuje Etap 6):** §4.7 (kolejność kroków) → §4.10 Bloki 2/6 (dlaczego INSERT-only) → §11 luka #1–3 → §12.2 checklist → §9.5 (czego nie testujemy).
- **Reviewer (ocenia PR):** §11 (czy PR domyka lukę z listy?) → §4.2 Bloki 3–4 (czy nowa transicja przeszła przez `_transition`?) → §8.6 (czy nowe zapytanie ma indeks?).
- **Architekt (debatuje trade-offy):** §13 (10 decyzji) → §7.1/§7.6 (rejestr ryzyk) → §8.3 (dlaczego at-least-once) → SPEC ADR (Kafka vs RabbitMQ, outbox vs 2PC).

> Dokument wygenerowany na podstawie kodu z `services/loan-application/` (domain, application, infrastructure, api, main, alembic, tests) oraz `docs/SPECYFIKACJA.md`. Przy zmianie maszyny stanów, kontraktów eventów lub outbox zaktualizuj ten przewodnik razem z kodem (reguła: PR bez aktualizacji §4/§5/§12.1 przy zmianie procesu = odrzuć).
