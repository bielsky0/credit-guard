# CrediGuard Gateway Service — Kompletny przewodnik techniczny

> **Wersja dokumentu:** 1.0
> **Zakres:** pełna analiza kodu źródłowego, architektury, bezpieczeństwa, testów i konfiguracji serwisu `gateway`.
> **Audytorium:** od laika (początkujący Pythonista) po seniora (architekt systemów).
> **Oryginalna specyfikacja:** `docs/SPECYFIKACJA.md` (kontrakty zdarzeń, maszyna stanów, struktura warstw).

---

## Spis treści

1. [Wstęp — czym jest Gateway Service](#1-wstęp--czym-jest-gateway-service)
2. [Architektura warstwowa](#2-architektura-warstwowa)
3. [Struktura projektu](#3-struktura-projektu)
4. [Analiza plików źródłowych — linijka po linijce](#4-analiza-plików-źródłowych)
5. [Ścieżki wywołań endpointów](#5-ścieżki-wywołań-endpointów)
6. [Koncepcje techniczne — słowniki, dekoratory, wzorce](#6-koncepcje-techniczne)
7. [Bezpieczeństwo — dogłębna analiza](#7-bezpieczeństwo)
8. [Konfiguracja i uruchamianie](#8-konfiguracja-i-uruchamianie)
9. [Testy — jednostkowe](#9-testy)
10. [Style i dobre praktyki](#10-style-i-dobre-praktyki)
11. [Kod źródłowy vs specyfikacja — luki i obszary do poprawy](#11-kod-źródłowy-vs-specyfikacja)
12. [Dodatki — diagramy, tabele porównawcze, glosariusz, FAQ](#12-dodatki)
13. [Podsumowanie — kluczowe decyzje architektoniczne](#13-podsumowanie)

---

## 1. Wstęp — czym jest Gateway Service

### 1.1 Miejsce w systemie CrediGuard

CrediGuard to fintechowy system **MVP** zbudowany z mikroserwisów. System obsługuje proces wnioskowania o pożyczkę (credit). W systemie występują między innymi:

- **Applicant Service** — serwis tożsamości (identity provider): rejestracja, logowanie, zarządzanie tokenami (JWT) klientów (wnioskodawców). Podpisuje tokeny kluczem **prywatnym** RSA.
- **Loan Application Service** — maszyna stanów wniosków pożyczkowych (to on, i **tylko** on, zmienia statusy wniosków).
- **ML Scoring Service** — ocena zdolności kredytowej.
- **Gateway** — (omawiany w tym dokumencie) **brama API** — jedyna brama od świata zewnętrznego do wnętrza systemu.

Gateway Service pełni w CrediGuard rolę **pośrednika (reverse proxy)** stojącego pomiędzy klientem (przeglądarka/aplikacja) a wewnętrznymi mikroserwisami. Jest **jedynym serwisem wystawionym na internet** (port 8000). Wszystkie pozostałe serwisy działają w zamkniętej sieci Docker i są osiągalne **tylko** przez gateway.

> **Analogia z życia:** wyobraź sobie biuro banku. Cały personel (serwisy) pracuje w wewnętrznych biurach. Recepcja (gateway) jest jedynym miejscem, do którego może wejść klient. Klient mówi recepcji, czego potrzebuje ("chcę złożyć wniosek o pożyczkę"), a recepcja sprawdza jego tożsamość (przepustkę — JWT), a następnie zaprasza właściwego pracownika (wewnętrzny serwis) do rozmowy. Recepcja sama nie załatwia spraw merytorycznych — ale pilnuje, kto wchodzi.

### 1.2 Co dokładnie robi ten serwis?

Gateway Service jest **bezstanowy (stateless)** — z definicji **nie posiada własnej bazy danych** (zasada database-per-service oznacza "ewentualnie swoją bazę", ale gateway świadomie nie przechowuje żadnego stanu trwałego swoich danych aplikacyjnych). Jego zadania:

1. **Weryfikacja tokenów JWT** — sprawdza poprawność access tokenów (RS256) przy użyciu **klucza publicznego** (klucz prywatny żyje TYLKO w Applicant Service). Na sukcesie ekstrahuje identyfikator wnioskodawcy (`applicant_id`) i wstrzykuje go do nagłówka `X-User-ID` dla wewnętrznych serwisów.
2. **Reverse proxy** — przekazuje żądania do odpowiednich wewnętrznych serwisów po adresach w zamkniętej sieci Docker (`http://applicant:8001`, `http://loan-application:8002`, `http://document:8003`), strimując odpowiedź z powrotem do klienta.
3. **Rate limiting** — ogranicza liczbę żądań (limitowanie tempa) przy użyciu Redis. Spec §8: `/auth/*` 5 req/min na adres IP, `/loans` POST 3 req/10 min na użytkownika.
4. **Propagacja Correlation ID** — generuje lub propaguje nagłówek `X-Correlation-ID` (współdzielona biblioteka `libs/observability`) i przekazuje go do wewnętrznych serwisów.
5. **Bezpieczeństwo nagłówków** — usuwa z żądań klienta nagłówki tożsamościowe (`X-User-ID`, `X-User-Roles`, `Authorization`), których klient nie powinien sam definiować (ochrona przed spoofingiem — podszywaniem), a następnie wstrzykuje zaufaną tożsamość po weryfikacji JWT.

#### Tabela endpointów

| Ścieżka | Metody | Proxy do | Auth | Rate limit |
|---------|--------|----------|------|------------|
| `/api/v1/auth/register` | POST | `applicant:8001` | ❌ publiczny | ✅ 5/min/IP |
| `/api/v1/auth/login` | POST | `applicant:8001` | ❌ publiczny | ✅ 5/min/IP |
| `/api/v1/auth/refresh` | POST | `applicant:8001` | ❌ publiczny | ❌ |
| `/api/v1/me` | GET | `applicant:8001` | ✅ JWT | ❌ |
| `/api/v1/loans` | GET/POST | `loan-application:8002` | ✅ JWT | ✅ 3/10min/użytkownik |
| `/api/v1/loans/{id}` | GET/PUT... | `loan-application:8002` | ✅ JWT | ❌ |
| `/api/v1/loans/{id}/documents` | GET/POST | `document:8003` | ✅ JWT | ❌ |
| `/api/v1/loans/{id}/events` | GET (SSE) | **nie proxy — lokalny `sse_router` (Etap 4)** | ✅ cookie `cg_access` (fallback: Bearer) | ❌ |
| `/api/v1/auth/logout` | POST | **lokalny (Etap 4, nie w tabeli tras)** | ✅ cookie (czyści oba) | ❌ |
| `/api/v1/webhooks/stripe` | POST | `loan-application:8002` | ❌ publiczny (webhook) | ❌ |

> **Aktualizacja (Etap 4):** wiersz `/events` nie jest już stubem — obsługuje go `src/api/sse.py` (pełna analiza protokołu w `docs/notification-service-guide.md` §4.5/§4.8, brzeg HTTP w §4.11 poniżej). Nota G-1/G-2 poniżej jest **nieaktualna**: `_match_route` naprawiono (normalizacja wiodącego `/` — commit `fix(gateway)` z Etapu 4 + `tests/unit/test_routing_match.py`), cała tabela działa. Wiersze z literalnym `{id}` w tabeli tras (`/loans/{id}`, `/documents`) nadal nie matchują jako osobne wpisy (longest-prefix łapie je pod `/api/v1/loans`) — opisują intencję, nie mechanikę.

#### Przykład: co gateway robi z requestem (ślad nagłówków)

Wejście (klient → `:8000`, próba spoofingu w cenie):

```http
POST /api/v1/loans HTTP/1.1
Authorization: Bearer eyJhbGciOiJSUzI1NiJ9...
X-User-ID: admin-uuid              ← podróbka (gateway wyrzuci!)
X-User-Roles: admin                ← podróbka (gateway wyrzuci!)
Content-Type: application/json
Idempotency-Key: 7c9e6679-...

{"amount": 15000, "term_months": 24, "monthly_income": 6000, "applicant_age": 30}
```

Wyjście (gateway → `loan-application:8002`, po JWT + rate limit):

```http
POST /api/v1/loans HTTP/1.1
X-User-ID: 550e8400-...            ← prawdziwy (z sub tokena!)
X-User-Roles: applicant            ← default (nie admin!)
X-Correlation-ID: 9f8e7d6c-...     ← propagowany lub świeży
Content-Type: application/json
Idempotency-Key: 7c9e6679-...      ← passthrough (gateway nie rusza!)

{"amount": 15000, ...}             ← body bajt-w-bajt (bez parsowania!)
```

- Zniknęło: `Authorization` (wewnętrzny serwis go nie potrzebuje), podróbki tożsamości. Zostało: biznes (`Content-Type`, `Idempotency-Key`, query, body). Doszło: zaufana tożsamość + korelacja. To jest cała „logika" gateway w jednym przykładzie.
- Odpowiedź wraca strumieniem (`StreamingResponse`, status transparentny — 202 z loan-app = 202 do klienta, patrz poprawiony diagram §12.1.3) + `X-Correlation-ID` doklejone (klient dostaje nić do supportu).

### 1.3 Dlaczego ten serwis jest ważny?

1. **Jedyny punkt wejścia** — bez niego klient nie ma jak dotrzeć do żadnego wewnętrznego serwisu. To pojedyncza, kontrolowana powierzchnia ataku.
2. **Ochrona tożsamości** — weryfikuje tokeny przy użyciu **wyłącznie klucza publicznego**, dzięki czemu nawet kompromitacja gateway nie pozwoli podszyć innych tokenów. Klucz prywatny trzyma tylko Applicant Service.
3. **Centralne bezpieczeństwo nagłówków** — zabezpiecza wewnętrzne serwisy przed podszywaniem się klientów przez spreparowane nagłówki tożsamości.
4. **Fundament mikroserwisowej architektury** — pokazuje, jak komunikacja **synchroniczna** (HTTP) między klientem a serwisami przebiega przez jeden, spójny punkt. Komunikacja **asynchroniczna** (zdarzenia) idzie przez Kafka obok gateway.
5. **Wzorzec dla brzegów systemu** — pokazuje wzorzec "API Gateway / BFF (Backend for Frontend)" — centralne miejsce dla cross-cutting concern (JWt, rate limiting, correlation ID).

### 1.4 Czego ten serwis NIE robi

- **Nie zarządza wnioskami pożyczkowymi** — to rola Loan Application Service.
- **Nie przechowuje danych** — brak bazy danych; wszystkie dane aplikacyjne mieszkają w wewnętrznych serwisach.
- **Nie czyta cudzych baz** — przestrzega "database-per-service".
- **Nie produkuje zdarzeń Kafka** — w obecnej wersji nie importuje `crediguard_events`; komunikacja async idzie obok niego.
- **Nie realizuje logiki biznesowej** — gateway jest "grubym proxy": pilnuje bezpieczeństwa i routingu, ale nie podejmuje decyzji biznesowych.
- **Nie hostuje szczegółów HTTP wewnętrznych serwisów** — nie definiuje schematów Pydantic dla wniosków, dokumentów itd. — tylko je przepuszcza (passthrough).

---

## 2. Architektura warstwowa

### 2.1 Zasada zależności (ang. Dependency Rule)

W serwisie Applicant obowiązywała klasyczna **Clean Architecture** (cztery warstwy: domain → application → infrastructure → api) z regułą "warstwa wewnętrzna NIE wie nic o zewnętrznych". Gateway świadomie NIE kopiuje tego układu, bo **pełni inną rolę**.

W Applicant Service warstwy chronią logikę biznesową (użycase'y, encje) przed frameworkami i infrastrukturą. Gateway **nie ma logiki biznesowej** — jest warstwą transportową (edge), więc układ jest prostszy:

```
core/          (konfiguracja)   →  wszystkie pozostałe warstwy zależą od Settings
  ↓
api/           (routing + katalog endpointów)   → zależy od services/, infrastructure/, core/
  ↓
services/      (proxy + token)                  → zależy od core/ (oraz httpx, python-jose)
  ↓
infrastructure/ (redis + rate_limit)            → zależy tylko od python-deps (redis)
```

Kierunek zależności: **api → services → infrastructure/core**. Nie ma osobnych warstw `domain` i `application`, ponieważ nie ma encji domenowych, portów ani use case'ów do "ukrywania" — cała logika to wąskie, dobrze zdefiniowane zadania proxy/edge.

**Czy to narusza Clean Architecture?** Nie. Clean Architecture to wzorzec dla serwisów z domeną. Gateway jest "kompozycyjnym korzeniem" i warstwą wejściową — nie implementuje domeny, więc nie ma jej warstwy. Najbliższy odpowiednik: wzorzec **API Gateway / Edge Service / BFF**, gdzie architektura jest z natury płaska (routing + middleware).

### 2.2 Diagram warstw (Mermaid)

```mermaid
flowchart TB
    subgraph EXT["Świat zewnętrzny"]
        C["Klient (curl / przeglądarka)"]
        RS256["Public key PEM (z Applicant)"]
        RED["Redis"]
        SVCS["Wewnętrzne serwisy: applicant, loan-application, document"]
    end

    subgraph GATEWAY["Gateway Service"]
        M["main.py (FastAPI, lifespan, health)"]
        PR["api/proxy_router.py (catch-all)"]
        RT["api/routing.py + api/routes.py (route table)"]
        CFG["core/config.py (Settings)"]
        PRX["services/proxy.py (ProxyService)"]
        TOK["services/token.py (TokenValidator)"]
        RL["infrastructure/rate_limit.py (SlidingWindowRateLimiter)"]
        REDI["infrastructure/redis.py"]
        OBS["libs/observability (CorrelationIdMiddleware)"]
    end

    C --> M
    C --> PR
    M --> PR
    M --> CFG
    PR --> RT
    PR --> CFG
    PR --> PRX
    PR --> TOK
    PR --> RL
    TOK --> CFG
    RL --> REDI
    REDI --> RED
    PRX --> SVCS
    RS256 --> TOK
    OBS --> M
```

### 2.3 Dlaczego taki podział?

- **core/ (= config)** — izoluje konfigurację od reszty; każdy komponent może pobrać `get_settings()` bez potrzeby odwoływania się do frameworka.
- **api/** — wszystko, co dotyczy HTTP (route table + pojedynczy catch-all endpoint). Gdyby zespół zamienił FastAPI na inny framework, trzeba by przepisać tylko ten folder.
- **services/** — czysta logika "transportowa": proxy (httpx) i walidacja tokenów (python-jose). To odpowiednik warstwy `application` w Applicant — ale zorientowanej na transport, nie na domenę.
- **infrastructure/** — adapters do zewnętrznych narzędzi (Redis). To odpowiednik warstwy `infrastructure` w Applicant.

### 2.4 Porównanie z Applicant Service — tabela

| Aspekt | Applicant Service | Gateway Service |
|--------|-------------------|-----------------|
| Warstwy | domain / application / infrastructure / api | core / api / services / infrastructure |
| Warstwa domeny | tak (entities, exceptions) | **nie** (brak logiki biznesowej) |
| Use case'y | tak (register, login, refresh, get_me) | **nie** (routing + policy) |
| Baza danych | PostgreSQL (SQLAlchemy) | **brak** |
| Cache/limit | brak | Redis (tylko rate limiting) |
| Kodowanie tokenów | **podpisuje** (klucz prywatny) | **weryfikuje** (klucz publiczny) |
| Struktura endpointów | osobne routy per endpoint | pojedynczy catch-all `/{path:path}` |
| DI w FastAPI | `Depends()` (kaskada) | modułowe **singletony** przez `init_gateway()` |
| Streaming | nie | tak (`httpx.AsyncClient` + `StreamingResponse`) |

---

## 3. Struktura projektu

### 3.1 Pełne drzewo katalogów

Serwis żyje w katalogu `services/gateway/`. Poniżej pełna struktura:

```
services/gateway/
├── .env.example                      # Wzór zmiennych środowiskowych
├── Dockerfile                        # Obraz kontenera
├── pyproject.toml                    # Metadane pakietu, zależności, konfiguracja ruff/mypy
├── src/
│   ├── __init__.py                   # (pusty) — pakiet
│   ├── main.py                       # Wejście FastAPI (lifespan, CORS, SSE+proxy, health)
│   ├── api/
│   │   ├── __init__.py               # (pusty)
│   │   ├── proxy_router.py           # Catch-all reverse proxy + translacja cookie-auth (Etap 4!)
│   │   ├── sse.py                    # Endpoint SSE (Etap 4 — protokół w notification guide)
│   │   ├── routes.py                 # Klasa Route (dataclass)
│   │   └── routing.py                # Budowa tabeli tras ze Settings
│   ├── core/
│   │   ├── __init__.py               # (pusty)
│   │   └── config.py                 # Settings (pydantic-settings + pola cookie/CORS z Etapu 4)
│   ├── infrastructure/
│   │   ├── __init__.py               # (pusty)
│   │   ├── rate_limit.py             # Sliding-window rate limiter (Redis)
│   │   └── redis.py                  # Cykl życia klienta Redis
│   └── services/
│       ├── __init__.py               # (pusty)
│       ├── cookies.py                # HttpOnly cookie-auth (Etap 4 — §4.10)
│       ├── proxy.py                  # Reverse proxy (httpx) + przepisywanie nagłówków
│       └── token.py                  # Walidacja JWT (RS256 + metody cookie z Etapu 4)
└── tests/
    ├── __init__.py                   # (pusty)
    ├── unit/
    │   ├── __init__.py               # (pusty)
    │   ├── test_cookies.py           # Cookie-auth: nagłówki, sanitacja, logout (Etap 4)
    │   ├── test_proxy_headers.py     # Testy przepisywania nagłówków
    │   ├── test_rate_limit.py        # Testy rate limitera (FakeRedis)
    │   ├── test_routing_match.py     # Regresja na fix G-1 (Etap 4)
    │   ├── test_sse.py               # Ramki SSE: format, kroki, decision (Etap 4)
    │   ├── test_token.py             # Testy walidacji JWT (Bearer)
    │   └── test_token_cookie.py      # Walidacja z cookie + RSA w teście (Etap 4)
    └── integration/
        └── __init__.py               # (pusty; brak testów integracyjnych na razie)
```

Uwaga: w przeciwieństwie do Applicant Service, w katalogu serwisu **brak** `keys/` (gateway nie trzyma klucza prywatnego), **brak** `alembic/` (brak bazy danych) oraz **brak** `README.md` (plik `pyproject.toml` odwołuje się do niego jako do `readme`, lecz plik jeszcze nie istnieje).

### 3.2 Po co podział na `src/`?

Analogicznie jak w Applicant: `src/` odizolowuje kod źródłowy od konfiguracji i testów, umożliwia instalację editable (`pip install -e .`) oraz jednoznaczną bazę dla mypy/ruff (`src = ["src"]`).

### 3.3 Po co `__init__.py`?

Opakowują katalogi w pakiety Pythona, dzięki czemu działają importy hierarchiczne:

```python
from src.api.routing import build_routes
from src.services.proxy import ProxyService
from src.infrastructure.rate_limit import SlidingWindowRateLimiter
```

### 3.4 Dlaczego gateway nie ma `alembic/`, `keys/`, `README.md`?

- **Brak `alembic/`** — nie ma bazy danych do migracji. Gateway nie trzyma żadnego trwałego stanu aplikacyjnego.
- **Brak `keys/`** — gateway nie potrzebuje klucza prywatnego. Trzyma jedynie **klucz publiczny** (plik `public_key.pem`), który jest montowany przez wolumen Docker z Applicant Service (tej pary nie generuje).
- **Brak `README.md`** — to luka dokumentacyjna (plik nie powstał mimo odwołania w `pyproject.toml`). Kandydat do uzupełnienia.

---

## 4. Analiza plików źródłowych — linijka po linijce

> W tej sekcji przechodzimy przez KAŻDY plik źródłowy serwisu. Dla każdego pliku podajemy:
> cel, analizę blok po bloku (a tam, gdzie to istotne — linijka po linijce), przykłady,
> konsekwencje usunięcia danej linii oraz alternatywy. Kolejność wynika z warstw
> architektury: od najbardziej wewnętrznej (core/infrastructure) po zewnętrzną (api, main).

---

### 4.1 Plik: `src/core/config.py`

**Cel:** definiuje **konfigurację runtime'ową** serwisu za pomocą `pydantic-settings`. To jedyny plik w `core/` — centralne źródło prawdy dla wszystkich zmiennych środowiskowych i ścieżek.

#### Blok 1: importy

```python
"""Gateway configuration loaded from environment variables."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict
```

- **`from functools import lru_cache`** — dekorator "cache'ujący wynik funkcji na kluczach argumentów". Użyty na `get_settings()` — tworzy `Settings` **dokładnie raz** przy pierwszym wywołaniu; kolejne zwracają ten sam obiekt (Singleton bez zmiennej globalnej).
- **`from pathlib import Path`** — ścieżki plików; `Path(...).read_bytes()` to czytelny odczyt klucza PEM z dysku.
- **`from pydantic_settings import BaseSettings, SettingsConfigDict`** — `BaseSettings` to rozszerzenie Pydantic `BaseModel`, które automatycznie wczytuje zmienne z env. `SettingsConfigDict` to nowoczesny (Pydantic v2) słownik konfiguracyjny.

#### Blok 2: klasa `Settings`

```python
class Settings(BaseSettings):
    """Runtime configuration for the gateway service."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
```

- **`BaseSettings`** — dziedziczenie: klasa automatycznie szuka pól jako zmiennych środowiskowych (z prioritatem: env > `.env` file > default).
- **`model_config = SettingsConfigDict(...)`** — konfiguracja modelu:
  - `env_file=".env"` — odczytaj zmienne z pliku `.env` (jeśli istnieje).
  - `extra="ignore"` — zignoruj nadmiarowe zmienne env (nieznane pola nie powodują błędu). Przydatne, gdy `.env` zawiera zmienne przeznaczone dla innych serwisów.

```python
    gateway_port: int = 8000
```

- Port nasłuchiwania. Domyślnie `8000`. Wystawiony przez Dockerfile (`EXPOSE 8000`). W `.env` — `GATEWAY_PORT=8000`.

```python
    jwt_public_key_path: str = "/app/keys/public_key.pem"
```

- Ścieżka do **klucza publicznego RSA** (PEM). Domyślnie `/app/keys/public_key.pem` (ścieżka w kontenerze Docker). Lokalnie może być `./keys/public_key.pem`. Klucz generowany jest w `services/applicant/src/infrastructure/security/keygen.py` i montowany do gateway przez wolumen Docker.

```python
    applicant_service_url: str = "http://applicant:8001"
    loan_service_url: str = "http://loan-application:8002"
    document_service_url: str = "http://document:8003"
```

- **Wewnętrzne adresy URL serwisów** — hostname to nazwa kontenera Docker w zamkniętej sieci (plik `docker-compose`). Klient z zewnątrz NIE może się połączyć z tymi adresami (brak port-forwarding). Porty wewnętrzne (8001, 8002, 8003) odpowiadają portom Uvicorn tych serwisów w ich Dockerfile.

```python
    redis_url: str = "redis://localhost:6380/0"
```

- Adres Redis. Domyślnie `localhost:6380` (port 6380 to konwencja projektu CrediGuard — port 6379 jest domyślnym portem Redis, ale 6380 jest używany, by uniknąć konfliktów z innymi instancjami). `0` to numer bazy (Redis supports 16 baz danych 0-15).

```python
    rate_limit_auth_per_minute: int = 5
    rate_limit_loans_per_10_min: int = 3
```

- **Limity rate limitingu** — pobierane z env (konfigurowalne bez zmiany kodu). Wartości zgodne z spec §8:
  - Auth (register/login): max 5 żądań na minutę na adres IP.
  - Loans (POST): max 3 żądania na 10 minut na zalogowanego użytkownika.

#### Blok 3: property `jwt_public_key`

```python
    @property
    def jwt_public_key(self) -> bytes:
        return Path(self.jwt_public_key_path).read_bytes()
```

- **`@property`** — metoda dostępna jako atrybut: `settings.jwt_public_key` (bez nawiasów). Zwraca bajty PEM z pliku klucza.
- **`Path(...).read_bytes()`** — odczyt pliku w formacie bajtowym (python-jose `jwt.decode` akceptuje bajty lub string PEM).
- **Dlaczego property, a nie pole?** Bo ścieżka może się zmienić dynamicznie (env), a odczyt pliku jest operacją I/O. Property wywoływane jest raz przy starcie (w `init_gateway` → `TokenValidator(settings.jwt_public_key)`), a nie przy każdym żądaniu.

#### Blok 4: `get_settings`

```python
@lru_cache
def get_settings() -> Settings:
    return Settings()
```

- **`@lru_cache`** — cache'owanie: `Settings()` wywoływane jest **dokładnie raz**. Kolejne wołania `get_settings()` zwracają ten sam obiekt (Singleton). `lru_cache` jest tu lepsze od "ręcznego singletona" (`if _settings is None: ...`), bo jest thread-safe, testowalne (można go wyczyścić: `get_settings.cache_clear()`) i nie wymaga `global`.
- **Dlaczego `Settings()` wewnątrz?** `Settings()` przy wywołaniu czyta `.env` i zmienne env. Dzięki `lru_cache` plik jest czytany raz.

**Co by się stało bez `lru_cache`?** `get_settings()` tworzyłoby nowy obiekt `Settings` przy każdym żądaniu (w zależności od tego, kto je wywołuje). Przy starcie `main.py` wywoływane jest raz; ale gdyby w przyszłości `get_settings()` pojawiał się w wielu miejscach (np. w endpointach), bez cache'u mielibyśmy powtórne odczyty .env i powtarzanie walidacji — nieefektywne i potencjalnie problematyczne, jeśli `.env` zostałby usunięty.

---

### 4.2 Plik: `src/infrastructure/redis.py`

**Cel:** zarządza **cyklem życia klienta Redis** — tworzenie i zamykanie jednego wspólnego połączenia dla całego procesu serwera.

#### Blok 1: zmiena globalna i funkcja get

```python
"""Redis client lifecycle for the gateway."""

from __future__ import annotations

from redis.asyncio import Redis

_redis: Redis | None = None


async def get_redis(url: str) -> Redis:
    global _redis
    if _redis is None:
        _redis = Redis.from_url(url, decode_responses=False)
    return _redis
```

- **`_redis: Redis | None = None`** — zmienna modułowa. `None` oznacza "nie połączono jeszcze". Po `get_redis()` trzyma instancję `redis.asyncio.Redis` (asynchronicznego klienta Redis).
- **`global _redis`** — modyfikacja zmiennej modułowej wewnątrz funkcji (bez `global` Python potraktowałby `_redis` jako lokalną).
- **`if _redis is None: ...`** — Lazy Initialization: tworzymy klienta tylko przy pierwszym użyciu. Wzorzec "Lazy Singleton" — efficient, bo unika połączenia z Redis, jeśli nikt nie pyta o limiter (choć w praktyce `get_redis` jest wołane w `lifespan`, więc zawsze).
- **`Redis.from_url(url, decode_responses=False)`** — fabryczna metoda tworząca klienta z URL. `decode_responses=False` oznacza, że dane zwracane są jako `bytes`, a nie `str` — ważne, boSorted Sets rate limitera traktują member jako timestamp (nie tekst).
- **`async def`** — funkcja jest asynchroniczna, choć nie zawiera `await`. Słowo `async` jest tu formalne (wymóg interfejsu); `Redis.from_url()` jest synchroniczne. Jednak `close_redis()` wywołuje `await _redis.aclose()`, więc całość musi być async.

#### Blok 2: zamykanie

```python
async def close_redis() -> None:
    global _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None
```

- **`await _redis.aclose()`** — zamknięcie połączenia Redis (zwolnienie socketa). `aclose()` jest asynchronicznym odpowiednikiem `close()`.
- **`_redis = None`** — czyszczenie referencji (pozwala na GC, unika referencji do zamkniętego obiektu).
- **`if _redis is not None`** — strażnik: jeśli `get_redis()` nie zostało wywołane, nie próbujemy zamykać. Chroni przed `AttributeError` w trakcie error recovery.

**Dlaczego modułowy Singleton, a nie Singleton w klasie?** Po pierwsze, prostota — moduł Pythona jest naturalnym Singlestonem (import wykonywany jest raz). Po drugie, `Redis` z `redis.asyncio` jest już wewnętrznie "shared connection pool" — nie trzeba go owijać w klasę. Wystarczy jeden obiekt na cały proces.

---

### 4.3 Plik: `src/infrastructure/rate_limit.py`

**Cel:** implementuje **mechanizm limitowania żądań (rate limiting)** za pomocą Redis Sorted Sets. To kluczowy mechanizm bezpieczeństwa — chroni przed atakami brute-force i DDoS na wewnętrzne serwisy.

#### Blok 1: docstring i importy

```python
"""Rate limiting backed by Redis (spec §8).

Sliding window implemented with two Redis sorted-set commands per check:
`ZREMRANGEBYSCORE` to prune expired entries, then `ZCARD` to count windows in
the current interval. This is O(log n) and avoids a Lua script while still being
tolerant of clock skew across clients.
"""

from __future__ import annotations

import time
from typing import Protocol

from redis.asyncio import Redis
```

- **`import time`** — dostep do `time.time()` — zwraca aktualny czas jako float (sekundy od 1970-01-01, Unix timestamp z ułamkami milisekund).
- **`from typing import Protocol`** — Python 3.8+ (standard library). `Protocol` pozwala definiować **structural subtyping** (duck typing z adnotacjami) — klasa `SlidingWindowRateLimiter` nie musi jawnie (explicit) dziedziczyć po `RateLimiter`, by móc być użytą w miejscu oczekującym tego typu.

#### Blok 2: protokół `RateLimiter`

```python
class RateLimiter(Protocol):
    async def allowed(self, key: str, limit: int, window_seconds: int) -> bool:
        """Return True if the request passes, False if it exceeds the limit."""
        ...
```

- **`Protocol`** — interfejs (kontrakt): cokolwiek, co ma `async def allowed(key, limit, window) -> bool`, jest typem `RateLimiter`. Dlaczego Protocol zamiast `ABC`? Bo nie wymaga jawnego `@abstractmethod` i nie wymaga dziedziczenia — duck typing z type-checkingiem. Upraszcza testy (FakeRedis, FakeRedisLimiter nie muszą jawnie implementować `RateLimiter`).
- **`...` w ciele** — "placeholder" dla metody abstrakcyjnej (nie implemented). Podobnie do `@abstractmethod`.

#### Blok 3: `SlidingWindowRateLimiter`

```python
class SlidingWindowRateLimiter:
    """Redis-backed sliding window rate limiter."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis
```

- Konstruktor: przyjmuje instancję `Redis` (singleton z `redis.py`).
- **`SlidingWindow`** — "okno przesuwne" (w przeciwieństwie do "fixed window"). Fixed window: 0:00-0:09 = 10 req. Sliding window: bramka przesuwa się z każdym żądaniem. W praktyce sliding window jest bardziej sprawiedliwy (nie ma "skoku" o północy okna).

#### Blok 4: metoda `allowed`

```python
    async def allowed(self, key: str, limit: int, window_seconds: int) -> bool:
        now = time.time()
        window_start = now - window_seconds
        zset_key = f"ratelimit:{key}"

        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.zremrangebyscore(zset_key, 0, window_start)
            pipe.zcard(zset_key)
            pipe.zadd(zset_key, {str(now): now})
            pipe.expire(zset_key, window_seconds)
            result = await pipe.execute()

        count = int(result[1])
        return count < limit
```

Krok po kroku:

1. **`now = time.time()`** — timestamp bieżącego momentu (float). `window_start` — moment `window_seconds` sekund temu.

2. **`zset_key = f"ratelimit:{key}"`** — klucz w Redis. Prefix `ratelimit:` chroni przed kolidowaniem z innymi kluczami. `key` pochodzi z `proxy_router._rate_key()` — np. `auth:192.168.1.1` lub `loans:550e8400-...`.

3. **`async with self._redis.pipeline(transaction=True) as pipe:`** — otwiera **pipeline** Redis. Pipeline to "kolejka komend" wysyłanych hurtowo (round-trip zamiast N round-trips). `transaction=True` oznacza, że wszystkie komendy w pipeline wykonywane są atomowo (MULTI/EXEC) — nikt nie "wepchnie" swojej operacji w międzyczasie. To krytyczne dla rate limiting: pozwala uniknąć race condition, gdy dwa żądania gleichzeitig sprawdzają limit.

4. **`pipe.zremrangebyscore(zset_key, 0, window_start)`** — **usunięcie wygasłych wpisów** z Sorted Set. `ZREMRANGEBYSCORE` usuwa wszystkie elementy z score w zakresie `[0, window_start]`. Score to timestamp; wpisy starsze niż `window_start` są bezużyteczne (poza oknem). To operacja "sprzątająca" (pruning) — bez niej zset rósłby w nieskończoność.

5. **`pipe.zcard(zset_key)`** — liczba elementów w Sorted Set (po usunięciu wygasłych). To jest **rzeczywista liczba żądań w bieżącym oknie**.

6. **`pipe.zadd(zset_key, {str(now): now})`** — dodanie bieżącego żądania do zbioru. `str(now)` to "member" (unikalna nazwa wpisu — timestamp jako string), `now` to score (numer sortujący). Dlaczego `str(now)?` Bo `ZADD` wymaga unikalnych memberów; timestamp jako float jest wystarczająco unikalny (choć przy races na tym samymms mogłyby się powtórzyć — ale transakcja serializuje).

7. **`pipe.expire(zset_key, window_seconds)`** — ustawienie TTL na klucz Redis. Po `window_seconds` sekundach klucz zostanie automatycznie usunięty przez Redis (oszczędność pamięci). To dodatkowa warstwa cleanupu — niezależna od `zremrangebyscore`.

8. **`result = await pipe.execute()`** — wysłanie wszystkich 4 komend hurtowo. `result` to lista wyników: `[zrem_count, zcard_count, zadd_result, expire_result]`.

9. **`count = int(result[1])`** — `result[1]` to wynik `ZCARD` (liczba elementów **przed dodaniem bieżącego żądania**!). Ważne: `ZCARD` jest wykonywane **przed** `ZADD` w kolejności pipeline, więc `count` to liczba żądań **bez** bieżącego. Jeśli `count < limit`, bieżące żądanie jest dozwolone. Jeśli `count >= limit`, bieżące żądanie jest **odrzucone** — ale uwaga: `ZADD` i tak zostało wykonane (pipeline jest atomowy). To oznacza, że po odrzuceniu żądania zliczane jest w zbiorze (kolejne żądanie zobaczy o 1 więcej). To jest **celowe**: zliczamy próbę, nawet jeśli odrzucona.

10. **`return count < limit`** — zwraca `True` jeśli limit nie został przekroczony.

**Dlaczego nie Lua Script?** Redis pozwala na skrypty Lua (np. do operacji atomowych), ale pipeline transakcyjny (MULTI/EXEC) jest równie atomowy i prostszy w utrzymaniu. Lua jest potrzebna, gdy trzeba podejmować decyzje warunkowe wewnątrz Redis; tu decyzja (`count < limit`) jest podejmowana po stronie Pythona.

**Dlaczego Sorted Set, a nie zwykłego licznika (INCR)?** Bo Sorted Set pozwala na **sliding window** — dowolny moment czasu jako "kreska" okna. Zwykły `INCR` z `EXPIRE` daje "fixed window" (okno resetuje się po upływie TTL), co jest mniej sprawiedliwe (ruch o północy okna może podwoić liczbę żądań).

---

### 4.4 Plik: `src/services/token.py`

**Cel:** weryfikacja **tokenów JWT** (RS256) za pomocą klucza publicznego. Gateway NIE podpisuje tokenów — tylko je sprawdza.

#### Blok 1: importy

```python
"""JWT validation for the gateway (spec §4.1, §8).

The gateway holds only the *public* key (RS256) and uses it to validate a
Bearer access token issued by the Applicant Service. On success it extracts the
applicant ID and binds it as X-User-ID for downstream services.
"""

from __future__ import annotations

from uuid import UUID

from jose import jwt
from jose.exceptions import JWTError

from src.core.config import Settings
```

- **`from jose import jwt`** — funkcje `encode` (tutaj nieużywana) i `decode` z biblioteki `python-jose`. Biblioteka obsługuje RS256, HS256, ES256 itd.
- **`from jose.exceptions import JWTError`** — klasa błędów JWT (niepoprawny podpis, wygasły token, błędny format).
- **`from src.core.config import Settings`** — import config (choć tu Settings użyty jest tylko w fabryce `build_token_validator`, nie w samej klasie).

#### Blok 2: stała algorytmu

```python
ALGORITHM = "RS256"
```

- **`RS256`** — algorytm (RSA Signature with SHA-256). Asymetryczny: do podpisu służy klucz prywatny, do weryfikacji — publiczny. Stała w module, nie w klasie, bo jest to wartość stała dla całego serwisu (nie ma powodu, by była zmienną instancji).

#### Blok 3: wyjątek `InvalidTokenError`

```python
class InvalidTokenError(Exception):
    """Raised when the access token is missing or invalid."""
```

- Własny wyjątek (nie dziedziczy po `JWTError` — bo Represents wyższy poziom abstrakcji: "token jest nieważny" z dowolnego powodu). Jest łapany w `proxy_router.py` i zamieniany na HTTP 401.
- **Dlaczego własny wyjątek zamiast `JWTError`?** Bo `InvalidTokenError` reprezentuje "token nie nadaje się do użytku" (może brakować nagłówka, może być zły format, może być wygasły, może być refresh tokenem zamiast access). `JWTError` jest bardziej specyficzny — dotyczy tylko błędów parsowania/podpisu. `InvalidTokenError` jest "parasolem" dla wszystkich scenariuszy odrzucenia.

#### Blok 4: `TokenValidator`

```python
class TokenValidator:
    """Validates RS256 access tokens using the public key."""

    def __init__(self, public_key: bytes) -> None:
        self._public_key = public_key
```

- Klasa przechowuje **klucz publiczny** (bajty PEM). Nie przechowuje klucza prywatnego — to kluczowa cecha bezpieczeństwa (klucz prywatny mieszka TYLKO w Applicant Service).
- **`bytes`** — bajty PEM (z `Settings.jwt_public_key` → `Path(...).read_bytes()`). python-jose akceptuje bajty lub string.

#### Blok 5: metoda `validate`

```python
    def validate(self, authorization: str | None) -> UUID:
        """Parse `Authorization: Bearer <token>` and return the applicant UUID.

        Raises InvalidTokenError for any missing/malformed/expired token.
        """
```

- Zwraca `UUID` — identyfikator wnioskodawcy (extrahowany z claim `sub` tokena).
- `authorization: str | None` — nagłówek `Authorization` z żądania. `None` jeśli brak.
- **Dlaczego `str | None` zamiast wymagać `str`?** Bo FastAPI/ASGI może nie dostarczyć tego nagłówka (gdy klient nie wyśle `Authorization`). Brawurowe podejście zamiast wymagać: jeśli brak → `InvalidTokenError`.

```python
        if authorization is None or not authorization.startswith("Bearer "):
            raise InvalidTokenError("Missing or invalid Authorization header")
```

- **Dwie kontrole w jednej linii:**
  - `authorization is None` — nagłówek nie istnieje (klient nie jest zalogowany).
  - `not authorization.startswith("Bearer ")` — nagłówek istnieje, ale nie zaczyna się od `Bearer ` (zły schemat autoryzacji, np. `Token xyz` lub `Basic ...`).
- **`Bearer ` (z spacją!)** — standard OAuth 2.0: `Authorization: Bearer <token>`. Spacja po "Bearer" jest częścią prefiksu.
- **Wyrzucenie `InvalidTokenError` zamiast `HTTPException`** — celowo: wyjątkiem serwisowym, nie HTTP. Mapowanie na HTTP odbywa się w `proxy_router.py` (sek. 4.8). Pozwala to testować `TokenValidator` niezależnie od FastAPI.

```python
        token = authorization[7:]
```

- **Wycinanie tokena:** `"Bearer " + token` → `[7:]` = od indeksu 7 (po "Bearer"). Przykład:
  - `authorization = "Bearer eyJhbGci..."` → `token = "eyJhbGci..."`.
  - **Dlaczego 7, a nie `len("Bearer ")`?** `len("Bearer ")` = 7 — to to samo, ale magiczna liczba 7 jest mniej czytelna. Stała `PREFIX_LEN = 7` byłaby lepsza (choć w praktyce "Bearer" jest standardem i mało prawdopodobne jest, by się zmienił).

```python
        try:
            payload = jwt.decode(
                token,
                self._public_key,
                algorithms=[ALGORITHM],
            )
```

- **`jwt.decode(token, self._public_key, algorithms=["RS256"])`** — odczytanie payloadu tokena i weryfikacja:
  - **Weryfikacja podpisu** kluczem publicznym (RS256): jeśli podpis nie pasuje → `JWTError`.
  - **Weryfikacja `exp` (expiry)**: jeśli token wygasł → `JWTError` (o ile nie podano `options={"verify_exp": False}`).
  - **`algorithms=[ALGORITHM]`** — **biała lista algorytmów**. KRYTYCZNE dla bezpieczeństwa: zapobiega atakowi "algorithm confusion", gdy atakujący próbuje podać `HS256` (symetryczny) zamiast `RS256` i podpisać token publicznym kluczem jako sekretem. Biała lista wymusza, by token był podpisany dokładnie RS256.

```python
            if payload.get("type") != "access":
                raise InvalidTokenError("Not an access token")
            return UUID(payload["sub"])
```

- **`payload.get("type") != "access"`** — sprawdzenie, czy to **access token**, a nie refresh. Gateway akceptuje TYLKO access tokeny. Refresh tokeny (z `type: "refresh"`) są odrzucane. Dlaczego? Bo refresh tokeny są przeznaczone WYŁĄCZNIE do endpointu `/api/v1/auth/refresh` (w Applicant Service), który działa inaczej (nie jest chroniony przez gateway — patrz tab. endpointów, `/refresh` jest `public=True, rate_limit=False`).

```python
        except (JWTError, KeyError, ValueError) as e:
            raise InvalidTokenError(f"Invalid token: {e}") from e
```

- **Trzy rodzaje błędów:**
  - `JWTError` — niepoprawny podpis, wygasły token, zły format.
  - `KeyError` — brak klucza `sub` w payloadzie (token nie jest zgodny z kontraktem CrediGuard).
  - `ValueError` — `UUID(payload["sub"])` nie może sparsować stringa jako UUID.
- **`from e`** — zachowuje oryginalny wyjątek w łańcuchu (`__cause__`). Dla debuggowania: `InvalidTokenError: Invalid token: Not enough segments` → wiemy, co się stało wewnątrz.

#### Blok 6: fabryka

```python
def build_token_validator(settings: Settings) -> TokenValidator:
    return TokenValidator(settings.jwt_public_key)
```

- **Factory pattern:** tworzy `TokenValidator` z `Settings`. Nie używana obecnie (w `init_gateway` tworzymy wprost `TokenValidator(settings.jwt_public_key)`), ale istnieje jako wygodny (convenience) helper. Alternatywnie mogłaby być użyta w przyszłości, np. do lazy loadingu klucza.

---

### 4.5 Plik: `src/api/routes.py`

**Cel:** definiuje **model danych `Route`** — reprezentację pojedynczego wpisu w tabeli tras gateway. Czysta struktura danych, bez logiki.

```python
"""Route table for the gateway (spec §4.1).

Each entry binds a public path pattern to an internal service URL and declares
whether JWT auth is required and whether rate limiting applies.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Route:
    path_prefix: str
    base_url: str
    requires_auth: bool
    rate_limit: bool
    public: bool = False
    rate_key_prefix: str | None = None
    rate_window_seconds: int = 0
    rate_limit_count: int = 0
```

**Analiza pól (linijka po linijce):**

- **`@dataclass(frozen=True)`** — `frozen=True` oznacza, że obiekt jest **niemutowalny** (nie można zmienić jego pól po utworzeniu). W Pythonie 3.10+ jest to ekwiwalent `@dataclass(frozen=True)`, ale w 3.9+ trzeba dodać `frozen=True` ręcznie. Niemutowalność jest tu kluczowa: tablica tras jest budowana RAZ przy starcie (`build_routes`) i nigdy nie powinna się zmieniać. `frozen=True` chroni przed przypadkową mutacją (np. `route.base_url = "..."` rzuci `FrozenInstanceError`).

- **`path_prefix: str`** — prefiks ścieżki (np. `/api/v1/auth/register`). Długość prefiksu decyduje o priorytecie w matchowaniu (longest prefix match).

- **`base_url: str`** — docelowy URL serwisu (np. `http://applicant:8001`). Pusta string `""` oznacza "nie proxy" (obsługa lokalna, np. SSE w Etap 4).

- **`requires_auth: bool`** — `True` = wymagany JWT access token (gateway weryfikuje, wstrzykuje `X-User-ID`). `False` = publiczny endpoint (np. `/auth/register`).

- **`rate_limit: bool`** — `True` = limitowanie żądań (gateway sprawdza limity przed forwarding). `False` = brak limitu.

- **`public: bool = False`** — `True` = endpoint publiczny (nie wymaga auth, ale może mieć rate limit). Odróżnia od chronionych endpointów. Wartość domyślna: `False`.

- **`rate_key_prefix: str | None = None`** — prefix klucza rate limitingu. `"auth"` = klucz `auth:{client_ip}`. `"loans"` = klucz `loans:{applicant_id}`. `None` = brak rate limitu.

- **`rate_window_seconds: int = 0`** — szerokość okna rate limitingu w sekundach. `60` = 1 minuta. `600` = 10 minut.

- **`rate_limit_count: int = 0`** — maksymalna liczba żądań w oknie. `5` (auth) lub `3` (loans).

**Dlaczego `frozen=True`, a nie `NamedTuple`?** `NamedTuple` jest też niemutowalny, ale nie pozwala na wartości domyślne `None` dla pól bez powtarzania się w konstruktorze; ponadto `frozen dataclass` jest bardziej elastyczny i czytelny. `frozen=True` jest lepsze niż zwykły `@dataclass` bo jawnie komunikuje "ten obiekt nie powinien się zmieniać".

---

### 4.6 Plik: `src/api/routing.py`

**Cel:** buduje **tabelę tras** (listę obiektów `Route`) na podstawie `Settings`. To tutaj deklarowane są wszystkie endpointy, do których gateway proxyje żądania.

#### Blok 1: importy

```python
"""Builds the gateway route table from runtime settings (spec §4.1).

Ordering matters: the catch-all matches the longest prefix. Public routes
(register/login/refresh/webhooks) need no JWT; authenticated routes do. Rate
limits come from the spec §8 (auth: 5/min/IP, loans: 3/10min/user).
"""

from __future__ import annotations

from src.api.routes import Route
from src.core.config import Settings
```

- Docstring odwołuje się do spec §4.1 i §8 — konwencja projektu (referencje do dokumentacji).
- **`build_routes(settings: Settings) -> list[Route]`** — czysta funkcja (brak stanu, brak side-effectów poza tworzeniem obiektów). Dostaje `Settings`, zwraca listę `Route`.

#### Blok 2: tabela tras

```python
def build_routes(settings: Settings) -> list[Route]:
    return [
        Route(
            path_prefix="/api/v1/auth/register",
            base_url=settings.applicant_service_url,
            requires_auth=False,
            rate_limit=True,
            public=True,
            rate_key_prefix="auth",
            rate_window_seconds=60,
            rate_limit_count=settings.rate_limit_auth_per_minute,
        ),
```

**Opis każdego wpisu:**

1. **`/api/v1/auth/register`** → `applicant:8001`. Publiczny, rate-limited (5/min/IP). `rate_key_prefix="auth"` oznacza, że klucz to `auth:{client_ip}` — limity per IP.

```python
        Route(
            path_prefix="/api/v1/auth/login",
            base_url=settings.applicant_service_url,
            requires_auth=False,
            rate_limit=True,
            public=True,
            rate_key_prefix="auth",
            rate_window_seconds=60,
            rate_limit_count=settings.rate_limit_auth_per_minute,
        ),
```

2. **`/api/v1/auth/login`** — identyczny rate limit jak register (oba dzielą prefix `auth`). To oznacza: register + login DZIELĄ ten sam limit 5/min/IP. Klient, który 4 razy zarejestrował się i raz zalogował, przekroczy limit.

```python
        Route(
            path_prefix="/api/v1/auth/refresh",
            base_url=settings.applicant_service_url,
            requires_auth=False,
            rate_limit=False,
            public=True,
        ),
```

3. **`/api/v1/auth/refresh`** — publiczny, **bez rate limitingu**. Dlaczego? Refresh token jest walidowany przez Applicant Service (nie gateway); limity powinny być tam, gdzie tokens są weryfikowane. Gateway tylko forwarduje.

```python
        Route(
            path_prefix="/api/v1/me",
            base_url=settings.applicant_service_url,
            requires_auth=True,
            rate_limit=False,
        ),
```

4. **`/api/v1/me`** — chroniony (wymaga JWT), **bez rate limitingu**. `GET /me` jest operacją odczytu (nie tworzy zasobów), więc rate limiting nie jest potrzebny.

```python
        Route(
            path_prefix="/api/v1/loans",
            base_url=settings.loan_service_url,
            requires_auth=True,
            rate_limit=True,
            rate_key_prefix="loans",
            rate_window_seconds=600,
            rate_limit_count=settings.rate_limit_loans_per_10_min,
        ),
```

5. **`/api/v1/loans`** — chroniony, rate-limited (3/10min/użytkownik). `rate_key_prefix="loans"` → klucz `loans:{applicant_id}` — limit per zalogowany użytkownik (nie per IP). `rate_window_seconds=600` = 10 minut.
   - **Uwaga deviation od spec:** rate limiting jest na CAŁY `/api/v1/loans` (GET i POST), podczas gdy spec §8 mówi o limicie 3/10min na **POST** (tworzony wniosek). GET (odczyt listy) nie powinien być limitowany. To już Implemented deviation (patrz sekcja 11).

```python
        Route(
            path_prefix="/api/v1/loans/{id}",
            base_url=settings.loan_service_url,
            requires_auth=True,
            rate_limit=False,
        ),
```

6. **`/api/v1/loans/{id}`** — chroniony, bez rate limitu. CRUD na konkretnym wniosku (GET/PUT/PATCH/DELETE).

```python
        Route(
            path_prefix="/api/v1/loans/{id}/documents",
            base_url=settings.document_service_url,
            requires_auth=True,
            rate_limit=False,
        ),
```

7. **`/api/v1/loans/{id}/documents`** — chroniony, bez rate limitu. Proxy do `document:8003`.

```python
        Route(
            path_prefix="/api/v1/loans/{id}/events",
            base_url="",  # handled locally as SSE (Etap 4)
            requires_auth=True,
            rate_limit=False,
        ),
```

8. **`/api/v1/loans/{id}/events`** — `base_url=""` → **nie proxy**. Historycznie zwracał 404 (stub SSE). **Zrealizowane w Etapie 4:** request nigdy tu nie dociera (osobny `sse_router` rejestrowany przed catch-all w `main.py` — §4.11); gałąź `base_url == ""` została jako strażnik przyszłych stubów. Pełna analiza protokołu: `docs/notification-service-guide.md` §4.5/§4.8.

```python
        Route(
            path_prefix="/api/v1/webhooks/stripe",
            base_url=settings.loan_service_url,
            requires_auth=False,
            rate_limit=False,
            public=True,
        ),
    ]
```

9. **`/api/v1/webhooks/stripe`** — publiczny, bez rate limitu (webhook Stripe). proxy do `loan-application:8002`.
   - **Deviation od spec:** spec §4.1 mówi, że webhook Stripe proxyje do **Disbursement** (serwis wypłat). Kod proxyje do `loan_application_service_url`. Zob. sekcja 11.

---

### 4.7 Plik: `src/services/proxy.py`

**Cel:** **reverse proxy** — przekazywanie żądań HTTP do wewnętrznych serwisów za pomocą `httpx.AsyncClient` ze **strumieniowaniem (streaming)** odpowiedzi. To sercegateway — bez tego pliku serwis nie mógłby przekazywać żądań.

#### Blok 1: docstring i importy

```python
"""Reverse proxy to internal services (spec §4.1, §8).

Security rules for header rewriting:
- Incoming requests from outside may carry a spoofed `X-User-ID` / `X-User-Roles`
  header. The gateway *always* removes these from the client request before
  forwarding (protection against spoofing, §8).
- After JWT validation, the gateway injects the trusted `X-User-ID` (and a
  default role set). Internal services trust these headers because they are only
  reachable on the closed internal network.
- The original `Authorization` header is stripped before forwarding.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import Any

import httpx
```

- **`AsyncIterator`** — typ iteratora asynchronicznego (z `yield` w `body_stream`). Używany w sygnaturze `forward()` i w `StreamingResponse`.
- **`Mapping`** — abstrakcyjna klasa dla "mapowalnych" struktur (dict). `Mapping[str, str]` = dowolny słownik-like (nie wymusza `dict`, jest bardziej elastyczny).
- **`Any`** — typ powrotu `forward()` jest `dict[str, Any]` (słówkowy wynik: status, headers, body_iterator).

#### Blok 2: zestawy nagłówków

```python
# Headers set/controlled exclusively by the gateway — removed from any client
# request before forwarding.
_STRIPPED_HEADERS = frozenset({"x-user-id", "x-user-roles", "authorization", "x-correlation-id"})

# Headers the gateway never lets through from the client (hop-by-hop / security).
_BLOCKED_HEADERS = frozenset(
    {"host", "content-length", "connection", "transfer-encoding", "upgrade"}
)

_DEFAULT_ROLES = "applicant"
```

**`_STRIPPED_HEADERS` — nagłówki usuwane zawsze (niezależnie od autoryzacji):**
- `x-user-id` — tożsamość użytkownika. Klient MOGŁBY spreparować fałszywy nagłówek `X-User-ID: admin-uuid` — wewnętrzny serwis mógłby mu zaufać (bo normalnie gateway go wstrzykuje). Dlatego **zawsze usuwamy** go z żądania klienta i wstrzykujemy własny po weryfikacji JWT.
- `x-user-roles` — role. Analogicznie — klient nie powinien deklarować swoich ról.
- `authorization` — oryginalny token JWT. Klient nie musi go przekazywać do wewnętrznych serwisów (token nie jest im potrzebny; tożsamość jest w `X-User-ID`).
- `x-correlation-id` — nagłówek jest w `_STRIPPED_HEADERS` (oryginał klienta nie przechodzi wprost), ale `proxy_router` odczytuje go wcześniej (`correlation_id = client_headers.get("x-correlation-id") or str(uuid4())` — §4.8) i `_rewrite_headers` wstrzykuje tę wartość z powrotem jako pojedynczy, kanoniczny `X-Correlation-ID`. Efekt: ID klienta jest **propagowane** (nie losowane na nowo), ale znormalizowane (bez duplikatów). Klient może narzucić własny correlation ID — to celowe (frontendowy trace!), nie dziura (ID nie niesie uprawnień).

**`_BLOCKED_HEADERS` — nagłówki blokowane ze względów technicznych i bezpieczeństwa:**
- `host` — nagłówek HostHTTP/1.1. Klient mógłby ustawić `Host: evil.com`, co mogłoby wpłynąć na routing w serwisie docelowym (choć w praktyce httpx nadpisuje go na podstawie URL).
- `content-length` — httpx przelicza go automatycznie (bo body jest strumieniowane; rozmiar może się zmieniać). Przekazanie starego `Content-Length` mogłoby spowodować błędy parsowania.
- `connection`, `transfer-encoding`, `upgrade` — **nagłówki hop-by-hop** (RFC 2616 §13.5.1): powinny być usuwane przy przekazywaniu. `Connection: keep-alive` między klientem a gateway nie powinno być przekazywane do serwisu docelowego (każde połączenie jest osobne).

**`_DEFAULT_ROLES = "applicant"`** — stała. Domyślna rola: `applicant` (wnioskodawca). Gateway nie rozróżnia ról (nie ma mechanizmu RBAC); wstrzykiwana domyślna rola jest dla spójności kontraktu z serwisami wewnętrznymi i przyszłego rozszerzenia (gdy pojawi się RBAC).

#### Blok 3: `ProxyService`

```python
class ProxyService:
    """Forwards requests to internal services over the closed network."""

    def __init__(self) -> None:
        self._client = httpx.AsyncClient(timeout=30.0)
```

- **`httpx.AsyncClient(timeout=30.0)`** — klient HTTP nowej generacji (alternatywa dla `aiohttp`). `timeout=30.0` — timeout na CAŁE żądanie (łącznie z read/write). 30 sekund wystarcza dla większości operacji; jeśli wewnętrzny serwis zwróci 504, gateway przekaże ten błąd (nie przetrzymuje klienta w nieskończoność).
- **Jeden klient na cały proces** — współdzieli pulę połączeń (connection pooling). Szybsze niż tworzenie nowego klienta na żądanie.

```python
    async def aclose(self) -> None:
        await self._client.aclose()
```

- Zamyka pulę połączeń HTTP przy zamykaniu serwera (w `close_gateway()`). Bez tego TCP sockets mogłyby "wyciekać" (leak).

#### Blok 4: `_rewrite_headers` — krytyczna logika bezpieczeństwa

```python
    def _rewrite_headers(
        self,
        client_headers: Mapping[str, str],
        *,
        applicant_id: str | None,
        correlation_id: str,
    ) -> list[tuple[bytes, bytes]]:
        """Build the header list sent to the downstream service."""
        out: list[tuple[bytes, bytes]] = []
        for k, v in client_headers.items():
            lk = k.lower()
            if lk in _STRIPPED_HEADERS or lk in _BLOCKED_HEADERS:
                continue
            out.append((k.encode("latin-1", "ignore"), v.encode("latin-1", "ignore")))
        # Inject trusted identity + correlation id.
        if applicant_id is not None:
            out.append((b"X-User-ID", applicant_id.encode()))
            out.append((b"X-User-Roles", _DEFAULT_ROLES.encode()))
        out.append((b"X-Correlation-ID", correlation_id.encode()))
        return out
```

**Krok po kroku:**

1. **`out: list[tuple[bytes, bytes]]`** — wynikowa lista nagłówków. HTTP/1.1 w ASGI reprezentowane jest jako lista krotek `(key_bytes, value_bytes)` — format niskopoziomowy (httpx też takie akceptuje).

2. **Iteracja po nagłówkach klienta** — dla każdego nagłówka sprawdzamy, czy nie jest w `_STRIPPED_HEADERS` ani `_BLOCKED_HEADERS`. Jeśli nie → przepisujemy (kodujemy do `latin-1`, bo HTTP wymaga takiego kodowania). **`latin-1`** (ISO 8859-1) obsługuje ASCII (0-127) i dodatkowe znaki europejskie; jest domyślnym kodowaniem nagłówków HTTP/1.1.

3. **Iniekcja `X-User-ID` i `X-User-Roles`** — tylko gdy `applicant_id is not None` (czyli gateway zweryfikował JWT). `X-User-ID` zawiera UUID wnioskodawcy (string); `X-User-Roles` zawiera stałą `"applicant"`.

4. **Iniekcja `X-Correlation-ID`** — zawsze (niezależnie od auth). Zawiera ID korelacji ( UUID lub propagated from client).

**Co by się stało, gdyby `_STRIPPED_HEADERS` było puste?** Atakujący mógłby wysłać `X-User-ID: admin-uuid` — wewnętrzny serwis zaufałby temu nagłówkowi i potraktował żądanie jako od admina. To **spoofing tożsamości** — jeden z najpoważniejszych wektorów ataku w architekturze mikroserwisów, gdzie serwisy ufają nagłówkom z gateway.

#### Blok 5: `forward()` — przekazywanie żądania

```python
    async def forward(
        self,
        *,
        method: str,
        path: str,
        query_string: bytes,
        base_url: str,
        client_headers: Mapping[str, str],
        body: AsyncIterator[bytes] | None,
        applicant_id: str | None,
        correlation_id: str,
    ) -> dict[str, Any]:
        """Forward a request and stream the response back.

        Returns a dict with status_code, headers, and an async body iterator.
        """
        headers = self._rewrite_headers(
            client_headers,
            applicant_id=applicant_id,
            correlation_id=correlation_id,
        )
        url = base_url.rstrip("/") + "/" + path.lstrip("/")
```

- **`base_url.rstrip("/") + "/" + path.lstrip("/")`** — tworzenie pełnego URL. `rstrip("/")` i `lstrip("/")` zapobiegają duplikowaniu slashy:
  - `base_url = "http://applicant:8001"` + `path = "api/v1/me"` → `"http://applicant:8001/api/v1/me"`.
  - `base_url = "http://applicant:8001/"` + `path = "/api/v1/me"` → `"http://applicant:8001/api/v1/me"`.

```python
        req = self._client.build_request(
            method=method,
            url=url,
            headers=headers,
            params=None,
            content=body,
        )
        # Override query string manually to avoid httpx re-encoding surprises.
        req.url = req.url.copy_with(query=query_string.decode("latin-1", "ignore"))
```

- **`self._client.build_request(...)`** — buduje obiekt `Request` httpx, ale **go nie wysyła** (można go jeszcze modyfikować).
- **`params=None`** — nie dodawaj query params (są w `query_string`).
- **`content=body`** — body żądania (strumień bajtów z klienta). `body` to `request.stream()` z ASGI — iterator asynchroniczny.
- **`req.url = req.url.copy_with(query=...)`** — **nadpisanie query stringa ręcznie**. Dlaczego? `httpx` mogłaby zdekodować i przekodować query string, co mogłoby zmienić znaki specjalne (np. `%20` → `+`, lub odwrotnie). Aby zachować oryginalny query string z żądania klienta (byte-for-byte), dekodujemy go `latin-1` i wstawiamy z powrotem. To "defensive programming" — unikamy nieoczekiwanych transformacji.

```python
        resp = await self._client.send(req, stream=True)
```

- **`stream=True`** — kluczowe: odpowiedź nie jest buforowana w pamięci. `resp` to "pending response" — nagłówki są dostępne od razu, ale body trzeba strumieniować ręcznie. To pozwala na **proxy bez buforowania** — nawet jeśli wewnętrzny serwis zwróci plik 100MB, gateway przekazuje go "na bieżąco", nie ładując do pamięci.

```python
        async def body_stream() -> AsyncIterator[bytes]:
            try:
                async for chunk in resp.aiter_bytes():
                    yield chunk
            finally:
                await resp.aclose()
```

- **`body_stream()`** — generator asynchroniczny, który strumieniuje chunki odpowiedzi. `aiter_bytes()` iteruje po bajtowych fragmentach (chunked transfer encoding lub buforowany odczyt).
- **`finally: await resp.aclose()`** — ZAMKNIECIE odpowiedzi w `finally`. To **krytyczne**: jeśli `StreamingResponse` (w `proxy_router`) zostanie przerwane (np. klient rozłączy się), `body_stream()` może nie zostać w pełni zużyty. `finally` zapewnia, że `resp.aclose()` zostanie wywołane ZAWSZE (zwalnia socket i pulę połączeń). Bez tego mielibyśmy **wyciek połączeń** (connection leak).

```python
        return {
            "status_code": resp.status_code,
            "response_headers": resp.headers,
            "body_iterator": body_stream(),
        }
```

- Zwraca krotkę wynikową (słówek) z trzema kluczami: kod statusu, nagłówki (do przefiltrowania w `proxy_router`), iterator body (do `StreamingResponse`).

---

### 4.8 Plik: `src/api/proxy_router.py`

**Cel:** serce gateway — pojedynczy **catch-all endpoint** `/{path:path}`, który orkiestruje cały przepływ żądania: dopasowanie trasy, autoryzacja JWT, rate limiting, forwarding i strumieniowanie odpowiedzi.

#### Blok 1: importy i zmienne modułowe

```python
"""Catch-all reverse proxy endpoint (spec §4.1).

One `/{path:path}` route inspects the request against the route table, applies
JWT auth and rate limiting where required, forwards the request to the matching
internal service, and streams the response back to the client. Internal-service
routing and header rewriting happen inside `ProxyService`.
"""

from __future__ import annotations

from uuid import uuid4

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from redis.asyncio import Redis

from src.api.routes import Route
from src.api.routing import build_routes
from src.core.config import Settings
from src.infrastructure.rate_limit import SlidingWindowRateLimiter
from src.services.proxy import ProxyService
from src.services.token import InvalidTokenError, TokenValidator

router = APIRouter()

_routes: list[Route] = []
_proxy: ProxyService | None = None
_validator: TokenValidator | None = None
_limiter: SlidingWindowRateLimiter | None = None
```

- **`router = APIRouter()`** — pusty router (bez prefixu). Zostanie dołączony do `app` w `main.py` bezpośrednio (bez `/api/v1` prefixu — bo catch-all reaguje na WSZYSTKIE ścieżki i nie ma sensu dodawać prefiksu).
- **Zmienne modułowe (`_routes`, `_proxy`, `_validator`, `_limiter`)** — singletony (lazy-initialized). `None` = "jeszcze nie zainicjalizowane". Inicjalizowane w `init_gateway()` przy starcie aplikacji.
- **Dlaczego zmienne modułowe zamiast Depends()?** Wcatch-all endpoincie nie ma potrzeby kaskady Depends (nie ma use case'ów do tworzenia). Singletony modułowe są prostsze i wystarczające — lifespan inicjalizuje je RAZ, endpointy tylko czytają.

#### Blok 2: `init_gateway()` i `close_gateway()`

```python
def init_gateway(settings: Settings, redis: Redis) -> None:
    """Wire gateway singletons. Called once at startup (see main.py)."""
    global _routes, _proxy, _validator, _limiter

    _routes = build_routes(settings)
    _proxy = ProxyService()
    _validator = TokenValidator(settings.jwt_public_key)
    _limiter = SlidingWindowRateLimiter(redis)
```

- **`global`** — modyfikacja zmiennych modułowych.
- **Kolejność inicjalizacji:**
  1. `_routes = build_routes(settings)` — budowa tabeli tras (listy `Route`).
  2. `_proxy = ProxyService()` — tworzenie klienta httpx (wspólna pula połączeń).
  3. `_validator = TokenValidator(settings.jwt_public_key)` — odczyt klucza publicznego z dysku i stworzenie walidatora.
  4. `_limiter = SlidingWindowRateLimiter(redis)` — podpięcie Redis do limitera.

**Co się stanie, jeśli `settings.jwt_public_key` nie istnieje?** `Path(...).read_bytes()` rzuci `FileNotFoundError`. Błąd pojawi się przy starcie aplikacji (w `lifespan`), co jest dobre — fail fast, zamiast odkrywać brak klucza przy pierwszym żądaniu.

```python
async def close_gateway() -> None:
    global _proxy
    if _proxy is not None:
        await _proxy.aclose()
        _proxy = None
```

- Zamyka httpx client (zwalnia socket i pulę połączeń). Redis zamykany osobno (`close_redis()`).

#### Blok 3: `_match_route()` — longest-prefix matching

```python
def _match_route(path: str) -> Route | None:
    """Return the most specific (longest-prefix) route matching the request path."""
    candidates = [
        r for r in _routes if path == r.path_prefix or path.startswith(r.path_prefix + "/")
    ]
    return max(candidates, key=lambda r: len(r.path_prefix)) if candidates else None
```

- **Longest-prefix matching:** dla żądania `/api/v1/loans/123/documents` pasują:
  - `/api/v1/loans` (pasuje: path zaczyna się od prefixu)
  - `/api/v1/loans/{id}` (nie pasuje: `{id}` jest placeholderem w definicji, ale w `path_prefix` jest dosłownym stringiem `/api/v1/loans/{id}`)
  - **Uwaga:** `/api/v1/loans/{id}` w `path_prefix` to DOSŁOWNY string `"/api/v1/loans/{id}"`. Czyli `path.startswith("/api/v1/loans/{id}/")` → `"api/v1/loans/123/documents".startswith("/api/v1/loans/{id}/")` → **False**. `{id}` nie jest ekspandowane w `path_prefix`! To oznacza, że `/api/v1/loans/{id}` **NIE pasuje** do żądań z rzeczywistym ID. Jedyny pasujący route to `/api/v1/loans`.
  - **To jest celowe (i pewna anomalia):** `path_prefix` nie jest "patternem", lecz dosłownym stringiem. Dlatego `/api/v1/loans/{id}` jest de facto ** nieużywalny** jako route w obecnej implementacji — żadne żądanie do `/api/v1/loans/123` nie spasuje się z prefixem `/api/v1/loans/{id}`. Jedyny pasujący route to `/api/v1/loans`. Wymaga to poprawy (patrz sekcja 11).
- **`max(candidates, key=lambda r: len(r.path_prefix))`** — wybieramy route z **najdłuższym** pasującym prefixem. Dla `/api/v1/auth/register` pasuje tylko `/api/v1/auth/register` (nie ma dłuższego prefixu). Dla `/api/v1/me` pasuje tylko `/api/v1/me`.
- **`path == r.path_prefix`** — dokładne dopasowanie (bez `/` na końcu). Przykład: `path = "/api/v1/me"` i `r.path_prefix = "/api/v1/me"` → match.

#### Blok 4: `_rate_key()` — klucz rate limitingu

```python
def _rate_key(route: Route, client_ip: str, applicant_id: str | None) -> str | None:
    if route.rate_key_prefix == "auth":
        return f"{route.rate_key_prefix}:{client_ip}"
    if route.rate_key_prefix == "loans":
        if applicant_id is None:
            return None
        return f"{route.rate_key_prefix}:{applicant_id}"
    return None
```

- **Auth routes:** klucz to `auth:{client_ip}` — limit per IP (nie per użytkownik, bo register/login nie wymagają auth, więc nie ma `applicant_id`).
- **Loans routes:** klucz to `loans:{applicant_id}` — limit per zalogowany użytkownik. `applicant_id` pochodzi z weryfikacji JWT.
  - **`if applicant_id is None: return None`** — jeśli auth wymagane, ale `applicant_id` nie został ustalony (cos się zepsuło), zwracamy `None` → `proxy_endpoint` zwróci 400 (patrz niżej). To powinno się nigdy nie zdarzyć (route z `rate_limit=True` jest zawsze `requires_auth=True`), ale chroni przed bugami.

#### Blok 5: `proxy_endpoint()` — catch-all endpoint

```python
@router.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
    include_in_schema=False,
)
async def proxy_endpoint(path: str, request: Request) -> Response:
```

- **`"/{path:path}"`** — **catch-all route**. `{path:path}` to specialna konwersja FastAPI: przechwytuje WSZYSTKIE ścieżki (włącznie z `/`). `path` w ciele funkcji to ścieżka BEZ pierwszego `/` (np. `api/v1/auth/register`).
- **`methods=[...]`** — obsługuje WSZYSTKIE metody HTTP (GET, POST, PUT, PATCH, DELETE). Gateway jest "przezroczysty" — nie filtruje metod.
- **`include_in_schema=False`** — endpoint NIE pojawia się w dokumentacji Swagger/OpenAPI. To celowe: Swagger nie powinien pokazywać catch-all (generowałby fałszywe endpointy).

```python
    if _proxy is None or _validator is None:
        return JSONResponse({"detail": "Gateway not ready"}, status_code=503)
```

- **Strażnik gotowości:** jeśli gateway nie został jeszcze zainicjalizowany (np. request przyszedł przed `init_gateway`), zwracamy 503 Service Unavailable. To "odporny" pattern: zamiast crashować (AttributeError na `None`), zwracamy czytelny błąd.

```python
    route = _match_route(path)
    if route is None or route.base_url == "":
        # Not a routable path (or handled later, e.g. SSE in Etap 4).
        return JSONResponse({"detail": "Not Found"}, status_code=404)
```

- **Dopasowanie trasy:** jeśli żaden route nie pasuje → 404. Jeśli route pasuje, ale `base_url == ""` (SSE stub) → też 404 (traktujemy to jak "nieobsługiwane").

```python
    client_headers = dict(request.headers.items())
```

- **Kopiowanie nagłówków** do zwykłego `dict`. ASGI przekazuje nagłówki jako listę krotek; `dict(...)` konwertuje je na słownik (łatwiesza manipulacja). `request.headers` jest "read-only view" — kopiowanie jest bezpieczne.

```python
    # JWT auth where required.
    applicant_id: str | None = None
    if route.requires_auth:
        try:
            uid = _validator.validate(client_headers.get("authorization"))
            applicant_id = str(uid)
        except InvalidTokenError:
            return JSONResponse(
                {"detail": "Unauthorized"},
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
            )
```

- **Weryfikacja JWT tylko dla chronionych endpointów.**
- **`client_headers.get("authorization")`** — zwraca `None` jeśli brak nagłówka (Obsługuje przypadek "brak auth").
- **`_validator.validate(...)`** — rzuca `InvalidTokenError` dla: brak nagłówka, zły format, wygasły token, refresh token, błąd podpisu.
- **`WWW-Authenticate: "Bearer"`** — nagłówek odpowiedzi HTTP 401. Mówi klientowi: "autoryzacja wymagana; użyj schematu Bearer". Standard z RFC 6750. Umożliwia przeglądarce/App automatyczne wyświetlenie dialogu logowania.
- **`applicant_id = str(uid)`** — UUID z JWT → string (do wstrzyknięcia w `X-User-ID` i do rate limitingu).

```python
    # Rate limiting.
    if route.rate_limit and _limiter is not None:
        client_host = request.client.host if request.client is not None else "unknown"
        rk = _rate_key(route, client_host, applicant_id)
        if rk is None:
            return JSONResponse({"detail": "Rate limit not applicable"}, status_code=400)
        allowed = await _limiter.allowed(rk, route.rate_limit_count, route.rate_window_seconds)
        if not allowed:
            return JSONResponse({"detail": "Rate limit exceeded"}, status_code=429)
```

- **`if route.rate_limit and _limiter is not None`** — rate limiting tylko gdy route go wymaga i Redis jest aktywny.
- **`request.client.host`** — adres IP klienta. `request.client` może być `None` (np. w testach, gdy brak połączenia TCP) → fallback `"unknown"`.
- **`_rate_key(...)`** — buduje klucz (np. `auth:192.168.1.1` lub `loans:550e8400-...`).
- **`rk is None`** — błąd (patrz `_rate_key` — zwraca `None` gdy `applicant_id` brak dla loans). Zwracamy 400.
- **`await _limiter.allowed(...)`** — sprawdzenie limitu. `429 Too Many Requests` gdy limit przekroczony. Klient powinien spróbować ponownie po `Retry-After` (choć ten nagłówek nie jest tu dodawany — kandydat do poprawy).

```python
    # Forward and stream back.
    correlation_id = client_headers.get("x-correlation-id") or str(uuid4())
```

- **Correlation ID:** jeśli klient dostarcza `X-Correlation-ID` → propagated (zachowujemy ten sam ID). W przeciwnym razie generujemy nowy UUID. To pozwala na śledzenie żądań od klienta przez wszystkie serwisy.

```python
    result = await _proxy.forward(
        method=request.method,
        path=path,
        query_string=request.scope.get("query_string", b""),
        base_url=route.base_url,
        client_headers=client_headers,
        body=request.stream(),
        applicant_id=applicant_id,
        correlation_id=correlation_id,
    )
```

- **`request.scope.get("query_string", b"")`** — query string z ASGI scope (bajty). Domyślnie `b""` (brak parametrów).
- **`request.stream()`** — iterator asynchroniczny body żądania (strumień bajtów). Przekazany do httpx jako `content=body` → forward bez buforowania.

```python
    resp_headers = {
        k: v
        for k, v in result["response_headers"].items()
        if k.lower() not in {"transfer-encoding", "connection", "content-length"}
    }
```

- **Filtrowanie nagłówków odpowiedzi:** usuwamy hop-by-hop (`transfer-encoding`, `connection`) oraz `content-length` (httpx/Starlette przeliczy go dla `StreamingResponse`). Bez tego moglibyśmy dostać "Content-Length mismatch" (gdy proved odpowiedzi buforowany różni się od strumieniowanego).

```python
    if correlation_id:
        resp_headers["X-Correlation-ID"] = correlation_id
```

- **Wstrzyknięcie `X-Correlation-ID`** w odpowiedzi (jeśli istnieje). Klient otrzymuje go z powrotem — może użyć do logów/supportu.

```python
    return StreamingResponse(
        result["body_iterator"],
        status_code=result["status_code"],
        headers=resp_headers,
    )
```

- **`StreamingResponse`** — FastAPI/Starlette strumieniuje body iteratora do klienta. Nie buforuje — najpierw wysyła status i nagłówki, potem strumieniuje chunki w miarę napływania z serwisu wewnętrznego. Pozwala obsłużyć odpowiedzi dowolnej wielkości (MB/GB) bez zużycia pamięci.

---

### 4.9 Plik: `src/main.py`

**Cel:** punkt wejścia aplikacji FastAPI. Tworzy aplikację, konfiguruje logging, zarządza cyklem życia (lifespan), rejestruje routery i healthchecki.

#### Blok 1: importy

```python
"""API Gateway FastAPI application (spec §4.1).

Stateless by design: no database. Validates JWT with the public key, rate-limits
with Redis, and reverse-proxies to internal services over the closed network.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from crediguard_observability import CorrelationIdMiddleware, configure_logging, get_logger
from fastapi import FastAPI

from src.api.proxy_router import close_gateway, init_gateway
from src.api.proxy_router import router as proxy_router
from src.core.config import get_settings
from src.infrastructure.redis import close_redis, get_redis
```

- **`from crediguard_observability import CorrelationIdMiddleware, configure_logging, get_logger`** — współdzielona biblioteka z `libs/observability`:
  - `configure_logging("gateway")` — konfiguruje strukturalny JSON logging (structlog) z nazwą serwisu "gateway".
  - `get_logger()` — pobiera logger bound z nazwą serwisu.
  - `CorrelationIdMiddleware` — ASGI middleware: propaguje `X-Correlation-ID` (omówione w sekcji 6).

#### Blok 2: konfiguracja na poziomie modułu

```python
configure_logging("gateway")
logger = get_logger()
settings = get_settings()
```

- **`configure_logging("gateway")`** — wywoływane RAZ przy importowaniu `main.py`. Konfiguruje structlog: procesory (timestamp, log level, JSON renderer), kontekst `service=gateway`. Wywoływane **przed** utworzeniem `FastAPI` — wymaga, by logger był skonfigurowany przed jakimkolwiek logowaniem.
- **`logger = get_logger()`** — instancja loggera bound do serwisu "gateway". Używana do logów w `lifespan`.
- **`settings = get_settings()`** — singleton `Settings` (lru_cache).

#### Blok 3: lifespan

```python
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    redis = await get_redis(settings.redis_url)
    init_gateway(settings, redis)
    logger.info(
        "Gateway started",
        applicant_url=settings.applicant_service_url,
        loan_url=settings.loan_service_url,
    )
    yield
    await close_gateway()
    await close_redis()
    logger.info("Gateway stopped")
```

- **Kolejność startowa:**
  1. `redis = await get_redis(...)` — tworzy klienta Redis (lazily).
  2. `init_gateway(settings, redis)` — inicjalizuje singletony: route table, proxy client, token validator, rate limiter.
  3. Log startu z adresami serwisów (dla debuggowania).
  4. `yield` — serwer pracuje (obsługuje żądania).
  5. `close_gateway()` — zamyka httpx client.
  6. `close_redis()` — zamyka Redis client.
  7. Log stopu.

**Dlaczego `get_redis()` jest `await`?** `Redis.from_url()` jest synchroniczne, ale `get_redis()` jest `async def` (dla spójności z `close_redis()`, które ma `await aclose()`). W praktyce `get_redis()` nie robi `await` wewnątrz (choć mogłaby, gdyby Redis client wymagał async init — w tej wersji nie wymaga).

#### Blok 4: aplikacja i middleware

```python
app = FastAPI(
    title="CrediGuard API Gateway",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(CorrelationIdMiddleware)
app.include_router(proxy_router)
```

- **`FastAPI(...)`** — tworzenie aplikacji. `lifespan=lifespan` — podpięcie lifecycle hook.
- **`app.add_middleware(CorrelationIdMiddleware)`** — dodaje middleware na **szczycie** stosu (kolejność: middleware → endpoint). `CorrelationIdMiddleware` czyta/generuje `X-Correlation-ID` i wiąże go z kontekstem structlog (via `contextvars`). Dzieje się to PRZED `proxy_endpoint`, więc `correlation_id` jest dostępny w logach gateway.
- **`app.include_router(proxy_router)`** — dołącza catch-all router. Brak prefixu `/api/v1` — bo catch-all reaguje na WSZYSTKIE ścieżki i dodanie prefiksu byłoby bez sensu (filtr routingowy robi robotę).

#### Blok 5: healthchecki

```python
@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready", tags=["health"])
async def ready() -> dict[str, str]:
    return {"status": "ready"}
```

- **`/health`** — liveness probe (k8s/Docker). Zawsze zwraca `200 OK`. Sprawdza: "czy proces żyje?" — niezależnie od Redis, wewnętrznych serwisów itd.
- **`/ready`** — readiness probe. Zwraca `200 OK`. **Uwaga:** nie sprawdza realnej gotowości (nie pinguje Redis, nie sprawdza wewnętrznych serwisów). Spec §9.3 opisuje readiness, który powinien sprawdzać DB/Kafka/Redis — ale w obecnej implementacji to uproszczenie. Zob. sekcja 11.

### 4.10 Plik: `src/services/cookies.py` (Etap 4 — HttpOnly cookie-auth)

**Cel:** translacja brzegowa — upstream Applicant zwraca goły JSON z tokenami, gateway zamienia go na dwa HttpOnly cookie i czyści body z sekretów. Surowe JWT nigdy nie docierają do JS przeglądarki.

- **Dwa ciasteczka, dwa reżimy:** `cg_access` (Path=`/`, 15 min — leci do API i SSE) i `cg_refresh` (Path=`/api/v1/auth/refresh`, 7 dni — nigdy nie leci do SSE ani loan-endpointów). Scoping `Path` to mechanizm, nie ozdobnik: zmiana Path refresha na `/` wysłałaby długowieczny sekret do każdego endpointu.
- **`sanitize_auth_body(body) -> (safe, access, refresh)`** — whitelist, nie passthrough: safe body zawiera tylko `token_type` + `expires_in`. Nowe pole w `TokenResponse` upstreamu NIE wycieknie samo (trzeba je dopisać do `safe` świadomie). Trzy strażnicy (zły JSON / nie-słownik / brak tokenów) z jednym komunikatem (anty-enumeracja kształtu upstreamu, jak `InvalidCredentials` w applicancie).
- **`build_cookie_auth_response` / `build_logout_response`** — dwa `headers.append("Set-Cookie", ...)` (osobne linie! RFC 6265 zabrania łączenia przecinkiem); status z upstreamu przekazywany (201 z register = 201 do klienta); logout to lustro z `Max-Age=0` (ten sam `Path` — czyszczenie z innym Path nie zadziała!).
- **`build_refresh_body_from_cookie`** — przeglądarka mówi cookie, Applicant mówi JSON; to zdanie je tłumaczy (refresh-only-cookie: §4.11).
- **Flaga `Secure`** z `settings.cookie_secure` (`False` lokalnie — przeglądarka odrzuciłaby `Secure` na `http://localhost` po cichu; `True` na prod-HTTPS).
- Pełna analiza linijka-po-linijce (w tym mapa pokrycia testami `test_cookies.py` i wykryte luki: nietestowane `Max-Age` refresh, brak asercji na Path przy czyszczeniu, zero testów builderów `build_*_response`): `docs/notification-service-guide.md` §4.9.

### 4.11 Etap 4 w gateway: SSE + cookie-fallback + CORS (przegląd zmian)

**Cel sekcji:** zebrać w jednym miejscu wszystko, co Etap 4 zmienił w gateway (poza `cookies.py` z §4.10). Protokół SSE (ramki, resume, heartbeat) opisuje `docs/notification-service-guide.md` §4.5/§4.8 — tu tylko brzeg.

- **`src/api/sse.py` (nowy, 248 linii):** `GET /api/v1/loans/{loan_id}/events` (`response_model=None` — FastAPI nie modeluje unii Response). Kolejność strażników: 503 (lifespan) → 403 (Origin/Referer — `SameSite=Lax` + check domyka CSRF) → 401 (cookie `cg_access` first, Bearer fallback) → 404 (ownership przez `_fetch_loan` z `X-User-ID`; 404 zamiast 403 — anty-enumeracja). `event_stream`: initial-state z REST (spóźniony nie wisi) → subscribe `loan-status:{applicant_id}` → filtr `loan_id` → resume po `Last-Event-ID` → heartbeat `: ping` co 15 s → terminal-break. Znane długi: `startswith` na Origin (prefix-atak — notification guide §11 luka #10), osobny `httpx.AsyncClient` na strumień (luka #9), stub `_disconnect_hint`.
- **`src/services/token.py`:** rozpad `validate(authorization)` na `validate_token` (serce bez transportu: podpis, `exp`, `type == "access"`, `UUID(sub)`) + wrappery `validate` (Bearer, kompatybilność z Etapem 2) i `validate_cookie` (nowość). Jeden punkt weryfikacji podpisu — bez dwuklasowości bezpieczeństwa.
- **`src/services/proxy.py`:** `cookie` w `_STRIPPED_HEADERS` (sekrety nie jadą w głąb) + filtr `set-cookie` z odpowiedzi upstreamu (obce ciasteczka nie wracają do przeglądarki).
- **`src/api/proxy_router.py`:** logout przed matchowaniem (endpoint lokalny, nie w tabeli — pierwsza wersja zwracała 404!); refresh-only-cookie (gateway wstrzykuje `{"refresh_token"}` z cookie do body upstreamu); buforowanie małego `TokenResponse` → `Set-Cookie` + sanitacja (sukces z nieparsowalnym body → 502, nie 500); błędy upstreamu 1:1; cookie-fallback na trasach chronionych (Bearer first). **Fix krytyczny przy okazji:** `_match_route` normalizuje wiodący `/` (G-1 — cały proxy zwracał 404 od Etapu 2!) + `tests/unit/test_routing_match.py`.
- **`src/core/config.py`:** 5 nowych pól (`frontend_url`, `cookie_secure`, `access/refresh_cookie_name`, `access/refresh_cookie_max_age` — parzystość z czasami JWT w applicancie).
- **`src/main.py`:** `CORSMiddleware` (jeden origin + `allow_credentials`, `expose_headers=["X-Correlation-ID"]`, kolejność middleware jako stos!) + rejestracja `sse_router` PRZED catch-all proxy (inaczej catch-all połknąłby `/events`).
- **Testy (4 nowe pliki):** `test_cookies.py` (nie-wyciek, scoping, cykl sekretu), `test_token_cookie.py` (RSA-2048 generowane w teście, negatywy, regresja Bearera), `test_sse.py` (format drutu, mapa kroków, decision), `test_routing_match.py` (strażnik fixa G-1). Pełne analizy: notification guide §9.4.

---

## 5. Ścieżki wywołań endpointów

W tej sekcji przeprowadzamy przez **pełną, krok po kroku** drogę żądania HTTP przez gateway — od curl po odpowiedź JSON — dla trzech reprezentatywnych scenariuszy: endpointu publicznego, chronionego i z rate limitingiem. Zaznaczamy punkty `await`, momenty weryfikacji JWT i podejmowania decyzji routingowych.

Wspólne założenia:
- Gateway działa na `http://localhost:8000`.
- Klucz publiczny RSA wygenerowany (w `services/applicant`) i udostępniony do gateway.
- Redis działa na `localhost:6380`.
- Używamy `curl`.

---

### 5.1 Endpoint publiczny: `POST /api/v1/auth/register`

Scenariusz: niezalogowany klient rejestruje się. Brak auth, jest rate limit (IP-based).

#### Krok 1: Żądanie HTTP (curl)

```bash
curl -X POST http://localhost:8000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"jan@test.pl","password":"haslo123","first_name":"Jan","last_name":"Kowalski"}'
```

- Klient łączy się z **gateway** (port 8000), nie bezpośrednio z Applicant Service (port 8001). Gateway jest jedynym punktem wejścia.

#### Krok 2: Uvicorn i middleware

- Uvicorn (serwer ASGI) odbiera TCP, parsuje HTTP.
- **`CorrelationIdMiddleware`** (w `main.py`) czyta nagłówek `X-Correlation-ID` z żądania (jeśli istnieje) lub generuje nowy UUID. Zapisuje go w `contextvars` (kontekst structlog). Dołącza go do nagłówka odpowiedzi.
- FastAPI dopasowuje ścieżkę `/api/v1/auth/register` do catch-all endpointu `/{path:path}` w `proxy_router.py`. `path = "api/v1/auth/register"`.

#### Krok 3: Sprawdzenie gotowości

```python
if _proxy is None or _validator is None:
    return JSONResponse({"detail": "Gateway not ready"}, status_code=503)
```

- Jeśli inicjalizacja nie zakończyła się (np. brak klucza RSA) → 503. W normalnym scenariuszu pomijamy.

#### Krok 4: Dopasowanie trasy

```python
route = _match_route(path)
```

- `path = "api/v1/auth/register"`. `candidates = [route dla /api/v1/auth/register]`. `max` → ten route.
- `route.base_url = "http://applicant:8001"` (nie pusty → jest docelowy serwis).

#### Krok 5: Autoryzacja (brak)

```python
if route.requires_auth:
    ...
```

- `route.requires_auth = False` → **pomijamy** weryfikację JWT. `applicant_id = None`.

#### Krok 6: Rate limiting

```python
if route.rate_limit and _limiter is not None:
    client_host = request.client.host if request.client is not None else "unknown"
    rk = _rate_key(route, client_host, applicant_id)
```

- `route.rate_limit = True`, `_limiter` jest skonfigurowany.
- `client_host = "127.0.0.1"` (lub `::1` dla IPv6).
- `_rate_key(route, "127.0.0.1", None)` → `route.rate_key_prefix == "auth"` → `"auth:127.0.0.1"`.

```python
    allowed = await _limiter.allowed(rk, route.rate_limit_count, route.rate_window_seconds)
    if not allowed:
        return JSONResponse({"detail": "Rate limit exceeded"}, status_code=429)
```

- `_limiter.allowed("auth:127.0.0.1", 5, 60)` → sprawdzenie w Redis: ile żądań `auth:127.0.0.1` w ostatnich 60s.
- Jeśli ≤ 4 (bo count < 5) → `True` → kontynuujemy.
- Jeśli ≥ 5 → `False` → **429 Too Many Requests**. Klient nie wie, ile żądań mu zostało (nie zdradzamy limitu — dobra praktyka security).

#### Krok 7: Forward

```python
correlation_id = client_headers.get("x-correlation-id") or str(uuid4())
result = await _proxy.forward(...)
```

- `correlation_id` — propagated lub newly generated.
- `ProxyService.forward()`:
  - `_rewrite_headers(...)` → filtruje nagłówki (usuwa `Authorization`, `X-User-ID`, `X-User-Roles`; nie wstrzykuje `X-User-ID` bo `applicant_id=None`).
  - `url = "http://applicant:8001/api/v1/auth/register"`.
  - Wysyła żądanie do Applicant Service (`http://applicant:8001`).

#### Krok 8: Applicant Service przetwarza

- Applicant Service otrzymuje żądanie z przefiltrowanymi nagłówkami: `Content-Type`, `X-Correlation-ID`. Brak `X-User-ID` (bo to endpoint publiczny).
- Wykonuje `RegisterUseCase.execute(request)` (sekwencja jak w przewodniku Applicant Service).
- Zwraca 201 z `TokenResponse` (access_token, refresh_token, expires_in).

#### Krok 9: Odpowiedź do klienta

- `result["status_code"] = 201`.
- `result["response_headers"]` — przefiltrowane (usunięte `transfer-encoding`, `connection`, `content-length`).
- `resp_headers["X-Correlation-ID"] = correlation_id` — dodane.
- `StreamingResponse` strumieniuje body (JSON z tokenami).

Przykładowa odpowiedź:
```json
{
  "access_token": "eyJhbGciOiJSUzI1NiIs...",
  "refresh_token": "eyJhbGciOiJSUzI1NiIs...",
  "token_type": "bearer",
  "expires_in": 899
}
```

---

### 5.2 Endpoint chroniony: `GET /api/v1/me`

Scenariusz: zalogowany klient pobiera profil. JWT wymagany, brak rate limitu.

#### Krok 1: Żądanie HTTP

```bash
curl http://localhost:8000/api/v1/me \
  -H "Authorization: Bearer eyJhbGciOiJSUzI1NiIs..."
```

- Klient dołącza token JWT w nagłówku `Authorization`.

#### Krok 2-3: Middleware + gotowość

- `CorrelationIdMiddleware` przetwarza (brak `X-Correlation-ID` w żądaniu → generuje nowy).
- Gotowość OK.

#### Krok 4: Dopasowanie trasy

- `path = "api/v1/me"`. `_match_route` znajduje route `/api/v1/me` → `route.base_url = "http://applicant:8001"`.

#### Krok 5: Autoryzacja JWT (KLUCZOWY MOMENT)

```python
if route.requires_auth:
    try:
        uid = _validator.validate(client_headers.get("authorization"))
        applicant_id = str(uid)
    except InvalidTokenError:
        return JSONResponse(
            {"detail": "Unauthorized"},
            status_code=401,
            headers={"WWW-Authenticate": "Bearer"},
        )
```

- `route.requires_auth = True` → weryfikacja JWT.
- `_validator.validate("Bearer eyJhbGci...")`:
  - Sprawdza prefiks `Bearer `.
  - `token = "eyJhbGci..."` (bez prefiksu).
  - `jwt.decode(token, public_key, algorithms=["RS256"])` → weryfikacja podpisu, wygaśnięcia, algorytmu.
  - Sprawdza `type == "access"` (nie refresh).
  - `UUID(payload["sub"])` → np. `UUID("550e8400-e29b-41d4-a716-446655440000")`.
- `applicant_id = "550e8400-e29b-41d4-a716-446655440000"` (string UUID).

**Scenariusze błędu:**
- Brak nagłówka `Authorization` → `InvalidTokenError` → 401 + `WWW-Authenticate: Bearer`.
- Token wygasł → `JWTError` w `jwt.decode` → `InvalidTokenError` → 401.
- Token refresh (nie access) → `InvalidTokenError("Not an access token")` → 401.
- Zły podpis → `JWTError` → `InvalidTokenError` → 401.
- Zły format UUID w `sub` → `ValueError` → `InvalidTokenError` → 401.

#### Krok 6: Rate limiting (brak)

- `route.rate_limit = False` → pomijamy.

#### Krok 7: Forward z zaufaną tożsamością

```python
result = await _proxy.forward(
    ...,
    applicant_id=applicant_id,  # "550e8400-..."
    ...
)
```

- `_rewrite_headers(...)`:
  - Usuwa z nagłówków klienta: `Authorization`, `X-User-ID`, `X-User-Roles`, `X-Correlation-ID`.
  - Wstrzykuje `X-User-ID: 550e8400-e29b-41d4-a716-446655440000`.
  - Wstrzykuje `X-User-Roles: applicant`.
  - Wstrzykuje `X-Correlation-ID: <id>`.
- URL: `http://applicant:8001/api/v1/me`.
- `request.stream()` → body (brak w GET).

#### Krok 8: Applicant Service przetwarza

- Applicant Service otrzymuje żądanie z `X-User-ID: 550e8400-...`.
- `GetMeUseCase.execute(applicant_id)` → `get_by_id(applicant_id)` → zwraca profil.
- Zwraca 200 z `ApplicantResponse`.

#### Krok 9: Odpowiedź

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "email": "jan@test.pl",
  "first_name": "Jan",
  "last_name": "Kowalski",
  "created_at": "2026-01-15T10:30:00Z"
}
```

---

### 5.3 Endpoint chroniony + rate limit: `POST /api/v1/loans`

Scenariusz: zalogowany klient tworzy wniosek pożyczkowy. JWT wymagany, rate limit (3/10min per user).

#### Krok 1: Żądanie HTTP

```bash
curl -X POST http://localhost:8000/api/v1/loans \
  -H "Authorization: Bearer eyJhbGci..." \
  -H "Content-Type: application/json" \
  -d '{"amount": 5000, "currency": "PLN", "term_months": 12}'
```

#### Krok 2-4: Middleware, gotowość, routing

- `path = "api/v1/loans"`. `_match_route` → route `/api/v1/loans` → `loan_service_url`.

#### Krok 5: JWT

- `applicant_id = "550e8400-..."`.

#### Krok 6: Rate limiting (KLUCZOWY MOMENT)

```python
client_host = request.client.host  # "127.0.0.1"
rk = _rate_key(route, client_host, applicant_id)
# route.rate_key_prefix == "loans" → rk = "loans:550e8400-e29b-41d4-a716-446655440000"
allowed = await _limiter.allowed("loans:550e8400-...", 3, 600)
```

- Klucz: `loans:550e8400-...` — limit per **użytkownik** (nie per IP). Dlaczego? Bo rate limit loans chroni przed nadużyciami przez JEDNEGO użytkownika (np. automatyczne tworzenie setek wniosków). Limit per IP byłby niesprawiedliwy wobec użytkowników za NAT/proxy.
- `limit = 3`, `window = 600` (10 minut).
- `SlidingWindowRateLimiter.allowed("loans:550e8400-...", 3, 600)`:
  - `now = time.time()` → np. `1736951400.123`.
  - `window_start = 1736951400.123 - 600 = 1736950800.123`.
  - Pipeline Redis:
    1. `ZREMRANGEBYSCORE ratelimit:loans:550e8400-... 0 1736950800.123` — usunięcie starych wpisów.
    2. `ZCARD ratelimit:loans:550e8400-...` → np. `1` (jedno żądanie w ostatnich 10 min).
    3. `ZADD ratelimit:loans:550e8400-... 1736951400.123 "1736951400.123"` — dodanie bieżącego.
    4. `EXPIRE ratelimit:loans:550e8400-... 600` — TTL.
  - `count = 1` (przed dodaniem bieżącego).
  - `1 < 3` → `True` → kontynuujemy.

**Scenariusz błędu (3. żądanie w 10 min):**
- `count = 2` (po usunięciu starych) → `2 < 3` → `True`.
- 4. żądanie: `count = 3` → `3 < 3` → `False` → **429 Too Many Requests**.

#### Krok 7: Forward

- `_rewrite_headers(...)` → wstrzykuje `X-User-ID`, `X-User-Roles`, `X-Correlation-ID`.
- `url = "http://loan-application:8002/api/v1/loans"`.
- Body strumieniowane (JSON z kwotą).

#### Krok 8: Loan Application Service przetwarza

- Otrzymuje żądanie z `X-User-ID: 550e8400-...`.
- Tworzy wniosek pożyczkowy (maszyna stanów → status `PENDING`).
- Zwraca 201 z `LoanApplicationResponse` (ID wniosku, status).

#### Krok 9: Odpowiedź

- 201 Created z danymi nowego wniosku.

---

### 5.4 Scenariusze błędów

#### 5.4.1 Nieznana ścieżka → 404 (ROZWIĄZANE: G-1 naprawiony w Etapie 4)

```bash
curl http://localhost:8000/api/v1/unknown
```

- `_match_route("api/v1/unknown")` → brak kandydatów → `None`.
- Zwraca `JSONResponse({"detail": "Not Found"}, status_code=404)`.
- **Aktualizacja (Etap 4):** historycznie ten sam 404 dostawała KAŻDA ścieżka (luka G-1: brak wiodącego `/` po stronie Starlette — §11). Naprawiono normalizacją w `_match_route` + testem `test_routing_match.py`. Od Etapu 4 404 oznacza naprawdę „nieznana ścieżka" (test `test_unknown_path_returns_none`). Poniższy akapit zostaje jako dokumentacja znalezionego false positive.

#### 5.4.2 SSE endpoint (ZREALIZOWANE w Etapie 4 — historia stubu poniżej)

```bash
curl http://localhost:8000/api/v1/loans/123/events \
  -H "Authorization: Bearer eyJhbGci..."
```

- **Droga 1 (realna, przez Starlette):** catch-all `/{path:path}` daje `path = "api/v1/loans/123/events"` (BEZ wiodącego `/`!). `_match_route` porównuje z prefixami ZE slashem → brak kandydatów → `None` → 404 (luka krytyczna G-1, §11).
- **Droga 2 (hipotetyczna, gdyby slash był):** nawet z wiodącym `/` kandydatem byłby `/api/v1/loans` (prefix + `/` pasuje!), a NIE literalny `/api/v1/loans/{id}/events` (ten string nie występuje w realnych ścieżkach — §11 luka `path_prefix` z `{id}`). Gałąź `route.base_url == ""` (SSE stub) jest więc **nieosiągalna** — request poszedłby proxy do `loan-application:8002` zamiast 404!
- Wniosek: stub SSE nie działa ani jako 404 (realnie: 404, ale z powodu G-1, nie stubu), ani jako przyszły punkt zaczepienia (niedostępny routing). Etap 4 musi naprawić oba poziomy. W przyszłości (Etap 4) zostanie tu zaimplementowane SSE.

> **Aktualizacja (Etap 4 — zrealizowane):** oba poziomy naprawione: (1) `_match_route` normalizuje wiodący `/` (fix G-1); (2) `/events` obsługuje osobny `sse_router` (`src/api/sse.py`) rejestrowany PRZED catch-all w `main.py` — gałąź `base_url == ""` w tabeli jest już nieosiągalna dla tej ścieżki (została jako strażnik przyszłych stubów). Weryfikacja: `curl` bez cookie → 401 z handlera SSE (nie 404 z proxy!), obcy `Origin` → 403. Pełna analiza protokołu: `docs/notification-service-guide.md` §4.5/§4.8 (§4.11 powyżej dla brzegu).

#### 5.4.3 Brak autoryzacji → 401

```bash
curl http://localhost:8000/api/v1/me
```

- `route.requires_auth = True`. `client_headers.get("authorization")` → `None`.
- `_validator.validate(None)` → `InvalidTokenError("Missing or invalid Authorization header")`.
- `401 Unauthorized` z `WWW-Authenticate: Bearer`.

#### 5.4.4 Wygasły token → 401

- JWT z `exp` w przeszłości → `jwt.decode` rzuca `JWTError` → `InvalidTokenError` → 401.

#### 5.4.5 Przekroczenie rate limitu → 429

- Po 5 żądaniach `POST /api/v1/auth/register` z tego samego IP w ciągu minuty.
- `_limiter.allowed("auth:1.2.3.4", 5, 60)` → `False` (count = 5).
- `429 Too Many Requests`.

#### 5.4.6 Gateway nie gotowy → 503

- Żądanie przed `init_gateway()` (np.Race condition przy starcie).
- `503 Service Unavailable`.

### 5.5 Tabela kodów — kto, kiedy, co zwraca

| Kod | Kto generuje | Kiedy | Treść | Retry klienta? |
|-----|--------------|-------|-------|----------------|
| 200/201/202 | serwis wewnętrzny (passthrough!) | sukces upstream | body serwisu | nie (sukces) |
| 400 | gateway (`Rate limit not applicable`) | route loans bez `applicant_id` (praktycznie niemożliwe — wymaga auth) | `{"detail": ...}` | nie (bug/config) |
| 401 | gateway (JWT) | brak/zły/wygasły token, refresh zamiast access | `{"detail": "Unauthorized"}` + `WWW-Authenticate: Bearer` | tak — po odświeżeniu tokena |
| 404 | gateway (routing) | brak route / stub SSE (historycznie: KAŻDY request przy luce G-1 — naprawione w Etapie 4!) | `{"detail": "Not Found"}` | nie |
| 405 | Starlette | OPTIONS / nieobsługiwana metoda (preflight CORS!) | generyczne | nie (luka G-3) |
| 422 | serwis wewnętrzny (passthrough) | zły JSON (Pydantic w serwisie) | `{"detail": [...]}` | tak — po poprawie body |
| 429 | gateway (limiter) | >5/min/IP (auth) lub >3/10min/user (loans) | `{"detail": "Rate limit exceeded"}` (bez `Retry-After`!) | tak — po oknie (60 s / 600 s) |
| 500 | FastAPI (nieobsłużony wyjątek) | timeout upstream (G-4!), błąd serwisu, bug | generyczne | tak — z backoff (ostrożnie: 500 po timeout mogło wykonać operację!) |
| 503 | gateway (strażnik) | request przed `init_gateway` | `{"detail": "Gateway not ready"}` | tak — za chwilę (startup) |

- Kluczowa własność: gateway **nie mapuje** statusów upstream (404 serwisu = 404 klienta). Mapuje tylko własne decyzje (401/404-routing/429/503). Wyjątek: błąd sieci upstream → 500 (G-4 — powinno być 502/503/504!).

---

## 6. Koncepcje techniczne

Ta sekcja wyjaśnia — prostym językiem, z przykładami — kluczowe konstrukcje Pythona, wzorce i mechanizmy użyte w gateway. Podstawowe konstrukcje Pythona (async/await, dataclass, property, from __future__) są omówione w przewodniku Applicant Service; tutaj skupiamy się na elementach **unikalnych dla gateway**.

### 6.1 Wzorzec Reverse Proxy

**Reverse proxy** to serwer, który stoi "odwrotnie" (stąd "reverse") wobec zwykłego proxy: klient łączy się z proxy (gateway), a proxy przekazuje żądanie do właściwego serwera docelowego. Klient **nie zna** adresu serwera docelowego.

> **Analogia:** biuro tłumaczeń. Klient wysyła tekst do biura (gateway). Biuro kieruje tekst do właściwego tłumacza (internal service) i zwraca przetłumaczony tekst. Klient nie musi znać adresu tłumacza.

**Cechy gateway jako reverse proxy:**
- **Przezroczystość** — klient łączy się z jednym adresem (gateway:8000); nie wie, że za nim są trzy serwisy.
- **Bezpieczeństwo** — wewnętrzne serwisy nie są wystawione na internet (brak port-forwardingu).
- **Walidacja** — gateway sprawdza tożsamość (JWT) PRZED przekazaniem do serwisu docelowego.
- **Optymalizacja** — shared connection pooling (jeden `httpx.AsyncClient` na wiele żądań).

**Buffered vs Streaming proxy:**
- **Buffered:** proxy buforuje CAŁĄ odpowiedź w pamięci, potem wysyła do klienta. Prostsze, ale: pamięć rośnie z rozmiarem odpowiedzi; przy 100MB pliku → 100MB RAM.
- **Streaming:** proxy strumieniuje chunki odpowiedzi "na bieżąco". Gateway używa `stream=True` w httpx + `StreamingResponse` w FastAPI. Pamięć stała (~4KB na chunk) niezależnie od rozmiaru odpowiedzi. To jest podejście użyte w projekcie.

### 6.2 Longest-Prefix Matching

Gateway dopasowuje żądania do tabeli tras za pomocą **najdłuższego pasującego prefiksu**. To ten sam algorytm, co w routing IP (longest prefix match w tablicach routingu).

**Przykład:**

```
Tabela tras:
  /api/v1/auth/register  → applicant
  /api/v1/auth/login     → applicant
  /api/v1/loans          → loan-application
  /api/v1/me             → applicant
```

Żądanie `GET /api/v1/loans/123/documents`:
- Kandydaci: `/api/v1/loans` (pasuje: startsWith + /)
- Najdłuższy: `/api/v1/loans` → proxy do `loan-application:8002`.

Żądanie `POST /api/v1/auth/register`:
- Kandydaci: `/api/v1/auth/register` (dokładne dopasowanie)
- Najdłuższy: `/api/v1/auth/register` → proxy do `applicant:8001`.

**Dlaczego longest-prefix, a nie exact match?** Bo wiele żądań ma dynamiczne ścieżki (np. `/api/v1/loans/123`). Gateway nie zna z góry wszystkich ID; dopasowuje do najbliższego "drzewa" prefiksów.

### 6.3 Sliding Window Rate Limiting

**Fixed window:** okno czasowe o stałych granicach (np. 0:00-0:09, 0:10-0:19). Problem: żądania o północy okna (0:09 i 0:10) "widzą" dwa różne okna, choć minęła tylko 1 sekunda → podwójna pojemność.

**Sliding window:** okno przesuwa się z każdym żądaniem. "Ostatnie 60 sekund" zawsze obejmuje bieżący moment. Sprawiedliwszy, bo nie ma "skoku" o północy okna.

**Implementacja w Redis Sorted Sets:**
- Klucz: `ratelimit:{key}` (np. `ratelimit:auth:192.168.1.1`).
- Sorted Set: elementy = timestampy (string), score = timestamp (float).
- Operacje (pipeline, atomowe):
  1. `ZREMRANGEBYSCORE 0 window_start` — usuń stare.
  2. `ZCARD` — ile żądań w oknie (przed dodaniem bieżącego).
  3. `ZADD now` — dodaj bieżące żądanie.
  4. `EXPIRE window_seconds` — TTL na klucz.

**Dlaczego Sorted Set zamiast counter (INCR)?** Counter + EXPIRE daje fixed window (reset po TTL). Sorted Set pozwala na dowolny moment czasu jako "kreskę" okna. Sliding window jest sprawiedliwszy i bezpieczniejszy.

**Dlaczego pipeline transaction?** Bez `transaction=True` istnieje race condition: dwa żądania gleichzeitig mogłyby oba zobaczyć `ZCARD = 2` (limit 3), oba dodać `ZADD`, i oba przejść (a powinno jedno). Pipeline z `transaction=True` gwarantuje atomowość: `MULTI ... EXEC` serializuje operacje.

### 6.4 RS256 JWT Validation

**Asymetryczna kryptografia RSA:**
- Para kluczy: prywatny (tajny) + publiczny (jawny).
- Podpis: tworzony kluczem **prywatnym** (podpisuje Applicant Service).
- Weryfikacja: kluczem **publicznym** (gateway).
- **Klucz publiczny NIGDY nie pozwala podpisać nowych tokenów** — jednokierunkowa zależność matematyczna.

**Algorytm RS256:**
1. Weź payload JWT (JSON z claims: sub, exp, type...).
2. Skróć Hash SHA-256.
3. Podpisz skrót kluczem prywatnym RSA (PKCS#1 v1.5 padding).
4. Wynik: podpis (256 bajtów, base64url).

**Weryfikacja (gateway):**
1. Odczytaj payload (base64url decode).
2. Odczytaj podpis (base64url decode).
3. Skróć payload SHA-256.
4. Weryfikuj podpis kluczem publicznym RSA.
5. Sprawdź `exp` (nie wygasł).
6. Sprawdź `type == "access"` (nie refresh).
7. Zwróć `sub` (UUID).

**Dlaczego wymuszamy `algorithms=["RS256"]`?** Atak "algorithm confusion": jeśli nie wymusimy białej listy, atakujący może zmienić `alg` na `HS256` i podpisać token publicznym kluczem (który jest jawny!) jako sekretem symetrycznym. Wymuszenie `algorithms=["RS256"]` blokuje ten atak.

### 6.5 Header Rewriting (Security Model)

Gateway jest **jedynym punktem**, który może ustanowić zaufane tożsamości. Wewnętrzne serwisy ufają nagłówkom `X-User-ID` i `X-User-Roles`, bo:
1. Gateway **zawsze usuwa** te nagłówki z żądania klienta (niezależnie od logiki).
2. Gateway **zawsze wstrzykuje** je po weryfikacji JWT.
3. Wewnętrzne serwisy NIE SĄ dostępne z internetu (brak port-forwardingu) → żaden klient nie może wysłać bezpośrednio.

**Atak bez header rewriting:**
1. Atakujący wysyła `X-User-ID: admin-uuid` do internal service.
2. Internal service ufa temu nagłówkowi → traktuje atakującego jako admina.
3. Atakujący ma dostęp do cudzych danych.

**Obrona (header rewriting):**
1. Gateway usuwa `X-User-ID` z żądania klienta.
2. Gateway weryfikuje JWT → wstrzykuje prawdziwy `X-User-ID` (z tokena).
3. Atakującego `X-User-ID` nigdy nie dociera do internal service.

### 6.6 Hop-by-Hop Headers

**End-to-end headers** (np. `Content-Type`, `Authorization`) są przeznaczone dla odbiorcy końcowego i powinny być przekazywane przez proxy.

**Hop-by-hop headers** (np. `Connection`, `Transfer-Encoding`, `Upgrade`) są przeznaczone dla POJEDYNCZEGO połączenia (hop) i NIE powinny być przekazywane dalej. Definicja: RFC 2616 §13.5.1.

Gateway usuwa hop-by-hop (`Connection`, `Transfer-Encoding`, `Host`) i dodatkowe techniczne (`Content-Length`, `Upgrade`) z nagłówków klienta, bo:
- `Connection: keep-alive` jest relewantne między klientem a gateway, nie między gateway a internal service.
- `Transfer-Encoding: chunked` jest relewantne między gateway a internal service (nie między klientem a gateway).
- `Content-Length` jest przeliczane automatycznie przez httpx/Starlette dla `StreamingResponse`.
- `Host` mógłby wpłynąć na routing w internal services.

### 6.7 `httpx.AsyncClient` + connection pooling — jedna pula na proces

`ProxyService.__init__` tworzy **jeden** `httpx.AsyncClient(timeout=30.0)` na cały proces (singleton modułowy przez `init_gateway`). Dlaczego nie nowy klient per request?

- **Pooling połączeń:** klient trzyma otwarte keep-alive TCP do `applicant:8001`, `loan-application:8002`, `document:8003`. Nowy request reuse'uje socket (bez handshake TCP + bez negocjacji HTTP/1.1 od zera — ~1–5 ms oszczędności na zimnym połączeniu). Przy 100 RPS to różnica między „płynnie" a „TIME_WAIT wszędzie".
- **`timeout=30.0`:** całkowity deadline requestu (connect + write + read). Wewnętrzny serwis wolniejszy niż 30 s → `httpx.TimeoutException` → 500 (nie wiszący request w nieskończoność!). 30 s to dużo jak na API (loan-app odpowiada w ms) — ustawione z zapasem na zimny start / GC / spike. Alternatywa: granularne timeouty (`httpx.Timeout(connect=2, read=10, ...)`) — precyzyjniejsze (odróżnia „serwis nieosiągalny" od „serwis wolny"), dziś nieużywane.
- **`aclose()` w `close_gateway`:** zwrot socketów przy shutdown (bez tego: warningi `Unclosed client`, wiszące FD do SIGKILL).
- **Pułapka testowa:** `ProxyService()` w testach nagłówków tworzy klienta (nieużywanego — §9.3). Konstruktor miesza dwie odpowiedzialności (klient HTTP + przepisywanie nagłówków) — `_rewrite_headers` jako `@staticmethod` lub osobna funkcja usunęłaby potrzebę klienta w testach.

### 6.8 `StreamingResponse` — odpowiedź bez bufora

Gateway **nigdy nie trzyma całej odpowiedzi w pamięci**: `forward()` zwraca `body_iterator` (async generator nad `resp.aiter_bytes()`), a `proxy_endpoint` owija go w Starlette `StreamingResponse`. Dla laika: zwykła odpowiedź to „najpierw cała paczka, potem wysyłka" (bufor = rozmiar odpowiedzi w RAM); streaming to „czytam kawałek → wysyłam kawałek" (bufor = jeden chunk ~4 KB).

- **`stream=True` w `client.send`:** httpx nie pobiera body od razu (tylko status + nagłówki); treść płynie przez `aiter_bytes()` na żądanie iteratora. Bez `stream=True` cały body lądowałby w pamięci gateway (upload 100 MB przez gateway = 100 MB RAM na request!).
- **`body_stream()` z `try/finally`:** generator zamyka odpowiedź upstream (`await resp.aclose()` — zwrot socketu do puli!) także przy przerwaniu przez klienta (rozłączenie w połowie pobierania). Bez `finally`: zerwane pobieranie = wyciek połączenia z puli (pool wyczerpany po N zerwaniach — DoS przez zamykanie kart!).
- **Filtrowanie nagłówków odpowiedzi** (`transfer-encoding`, `connection`, `content-length` — §4.8): `StreamingResponse` liczy `Content-Length` sam (albo używa chunked); przepuszczenie upstreamowego `Content-Length` dałoby mismatch (gateway strumieniuje, długość inna niż deklarowana → klient ucina/wiesza się).
- **Status transparentny:** `status_code=result["status_code"]` — gateway nie mapuje (404 z serwisu = 404 do klienta; 500 = 500). Brama jest przezroczysta dla semantyki, nieprzezroczysta dla tożsamości.

### 6.9 Correlation ID end-to-end — nić Ariadny

```python
correlation_id = client_headers.get("x-correlation-id") or str(uuid4())
```

- **Propaguj albo generuj:** frontend (lub test) może nadać `X-Correlation-ID` (własny trace!), gateway przejmuje; inaczej losowy UUIDv4. ID płynie dalej: wstrzyknięte do upstream (`X-Correlation-ID` w `_rewrite_headers` — §4.7) + zwrócone klientowi w odpowiedzi (debug: „zgłoś ten ID do supportu").
- **Asymetria ze SPEC:** gateway propaguje **klientowy** ID (nie generuje zawsze własnego) — dobre dla frontendu (jeden ID od kliknięcia), ryzykowne przy kolizjach (klient śle stały `"123"` we wszystkim — korelacja rozmyta; akceptowane w MVP). Loan-app worker i tak gubi nić (świeży UUID w kopercie — luka w loan-app guide §11!).
- **`x-correlation-id` w `_STRIPPED_HEADERS`:** klientowy nagłówek jest **usuwany** z forwardowanych i **wstrzykiwany** z powrotem jako pojedynczy, kanoniczny (bez duplikatów! — dwa `X-Correlation-ID` w upstream to rozjazd). Normalizacja, nie tylko passthrough.
- **Middleware `CorrelationIdMiddleware`** (z `libs/observability`, wpięty w `main.py`): dokleja ID do logów structlog (każdy wpis gateway ma pole korelacji — `docker compose logs | grep <id>` pokazuje pełną historię requestu). Dwie warstwy: middleware (logi) + proxy_endpoint (propagacja) — razem pełny ślad.

### 6.10 Catch-all `/{path:path}` — jeden endpoint na wszystko

```python
@router.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"], include_in_schema=False)
async def proxy_endpoint(path: str, request: Request) -> Response:
```

- **`{path:path}` (konwerter `path`):** przechwytuje wszystko łącznie ze slashami (`api/v1/loans/123/documents` jako jedna zmienna). Zwykły `{path}` (konwerter `str`) zatrzymałby się na pierwszym `/`. Dla laika: `:path` to „weź resztę URL, nieważne jak długą".
- **`path` BEZ wiodącego `/`** (Starlette obcina: request `/api/v1/x` → `path="api/v1/x"`) — a `_match_route` porównuje z prefixami **ZE** slashem (`/api/v1/x`)! Stąd luka krytyczna G-1 (§11): realne requesty nigdy nie matchują (zweryfikowane empirycznie — §11). Catch-all łapie, matcher gubi.
- **Brak `OPTIONS`** w `methods` (GET/POST/PUT/PATCH/DELETE tylko): preflight CORS przeglądarki (OPTIONS) dostaje 405 (do tego brak `CORSMiddleware` — §11 luka G-3: frontend z przeglądarki nie zadzwoni wcale!).
- **`include_in_schema=False`:** catch-all poza OpenAPI (Swagger pokazałby jeden endpoint `/{path}` zamiast prawdziwych — mylące; prawdziwe ścieżki dokumentują wewnętrzne serwisy, nie brama).
- **Surowy `Request`, nie modele Pydantic:** gateway nie parsuje body (nie zna schematów wniosków — passthrough!). `request.stream()` (async iterator bajtów) leci do httpx jako `content` (zero buforowania, zero walidacji — brama nie zagląda do koperty).
- **`request.scope.get("query_string", b"")` + ręczne `copy_with(query=...)`:** query przekazywane bajt-w-bajt (bez re-enkodowania httpx — polskie znaki, `+`, `%2F` docierają nietknięte). `params=None` w `build_request` (żadnego „mądrego" parsowania — przezroczystość).

### 6.11 `frozenset` i `Mapping` — typy dla bezpieczeństwa i elastyczności

```python
_STRIPPED_HEADERS = frozenset({"x-user-id", "x-user-roles", "authorization", "x-correlation-id"})
_BLOCKED_HEADERS = frozenset({"host", "content-length", "connection", "transfer-encoding", "upgrade"})
```

- **`frozenset` (nie `set`, nie `list`):** niemutowalny zbiór stałych bezpieczeństwa. `set` pozwalałby `add` w runtime (ktoś „tymczasowo" dodaje wyjątek i zapomina — dziura); `list` dawałaby `in` w O(n) + mutowalność. `frozenset` mówi: „te zbiory są kompletne od importu, próba zmiany to `AttributeError`". Test przynależności O(1) (jak `VALID_TRANSITIONS` w loan-app — ten sam idiom w obu serwisach!).
- **Małe litery w zestawach + `lk = k.lower()`:** porównanie case-insensitive (HTTP/1.1 nagłówki case-insensitive! `X-User-ID`, `x-user-id`, `X-USER-ID` to to samo). Bez lowerowania atakujący ominąłby strip (`x-uSeR-iD` przeszłoby!). Testy wołają wielkoliterowe warianty — case-insensitivity implementacji niepokryta testem (luka w §9.3!).
- **`Mapping[str, str]` (nie `dict`):** abstrakcja „coś słownikopodobnego" (protokoł `__getitem__`/`__iter__`/`__len__`). `_rewrite_headers` przyjmuje `dict` z endpointu i każdy inny mapping w teście (elastyczność bez kosztów). Z `collections.abc` (stdlib), nie `typing` (nowocześnie).

### 6.12 Modułowe singletony zamiast `Depends()` — kompozycja bez frameworka

```python
_routes: list[Route] = []
_proxy: ProxyService | None = None
_validator: TokenValidator | None = None
_limiter: SlidingWindowRateLimiter | None = None

def init_gateway(settings: Settings, redis: Redis) -> None:
    global _routes, _proxy, _validator, _limiter
    ...
```

- **Dlaczego nie `Depends` jak w loan-app?** Bo gateway nie ma use case'ów do komponowania per request (4 obiekty na proces: trasy, klient HTTP, walidator, limiter — wszystkie bezstanowe/współdzielone). `Depends` per request budowałby fabryki dla czegoś, co istnieje raz. Singletony modułowe (import raz = jedna instancja) + `init_gateway` w lifespan (jawne okablowanie) = prostsze i szybsze (zero narzutu DI na request).
- **Cena:** testowalność (testy muszą wołać `init_gateway` albo grzebać w globalach — unit testy omijają problem, testując klasy wprost, nie endpoint!). Endpoint testowany przez TestClient wymagałby `init_gateway` z prawdziwym Redisem albo monkeypatch globali (nieeleganckie — dlatego integracji nie ma; błędne koło: architektura zniechęca do testów, brak testów ukrywa bugi jak G-1!).
- **`_limiter` pominięte w `close_gateway`** (zamyka tylko `_proxy` — limiter nie trzyma zasobów (Redis zamyka `close_redis` osobno), więc poprawne; ale asymetria `init` (4 obiekty) vs `close` (1 obiekt) myli czytelnika — komentarz by pomógł).
- **Strażnik 503** (`if _proxy is None or _validator is None` — §4.8): request przed `init_gateway` (restart w locie) dostaje czytelną odpowiedź, nie `AttributeError`. Odporność na starcie kosztem jednej gałęzi.

---

## 7. Bezpieczeństwo

Ta sekcja dogłębnie analizuje mechanizmy bezpieczeństwa gateway i ich uzasadnienie.

### 7.1 Asymetryczna weryfikacja JWT (RS256 — public key only)

**Kluczowa cecha:** gateway nigdy nie widzi klucza prywatnego. Trzyma wyłącznie klucz publiczny.

| Cecha | Kto |
|-------|-----|
| Klucz prywatny | TYLKO Applicant Service (podpisuje tokeny) |
| Klucz publiczny | Gateway + inne serwisy (weryfikują) |

**Co to oznacza w praktyce?**
1. Jeśli gateway zostanie skompromitowany, atakujący NIE MOŻE podpisać nowych tokenów.
2. Nawet jeśli ktoś ukradnie klucz publiczny — nie może podpisać tokenów.
3. Tokeny mogą być weryfikowane przez dowolną liczbę serwisów bez współdzielenia sekretu.

### 7.2 Header Anti-Spoofing

**Wektor ataku:** klient wysyła nagłówki `X-User-ID` i `X-User-Roles` z dowolną wartością. Jeśli internal service zaufa tym nagłówkom, klient może podszyć się pod dowolnego użytkownika.

**Obrona — zawsze usuwaj i wstrzykuj:**

```
Klient → [X-User-ID: attacker-uuid] → Gateway → [usunięcie] → [X-User-ID: real-uuid] → Internal Service
```

**Ochrona jest WIELOWARSTWOWA:**
1. **Usunięcie** — `_STRIPPED_HEADERS` zawiera `x-user-id`, `x-user-roles`, `authorization`, `x-correlation-id`.
2. **Iniekcja** — po weryfikacji JWT, gateway wstrzykuje prawdziwy `X-User-ID`.
3. **Izolacja sieci** — internal services nie są dostępne z internetu.
4. **Usunięcie Authorization** — token JWT nie jest przekazywany do internal services.

### 7.3 Hop-by-Hop Header Blocking

Blokada `host`, `content-length`, `connection`, `transfer-encoding`, `upgrade` chroni przed:
- **Host header injection** — klient mógłby ustawić `Host: evil.com`.
- **Content-Length manipulation** — klient mógłby podać zły `Content-Length`.
- **Connection header leaks** — `Connection: keep-alive` między klientem a gateway nie powinno być przekazywane.
- **Transfer-Encoding manipulation** — `Transfer-Encoding: chunked` mogłoby wpłynąć na parsowanie body.

### 7.4 Rate Limiting — ochrona przed brute-force i DDoS

| Endpoint | Limit | Klucz | Powód |
|----------|-------|-------|-------|
| `/api/v1/auth/register` | 5 / min | `auth:{client_ip}` | Ochrona przed masową rejestracją |
| `/api/v1/auth/login` | 5 / min | `auth:{client_ip}` | Ochrona przed brute-force hasła |
| `/api/v1/loans` POST | 3 / 10 min | `loans:{applicant_id}` | Ochrona przed masowym tworzeniem wniosków |
| `/api/v1/auth/refresh` | — | — | Token weryfikowany przez Applicant Service |
| `/api/v1/me` | — | — | Odczyt (GET) nie wymaga limitu |
| `/api/v1/webhooks/stripe` | — | — | Webhook Stripe zwnetrzny i bezpieczny |

**Dlaczego auth limity per IP, a loans per user?**
- Auth: klient nie jest jeszcze zalogowany → nie ma `applicant_id`. Ochrona per IP jest jedyną opcją.
- Loans: klient jest zalogowany → mamy `applicant_id`. Limit per user jest bardziej precyzyjny.

### 7.5 Token Type Enforcement

Gateway akceptuje **TYLKO tokeny access** (`type: "access"`). Tokeny refresh (`type: "refresh"`) są odrzucane.

**Dlaczego?** Refresh token jest przeznaczony wyłącznie do endpointu `/api/v1/auth/refresh` (w Applicant Service). Ten endpoint jest **publiczny** — gateway go nie weryfikuje. Gdyby gateway akceptował refresh tokeny jako "auth tokeny", atakujący mógłby użyć refresh tokena do dostępu do chronionych endpointów.

### 7.6 Brak wycieku informacji (error responses)

Gateway zwraca **minimalne** komunikaty błędów:
- `{\"detail\": \"Unauthorized\"}` — nie zdradza, czy token wygasł, czy jest niepoprawny.
- `{\"detail\": \"Rate limit exceeded\"}` — nie zdradza limitu.
- `{\"detail\": \"Not Found\"}` — nie informuje, jakie ścieżki istnieją.
- `{\"detail\": \"Gateway not ready\"}` — nie informuje o przyczynie.

**Dlaczego to ważne?** Minimalizacja informacji (information leakage) utrudnia atakującemuenumerację zasobów i reconnaissance.

---

## 8. Konfiguracja i uruchamianie

### 8.1 Szybki start — krok po kroku

```bash
# 1. Wygeneruj klucze RSA (w Applicant Service — gateway ich potrzebuje)
cd services/applicant
python -m src.infrastructure.security.keygen

# 2. Wejdź do gateway
cd services/gateway

# 3. Przygotuj zmienne środowiskowe
cp .env.example .env

# 4. Zainstaluj zależności
pip install -e ".[dev]"

# 5. Uruchom infrastrukturę (z katalogu głównego)
make infra-up

# 6. Uruchom gateway
uvicorn src.main:app --reload --port 8000
```

### 8.2 Zmienne środowiskowe

| Zmienna | Domyślna | Znaczenie |
|---------|----------|-----------|
| `GATEWAY_PORT` | `8000` | Port nasłuchiwania |
| `JWT_PUBLIC_KEY_PATH` | `/app/keys/public_key.pem` | Ścieżka do klucza publicznego RSA |
| `APPLICANT_SERVICE_URL` | `http://applicant:8001` | Adres internal (Docker network) |
| `LOAN_SERVICE_URL` | `http://loan-application:8002` | Adres internal |
| `DOCUMENT_SERVICE_URL` | `http://document:8003` | Adres internal |
| `REDIS_URL` | `redis://localhost:6380/0` | Adres Redis |
| `RATE_LIMIT_AUTH_PER_MINUTE` | `5` | Limit: auth req/min per IP |
| `RATE_LIMIT_LOANS_PER_10_MIN` | `3` | Limit: loans req/10 min per user |
| `FRONTEND_URL` | `http://localhost:3000` | Jedyny origin CORS z credentials (Etap 4; nigdy `*`) |
| `COOKIE_SECURE` | `false` | Flaga `Secure` na cookie (Etap 4; `false` lokalnie na HTTP, `true` na prod-HTTPS) |
| `ACCESS_COOKIE_NAME` | `cg_access` | Nazwa cookie access (Etap 4) |
| `REFRESH_COOKIE_NAME` | `cg_refresh` | Nazwa cookie refresh (Etap 4) |

### 8.3 Analiza Dockerfile

```dockerfile
FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml ./
RUN pip install --no-cache-dir -e ".[dev]"
COPY src ./src
RUN mkdir -p /app/keys
EXPOSE 8000
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

**Analiza kluczowych linii:**

1. **`FROM python:3.12-slim AS base`** — obraz bazowy: Python 3.12 "slim" (odchudzony).
2. **`ENV PYTHONDONTWRITEBYTECODE=1`** — nie generuj plików `.pyc`.
3. **`PYTHONUNBUFFERED=1`** — wyłącz buforowanie stdout/stderr (logi natychmiast).
4. **`gcc`** — potrzebny do budowania `cryptography` (zależność python-jose).
5. **`COPY pyproject.toml` + `pip install`** — cache dependency layer (zmiana kodu nie wymusza reinstalacji).
6. **`COPY src ./src`** — kopiowanie kodu po instalacji zależności.
7. **`mkdir -p /app/keys`** — katalog na klucze (montowane przez Docker volume).
8. **`CMD uvicorn ... --host 0.0.0.0`** — nasłuchuj na wszystkich interfejsach (wymagane w Docker).

**Znana luka:** `libs/observability` nie jest kopiowany ani installowany. `src/main.py` importuje `crediguard_observability`, ale w obrazie Docker ten pakiet nie istnieje. **W Docker build obraz się wysypie na starcie.** Rozwiązanie: dodać `crediguard-observability` do dependencies lub skopiować `libs/observability`.

### 8.4 Analiza `pyproject.toml` — linia po linii (co tu nie gra)

```toml
[project]
name = "crediguard-gateway"
version = "0.1.0"
description = "API Gateway - JWT validation, reverse proxy, rate limiting, SSE"
readme = "README.md"              # ← pliku brak w katalogu! (jak w loan-app; pip ostrzega)
requires-python = ">=3.12"        # ← stąd StrEnum-frozensety, X | None, datetime.UTC
dependencies = [
    "fastapi>=0.115.0", "uvicorn[standard]>=0.32.0",
    "pydantic>=2.9.2", "pydantic-settings>=2.5.2",   # ← pydantic BEZPOŚREDNIO nieużywane?
    "python-jose[cryptography]>=3.3.0",              # ← transitive cryptography dla testów (G-5!)
    "structlog>=24.4.0",                             # ← używane pośrednio (via libs/observability)
    "httpx>=0.27.2", "redis>=5.0.0",                 # ← oba używane (proxy, limiter)
]
```

- `pydantic` w deps, ale kod importuje tylko `pydantic_settings` (config) — goły `pydantic` przychodzi transitive. Deklaracja nieszkodliwa (jawność), ale myląca (sugeruje modele Pydantic w gateway — a gateway celowo ich nie ma!).
- `structlog` deklarowane, importowane tylko przez `libs/observability` (które... nie jest w deps ani kopiowane do obrazu — §8.3 luka! Dwie luki się zazębiają: obraz nie wstanie z importu w `main.py`).
- Brak `crediguard-observability` w deps (do naprawy razem z Dockerfile — §8.3).

```toml
[project.optional-dependencies]
dev = ["pytest>=8.3.3", "pytest-asyncio>=0.23.8", "pytest-httpx>=0.27.0",
       "ruff>=0.6.9", "mypy>=1.13.0", "types-python-jose>=3.3.0"]
# pytest-httpx: MARTWE (zero importów — G-6, użyć w §9.5 albo wywalić!)
# types-python-jose: stubby typów (mypy strict widzi jwt.decode — bez nich Any!)

[tool.ruff]
target-version = "py312"          # ← 3.12: natywne generyki, StrEnum, X | None
line-length = 100
src = ["src"]                     # ← baza from src... (jak wszędzie w repo)
[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM", "C4"]   # ← identyczne jak loan-app (konwencja repo!)
ignore = ["B008", "B904", "UP045"]                 # ← B008: Depends() w defaultach (FastAPI!),
                                                   #    B904: raise-from w except, UP045: Optional vs X|None
[tool.mypy]
python_version = "3.12"
strict = true                     # ← sygnał seniorski (adnotacje wszędzie)
namespace_packages = true
explicit_package_bases = true     # ← jawne korzenie (7 serwisów × src/ w monorepo!)
mypy_path = ["../../../libs/observability/src", "../../../libs/events/src"]
# events/src w path mimo że gateway events NIE importuje (kopia configu z szablonu — nieszkodliwe)
[tool.pytest.ini_options]
asyncio_mode = "auto"                           # ← testy async bez markerów...
asyncio_default_fixture_loop_scope = "function" # ← ...ale pętla per fixture (izolacja!)
testpaths = ["tests"]                           # ← pytest bez args zbiera tests/ (nie src/!)
```

- Ruff/mypy/pytest config **identyczny** jak loan-app (poza kolejnością kluczy) — konwencja repo: nowy serwis kopiuje blok jakości 1:1 (pre-commit egzekwuje tak samo wszędzie).

---

## 9. Testy — jednostkowe (i luka integracyjna)

> Piramida testów gateway jest ścięta: szeroka podstawa unit (3 pliki, 12 testów, milisekundy, zero Dockera) i **puste piętro integracji** (`tests/integration/` zawiera tylko `__init__.py`). To boli najbardziej właśnie tutaj: krytyczny bug `_match_route` (§11 luka G-1) przeszedłby każdy test integracyjny, a żaden unit go nie łapie (unit testuje `_rewrite_headers` i limiter w izolacji, nie dopasowanie ścieżek!).

### 9.0 Taksonomia — co gdzie i po co

| Warstwa | Plik | Testów | Zależności | Co udowadnia |
|---------|------|--------|------------|--------------|
| unit | `test_token.py` | 5 | `cryptography` + `python-jose` (generowanie JWT w teście!) | walidacja RS256, odrzucanie refresh/śmieci |
| unit | `test_proxy_headers.py` | 4 | brak (czysty `ProxyService`) | anti-spoofing nagłówków, correlation ID |
| unit | `test_rate_limit.py` | 3 | `FakeRedis`/`FakePipe` (dict w pamięci) | sliding window: limit, blokada, izolacja kluczy |
| integration | *(brak)* | 0 | — | dopasowanie tras, 401/404/429 end-to-end — NIEPOKRYTE |

- Reguła jak w loan-app: logika w unit (szybko), granice (HTTP, Redis, trasy) w integracji. Tutaj granice nie są testowane wcale — stąd luka G-1 w §11.

### 9.1 Narzędzia testowe — linia po linii

- **pytest** — framework (asercje `assert` + `pytest.raises` + fixture'y). `asyncio_mode = "auto"` w `pyproject.toml` (testy `async def` bez markerów) + `asyncio_default_fixture_loop_scope = "function"` (osobna pętla per fixture — izolacja).
- **`cryptography`** — generowanie pary RSA 2048-bit w fixture (`keypair`). Import wprost w teście (`from cryptography.hazmat...`), ale w `pyproject.toml` brak jawnego wpisu — działa tylko dzięki transitive `python-jose[cryptography]`! Usunięcie extra z jose wysypie testy (luka G-5 w §11: dopisać `cryptography` do dev-deps).
- **`python-jose`** — kodowanie tokenów testowych (`jwt.encode(payload, private_pem, algorithm="RS256")`) — test podpisuje **prywatnym** (jak Applicant w produkcji), waliduje **publicznym** (jak gateway). Symetria test↔prod.
- **`FakeRedis` / `FakePipe`** — ręczna symulacja pipeline Redis na `dict[str, list[float]]` (patrz §9.4).
- **`ProxyService._rewrite_headers()`** — metoda prywatna testowana bezpośrednio (biała skrzynka: test zna implementację, nie tylko HTTP). Uzasadnione: to serce bezpieczeństwa nagłówków; test przez HTTP wymagałby TestClienta + mocka httpx (ciężkie). Cena: refaktor nazwy/metody psuje testy (sprzężenie test↔implementacja — akceptowane dla 4 testów).
- **`pytest-httpx` w dev-deps — martwe!** Żaden test go nie importuje (grep pusty). Zależność z szablonu, nieużywana — do wywalenia (luka G-4). To właśnie nim napisano by testy integracyjne proxy (mock transportu httpx) — albo TestClient + respx. Narzędzie czeka na użycie.

### 9.2 `tests/unit/test_token.py` (74 linie) — walidacja JWT, test po teście

```python
"""Unit tests for JWT token validation in the gateway."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from jose import jwt
from src.services.token import InvalidTokenError, TokenValidator

KeyPair = tuple[bytes, bytes]
```

- **Linia `KeyPair = tuple[bytes, bytes]`:** alias typu (prywatny PEM, publiczny PEM) — czytelność sygnatur fixture'ów (`def keypair() -> KeyPair` zamiast krotki bez znaczenia). Dla laika: alias nie tworzy nowego typu (mypy widzi zwykłą krotkę!), tylko nazwę-dokumentację.
- **Import `RSAPrivateKey`:** tylko do adnotacji (`key: RSAPrivateKey` — mypy wie, że `.private_bytes()` istnieje). Bez adnotacji `rsa.generate_private_key` zwracałby... to samo, ale niejawną wiedzę (IDE nie podpowiada).

```python
@pytest.fixture()
def keypair() -> KeyPair:
    from cryptography.hazmat.primitives.asymmetric import rsa

    key: RSAPrivateKey = rsa.generate_private_key(public_exponent=65537, key_size=2048)
```

- **`@pytest.fixture()`** (z nawiasami — równoważne `@pytest.fixture`; nawiasy sugerują parametryzację w przyszłości). Scope default (`function` — świeża para kluczy **per test**! izolacja: test nie widzi kluczy innego).
- **Import `rsa` wewnątrz fixture (defer):** ciężki import kryptografii tylko gdy fixture użyte (testy nagłówków go nie potrzebują — szybszy start kolekcji). Konwencja dyskusyjna (jak defer w loan-app workerze), tu uzasadniona kosztem importu.
- **`public_exponent=65537, key_size=2048`:** standardowe parametry RSA (65537 = Fermat F4 — szybka weryfikacja; 2048 bitów = minimum prod, generowanie ~0,1–0,5 s per test — cena izolacji; 5 testów × 2 fixture'ów... `keypair` wołane raz per test (cache fixture w teście!), więc ~5 generowań na plik).

```python
    private_pem: bytes = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_pem: bytes = key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return private_pem, public_pem
```

- **Formaty:** prywatny PKCS#8 bez hasła (`NoEncryption` — test, nie prod; klucz istnieje milisekundy w pamięci), publiczny SubjectPublicKeyInfo (format, który `Settings.jwt_public_key` czyta z pliku w produkcji — ten sam kształt PEM!). Test mówi tym samym „dialektem kluczy" co prod.
- **Kolejność krotki** (prywatny, publiczny) — konwencja pliku; `validator` bierze `[1]`, `_access_token` bierze `[0]`. Pomyłka indeksu = test szyfruje publicznym (fail) — jawne rozpakowanie (`private_pem, _ = keypair`) zamiast `[0]` eliminuje pomyłkę.

```python
@pytest.fixture()
def validator(keypair: KeyPair) -> TokenValidator:
    _, public_pem = keypair
    return TokenValidator(public_pem)
```

- **Kompozycja fixture'ów** (`validator` zależy od `keypair` — pytest buduje graf, jak FastAPI `Depends`!). Validator zna **tylko publiczny** (jak w produkcji — test nie mógłby oszukiwać, nawet gdyby chciał).

```python
def _access_token(keypair: KeyPair, sub: str) -> str:
    private_pem, _ = keypair
    payload: dict[str, Any] = {"sub": sub, "email": "a@b.c", "type": "access"}
    return jwt.encode(payload, private_pem, algorithm="RS256")
```

- **Helper (nie fixture!):** funkcja z parametrem `sub` (fixture'y nie przyjmują argumentów — helper tak). Payload minimalny, ale kompletny dla walidatora (`sub` + `type: access`; `email` — bo prod-tokeny je niosą, choć gateway ignoruje). `dict[str, Any]` (wartości heterogeniczne — jak payload outbox w loan-app).

```python
def test_valid_access_token_returns_applicant_id(
    keypair: KeyPair, validator: TokenValidator
) -> None:
    uid = uuid4()
    token = _access_token(keypair, str(uid))
    assert validator.validate(f"Bearer {token}") == uid
```

- Losowy `uid` per wykonanie (nie stały! — stały UUID w teście kusiłby do „test przechodzi, bo dane pasują", losowy dowodzi ogólności). Asercja `== uid` (UUID, nie string — walidator zwraca sparsowany UUID, test to weryfikuje).
- **Testy synchroniczne** (`def` — `validate` jest synchroniczne; brak I/O w walidacji — celowo, patrz §6.4 loan-app... tu: §6.4 tego guide).

```python
def test_missing_authorization_header_raises(validator: TokenValidator) -> None:
    with pytest.raises(InvalidTokenError):
        validator.validate(None)


def test_non_bearer_header_raises(validator: TokenValidator) -> None:
    with pytest.raises(InvalidTokenError):
        validator.validate("Token abc")
```

- Dwa warianty „brak poprawnego Bearer": `None` (klient nic nie wysłał) i `"Token abc"` (zły schemat — uwaga: `"Token abc"` nie zaczyna się od `"Bearer "`, więc odpada na prefiksie, nie na dekodowaniu!). Bez asercji komunikatu (luźniej niż loan-app — zmiana zdania nie psuje testu; test pinuje typ wyjątku, nie treść).

```python
def test_refresh_token_is_rejected(keypair: KeyPair, validator: TokenValidator) -> None:
    private_pem, _ = keypair
    payload: dict[str, Any] = {"sub": str(uuid4()), "type": "refresh"}
    refresh = jwt.encode(payload, private_pem, algorithm="RS256")
    with pytest.raises(InvalidTokenError):
        validator.validate(f"Bearer {refresh}")
```

- **Najważniejszy test bezpieczeństwa pliku:** poprawnie podpisany, niewygasły token — ale `type: refresh` (nie `access`) → odrzut. Bez linii `payload.get("type") != "access"` w `token.py` ten test pada (a refresh token wędrujący po wewnętrznych serwisach jako tożsamość to dziura: refresh żyje 7 dni, access 15 min!). Test pinuje politykę „tylko access".
- **Dlaczego własny payload, nie `_access_token`?** Bo helper szyje `type: access` na sztywno (parametr `sub` tylko). Test negatywny potrzebuje innego typu — buduje payload ręcznie. Alternatywa: parametr `type` w helperze (`_token(keypair, sub, type="access")`) — czytelniej, mniej duplikacji (dług kosmetyczny).

```python
def test_garbage_token_raises(validator: TokenValidator) -> None:
    with pytest.raises(InvalidTokenError):
        validator.validate("Bearer not.a.token")
```

- `"not.a.token"` (trzy segmenty jak JWT, ale losowe bajty — podpis się nie zgadza → `JWTError` → `InvalidTokenError`). Test pinuje, że śmieci nie przechodzą (brzmi banalnie — ale bez `algorithms=[RS256]` (whitelista) atak „algorithm confusion" (HS256 z publicznym jako sekretem) mógłby przejść! Ten test + `test_refresh_token_is_rejected` to dwa strażnicy kryptografii.
- **Czego brak w pliku:** test wygasłego tokena (`exp` w przeszłości → `JWTError` — niepokryte! `python-jose` weryfikuje `exp` domyślnie, ale żaden test tego nie dowodzi; ktoś wyłączający weryfikację (`options={"verify_exp": False}`) nie zostanie złapany), test złego podpisu (token podpisany **innym** kluczem — najważniejszy scenariusz! dziś niepokryty wprost), test `sub` nie-UUID (`ValueError` → `InvalidTokenError` — niepokryte). Trzy testy do dopisania (luka G-3 w §11).

### 9.3 `tests/unit/test_proxy_headers.py` (63 linie) — anti-spoofing, test po teście

```python
"""Unit tests for the gateway reverse-proxy header rewriting (spec §8). ... """

from __future__ import annotations

from src.services.proxy import ProxyService


def _proxied_headers(
    proxy: ProxyService, incoming: dict[str, str], *, applicant_id: str | None
) -> dict[str, str]:
    headers = proxy._rewrite_headers(
        incoming,
        applicant_id=applicant_id,
        correlation_id="corr-123",
    )
    return {k.decode(): v.decode() for k, v in headers}
```

- **Helper `_proxied_headers`:** woła **prywatną** `_rewrite_headers` (biała skrzynka — §9.1) ze stałym `correlation_id="corr-123"` i konwertuje wynik (`list[tuple[bytes, bytes]]` → `dict[str, str]`) do wygodnych asercji. Stały correlation izoluje testy nagłówków od logiki correlation (testowana osobno w `test_correlation_id_is_always_forwarded`).
- **`*, applicant_id`:** keyword-only (po `*` — wołający musi nazwać: `applicant_id="..."`, nie pozycyjnie; czytelność + odporność na przestawienie).
- **Konwersja `.decode()` (UTF-8 default):** nagłówki testowe to ASCII — bezpieczne. Gdyby test słał latin-1 (`"zażółć"`), decode padłby (testy trzymają się ASCII — słusznie, bo `_rewrite_headers` koduje `latin-1, ignore`).

```python
def test_strips_spoofed_user_headers_and_authorization() -> None:
    proxy = ProxyService()
    incoming = {
        "X-User-ID": "attacker-supplied",
        "X-User-Roles": "admin",
        "Authorization": "Bearer attacker-token",
        "Content-Type": "application/json",
    }
    headers = _proxied_headers(proxy, incoming, applicant_id="real-uuid")

    assert "X-User-ID" in headers
    assert headers["X-User-ID"] == "real-uuid"
    assert "X-User-Roles" in headers
    assert headers["X-User-Roles"] == "applicant"
    assert "Authorization" not in headers
```

- **Scenariusz ataku wprost:** klient podrabia `X-User-ID`, nadaje sobie `admin`, podtyka cudzy Bearer. Oczekiwane: fałszywe wyrzucone, wstrzyknięte prawdziwe (`real-uuid` + `applicant`), `Authorization` nie idzie dalej (wewnętrzne serwisy go nie potrzebują — tożsamość jest w `X-User-ID`).
- **`ProxyService()` per test** (konstruktor tworzy `httpx.AsyncClient` — nieużywany w teście nagłówków! Każde `ProxyService()` otwiera pulę połączeń, która nigdy nie jest zamykana (`aclose` nie wołane — warning `Unclosed client` w logach? `httpx.AsyncClient` bez atm... tworzy obiekty bez socketów do pierwszego requestu, więc cicho — ale marnotrawstwo + zapach: test nagłówków nie powinien tworzyć klienta HTTP. Dług: fabryka/klasa-metoda dla `_rewrite_headers` (staticmethod?) — §11).
- **Asercja `"X-User-Roles" == "applicant"`** (nie `"admin"`!): gateway **degraduje** role do defaultu (nie ma RBAC — każdy zalogowany to applicant). Test pinuje, że nawet jawne `admin` od klienta nie przechodzi.
- **Czego brak:** test, że `Content-Type: application/json` **przechodzi** (passthrough zwykłych nagłówków — niepokryte! ktoś rozszerzający `_BLOCKED_HEADERS` o `content-type` zepsułby POST-y, a testy milczą), test case-insensitive (`"x-user-id"` małymi też stripowane? implementacja lowercases (`lk = k.lower()`), ale test woła tylko wielkoliterowe warianty — luka pokrycia), test `X-Correlation-ID` od klienta (stripowany + nadpisywany `corr-123`? niepokryte!).

```python
def test_public_routes_forward_without_identity() -> None:
    proxy = ProxyService()
    headers = _proxied_headers(proxy, {"Content-Type": "application/json"}, applicant_id=None)
    assert "X-User-ID" not in headers
    assert "X-User-Roles" not in headers
    assert "X-Correlation-ID" in headers
```

- `applicant_id=None` (ścieżka publiczna — brak tożsamości do wstrzyknięcia). Trzy asercje: brak ID, brak ról, **obecny correlation** (śledzenie działa i bez logowania — ważne dla debugowania 401!).

```python
def test_correlation_id_is_always_forwarded() -> None:
    proxy = ProxyService()
    headers = _proxied_headers(proxy, {}, applicant_id=None)
    assert headers["X-Correlation-ID"] == "corr-123"
```

- Pusty input (`{}`) → i tak wychodzi correlation (gateway zawsze dokleja — downstream zawsze może korelować). Jedna asercja na równość (nie tylko obecność — pinuje wartość, czyli że to **ten** correlation, nie losowy).

```python
def test_hop_by_hop_headers_are_not_forwarded() -> None:
    proxy = ProxyService()
    headers = _proxied_headers(
        proxy,
        {"Connection": "keep-alive", "Transfer-Encoding": "chunked", "Host": "evil.com"},
        applicant_id=None,
    )
    for blocked in ("Connection", "Transfer-Encoding", "Host"):
        assert blocked not in headers
```

- `Host: evil.com` (atak Host-header: wewnętrzny serwis mógłby ufać Host przy generowaniu linków/resetów — gateway obcina). Pętla po trzech (jak test terminali w loan-app — ten sam dług: `parametrize` dałby osobne wyniki per nagłówek; dziś pierwszy fail przerywa).
- **Czego brak:** `Content-Length` i `Upgrade` z `_BLOCKED_HEADERS` niepokryte (5 blokowanych, 3 testowane — `content-length` krytyczne dla streamingu!).

### 9.4 `tests/unit/test_rate_limit.py` (92 linie) — FakeRedis rozebrany

```python
"""Unit tests for the sliding-window rate limiter using a fake Redis."""

from __future__ import annotations

from typing import Any

from src.infrastructure.rate_limit import SlidingWindowRateLimiter


class FakePipe:
    """In-process simulation of a Redis pipeline (transactions disabled, ordered)."""

    def __init__(self, store: dict[str, list[float]]) -> None:
        self._store = store
        self._ops: list[Any] = []
```

- **Docstring uczciwy:** „transactions disabled, ordered" — fake symuluje kolejność, nie atomowość (prawdziwy pipeline z `transaction=True` serializuje; fake wykonuje sekwencyjnie w jednym wątku — wyścigi nieprzetestowane! Test udowadnia logikę zliczania, nie bezpieczeństwo współbieżne. Do udowodnienia atomowości potrzebny prawdziwy Redis + równoległe taski — integracja, której nie ma).
- `store: dict[str, list[float]]` (klucz → lista timestampów; sorted-set udawany listą — bez sortowania i unikalności memberów! Prawdziwy ZADD z tym samym memberem nadpisuje score (nie duplikuje); fake `extend` duplikuje. Przy `str(now)` jako memberze kolizje praktycznie nie występują (float z mikrosekundami), więc rozjazd nie psuje testów — ale fake nie jest wierny (dokumentuje to tylko docstring).
- `_ops: list[Any]` (kolejka operacji — `zremrangebyscore`/`zcard`/itd. tylko **zapisują** intencję (`self._ops.append(...)`) i zwracają `self` (łańcuchowanie jak prawdziwy pipe!), wykonanie w `execute()`.

```python
    def zremrangebyscore(self, key: str, min_: float, max_: float) -> FakePipe:
        self._ops.append(("zrem", key, min_, max_))
        return self

    def zcard(self, key: str) -> FakePipe:
        self._ops.append(("zcard", key))
        return self

    def zadd(self, key: str, mapping: dict[str, float]) -> FakePipe:
        self._ops.append(("zadd", key, list(mapping.values())))
        return self

    def expire(self, key: str, seconds: int) -> FakePipe:
        self._ops.append(("expire", key, seconds))
        return self
```

- Cztery metody mirrorujące API `redis.asyncio` (nazwy małą literą jak w bibliotece: `zremrangebyscore`, nie `zRemRangeByScore`). `min_` z podkreślnikiem (`min` to wbudowana funkcja — `min_` unika cieniowania; konwencja PEP8 dla kolizji z builtinami). `zadd` bierze tylko `list(mapping.values())` (score'e; membery ignoruje — fake nie potrzebuje unikalności). Zwrot `self` (fluent interface — `pipe.zrem...zcard...zadd...expire` w jednym łańcuchu, dokładnie jak kod produkcyjny!).
- **Synchroniczne** (prawdziwe metody pipe są synchroniczne do `execute` — fake wierny! `async with pipeline() as pipe` + `await pipe.execute()` — jedyny await w całym teście).

```python
    async def __aenter__(self) -> FakePipe:
        return self

    async def __aexit__(self, *_: object) -> None:
        pass  # mutations happen inside execute()

    async def execute(self) -> list[float]:
        results: list[float] = []
        for op in self._ops:
            kind = op[0]
            if kind == "zrem":
                key: str = op[1]
                lo: float = float(op[2])
                hi: float = float(op[3])
                before = len(self._store.get(key, []))
                self._store[key] = [v for v in self._store.get(key, []) if not (lo <= v <= hi)]
                results.append(float(before - len(self._store[key])))
            elif kind == "zcard":
                results.append(float(len(self._store.get(op[1], []))))
            elif kind == "zadd":
                key = op[1]
                self._store.setdefault(key, []).extend(op[2])
                results.append(1.0)
            elif kind == "expire":
                results.append(1.0)
        return results
```

- `__aenter__`/`__aexit__` (protokołu async context manager — `async with` w limiterze wymaga obu; `__aexit__` z `*_: object` (ignoruje typ wyjątku/wartość/traceback — fake nie sprząta). Komentarz `mutations happen inside execute()` dokumentuje uproszczenie (prawdziwy pipe z transaction wykonuje przy wyjściu z kontekstu... dokładnie: `execute()` jawnie — tu też jawnie; zgodne!).
- `execute` interpretuje kolejkę po kolei (krotki `("zrem", key, lo, hi)` — mini-VM z czterema instrukcjami!). `zrem`: filtr listy (`not (lo <= v <= hi)` — przedział domknięty jak `ZREMRANGEBYSCORE`), wynik = liczba usuniętych (jak Redis!). `zcard`: długość (jak Redis!). `zadd`/`expire`: `1.0` (sukces — jak Redis!).
- **Wyniki jako `float`** (prawdziwy Redis zwraca inty!): limiter robi `int(result[1])` — działa dla obu (int(3.0)==3). Fake celowo(?) luźniejszy niż prod — test przechodzi mimo rozjazdu typów. Gdyby limiter robił `result[1] == 3` (bez int), fake by nie wyłapał buga działającego na prod. Dług: `list[int]` w fake'u (wierność typom!).
- **Brak czyszczenia `_ops` po `execute`:** drugie `execute` na tym samym pipe powtórzyłoby operacje (prawdziwy pipe też tak się zachowuje? redis-py czyści... nieistotne: limiter woła raz per `allowed`).

```python
class FakeRedis:
    def __init__(self) -> None:
        self._store: dict[str, list[float]] = {}

    def pipeline(self, **_: object) -> FakePipe:
        return FakePipe(self._store)


def _limiter() -> SlidingWindowRateLimiter:
    return SlidingWindowRateLimiter(FakeRedis())  # type: ignore[arg-type]
```

- `pipeline(**_: object)` (połyka `transaction=True` — fake ignoruje transakcyjność, patrz docstring FakePipe). `# type: ignore[arg-type]` (fake nie jest `Redis` — jak `type: ignore` w loan-app `test_get_loan.py`; świadome, minimalne).
- `_limiter()` helper (świeży limiter + świeży store per test — izolacja: testy nie widzą swoich timestampów!).

```python
async def test_allows_up_to_limit() -> None:
    limiter = _limiter()
    results = [await limiter.allowed("loans:u1", 3, 600) for _ in range(3)]
    assert results == [True, True, True]


async def test_exceeding_limit_is_blocked() -> None:
    limiter = _limiter()
    for _ in range(3):
        assert await limiter.allowed("auth:1.2.3.4", 3, 60) is True
    assert await limiter.allowed("auth:1.2.3.4", 3, 60) is False


async def test_different_keys_do_not_interfere() -> None:
    limiter = _limiter()
    assert await limiter.allowed("auth:1.2.3.4", 2, 60) is True
    assert await limiter.allowed("auth:1.2.3.4", 2, 60) is True
    assert await limiter.allowed("auth:5.6.7.8", 2, 60) is True
    assert await limiter.allowed("auth:1.2.3.4", 2, 60) is False
    assert await limiter.allowed("auth:5.6.7.8", 2, 60) is True  # different key still has room
```

- Trzy scenariusze: limit (3× True na limit 3 — granica inkluzywna: `count < limit`, trzecie przechodzi bo count=2!), blokada (4. żądanie False), izolacja kluczy (IP .4 wyczerpany, .8 ma miejsce — limity per klucz, nie globalne!). Klucze jak w produkcji (`loans:u1`, `auth:1.2.3.4` — format `prefix:id` z `_rate_key`).
- **`is True`/`is False`** (nie truthiness — `allowed` zwraca czysty bool; test pinuje typ, nie tylko „prawdziwość". `1 == True` w Pythonie, ale `1 is True` to False — gdyby limiter zwrócił `1`, test by padł. Dobre.).
- **Testy asynchroniczne** (jedyne `async def` w testach gateway — limiter jest async, bo prawdziwy Redis to I/O; fake też async dla zgodności interfejsu).
- **Czego brak:** test wygasania okna (time travel — `time.time` nie mockowany; okno 60 s nieprzetestowane: czy stare wpisy wypadają? Fake wspiera (`zrem` z zakresem!), ale żaden test nie cofa zegara. Do przetestowania: monkeypatch `time.time` + dwa ticki), test `expire` (TTL klucza — fake zwraca 1.0 i nic nie robi; wygaśnięcie klucza nieprzetestowane nigdzie!), test współbieżności (dwa taski naraz — atomowość pipeline; wymaga prawdziwego Redis).

### 9.5 Luka: brak testów integracyjnych (częściowo zasypana w Etapie 4)

`tests/integration/` zawiera tylko `__init__.py`. Niepokryte (a krytyczne dla bramy!):

- **Dopasowanie tras** (`_match_route` + pełna tabela): bug G-1 (§11) — każde żądanie 404 — przeszedłby każdy test integracyjny (`TestClient(app).post("/api/v1/auth/register")` → assert nie-404), a żaden unit go nie łapie. Jeden test wart więcej niż cała reszta pliku.
- **Ścieżki 401/404/429** przez `proxy_endpoint` (TestClient + mock `ProxyService.forward` + FakeRedis): dziś logika statusów (503/404/401/400/429) nieprzetestowana wcale (unit testuje składowe, nie orkiestrację!).
- **Deviacje routingu** (documents → zły serwis, events → proxy zamiast stub — §11 G-2): test `POST /api/v1/loans/123/documents` z mockiem httpx pokazałby zły `base_url` natychmiast.
- **Plan:** `pytest-httpx` (już w dev-deps! — wreszcie użycie) do mockowania transportu + `httpx.ASGITransport`/`TestClient` do wołania app. Najpierw test G-1 (czerwony!), potem fix kodu (osobny PR — ten guide kodu nie rusza).

> **Aktualizacja (Etap 4 — częściowo zrealizowane):** powstał `tests/unit/test_routing_match.py` (3 testy na `_match_route`: bez slasha, longest-prefix, nieznana → None) — zamraża fix G-1, ale to nadal unit prywatnej funkcji, nie test ścieżki. Smoke-testy TestClient (logout 200, SSE/refresh bez cookie 401, obcy Origin 403) wykonano ad-hoc przy implementacji, nie jako stałe testy. Nadal brak: pełny test integracyjny ścieżki (request → match → forward z mock-upstreamem) oraz test translacji cookie z mockiem applicanta (luka pokrycia `build_*_response` — notification guide §9.4).

---

## 10. Style i dobre praktyki

### 10.1 Modułowe singletony zamiast Depends()

W Applicant Service zależności budowane były przez `Depends()` (kaskada FastAPI). Gateway używa **modułowych singletonów** (`_proxy`, `_validator`, `_limiter` inicjalizowane w `init_gateway`). Dlaczego?

- Gateway ma mało zależności (4 obiekty); `Depends()` byłoby nadmiarowe.
- Singletony inicjalizowane RAZ w `lifespan` — brak narzutu na żądanie.
- Brak logiki biznesowej (use case'ów) → nie ma czego "komponować" przez DI.

### 10.2 Frozen dataclass dla Route

`Route` z `frozen=True` jest niemutowalny po utworzeniu. Chroni przed przypadkową mutacją (np. `route.base_url = "..."` → `FrozenInstanceError`). Tablica tras jest budowana RAZ i nigdy nie powinna się zmieniać.

### 10.3 Streaming bez buforowania

Gateway używa `stream=True` w httpx + `StreamingResponse` w FastAPI. Nie buforuje odpowiedzi w pamięci — pozwala na obsługę plików dowolnej wielkości bez użycia pamięci.

### 10.4 `lru_cache` zamiast ręcznego singletona

`get_settings()` używa `@lru_cache` zamiast `if _settings is None: ...`. Zalety:
- Thread-safe (wbudowane w CPython).
- Testowalne (`get_settings.cache_clear()`).
- Mniej kodu.

### 10.5 `Protocol` zamiast `ABC` dla RateLimiter

`RateLimiter` jest `Protocol` (structural subtyping), nie `ABC` (nominal subtyping). `FakeRedis` w testach nie musi jawnie (explicit) dziedziczyć po `RateLimiter` — wystarczy, że ma metodę `allowed`. Upraszcza testy.

### 10.6 Obszary do poprawy (wnioski z analizy)

1. **Brak testów integracyjnych** — `tests/integration/` jest pusty. Testy jednostkowe mockują Redis i httpx; brak testów z realnymi komponentami.
2. **`/ready` nie sprawdza realnej gotowości** — nie pinguje Redis, nie sprawdza wewnętrznych serwisów. Spec §9.3 wymaga sprawdzania DB/Kafka/Redis.
3. **Brak `README.md`** — `pyproject.toml` odwołuje się do niego, ale plik nie istnieje.
4. **Dockerfile nie kopiuje `libs/observability`** — obraz Docker się wysypie (`ModuleNotFoundError`).
5. **Rate limiting na `/loans` obejmuje GET i POST** — spec mówi o limicie tylko na POST.
6. **Stripe webhook proxyje do loan-service** — spec mówi o Disbursement.
7. **Brak Retry-After w 429** — klient nie wie, kiedy spróbować ponownie.
8. **`/ready` zawsze zwraca 200** — nie sprawdza Redis; powinien zwracać 503 gdy Redis niedostępny.
9. **path_prefix z `{id}` nie pasuje do żądań** — `/api/v1/loans/{id}` jest dosłownym stringiem i nie pasuje do `/api/v1/loans/123`.

---

## 11. Kod źródłowy vs specyfikacja — luki i obszary do poprawy

### 11.1 Tabela devations (odchyleń od specyfikacji)

| Element | Spec § | Kod | Status |
|---------|--------|-----|--------|
| SSE endpoint `/loans/{id}/events` | §4.1: strumieniowe powiadomienia | Lokalny `sse_router` (Etap 4) | **Zrealizowane (Etap 4)** — protokół w notification guide §4.5/§4.8 |
| Rate limit loans: tylko POST | §8: 3/10 min na POST | Limit na WSZYSTKIE metody `/loans` | Deviation (nadal otwarte!) |
| Stripe webhook → Disbursement | §4.1: webhook do Disbursement | Proxyje do `loan_service_url` | Deviation (nadal otwarte!) |
| `/ready` sprawdza Redis | §9.3: readiness = DB/Kafka/Redis | Zawsze 200 | Uproszczenie (nadal otwarte!) |
| Dockerfile: libs/observability | — | Nie skopiowany | Bug (nadal otwarte!) |
| `path_prefix` z `{id}` | — | Dosłowny string, nie pattern | Bug (nadal otwarte! — wpisy nie matchują, łapie je longest-prefix `/api/v1/loans`) |
| Brak Retry-After w 429 | HTTP RFC 7231 | Brak nagłówka | Missing feature (nadal otwarte!) |
| **G-1: `_match_route` nigdy nie matchuje (wiodący `/`)** | — | Każde żądanie → 404 | **NAPRAWIONE w Etapie 4** (normalizacja + `test_routing_match.py`) |
| **G-2: documents proxyjowane do złego serwisu** | §4.1: documents → Document | `/api/v1/loans/*` łapie wszystko → loan-application:8002 | **Bug krytyczny (nadal otwarte!)** |
| **G-3: brak CORS + brak OPTIONS** | §8: CORS tylko origin frontendu | Brak `CORSMiddleware`, brak OPTIONS w methods | **NAPRAWIONE w Etapie 4** (`CORSMiddleware` z credentials + OPTIONS w `allow_methods`) |
| **G-4: timeout upstream → 500 zamiast 504** | — | Brak `try/except` wokół `forward()` | Bug (nadal otwarte!) |
| **G-5: `cryptography` niejawną zależnością testów** | — | Import wprost w teście, tylko transitive via `python-jose[cryptography]` | Dług higieniczny (nadal otwarte!) |
| **G-6: `pytest-httpx` nieużywane** | — | W dev-deps, zero importów w testach | Dług higieniczny (nadal otwarte!) |
| **G-7 (nowe, Etap 4): prefix-atak na Origin** | §8 | `startswith(frontend_url)` | **Bug (nowy!)** — `http://localhost:3000.evil.com` przechodzi; fix: porównanie `netloc` + test |
| **G-8 (nowe, Etap 4): osobny `httpx.AsyncClient` na strumień SSE** | — | `_fetch_loan` tworzy klienta per request | **Dług wydajnościowy (nowy!)** — jeden modułowy klient w `init_sse` |

### 11.2 Szczegółowe opisy luk

#### SSE endpoint (Etap 4)

Spec §4.1 opisuje endpoint `/api/v1/loans/{id}/events` (SSE — Server-Sent Events), który strumieniuje aktualizacje statusu wniosku pożyczkowego do przeglądarki klienta. Strumień zasilany jest przez Redis Pub/Sub (Notification Service nasłuchuje `loan.status.changed.v1` z Kafka → publikuje do Redis → gateway subskrybuje).

Obecna implementacja: `base_url=""` → catch-all endpoint zwraca 404. Wymaga zaimplementowania:
- Handler SSE (StreamingResponse z `text/event-stream`).
- Subskrypcja Redis Pub/Sub na kanale `loan-status:{applicant_id}`.
- Walidacja JWT (identyczna jak inne chronione endpointy).

#### Rate limiting na loans — deviation

Spec §8 mówi: 3/10 min na **POST** `/api/v1/loans` (tworzenie wniosku). Kod implementuje limit na **WSZYSTKIE metody** `/api/v1/loans` (GET, POST). Oznacza to, że odczyt listy wniosków (GET) jest limitowany — niezgodnie z intencją specyfikacji.

#### Stripe webhook → Disbursement

Spec §4.1 mówi, że webhook Stripe proxyje do serwisu **Disbursement** (wypłaty). Kod proxyje do `loan_service_url` (`loan-application:8002`). Może to być celowa zmiana (disbursement jeszcze nie istnieje) lub bug.

#### Brak Retry-After w 429

HTTP RFC 7231 rekomenduje nagłówek `Retry-After` w odpowiedzi 429. Gateway nie dodaje go — klient nie wie, kiedy spróbować ponownie. Poprawka: dodać `Retry-After: <window_seconds>`.

#### `path_prefix` z `{id}` — bug

`/api/v1/loans/{id}` jest dosłownym stringiem w `path_prefix`. `_match_route` sprawdza `path.startswith("/api/v1/loans/{id}/")` — ale żądanie `/api/v1/loans/123` nie zaczyna się od `/api/v1/loans/{id}/`. W praktyce route `/api/v1/loans/{id}` nigdy nie pasuje (jedyny pasujący route to `/api/v1/loans`). Konsekwencja: nie ma osobnej obsługi per-loan (GET/PUT/PATCH/DELETE) — wszystko idzie przez route `/api/v1/loans`.

#### G-1 (KRYTYCZNY): `_match_route` nie matchuje niczego — wiodący `/`

**Weryfikacja empiryczna** (uruchomiony kod, nie spekulacja):

```python
_match_route('api/v1/loans/123')   # → None  (tak Starlette daje path: BEZ slash!)
_match_route('/api/v1/loans/123')  # → Route('/api/v1/loans', ...)  (tak testuje człowiek)
```

Mechanika: catch-all `/{path:path}` w Starlette obcina wiodący `/` (`/api/v1/x` → `path="api/v1/x"` — guide §4.8 to poprawnie dokumentuje!). Ale `_match_route` porównuje `path == r.path_prefix` (prefixy ZE slashem: `/api/v1/...`) i `path.startswith(prefix + "/")` (też ze slashem). `"api/..." == "/api/..."` → False; `"api/...".startswith("/api/...")` → False. **Żaden route nigdy nie pasuje → każde żądanie przez gateway kończy się 404** (register, login, loans — wszystko!).

- Dlaczego testy milczą: unit testuje składowe (`_rewrite_headers`, limiter, validator), a `_match_route` nie ma żadnego testu (ani unit, ani integracji — §9.5). Jeden test integracyjny (`TestClient(app).post("/api/v1/auth/register")` → assert ≠ 404) złapałby to natychmiast.
- Poprawka kodu (osobny PR — ten dokument kodu nie rusza): normalizacja na wejściu (`path = "/" + path.lstrip("/")` w `proxy_endpoint` albo w `_match_route`) + test integracyjny per route z tabeli (§1.2).
- Status: **DoD Etapu 2 („rejestracja/login działa wyłącznie przez :8000") jest formalnie niespełnione** dopóki G-1 żyje. Najwyższy priorytet w całym repo.

#### G-2 (KRYTYCZNY): upload dokumentów proxyjowany do złego serwisu

**Weryfikacja empiryczna** (pełna tabela tras, ścieżki ze slashem — czyli po hipotetycznej naprawie G-1):

```python
_match_route('/api/v1/loans/123/documents')  # → ('/api/v1/loans', 'http://loan-application:8002')
_match_route('/api/v1/loans/123/events')     # → ('/api/v1/loans', 'http://loan-application:8002')
```

Wpis `/api/v1/loans/{id}/documents → document:8003` jest martwy (literal `{id}` nie występuje w realnych ścieżkach), więc longest-prefix wybiera `/api/v1/loans` → **upload dokumentu ląduje w loan-application:8002** (który nie ma endpointu upload → 404/405 z serwisu, nie z gateway). Etap 6 (Document + dropzone na froncie) nie zadziała bez naprawy routingu — a stub SSE (`base_url=""`) jest nieosiągalny (request idzie proxy, nie w gałąź SSE — patrz poprawiony §5.4.2).

- Poprawka kodu: trasy z segmentami dynamicznymi jako regex/konwertery (np. prefix `/api/v1/loans/` + parsowanie segmentów) albo jawna kolejność „najpierw konkrety z `{}`" z podstawianiem. Minimum: test per wiersz tabeli routingu.

#### G-3: brak CORS i brak OPTIONS — przeglądarka nie zadzwoni

`main.py` wpina tylko `CorrelationIdMiddleware` (brak `CORSMiddleware`!), a catch-all obsługuje GET/POST/PUT/PATCH/DELETE (brak OPTIONS). Konsekwencje dla frontendu (Etap 5, Next.js w przeglądarce):

- Preflight CORS (OPTIONS przed POST z `Content-Type: application/json` / `Authorization`) → **405 Method Not Allowed** (brak handlera OPTIONS).
- Nawet prosty GET: brak nagłówka `Access-Control-Allow-Origin` w odpowiedzi → przeglądarka **blokuje odczyt** (same-origin policy), mimo że serwer odpowiedział 200.
- SPEC §8 wymaga „CORS: tylko origin frontendu" — gateway tego nie implementuje wcale (ani allow, ani deny — po prostu cisza).
- Poprawka: `CORSMiddleware(allow_origins=[frontend_url], allow_methods=[...], allow_headers=["Authorization", "Content-Type", "Idempotency-Key", ...])` + OPTIONS w catch-all (lub osobny handler). Bez tego DoD Etapu 5 („pełny flow w przeglądarce") jest niemożliwe — curl działa, przeglądarka nie.

#### G-4: timeout upstream → 500 po 30 s (zamiast 504)

Brak `try/except` wokół `await _proxy.forward(...)` (§4.8 Blok 5): `httpx.TimeoutException` (serwis wisi >30 s), `httpx.ConnectError` (serwis padł) i każdy inny błąd sieci propaguje się do FastAPI → generyczne **500** (nie 504/503/502!). Klient czeka pełne 30 s i dostaje najmniej informatywny kod. Poprawka: mapowanie (`TimeoutException → 504`, `ConnectError → 503`/`502`) + timeout per serwis (auth 5 s, loans 10 s, documents 30 s — upload!) + circuit breaker (FAQ w §12.4 już to postuluje).

#### G-5: `cryptography` niejawną zależnością testów

`test_token.py` importuje `cryptography` wprost (linia 9–10), ale `pyproject.toml` nie deklaruje go w dev-deps (jest tylko transitive przez `python-jose[cryptography]`). Działa dziś (`pip` dociąga extra), padnie jutro (gdy jose zmieni extra lub resolver wybierze wariant bez). Poprawka: jedna linia w dev-deps (`"cryptography>=41.0.0"`). Koszt minuty, ryzyko realne w CI z `--no-deps`-podobnymi optymalizacjami.

#### G-6: `pytest-httpx` w dev-deps, zero użyć w testach

`pytest-httpx>=0.27.0` zadeklarowane, żaden test go nie importuje (grep pusty — zweryfikowane). Dwie drogi: wywalić (higiena) albo **użyć** (testy integracyjne proxy z §9.5 — mock transportu httpx + TestClient!). Rekomendacja: zostawić i użyć (narzędzie czeka na zadanie, nie na śmietnik).

---

## 12. Dodatki

### 12.1 Diagramy Mermaid

#### 12.1.1 Architektura warstwowa gateway

```mermaid
flowchart LR
    subgraph Zew["Świat zewnętrzny"]
        C["Klient"]
        INT["Internal services"]
        R["Redis"]
    end

    subgraph GW["Gateway"]
        M["main.py"]
        PR["proxy_router"]
        TOK["token.py"]
        PROXY["proxy.py"]
        RL["rate_limit.py"]
        CFG["config.py"]
    end

    C -->|"HTTP :8000"| M
    M --> PR
    PR --> TOK
    PR --> PROXY
    PR --> RL
    TOK --> CFG
    RL --> R
    PROXY -->|"HTTP :8001/:8002/:8003"| INT
```

#### 12.1.2 Przepływ żądania chronionego (GET /api/v1/me)

```mermaid
sequenceDiagram
    participant C as Klient
    participant GW as Gateway
    participant TK as TokenValidator
    participant PR as ProxyService
    participant APP as Applicant Service

    C->>GW: GET /api/v1/me (Authorization: Bearer eyJ...)
    GW->>GW: CorrelationIdMiddleware (generuje X-Correlation-ID)
    GW->>GW: _match_route("api/v1/me") → route
    GW->>TK: validate("Bearer eyJ...")
    TK->>TK: jwt.decode(token, public_key, RS256)
    TK->>TK: sprawdź type == "access"
    TK-->>GW: UUID(applicant_id)
    GW->>GW: rate_limit: pomijamy (brak)
    GW->>PR: forward(X-User-ID=uuid, X-User-Roles=applicant, ...)
    PR->>PR: _rewrite_headers: usuwa Authorization, X-User-ID klienta
    PR->>APP: GET /api/v1/me (X-User-ID: uuid)
    APP-->>PR: 200 {ApplicantResponse}
    PR-->>GW: StreamingResponse
    GW-->>C: 200 {id, email, first_name, ...}
```

#### 12.1.3 Przepływ rate limiting (POST /api/v1/loans)

```mermaid
sequenceDiagram
    participant C as Klient
    participant GW as Gateway
    participant RL as RateLimiter
    participant RED as Redis
    participant LOAN as Loan Service

    C->>GW: POST /api/v1/loans (Authorization: Bearer eyJ...)
    GW->>GW: JWT → applicant_id = "uuid-123"
    GW->>RL: allowed("loans:uuid-123", 3, 600)
    RL->>RED: pipeline(ZREMRANGEBYSCORE, ZCARD, ZADD, EXPIRE)
    RED-->>RL: count = 1 (przed dodaniem)
    RL-->>GW: True (1 < 3)
    GW->>LOAN: POST /api/v1/loans (X-User-ID: uuid-123)
    LOAN-->>GW: 202 Accepted (loan-application zwraca 202, nie 201!)
    GW-->>C: 202 Accepted (gateway przekazuje status transparentnie)
```

#### 12.1.4 Przepływ błędu 429 (rate limit exceeded)

```mermaid
sequenceDiagram
    participant C as Klient
    participant GW as Gateway
    participant RL as RateLimiter
    participant RED as Redis

    C->>GW: POST /api/v1/auth/register (5. żądanie w minucie)
    GW->>GW: route: /api/v1/auth/register
    GW->>RL: allowed("auth:192.168.1.1", 5, 60)
    RL->>RED: pipeline(...)
    RED-->>RL: count = 5 (przed dodaniem)
    RL-->>GW: False (5 >= 5)
    GW-->>C: 429 "Rate limit exceeded"
```

### 12.2 Tabele porównawcze

#### Gateway vs Applicant Service

| Aspekt | Applicant Service | Gateway |
|--------|-------------------|---------|
| Rola | Identity provider (auth) | Edge proxy (routing, security) |
| Warstwy | domain / application / infra / api | core / services / infra / api |
| Baza danych | PostgreSQL | brak (Redis tylko rate limit) |
| Tokeny | podpisuje (private key) | weryfikuje (public key) |
| Endpointy | 4 (register, login, refresh, me) | 1 catch-all `/{path:path}` |
| DI | Depends() kaskada | modułowe singletony |
| Stream | nie | tak (httpx + StreamingResponse) |
| Testy | jednostkowe + integracyjne | tylko jednostkowe |

#### Buffered vs Streaming proxy

| | Buffered | Streaming |
|---|----------|-----------|
| Pamięć | rośnie z rozmiarem odpowiedzi | stała (~4KB chunk) |
| Duże pliki | problematyczne (OOM) | bezproblemowe |
| Złożoność | prostsza | wymaga `finally` cleanup |
| Retry | łatwiejszy (dane w pamięci) | trudniejszy (strumień jednokrotny) |
| Użycie w projekcie | brak | **wybrane** |

#### Fixed Window vs Sliding Window

| | Fixed Window | Sliding Window |
|---|--------------|----------------|
| Granice okna | stałe (0:00-0:09) | przesuwne z żądaniem |
| Burst | problem o północy okna | sprawiedliwszy |
| Implementacja | counter + INCR + EXPIRE | sorted set + ZADD + ZREM |
| Złożoność | O(1) | O(log n) |
| Wydajność | szybsza | nieco wolniejsza |
| Użycie | proste limity | **wybrane** (CrediGuard) |

### 12.3 Glosariusz (rozszerzony)

| Pojęcie | Proste wyjaśnienie |
|---------|--------------------|
| **Reverse proxy** | Serwer pośredni przekazujący żądania do właściwych serwerów docelowych (klient nie zna ich adresów) |
| **Edge service** | Serwis stojący na granicy systemu (internet → wewnętrzna sieć); jedyny wystawiony na świat |
| **BFF** | Backend for Frontend — serwis zoptymalizowany pod konkretnego klienta (gateway jest BFF dla przeglądarki) |
| **JWT** | JSON Web Token — podpisany token potwierdzający tożsamość (`header.payload.signature`, base64url) |
| **RS256** | RSA Signature with SHA-256 — asymetryczny algorytm podpisu JWT (prywatny podpisuje, publiczny weryfikuje) |
| **JWKS** | JSON Web Key Set — produkcyjny sposób dystrybucji kluczy publicznych (endpoint z kluczami + rotacja; tu: wolumen Docker) |
| **Header rewriting** | Usuwanie/wstrzykiwanie nagłówków HTTP przez proxy (tu: strip `X-User-ID` + inject po JWT) |
| **Anti-spoofing** | Ochrona przed podszywaniem tożsamości (klient nie może narzucić `X-User-ID` — gateway czyści) |
| **Hop-by-hop** | Nagłówki przeznaczone dla jednego "hop" połączenia (`Connection`, `Transfer-Encoding` — nie end-to-end; RFC 2616 §13.5.1) |
| **Sliding window** | Przesuwne okno czasowe dla rate limitingu („ostatnie N sekund", nie „bieżąca minuta") |
| **Fixed window** | Sztywne okno (minuta kalendarzowa) — prostsze, niesprawiedliwe o północy okna |
| **Sorted Set** | Struktura Redis: zbiór elementów posortowanych wg score (tu: member=timestamp-string, score=timestamp) |
| **Pipeline** | Kolejka komend Redis wysyłanych hurtowo (jeden round-trip zamiast N) |
| **Transaction (MULTI/EXEC)** | Atomowe wykonanie pipeline (`transaction=True` — nikt nie wchodzi w środek) |
| **Streaming** | Strumieniowanie danych "na bieżąco" bez buforowania (chunk ~4 KB, nie cały plik w RAM) |
| **httpx** | Nowoczesny klient HTTP dla Pythona (async, connection pooling, `stream=True`) |
| **Connection pooling** | Reużywanie otwartych socketów TCP (jeden `AsyncClient` na proces, nie per request) |
| **ASGI** | Asynchronous Server Gateway Interface — standard dla async serwerów Python (scope, stream, lifespan) |
| **FastAPI** | Framework web oparty o Pydantic + ASGI (DI przez `Depends`, OpenAPI za darmo) |
| **Catch-all route** | Jeden endpoint `/{path:path}` łapiący wszystkie ścieżki (tu: cała brama!) |
| **Longest-prefix match** | Dopasowanie do najdłuższego pasującego prefiksu (jak routing IP; tu: z luką G-1!) |
| **structlog** | Ustrukturyzowane logowanie JSON (pola zamiast zdań — grep po polach) |
| **Correlation ID** | Unikalny identyfikator żądania propagowany przez serwisy (`X-Correlation-ID` wte i wewte) |
| **Liveness probe** | K8s/Docker: sprawdzenie, czy proces żyje (`/health` — restartuj, gdy nie) |
| **Readiness probe** | K8s/Docker: czy serwis gotowy na ruch (`/ready` — nie ślij, gdy nie; tu: stub!) |
| **CORS / preflight** | Mechanizm przeglądarki: OPTIONS przed ryzykownym requestem + `Access-Control-Allow-*` (tu: brak — luka G-3!) |
| **Circuit breaker** | Bezpiecznik: po N błędach upstream odpowiada natychmiast 503 zamiast czekać (tu: brak — FAQ) |
| **Algorithm confusion** | Atak: podmiana `alg` RS256→HS256 + podpisanie publicznym kluczem jako sekretem (blokuje whitelista `algorithms`) |

> Uwaga: hasła oznaczone „(tu: …)" wiążą pojęcie z konkretnym miejscem w kodzie — glosariusz jest indeksem, nie esejem (eseje są w §6).

### 12.4 FAQ

#### "Czy gateway jest bezpieczny na produkcję?"

**Podstawa jest dobra:** RS256 (asymetryczny), header rewriting, rate limiting, minimalne błędy. Przed produkcją należy:
1. Dodać `Retry-After` w 429.
2. Naprawić `/ready` (ping Redis).
3. Dodać testy integracyjne (FakeRedis → real Redis).
4. Naprawić Dockerfile (dodać `crediguard-observability`).
5. Naprawić rate limiting loans (tylko POST, nie GET).
6. Dodać circuit breaker na wewnętrzne serwisy (gdy Applicant Service nie odpowiada, gateway powinien zwrócić 503 zamiast czekać 30s).
7. Dodać timeout per internal service (różne dla auth vs loans vs documents).
8. Rozważyć TLS termination (gateway jako jedyny terminating TLS).

#### "Co się stanie, gdy Applicant Service nie odpowiada?"

Gateway czeka do timeoutu httpx (30 sekund), po czym `httpx.TimeoutException` **propaguje się w górę** (kod nie ma `try/except` wokół `forward()`!) → FastAPI zamienia nieobsłużony wyjątek na **500 Internal Server Error** (nie 504! — 504 wymagałoby jawnego złapania timeoutu i mapowania, którego nie ma). Klient czeka 30 s i dostaje 500 — najgorsza kombinacja (długo + nieprecyzyjnie). Poprawka: `try/except TimeoutException → 504 Gateway Timeout` + niższy timeout per serwis (np. 5 s dla auth) + circuit breaker (po N błędach → natychmiastowy 503 bez czekania).

#### "Czy gateway może obsługiwać wiele instancji?"

Tak — jest **bezstanowy** (brak bazy, stan w Redis). Wiele instancji za load balancerem działa poprawnie. Rate limiting jest współdzielony (Redis) — limity są globalne, nie per-instancja.

#### "Dlaczego `/{path:path}` zamiast osobnych endpointów?"

Pojedynczy catch-all endpoint uprostrza kod (jedna logika routing/auth/rate-limit). Wady: brak automatycznej dokumentacji OpenAPI (dlatego `include_in_schema=False`), brak walidacji requestBody per endpoint (gateway nie zna schematów wewnętrznych serwisów).

#### "Jak dodać nowy serwis do routingu?"

1. Dodaj nowy `Settings` field (np. `payment_service_url`).
2. Dodaj nowy `Route(...)` w `routing.py`.
3. Gotowe — gateway automatycznie proxyje żądania z prefixem.

#### "Czym się różni `decode_responses=False` w Redis?"

Domyślnie `redis.asyncio` dekoduje odpowiedzi do `str`. `decode_responses=False` zwraca `bytes`. Dla rate limiting:`ZADD` z `str(now)` jako member jest OK; ale `ZCARD` zwraca `int` niezależnie od `decode_responses`. Użycie `False` jest bezpieczniejsze (unika problemów z kodowaniem przy kluczach binsrnych).

#### "Po co gateway w ogóle istnieje? Nie można wołać serwisów wprost?"

Można (technicznie), ale tracisz 5 rzeczy naraz: (1) **jedną powierzchnię ataku** (3 serwisy wystawione = 3× więcej do pilnowania; gateway = 1 punkt z JWT), (2) **centralne czyszczenie tożsamości** (każdy serwis musiałby sam weryfikować JWT + strippować nagłówki — N implementacji = N szans na bug), (3) **wspólny rate limiting** (limity per IP/user w jednym Redisie, nie rozproszone), (4) **stabilny kontrakt URL** (frontend woła `:8000`, a wewnętrzne porty/hosty zmieniają się bez jego wiedzy), (5) **obserwowalność wejścia** (jeden punkt korelacji + logów). Cena: dodatkowy hop sieciowy (~1 ms w Dockerze) + SPOF (pada gateway = pada wszystko — stąd multi-instancja za LB, FAQ wyżej).

#### "Dlaczego modułowe singletony, a nie Depends() jak w loan-app?"

Bo gateway nie ma use case'ów do komponowania per request (4 obiekty na proces, wszystkie współdzielone: trasy, klient HTTP, walidator, limiter). `Depends` budowałby fabryki dla czegoś, co istnieje raz — ceremoniał bez zysku. Singletony + `init_gateway` w lifespan = zero narzutu DI na request. Cena: testy endpointu wymagają grzebania w globalach (dlatego integracji nie ma — §6.12; błędne koło, które trzeba przerwać testem z §9.5).

#### "Co z endpointem `/events` (SSE stub)?"

Dziś: wpis `base_url=""` + gałąź 404 w `proxy_endpoint` (nieosiągalna z powodu G-1/G-2 — patrz §5.4.2!). Etap 4 wymaga: (a) naprawy routingu (G-1/G-2), (b) handlera SSE (`text/event-stream`, heartbeat `: ping` co 15 s, `Last-Event-ID`, initial state z REST), (c) subskrypcji Redis Pub/Sub `loan-status:{applicant_id}`, (d) autoryzacji tokenem w query/cookie (EventSource nie wysyła nagłówków `Authorization`!). Punkt (d) to nowa ścieżka auth w gateway (query-token → ten sam `TokenValidator`) — do zaprojektowania w Etapie 4.

#### "Skąd gateway bierze klucz publiczny i co przy rotacji?"

Z pliku (`jwt_public_key_path`, default `/app/keys/public_key.pem` — montowany wolumenem Docker z Applicant Service; lokalnie `./keys/`). Wczytywany **raz** przy starcie (`init_gateway` → `TokenValidator(settings.jwt_public_key)` — property czyta plik jeden raz!). Konsekwencja: rotacja kluczy (nowa para w Applicant) wymaga **restartu gateway** (stary klucz w pamięci do restartu!). W oknie między rotacją a restartem gateway odrzuca nowe tokeny (401 dla wszystkich!). Produkcja: JWKS (endpoint z kluczami + cache z TTL + `kid` w nagłówku JWT do wyboru klucza) — SPEC §8 to zapowiada („JWKS jako produkcyjny odpowiednik"). MVP: rotacja = skoordynowany restart obu serwisów (compose nie zrobi tego sam przy zmianie pliku — restart ręczny; dopisać do runbooka!).

#### "Dlaczego refresh endpoint nie ma rate limitu?"

`/api/v1/auth/refresh` jest `public=True, rate_limit=False` (tab. §1.2). Rozumowanie autora: refresh token weryfikuje Applicant Service (podpis + baza), nie gateway — limity „powinny być tam, gdzie weryfikacja". Kontrargument: gateway to tańsze miejsce na odcięcie floodu (atakujący zalewający refresh generuje pełne proxy + pracę Applicant za darmo). Brute-force refresh tokena (losowy UUID w payloadzie? nie — refresh to JWT RS256, nie do zgadnięcia) jest nieopłacalny, więc decyzja broni się kryptograficznie — ale flood (DoS, nie łamanie) przechodzi. Średniak: limit luźny (np. 30/min/IP) zamiast zera.

#### "Jak debugować 'działa curl, nie działa aplikacja'?"

Kolejność: (1) `X-Correlation-ID` z odpowiedzi (gateway zwraca!) → grep po logach gateway + serwisów (ta sama nić). (2) Status: 404 na wszystko → G-1 (slash); 404 tylko na documents → G-2; 401 → token (sprawdź `type: access` + `exp`!); 429 → limity (które okno? auth/IP czy loans/user?); 500 po 30 s → upstream timeout (G-4). (3) Bypass-test: curl wprost do serwisu wewnętrznego (z `X-User-ID` ręcznie!) — działa wprost, nie działa przez bramę → wina gateway (routing/nagłówki); nie działa wprost → wina serwisu. Ta checklista to zalążek runbooka (jak w loan-app §12.5).

---

## 13. Podsumowanie — kluczowe decyzje architektoniczne

1. **Pojedynczy catch-all endpoint** zamiast osobnych route'ów — uproszczenie kodu i logiki routing/auth/rate-limit. Wada: brak auto-dokumentacji OpenAPI.

2. **Brak warstwy domain/application** — gateway nie ma logiki biznesowej; jest warstwą transportową (edge). Stąd prostsza architektura (core / services / infrastructure / api) zamiast Clean Architecture.

3. **RS256 (asymetryczny)** — klucz prywatny tylko w Applicant Service; gateway weryfikuje kluczem publicznym. Minimalizacja powierzchni ataku.

4. **Header rewriting (anti-spoofing)** — usuwanie X-User-ID/X-User-Roles/Authorization z żądania klienta i wstrzykiwanie zaufanych nagłówków po weryfikacji JWT. Kluczowy mechanizm bezpieczeństwa.

5. **Streaming proxy** — `httpx.AsyncClient(stream=True)` + `StreamingResponse` — buforowanie zerowe, obsługa plików dowolnej wielkości.

6. **Sliding window rate limiting** — Redis Sorted Sets z pipeline transaction. Sprawiedliwszy niż fixed window; atomowy (brak race condition).

7. **Modułowe singletony** zamiast DI — proste, wystarczające dla małego zestawu zależności. Inicjalizowane RAZ w `lifespan`.

8. **Strukturalny logging JSON** — `libs/observability` z `structlog` i `CorrelationIdMiddleware` — spójny format logów przez wszystkie serwisy.

9. **Znane luki do naprawienia:** brak testów integracyjnych, `/ready` bez sprawdzania Redis, Dockerfile bez `crediguard_observability`, rate limiting loans na GET+POST (spec: tylko POST), `path_prefix` z `{id}` nie pasuje, brak Retry-After, Stripe webhook → loan-service (spec: Disbursement) — oraz **krytyczne G-1 (404 na wszystko), G-2 (documents do złego serwisu), G-3 (brak CORS), G-4 (timeout → 500)** (§11).

### Ścieżka czytania dla ról (jak w loan-app guide)

- **Laik:** §1 (analogia z recepcją) → §6.1–6.3 (proxy, matching, sliding window z przykładami) → §5.1 (jeden przepływ) → §12.4 FAQ.
- **Developer (naprawia G-1):** §4.8 `_match_route` → §11 G-1/G-2 (dowody empiryczne) → §9.5 (który test napisać najpierw) → §6.10 (catch-all mechanika).
- **Reviewer:** §11 (czy PR domyka lukę?) → §7 (czy zmiana dotyka bezpieczeństwa nagłówków?) → §9 (czy test pinuje zachowanie, nie implementację?).
- **Architekt:** §13 (decyzje) → §10.5/`Protocol` vs loan-app `ABC` (dwa style portów w repo — ujednolicić?) → SPEC ADR (gateway jako BFF vs service mesh).

> Dokument opracowany na podstawie pełnej analizy kodu źródłowego serwisu `services/gateway` **z weryfikacją empiryczną** (matchowanie tras, routing documents/events, test-deps — §11 G-1–G-6). Przy zmianie tabeli tras, nagłówków lub limitów zaktualizuj ten przewodnik razem z kodem (reguła: PR bez testu integracyjnego trasy = odrzuć — G-1 nie może się powtórzyć).

---

*Koniec przewodnika. Dokument opracowany na podstawie pełnej analizy kodu źródłowego serwisu `services/gateway`.*
