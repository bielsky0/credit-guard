# CrediGuard Applicant Service — Kompletny przewodnik techniczny

> **Wersja dokumentu:** 1.0
> **Zakres:** pełna analiza kodu źródłowego, architektury, bezpieczeństwa, testów i konfiguracji serwisu `applicant`.
> **Audytorium:** od laika (początkujący Pythonista) po seniora (architekt systemów).
> **Oryginalna specyfikacja:** `docs/SPECYFIKACJA.md` (kontrakty zdarzeń, maszyna stanów, struktura warstw).

---

## Spis treści

1. [Wstęp — czym jest Applicant Service](#1-wstęp--czym-jest-applicant-service)
2. [Architektura warstwowa](#2-architektura-warstwowa)
3. [Struktura projektu](#3-struktura-projektu)
4. [Analiza plików źródłowych — linijka po linijce](#4-analiza-plików-źródłowych)
5. [Ścieżki wywołań endpointów](#5-ścieżki-wywołań-endpointów)
6. [Koncepcje techniczne — słowniki, dekoratory, wzorce](#6-koncepcje-techniczne)
7. [Bezpieczeństwo — dogłębna analiza](#7-bezpieczeństwo)
8. [Baza danych i SQLAlchemy](#8-baza-danych-i-sqlalchemy)
9. [Testy — jednostkowe i integracyjne](#9-testy)
10. [Konfiguracja i uruchamianie](#10-konfiguracja-i-uruchamianie)
11. [Style i dobre praktyki](#11-style-i-dobre-praktyki)
12. [Dodatki — diagramy, tabele porównawcze, glosariusz, FAQ](#12-dodatki)
13. [Podsumowanie — kluczowe decyzje architektoniczne](#13-podsumowanie)

---

## 1. Wstęp — czym jest Applicant Service

### 1.1 Miejsce w systemie CrediGuard

CrediGuard to fintechowy system **MVP** zbudowany z mikroserwisów. Jest to system, który obsługuje proces wnioskowania o pożyczkę (credit). W systemie występują między innymi:

- **Applicant Service** (omawiany w tym dokumencie) — odpowiada za rejestrację, logowanie oraz zarządzanie tokenami (JWT) klientów (wnioskodawców, ang. *applicants*).
- **Loan Application Service** — odpowiada za maszynę stanów wniosków pożyczkowych (to on, i **tylko** on, zmienia statusy wniosków).
- **ML Scoring Service** — odpowiada za ocenę zdolności kredytowej.
- **Gateway** — brama API, która pełni rolę pośrednika (i weryfikuje tokeny JWT przy użyciu **klucza publicznego**).

Applicant Service jest więc **serwisem tożsamości** (ang. *identity provider*) — jego zadaniem jest dostarczenie mechanizmu autoryzacji użytkowników, zanim w ogóle rozpoczną oni proces wnioskowania o pożyczkę.

> **Analogia z życia:** wyobraź sobie bank. Zanim klient wejdzie do placówki i złoży wniosek o kredyt, musi najpierw okazać się przy wejściu (recepcja). Applicant Service to właśnie ta recepcja — potwierdza kim jesteś (rejestracja, logowanie) i wydaje Ci "przepustkę" (token JWT), którą pokazujesz wszędzie dalej w banku.

### 1.2 Co dokładnie robi ten serwis?

Serwis implementuje cztery operacje biznesowe:

| Operacja | Metoda HTTP | Ścieżka | Opis |
|----------|-------------|---------|------|
| Rejestracja | `POST` | `/api/v1/auth/register` | Tworzy nowego klienta w systemie |
| Logowanie | `POST` | `/api/v1/auth/login` | Weryfikuje dane logowania i wydaje tokeny |
| Odświeżenie tokena | `POST` | `/api/v1/auth/refresh` | Rotuje refresh token i wydaje nowy access token |
| Pobranie profilu | `GET` | `/api/v1/me` | Zwraca profil zalogowanego klienta |

Dodatkowo udostępnia dwa endpointy operacyjne (tzw. *health checks*):
- `GET /health` — proba żywotności (liveness probe) — serwis działa.
- `GET /ready` — proba gotowości (readiness probe) — serwis jest gotowy przyjmować ruch.

### 1.3 Dlaczego ten serwis jest ważny?

1. **Punkt wejścia do systemu** — bez niego żaden użytkownik nie może się zalogować, więc żaden inny serwis nie ma komu świadczyć usług.
2. **Bezpieczeństwo danych uwierzytelniających** — hasła są hashowane algorytmem **Argon2id** (jednym z najbezpieczniejszych obecnie dostępnych), tokeny są podpisywane asymetrycznie (**RS256**).
3. **Fundament mikroserwisowej architektury** — pokazuje, jak powinien wyglądać każdy serwis w CrediGuard: Clean Architecture, porty i adaptery, dependency injection, asynchroniczność, testy.
4. **Wzorzec dla innych serwisów** — zespół może kopiować ten układ warstw (domain → application → infrastructure → api) do pozostałych mikroserwisów.

### 1.4 Czego ten serwis NIE robi

- Nie zarządza wnioskami pożyczkowymi (to robi Loan Application Service).
- Nie czyta cudzych baz danych (zasada "database-per-service").
- Nie wykonuje oceny scoringowej.
- Nie wysyła e-maili (chociaż publikuje zdarzenie `applicant.registered.v1`, które w przyszłości może je uruchomić).

---

## 2. Architektura warstwowa

### 2.1 Zasada zależności (ang. Dependency Rule)

Clean Architecture (czysta architektura) opiera się na jednej fundamentalnej zasadzie:

> **Wewnętrzna warstwa NIE wie nic o warstwach zewnętrznych.**

W praktyce dla tego serwisu oznacza to:

- Warstwa **domain** (domena) nie importuje NICZEGO spoza standardowej biblioteki Pythona (stdlib). Nie wie, co to jest FastAPI, SQLAlchemy, JWT, Argon2.
- Warstwa **application** (aplikacja) zależy tylko od **domain** i od **abstrakcji** (interfejsów/portów), które sama definiuje. Nie wie, co to jest SQLAlchemy ani `python-jose`.
- Warstwa **infrastructure** (infrastruktura) zależy od **application** (implementuje jej porty) i od **domain**. To tutaj żyje SQLAlchemy, Argon2, JWT.
- Warstwa **api** (API) zależy od **application** (wywołuje use case'y) i **infrastructure** (buduje konkretne implementacje).

Kierunek zależności jest zawsze **od zewnątrz do wewnątrz**: `api → infrastructure → application → domain`. Nigdy odwrotnie!

### 2.2 Diagram warstw (Mermaid)

```mermaid
flowchart TB
    subgraph API["Warstwa API (src/api)"]
        A1["routes/auth.py"]
        A2["routes/me.py"]
        A3["dependencies.py"]
        A4["schemas.py"]
    end

    subgraph APP["Warstwa Application (src/application)"]
        P1["ports/ (interfejsy)"]
        U1["use_cases/register.py"]
        U2["use_cases/login.py"]
        U3["use_cases/refresh_token.py"]
        U4["use_cases/get_me.py"]
        DTO["dto.py"]
    end

    subgraph DOM["Warstwa Domain (src/domain)"]
        E1["entities.py"]
        E2["exceptions.py"]
    end

    subgraph INFRA["Warstwa Infrastructure (src/infrastructure)"]
        I1["persistence/models.py"]
        I2["persistence/repository.py"]
        I3["security/argon2_hasher.py"]
        I4["security/jwt_service.py"]
        I5["security/keygen.py"]
        I6["database.py"]
    end

    subgraph EXT["Świat zewnętrzny"]
        DB[(PostgreSQL)]
        KEYS["Klucze RSA (PEM)"]
        HTTP["Żądania HTTP"]
    end

    HTTP --> A1
    HTTP --> A2
    A1 --> A3
    A2 --> A3
    A3 --> U1
    A3 --> U2
    A3 --> U3
    A3 --> U4
    U1 --> DTO
    U1 --> P1
    U2 --> DTO
    U2 --> P1
    U3 --> DTO
    U3 --> P1
    U4 --> DTO
    U4 --> P1
    P1 --> E1
    P1 --> E2
    I1 --> E1
    I2 --> P1
    I3 --> P1
    I4 --> P1
    I5 --> KEYS
    I4 --> KEYS
    I6 --> DB
    I2 --> DB
```

### 2.3 Dlaczego podział na warstwy?

- **Testowalność** — warstwę domeny i aplikacji można testować bez bazy danych, bez HTTP, bez sieci. Wystarczy podmienić realne implementacje na mocki.
- **Niezależność od frameworków** — jeśli za 2 lata zespół zechce zamienić FastAPI na inny framework, wystarczy przepisać warstwę API. Jeśli zechce zamienić Postgres na inną bazę, wystarczy przepisać warstwę infrastruktury.
- **Zrozumiałość** — każdy plik ma jedno zadanie (zasada SRP — Single Responsibility Principle).
- **Kontrola zmian** — zmiana w bazie danych nie wymaga dotykania logiki biznesowej.

---

## 3. Struktura projektu

### 3.1 Pełne drzewo katalogów

Serwis żyje w katalogu `services/applicant/`. Poniżej pełna struktura:

```
services/applicant/
├── .env.example                      # Wzór zmiennych środowiskowych
├── Dockerfile                        # Obraz kontenera
├── README.md                         # Skrócona dokumentacja
├── alembic.ini                       # Konfiguracja Alembic
├── pyproject.toml                    # Metadane pakietu, zależności, konfiguracja ruff/mypy
├── keys/
│   ├── private_key.pem               # KLUCZ PRYWATNY RSA (NIGDY nie commituj!)
│   └── public_key.pem                # Klucz publiczny RSA
├── alembic/
│   ├── env.py                        # Środowisko migracji (async)
│   └── versions/
│       └── 0001_initial.py           # Migracja początkowa
├── src/
│   ├── main.py                       # Wejście aplikacji FastAPI
│   ├── api/
│   │   ├── __init__.py
│   │   ├── dependencies.py           # Fabryki zależności (Depends)
│   │   ├── schemas.py                # Schematy Pydantic dla API
│   │   └── routes/
│   │       ├── __init__.py
│   │       ├── auth.py               # Endpointy auth (register/login/refresh)
│   │       └── me.py                 # Endpoint /me
│   ├── application/
│   │   ├── __init__.py
│   │   ├── dto.py                    # Obiekty transferu danych
│   │   ├── ports/
│   │   │   ├── __init__.py
│   │   │   ├── password_hasher.py    # Port: hashowanie haseł
│   │   │   ├── repository.py         # Porty: repozytoria
│   │   │   └── token_service.py      # Port: usługa tokenów
│   │   └── use_cases/
│   │       ├── __init__.py
│   │       ├── get_me.py             # Use case: profil
│   │       ├── login.py              # Use case: logowanie
│   │       ├── refresh_token.py      # Use case: odświeżenie
│   │       └── register.py           # Use case: rejestracja
│   ├── domain/
│   │   ├── __init__.py
│   │   ├── entities.py               # Encje domenowe
│   │   └── exceptions.py             # Wyjątki domenowe
│   └── infrastructure/
│       ├── __init__.py
│       ├── database.py               # Zarządzanie sesjami DB
│       ├── persistence/
│       │   ├── __init__.py
│       │   ├── models.py             # Modele ORM (SQLAlchemy)
│       │   └── repository.py         # Implementacje repozytoriów
│       └── security/
│           ├── __init__.py
│           ├── argon2_hasher.py      # Hasher Argon2id
│           ├── jwt_service.py        # Usługa JWT (RS256)
│           └── keygen.py             # Generator kluczy RSA
└── tests/
    ├── integration/
    │   ├── conftest.py               # Fixtures testcontainers
    │   └── test_auth_flow.py         # Testy pełnego przepływu
    └── unit/
        ├── application/
        │   ├── test_get_me.py
        │   ├── test_login.py
        │   ├── test_refresh_token.py
        │   └── test_register.py
        └── domain/
            └── test_entities.py
```

### 3.2 Po co podział na `src/`?

Katalog `src/` (skrót od ang. *source*) odizolowuje kod źródłowy od pozostałych plików projektu (testy, konfiguracja, migracje). Dzięki temu:
- Pakiet można zainstalować jako editable (`pip install -e .`).
- Testy i narzędzia (mypy, ruff) mają jednoznaczną bazę (konfiguracja `src = ["src"]`).
- Unikamy przypadkowego importowania czegoś spoza pakietu.

### 3.3 Po co `__init__.py` w każdym katalogu?

Pliki `__init__.py` oznaczają, że dany katalog jest **pakietem Pythona**. Bez nich `import src.domain.entities` by nie zadziałało (przed Pythonem 3.3). Dzięki nim mamy czytelne, hierarchiczne importy, np.:

```python
from src.domain.entities import Applicant
from src.application.ports.repository import ApplicantRepository
```

Alternatywnie można by używać "namespace packages" (Python 3.3+), gdzie `__init__.py` jest opcjonalne — ale tutaj celowo są puste lub re-exportujące, co czyni strukturę jawną i czytelną.

---

## 4. Analiza plików źródłowych — linijka po linijce

> W tej sekcji przechodzimy przez KAŻDY plik źródłowy serwisu. Dla każdego pliku podajemy:
> cel, analizę blok po bloku (a tam, gdzie to istotne — linijka po linijce), przykłady,
> konsekwencje usunięcia danej linii oraz alternatywy. Kolejność wynika z warstw
> architektury: od najbardziej wewnętrznej (domain) po zewnętrzną (api, main).

---

### 4.1 Plik: `src/domain/entities.py`

**Cel:** definiuje **czyste encje domenowe** (`Applicant`, `RefreshToken`). Encje przenoszą dane i zachowania (metody) domenowe. Ten plik NIE importuje niczego spoza standardowej biblioteki — spełnia twardą regułę projektu (domain → tylko stdlib).

#### Blok 1: docstring i importy

```python
"""Domain entities for Applicant Service."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4
```

- **Linia 1:** docstring (ciąg dokumentacyjny) — pierwsza instrukcja modułu, opisująca jego przeznaczenie. Gdy ktoś wykona `help(entities)` albo `entities.__doc__`, zobaczy ten tekst.
- **Linia 3:** `from __future__ import annotations` — włącza leniwe (opóźnione) ewaluowanie adnotacji typów (PEP 563). Oznacza to, że zapisy typu `Mapped[UUID]`, `Optional[datetime]` są traktowane jako **ciągi znaków**, dopóki ktoś ich faktycznie nie odczyta (np. mypy, dataclass). Zalety:
  - Można używać typów, które są zdefiniowane **później** w pliku.
  - Można pisać natywne generyki (`list[str]`) nawet w Pythonie 3.9.
  - Mypy (strict) wymaga precyzyjnych adnotacji — ta linia je ułatwia.
  - **Co by było, gdyby jej zabrakło?** W Pythonie 3.9 zapis `list[RefreshToken]` na poziomie adnotacji pola klasy mógłby wywołać `TypeError` w momencie definicji klasy (gdyż `list` nie był subscriptable jako adnotacja bez `from __future__`). W Pythonie 3.10+ zapis jest poprawny, ale projekt celowo wspiera Pythona 3.9 (`requires-python = ">=3.9"` w `pyproject.toml`).
- **Linia 5:** `from dataclasses import dataclass, field` — importuje dekorator `@dataclass` (automatyczne generowanie `__init__`, `__repr__`, `__eq__` itd.) oraz funkcję `field()` używaną do ustawienia **domyślnej fabryki wartości** (aby nie używać wartości mutowalnych jako domyślnych).
- **Linia 6:** `from datetime import datetime, timezone` — `datetime` to klasa reprezentująca datę i czas; `timezone` to klasa dla stref czasowych; `timezone.utc` to stała reprezentująca strefę UTC. Używamy `timezone.utc` zamiast `datetime.UTC` ze względu na kompatybilność wsteczną (Python 3.11+ ma `datetime.UTC`, ale projekt wspiera 3.9).
- **Linia 7:** `from typing import Optional` — alias typu `Optional[X]` = `X | None`. W Pythonie 3.10+ można pisać `X | None`, ale dla kompatybilności z 3.9 używamy `Optional`.
- **Linia 8:** `from uuid import UUID, uuid4` — `UUID` to klasa reprezentująca uniwersalny identyfikator (128-bitowy), `uuid4()` to funkcja generująca losowy UUID w wersji 4.

#### Blok 2: klasa `Applicant`

```python
@dataclass
class Applicant:
    """Applicant aggregate root."""

    email: str
    password_hash: str
    first_name: str
    last_name: str
    id: UUID = field(default_factory=uuid4)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
```

- **Linia `@dataclass`:** dekorator nakazujący Pythonowi wygenerować metody `__init__`, `__repr__`, `__eq__` na podstawie pól klasy. Dzięki temu nie musimy pisać ręcznie konstruktora:
  ```python
  applicant = Applicant(email="a@b.pl", password_hash="...", first_name="Jan", last_name="Kowalski")
  ```
  **Alternatywy:** (1) ręczny `__init__` — dużo powtarzalnego kodu; (2) `NamedTuple` — niemutowalny, wymaga innych wzorców; (3) Pydantic `BaseModel` — dobre, ale encje domenowe mają być wolne od frameworków (a Pydantic to zewnętrzna zależność), dlatego wybrano stdlib `dataclass`.
- **Linia docstringu:** opisuje rolę klasy jako "aggregate root" (korzeń agregatu) — w DDD agregat to grupa obiektów traktowana jako jedna całość; tutaj `Applicant` jest samowystarczalny.
- **Pole `email: str`** — adres e-mail klienta. `str` to typ tekstowy (ciąg znaków). Zapis adnotacji typu po dwukropku jest informacją dla mypy i IDE (nie jest wymuszany w runtime przez czysty Python).
- **Pole `password_hash: str`** — **hash** hasła, NIE hasło w czystej postaci! Tu zapisujemy wynik działania Argon2id (naprz. `$argon2id$v=19$m=65536,t=3,p=4$...`). Dlaczego hash, a nie hasło? Patrz sekcja 7.
- **Pole `first_name: str`** — imię.
- **Pole `last_name: str`** — nazwisko.
- **Pole `id: UUID = field(default_factory=uuid4)`** — identyfikator klienta. `field(default_factory=uuid4)` oznacza: "jeśli w konstruktorze nie podano `id`, wygeneruj nowy przy tworzeniu instancji". Ważne: `default_factory` jest wywoływany przy każdej instancji OSOBNO (każdy klient dostaje inny UUID). Gdybyśmy napisali po prostu `id: UUID = uuid4()`, to default zostałby policzony RAZ przy definicji klasy i wszyscy klienci mieliby TEN SAM identyfikator — klasyczny błąd, którego `field` unika.
- **Pole `created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))`** — data utworzenia. `lambda:` tworzy funkcję bezargumentową, wywoływaną przy tworzeniu każdej instancji. `datetime.now(timezone.utc)` zwraca **aktualny** czas w strefie UTC z informacją o strefie (aware datetime). Używamy UTC, aby uniknąć problemów ze strefami czasowymi (przechowujemy jeden kanoniczny czas).
- **Pole `updated_at: ...`** — data ostatniej aktualizacji. Podobna logika jak `created_at`.

#### Metoda `update_timestamp`

```python
    def update_timestamp(self) -> None:
        """Update the updated_at timestamp."""
        self.updated_at = datetime.now(timezone.utc)
```

- Definicja metody instancyjnej. Pierwszy argument `self` to referencja do konkretnego obiektu (instancji), na którym wołamy metodę.
- Metoda aktualizuje znacznik czasu. **Gdzie jest używana?** W tej wersji kodu nie jest wywoływana bezpośrednio w use case'ach (aktualizacje wierszy `updated_at` w bazie robi `onupdate` w SQLAlchemy), ale istnieje jako czyste zachowanie domenowe — w przyszłości może być wywoływana przy edycji profilu.
- **Co by się stało bez tej metody?** Musielibyśmy ręcznie ustawiać `updated_at` w każdym miejscu zmiany danych, co łatwo pominąć. Posiadanie metody domenowej daje jedno źródło prawdy.

#### Właściwość `full_name`

```python
    @property
    def full_name(self) -> str:
        """Return full name."""
        return f"{self.first_name} {self.last_name}"
```

- **`@property`** — dekorator zamieniający metodę w "właściwość" (atrybut liczony na żądanie). Dostęp następuje bez nawiasów: `applicant.full_name`, a nie `applicant.full_name()`.
- **`f"..."`** — f-string: formatowany ciąg znaków, w którym `{zmienna}` jest wstawiane jako wartość. Dla `first_name="Jan"`, `last_name="Kowalski"` zwróci `"Jan Kowalski"`.
- **Dlaczego property, a nie zwykłe pole?** Bo `full_name` jest pochodne od `first_name` i `last_name` — jeśli jedno z nich się zmieni, `full_name` automatycznie podąży. Gdyby to było pole, trzeba by je ręcznie synchronizować.
- **Alternatywa:** metoda `def full_name(self)`. Property jest wygodniejsze (czytelniejsze użycie) i semantycznie sugeruje "to jest atrybut".

#### Blok 3: klasa `RefreshToken`

```python
@dataclass
class RefreshToken:
    """Refresh token entity (stored hashed)."""

    applicant_id: UUID
    token_hash: str
    expires_at: datetime
    id: UUID = field(default_factory=uuid4)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    revoked_at: Optional[datetime] = None
```

- **`applicant_id: UUID`** — do jakiego klienta należy token. To "klucz logiczny" łączący token z klientem — ale UWAGA: w bazie danych NIE ma klucza obcego (foreign key), zobacz sekcję 8.5.
- **`token_hash: str`** — **hash refresh tokena**, nie sam token! (szczegóły w sekcji 7.5).
- **`expires_at: datetime`** — termin ważności (domyślnie 7 dni od utworzenia).
- **`revoked_at: Optional[datetime] = None`** — moment unieważnienia. `None` oznacza "token wciąż aktywny". Jeśli `revoked_at` jest ustawione, token jest unieważniony (np. po użyciu w rotacji).

#### Metoda `is_valid`

```python
    def is_valid(self, now: Optional[datetime] = None) -> bool:
        """Check if token is valid (not expired, not revoked)."""
        if now is None:
            now = datetime.now(timezone.utc)
        return self.revoked_at is None and self.expires_at > now
```

- Parametr `now` z domyślnym `None` — pozwala na **wstrzyknięcie czasu** (testowalność). W testach podajemy konkretny moment, aby nie polegać na zegarze; w produkcji jeśli `None`, bierzemy aktualny czas.
- **`self.revoked_at is None`** — sprawdzenie, czy token nie został unieważniony. `is None` porównuje referencje do obiektu `None` (poprawne, bo `None` jest singletonem).
- **`self.expires_at > now`** — sprawdzenie, czy termin ważności nie minął.
- Zwraca `True` TYLKO wtedy, gdy oba warunki są spełnione (operator `and`).
- **Przykład:** token utworzony z `expires_at = teraz + 7 dni`, użyty po 2 dniach → `expires_at > now` → prawda, `revoked_at is None` → prawda → zwraca `True`. Token po 8 dniach → `expires_at > now` fałsz → `False`.

#### Metoda `revoke`

```python
    def revoke(self) -> None:
        """Mark token as revoked."""
        self.revoked_at = datetime.now(timezone.utc)
```

- Ustawia `revoked_at` na bieżący czas UTC. Token staje się nieaktywny.
- **Gdzie jest wywoływana?** w `RefreshTokenUseCase` — przy rotacji tokena (stary token jest unieważniany, nowy wydawany).

---

### 4.2 Plik: `src/domain/exceptions.py`

**Cel:** definiuje hierarchię **wyjątków domenowych**. Dzięki nim logika biznesowa sygnalizuje błędy w sposób niezależny od HTTP (warstwa application nie wie, co to `HTTPException`).

```python
"""Domain exceptions for Applicant Service."""

from __future__ import annotations


class ApplicantDomainError(Exception):
    """Base exception for applicant domain errors."""
```

- `ApplicantDomainError` dziedziczy po wbudowanym `Exception`. To **baza** dla wszystkich wyjątków domenowych. Dzięki temu w warstwie API możemy przechwycić jedną klasę i mapować na kody HTTP — albo w przyszłości dodać globalny handler dla całej hierarchii.

```python
class ApplicantNotFound(ApplicantDomainError):
    """Raised when applicant is not found."""

    def __init__(self, applicant_id: str) -> None:
        self.applicant_id = applicant_id
        super().__init__(f"Applicant not found: {applicant_id}")
```

- **`def __init__`** — własny konstruktor, który przyjmuje `applicant_id` (jako `str`, bo może pochodzić z tokena), zapisuje go jako atrybut instancji (`self.applicant_id`) i wywołuje `super().__init__(...)`, czyli konstruktor `Exception` z gotowym komunikatem.
- **Dlaczego zapisujemy `applicant_id` jako atrybut?** Umożliwia to testom sprawdzenie `exc_info.value.applicant_id == str(applicant_id)` (patrz test `test_get_me.py`), a w przyszłości — wzbogacenie logów czy odpowiedzi o kontekst.
- **`super().__init__`** — jawne wywołanie konstruktora klasy nadrzędnej. `Exception.__init__(message)` ustawia komunikat widoczny przy `str(exc)` i `repr(exc)`.
- **Przykład komunikatu:** `"Applicant not found: 3f2a..."`.

```python
class EmailAlreadyRegistered(ApplicantDomainError):
    """Raised when email is already registered."""

    def __init__(self, email: str) -> None:
        self.email = email
        super().__init__(f"Email already registered: {email}")
```

- Analogiczna struktura. Przechowuje `email` w atrybucie. Komunikat: `"Email already registered: test@example.com"`.
- **Dlaczego oddzielny wyjątek?** Rejestracja musi poinformować klienta, że e-mail jest zajęty (HTTP 409 Conflict), a to jest inny scenariusz niż np. błędne dane logowania.

```python
class InvalidCredentials(ApplicantDomainError):
    """Raised when login credentials are invalid."""

    def __init__(self) -> None:
        super().__init__("Invalid email or password")
```

- **Brak parametrów** — celowo. Komunikat jest JEDEN: `"Invalid email or password"`, niezależnie od tego, czy użytkownik nie istnieje, czy podał złe hasło. To świadoma decyzja bezpieczeństwa: **anty-enumeracja kont** (szczegóły w sekcji 7.6). Gdybyśmy zwracali "User not found" vs "Wrong password", atakujący mógłby sprawdzać, które e-maile istnieją w systemie.

```python
class InvalidRefreshToken(ApplicantDomainError):
    """Raised when refresh token is invalid or expired."""

    def __init__(self) -> None:
        super().__init__("Invalid or expired refresh token")


class RefreshTokenRevoked(ApplicantDomainError):
    """Raised when refresh token has been revoked (used already)."""

    def __init__(self) -> None:
        super().__init__("Refresh token has been revoked")
```

- `InvalidRefreshToken` — token nie istnieje, wygasł, albo nie pasuje do żadnego wpisu. Komunikat neutralny: `"Invalid or expired refresh token"`.
- `RefreshTokenRevoked` — token został już użyty (powtórna próba użycia tokena po rotacji). Komunikat: `"Refresh token has been revoked"`.

**Dlaczego te wyjątki są w `domain`, a nie w `application`?** Bo reprezentują **reguły domenowe** (np. "refresh token może być użyty raz"). Wyjątki są częścią języka domeny.

**Co się dzieje, gdy wyjątek zostanie wyrzucony w use case i nie przechwycony w API?** W obecnej wersji warstwa API NIE ma dedykowanego handlera dla `ApplicantDomainError` — FastAPI zwróci wtedy odpowiedź `500 Internal Server Error`. To jest słaby punkt implementacji (patrz sekcja 4.18 i FAQ), ale nie wpływa na poprawność logiki. W testach integracyjnych sprawdzamy komunikaty wyjątków bezpośrednio.

---

### 4.3 Plik: `src/application/ports/repository.py`

**Cel:** definiuje **porty** (interfejsy) dla repozytoriów. Port to abstrakcyjny kontrakt mówiący: "cokolwiek będzie przechowywać dane, musi umieć te rzeczy". Konkretną implementację (adapter) dostarcza warstwa infrastruktury (`SQLAlchemyApplicantRepository`). Dzięki temu use case'y zależą od **abstrakcji**, a nie od SQLAlchemy.

```python
"""Repository ports for Applicant Service."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional
from uuid import UUID

from src.domain.entities import Applicant, RefreshToken
```

- **`from abc import ABC, abstractmethod`** — `ABC` (Abstract Base Class) to klasa bazowa dla abstrakcyjnych typów; `abstractmethod` to dekorator oznaczający metodę abstrakcyjną. Klasa z metodą abstrakcyjną **nie może być instancjonowana** — musi być zaimplementowana przez klasę pochodną.
- **Ważne:** ten plik importuje `src.domain.entities` — to DOZWOLONA zależność (application → domain), zgodna z zasadą zależności.

#### Port `ApplicantRepository`

```python
class ApplicantRepository(ABC):
    """Port for applicant persistence."""

    @abstractmethod
    async def save(self, applicant: Applicant) -> None:
        """Save applicant."""

    @abstractmethod
    async def get_by_id(self, applicant_id: UUID) -> Optional[Applicant]:
        """Get applicant by ID."""

    @abstractmethod
    async def get_by_email(self, email: str) -> Optional[Applicant]:
        """Get applicant by email."""

    @abstractmethod
    async def exists_by_email(self, email: str) -> bool:
        """Check if applicant with email exists."""
```

Omówienie metod jedna po drugiej:

1. **`save(applicant: Applicant) -> None`**
   - Zapisywanie (utworzenie lub aktualizacja) klienta. Zwraca `None`, bo "zapis" to operacja efektowa (side effect), a nie zwracająca dane.
   - **Dlaczego `async`?** Bo dostęp do bazy to operacja wejścia/wyjścia (I/O). W Pythonie asynchronicznym I/O nie blokuje event loop (szczegóły w sekcji 6.1).

2. **`get_by_id(applicant_id: UUID) -> Optional[Applicant]`**
   - Pobranie klienta po identyfikatorze. Zwraca `Optional[Applicant]`, czyli `Applicant | None` — `None`, gdy klient nie istnieje. (Wybór: zwracanie `None` zamiast rzucania wyjątku — sprawdzenie w use case.)

3. **`get_by_email(email: str) -> Optional[Applicant]`**
   - Pobranie klienta po adresie e-mail. Używane w logowaniu.

4. **`exists_by_email(email: str) -> bool`**
   - Sprawdzenie, czy klient o danym e-mailu istnieje. **Zwraca tylko `bool`**, a nie pełną encję — to oszczędniejsze (baza może zwrócić samo `id`/flaga, bez czytania wszystkich kolumn). Używane w rejestracji.

**Alternatywy dla tego projektu portu:**
- Zamiast `exists_by_email` można by używać `get_by_email is not None` — ale to czyta cały wiersz; dedykowana metoda może wygenerować lżejsze SQL (patrz sekcja 8.6).
- Port mógłby mieć metodę `update`, ale encja `Applicant` nie ma osobnego `update` — zapis aktualizacji może iść przez `save` (upsert). W projekcie zachowano spójność: `save` mapuje pełną encję na wiersz.

#### Port `RefreshTokenRepository`

```python
class RefreshTokenRepository(ABC):
    """Port for refresh token persistence."""

    @abstractmethod
    async def save(self, token: RefreshToken) -> None:
        """Save refresh token."""

    @abstractmethod
    async def get_by_hash(self, token_hash: str) -> Optional[RefreshToken]:
        """Get refresh token by hash."""

    @abstractmethod
    async def revoke_all_for_applicant(self, applicant_id: UUID) -> None:
        """Revoke all refresh tokens for applicant (e.g., on password change)."""

    @abstractmethod
    async def update(self, token: RefreshToken) -> None:
        """Update existing refresh token."""

    @abstractmethod
    async def get_all_valid(self, now: datetime) -> list[RefreshToken]:
        """Get all valid (not expired, not revoked) refresh tokens."""
```

- **`save`** — zapis nowego tokena.
- **`get_by_hash`** — pobranie tokena po hash. Obecnie nieużywane w use case'ach, ale przydatne (np. przyszła kontrola "single-use"). Zauważ: szukamy po **hashu**, a nie po czystym tokenie — token nie jest przechowywany jawnie.
- **`revoke_all_for_applicant`** — unieważnienie wszystkich tokenów klienta (przydatne np. przy zmianie hasła, wymuszeniu wylogowania). Zadeklarowane jako część portu (przyszłe możliwości), choć w obecnych use case'ach nie jest wywoływane.
- **`update`** — aktualizacja istniejącego tokena (np. ustawienie `revoked_at` przy rotacji).
- **`get_all_valid(now)`** — pobranie wszystkich ważnych tokenów (niewygasłych i nieunieważnionych). To kluczowa metoda dla rotacji: use case iteruje po ważnych tokenach i sprawdza, czy hash któregoś pasuje do przesłanego tokena.

**Analiza wzorca Repository (po polsku):** Repozytorium to wzorzec projektowy, który udaje "kolekcję encji w pamięci" (jak lista czy zbiór), podczas gdy naprawdę trzyma dane w bazie. Analogia: biblioteka ma kartotekę (repository) — mówisz "znajdź mi książkę o tym tytule", a nie interesuje Cię, w którym regale leży. Podobnie use case mówi `repo.get_by_email(...)` i nie wie, czy dane leżą w Postgresie, MySQL czy pliku. Szczegóły wzorca — sekcja 6.4.

### 4.4 Plik: `src/application/ports/password_hasher.py`

**Cel:** port dla hashowania i weryfikacji haseł.

```python
"""Password hasher port for Applicant Service."""

from __future__ import annotations

from abc import ABC, abstractmethod


class PasswordHasher(ABC):
    """Port for password hashing."""

    @abstractmethod
    def hash(self, password: str) -> str:
        """Hash a password."""

    @abstractmethod
    def verify(self, password: str, password_hash: str) -> bool:
        """Verify a password against its hash."""
```

- **`hash(password: str) -> str`** — przyjmuje czyste hasło, zwraca ciąg znaków (hash). Konkretny algorytm NIE jest tu znany — to może być Argon2id (tak jest), bcrypt, scrypt, cokolwiek.
- **`verify(password: str, password_hash: str) -> bool`** — przyjmuje czyste hasło i zapisany wcześniej hash, zwraca `True`/`False` w zależności od zgodności.

**Czemu port a nie konkretna klasa?** Ponieważ logika rejestracji/logowania nie powinna zależeć od algorytmu kryptograficznego. Gdybyśmy jutro zmienili Argon2 na bcrypt, zmienilibyśmy tylko implementację w `infrastructure/security/`, a use case'y pozostałyby nietknięte. To esencja **Odwrócenia zależności (Dependency Inversion)**.

### 4.5 Plik: `src/application/ports/token_service.py`

**Cel:** port dla operacji na tokenach JWT.

```python
"""Token service port for Applicant Service (RS256 JWT)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from uuid import UUID


class TokenService(ABC):
    """Port for JWT token operations (RS256)."""

    @abstractmethod
    def create_access_token(self, applicant_id: UUID, email: str) -> str:
        """Create short-lived access token (15 min)."""

    @abstractmethod
    def create_refresh_token(self, applicant_id: UUID) -> str:
        """Create long-lived refresh token (7 days)."""

    @abstractmethod
    def decode_access_token(self, token: str) -> tuple[UUID, str]:
        """Decode and validate access token. Returns (applicant_id, email)."""

    @abstractmethod
    def get_access_token_expiry(self) -> datetime:
        """Get expiry datetime for access token."""

    @abstractmethod
    def get_refresh_token_expiry(self) -> datetime:
        """Get expiry datetime for refresh token."""
```

- **`create_access_token(applicant_id, email) -> str`** — tworzy krótkożyjący token dostępowy (15 minut). Przyjmuje `applicant_id` (podmiot) i `email` (do umieszczenia w claims).
- **`create_refresh_token(applicant_id) -> str`** — tworzy długożyjący token odświeżający (7 dni).
- **`decode_access_token(token) -> tuple[UUID, str]`** — weryfikuje i odczytuje access token; zwraca krotkę `(applicant_id, email)`. **`tuple[UUID, str]`** to typ krotki dwóch elementów. Używane w `get_current_applicant_id`.
- **`get_access_token_expiry() / get_refresh_token_expiry() -> datetime`** — zwracają moment wygaśnięcia (obliczany "od teraz"). Potrzebne do wyliczenia `expires_in` w odpowiedzi oraz `expires_at` encji `RefreshToken`.

**Czemu zwracać krotkę, a nie słownik?** Krotka jest lekka i pozycyjna; czytelna w miejscu użycia: `applicant_id, _ = token_service.decode_access_token(token)`. Słownik wymuszałby wyszukiwanie kluczy.

### 4.6 Plik: `src/application/dto.py`

**Cel:** definiuje **DTO** (Data Transfer Objects — obiekty transferu danych) oraz modele walidacji wejścia/wyjścia użycase'ów. DTO to "pudełka na dane", które podróżują między warstwami. Tu używamy Pydantic `BaseModel`, bo:
- zapewnia automatyczną **walidację** (np. format e-maila),
- serializację/deserializację JSON,
- obsługę w FastAPI jako `request body` i `response_model`.

> **Uwaga architektoniczna:** istnieje też `src/api/schemas.py`, które zawiera niemal identyczne klasy. To **duplikacja** — jedna z rzeczy, na które warto zwrócić uwagę (omówione w 4.20 i FAQ). W idealnym Clean Architecture DTO powinny żyć w jednym miejscu (np. warstwa application), a `api/schemas.py` nie powinien ich powielać.

```python
"""DTOs for Applicant Service use cases."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field
```

- **`from pydantic import BaseModel, EmailStr, Field`** — `BaseModel` to bazowa klasa Pydantic (v2). `EmailStr` to typ walidujący poprawność adresu e-mail. `Field` to funkcja pozwalająca dodać metadane walidacyjne (min/max długość itd.).

#### Klasa `RegisterRequest`

```python
class RegisterRequest(BaseModel):
    """Request to register a new applicant."""

    email: EmailStr
    password: str = Field(min_length=8, max_length=128)
    first_name: str = Field(min_length=1, max_length=100)
    last_name: str = Field(min_length=1, max_length=100)
```

- **`email: EmailStr`** — pole wymagane, typu "e-mail". Pydantic przy walidacji sprawdzi, czy wartość wygląda jak poprawny e-mail.
  - **Przykład:** wysyłasz `{"email": "jan.kowalski"}` (bez `@domena`) → FastAPI/Pydantic zwróci błąd `422 Unprocessable Entity` z komunikatem o niepoprawnym adresie e-mail. **Konkretny przykład odpowiedzi:**
    ```json
    {
      "detail": [
        {
          "type": "value_error",
          "loc": ["body", "email"],
          "msg": "value is not a valid email address",
          "input": "jan.kowalski"
        }
      ]
    }
    ```
  - **Dlaczego `EmailStr` zamiast `str`?** Bo walidacja formatu następuje **na granicy systemu** (przy wejściu), a nie dopiero w logice biznesowej. To realizuje zasadę "waliduj jak najwcześniej". Dodatkowo `EmailStr` normalizuje? Nie — w Pydantic nie normalizuje automatycznie, ale przynajmniej wymusza format.
- **`password: str = Field(min_length=8, max_length=128)`** — hasło musi mieć od 8 do 128 znaków. **Gdzie zapisujemy `password_hash`?** Hasło po walidacji trafia do hashera; w bazie ląduje hash o długości ~97 znaków (Argon2id z domyślnymi parametrami), a `String(255)` w kolumnie ma zapas. **Dlaczego 8 znaków?** Minimum bezpieczeństwa. **Dlaczego 128?** Ochrona przed DoS-em przez bardzo długie hasła (maksymalny koszt hashowania jest ograniczony).
- **`first_name` / `last_name: str = Field(min_length=1, max_length=100)`** — od 1 do 100 znaków. Zapobiega pustym lub absurdalnie długim wartościom.

#### Klasa `LoginRequest`

```python
class LoginRequest(BaseModel):
    """Request to login."""

    email: EmailStr
    password: str
```

- E-mail (walidowany format) i hasło (bez limitu długości — bo przy logowaniu nie chcemy odrzucać haseł, które mogły być ustawione dawniej, a także dlatego, że hasło NIE jest tu hashowane, więc długość nie wpływa na koszt).

#### Klasa `RefreshTokenRequest`

```python
class RefreshTokenRequest(BaseModel):
    """Request to refresh access token."""

    refresh_token: str
```

- Jeden wymagany ciąg — sam token odświeżający.

#### Klasa `TokenResponse`

```python
class TokenResponse(BaseModel):
    """Response with access and refresh tokens."""

    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int  # seconds until access token expires
```

- **`token_type: str = "bearer"`** — pole z wartością domyślną `"bearer"` (standard OAuth 2.0: klient wysyła token w nagłówku `Authorization: Bearer <token>`). Domyślna wartość = pole opcjonalne w odpowiedzi, zawsze obecne jako `"bearer"`.
- **`expires_in: int`** — liczba sekund do wygaśnięcia access tokena. Wartość liczona w use case (patrz niżej), typ `int` — ułamek jest odcinany przez `int()`.

#### Klasa `ApplicantResponse`

```python
class ApplicantResponse(BaseModel):
    """Applicant profile response."""

    id: UUID
    email: EmailStr
    first_name: str
    last_name: str
    created_at: datetime

    class Config:
        from_attributes = True
```

- **`id: UUID`** — Pydantic obsłuży konwersję ciągu na `UUID` przy walidacji wejścia i serializacji wyjścia.
- **`created_at: datetime`** — serializowany do ISO 8601, np. `"2026-08-26T12:00:00Z"`.
- **`class Config: from_attributes = True`** — to składnia **Pydantic v1** (w v2 odpowiednikiem jest `model_config = ConfigDict(from_attributes=True)`). Umożliwia tworzenie modelu **z obiektu/atrybutów**, np. `ApplicantResponse.model_validate(applicant)` — choć w kodzie use case buduje model ręcznie, przekazując pola. Warto wiedzieć, że `from_attributes` pozwalałoby też mapować wprost z atrybutów encji.

**Dlaczego `expires_in` jest liczone, a nie sztywno 900 (15×60)?** Ponieważ czas liczony jest od **momentu utworzenia tokena** do **wygaśnięcia**; klient otrzymuje wartość dokładną na swój moment. Obliczenie: `(get_access_token_expiry() - now).total_seconds()`.

---

### 4.7 Plik: `src/application/use_cases/register.py`

**Cel:** implementacja **use case'a** "Rejestracja". Use case to pojedyncza operacja biznesowa wykonywana przez użytkownika. Tutaj: sprawdź e-mail → haszuj hasło → zapisz klienta → wygeneruj tokeny → zapisz refresh token → zwróć odpowiedź.

```python
"""Register use case for Applicant Service."""

from __future__ import annotations

from datetime import datetime, timezone

from src.application.dto import RegisterRequest, TokenResponse
from src.application.ports.password_hasher import PasswordHasher
from src.application.ports.repository import ApplicantRepository, RefreshTokenRepository
from src.application.ports.token_service import TokenService
from src.domain.entities import Applicant, RefreshToken
from src.domain.exceptions import EmailAlreadyRegistered
```

- **Linia importów:** use case zależy od **DTO**, **portów** (abstrakcji!) i **encji domenowych**. Nie importuje żadnych klas z `infrastructure` ani `api`. To gwarantuje, że logika biznesowa jest testowalna bez bazy i frameworków.

#### Konstruktor (Wstrzykiwanie zależności — DI)

```python
class RegisterUseCase:
    """Use case for applicant registration."""

    def __init__(
        self,
        applicant_repo: ApplicantRepository,
        refresh_token_repo: RefreshTokenRepository,
        password_hasher: PasswordHasher,
        token_service: TokenService,
    ) -> None:
        self._applicant_repo = applicant_repo
        self._refresh_token_repo = refresh_token_repo
        self._password_hasher = password_hasher
        self._token_service = token_service
```

- **Konstruktor** przyjmuje cztery zależności i zapisuje je jako atrybuty prywatne (konwencja `self._nazwa` oznacza "proszę nie dotykać z zewnątrz").
- To jest **Constructor Injection** (wstrzykiwanie przez konstruktor) — najprostsza i najczystsza forma Dependency Injection: użycase jest tworzony z gotowymi zależnościami, nie tworzy ich sam.
- **Co by było, gdyby use case sam tworzył zależności (np. `ApplicantRepository(DB())`)?** Mielibyśmy silne sprzężenie z infrastrukturą, trudno by było testować (każdy test łączyłby się z bazą), a wymiana komponentów wymagałaby zmiany kodu. Dlatego zależności przychodzą z zewnątrz.

#### Metoda `execute` — krok 1: sprawdzenie e-maila

```python
    async def execute(self, request: RegisterRequest) -> TokenResponse:
        """Execute registration."""
        if await self._applicant_repo.exists_by_email(request.email):
            raise EmailAlreadyRegistered(request.email)
```

- **`async def execute`** — asynchroniczna metoda wykonująca operację. Zwraca `TokenResponse`.
- **`await self._applicant_repo.exists_by_email(...)`** — punkt asynchroniczny. Czekamy na odpowiedź bazy (I/O). W tym czasie event loop może obsługiwać inne żądania (patrz 6.1).
- Jeśli klient o tym e-mailu już istnieje → rzucamy `EmailAlreadyRegistered`. **Gdzie to jest złapane?** W FastAPI brakuje obecnie dedykowanego handlera → zwróciłoby 500. To jest znany obszar do poprawy (zob. 4.18, FAQ).

#### Krok 2: hashowanie hasła

```python
        password_hash = self._password_hasher.hash(request.password)
```

- **SYNCHRONICZNA** metoda (nie ma `await`). Dlaczego? Bo `argon2` wykonuje obliczenia **CPU-bound** (oparte o procesor i pamięć), a nie I/O. Blokowanie event loop przez ~100 ms przy każdym hashowaniu jest celowe? Nie idealne, ale dopuszczalne w MVP. W systemie produkcyjnym o dużym ruchu hashowanie powinno iść do osobnego worker'a/wątku (`asyncio.to_thread`). (Szczegóły: sekcja 6.1, tabela sync vs async.)
- Wynik: ciąg hashy Argon2id zapisywany w encji.

#### Krok 3: utworzenie encji domenowej

```python
        applicant = Applicant(
            email=request.email,
            password_hash=password_hash,
            first_name=request.first_name,
            last_name=request.last_name,
        )
```

- Tworzymy czystą encję `Applicant` (dataclass). `id`, `created_at`, `updated_at` zostaną wygenerowane automatycznie (default_factory). **Zauważ:** nie podajemy `id` — UUID wygeneruje się w tle.
- **Dlaczego DTO, potem encja, a nie od razu model bazy?** DTO jest walidowanym wejściem (granica systemu), encja jest obiektem domeny (reguły), model ORM jest szczegółem infrastruktury. Konwersja DTO→encja→model rozdziela te poziomy abstrakcji.

#### Krok 4: zapis klienta

```python
        await self._applicant_repo.save(applicant)
```

- Punkt asynchroniczny. W adapterze SQLAlchemy: `session.add(model)` + `await session.flush()` (szczegóły w 4.12, 8.3).

#### Krok 5: generowanie tokenów

```python
        access_token = self._token_service.create_access_token(applicant.id, applicant.email)
        refresh_token = self._token_service.create_refresh_token(applicant.id)
```

- **Synchroniczne** (JWT = obliczenia CPU, szybkie — ułamki milisekundy). Pierwszy token niesie `sub` (id klienta) i `email`; drugi — tylko `sub` i `type: refresh`.

#### Krok 6: hashowanie refresh tokena i zapis encji tokena

```python
        refresh_token_hash = self._password_hasher.hash(refresh_token)
        refresh_token_entity = RefreshToken(
            applicant_id=applicant.id,
            token_hash=refresh_token_hash,
            expires_at=self._token_service.get_refresh_token_expiry(),
        )
        await self._refresh_token_repo.save(refresh_token_entity)
```

- **`refresh_token_hash`** — znowu `self._password_hasher.hash(...)`. Uwaga: ten sam hasher haszuje i hasła, i refresh tokeny. To wygodne (jeden port), choć można by dyskutować nad osobnym "token hasher". Dzięki hashowaniu w bazie NIGDY nie leży czysty refresh token (szczegóły 7.5).
- **`expires_at`** — użycase pyta `token_service.get_refresh_token_expiry()` o moment wygaśnięcia (7 dni od teraz). W ten sposób encja i sam JWT mają spójny czas życia.
- Zapis encji tokena do repozytorium refresh tokenów.

#### Krok 7: zbudowanie odpowiedzi

```python
        return TokenResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=int(
                (
                    self._token_service.get_access_token_expiry() - datetime.now(timezone.utc)
                ).total_seconds()
            ),
        )
```

- **`get_access_token_expiry() - datetime.now(timezone.utc)`** — odejmowanie dwóch obiektów `datetime` daje `timedelta` (różnicę czasu).
- **`.total_seconds()`** — metoda `timedelta` zwracająca liczbę sekund (z ułamkiem).
- **`int(...)`** — ucięcie ułamka → pełne sekundy. Przykład: różnica `899.87 s` → `int()` → `899`. Gdyby nie było `int()`, klient dostałby `899.87`, co jest nieestetyczne w API.
- **Uwaga:** `get_access_token_expiry()` jest wywoływana DRUGI raz (raz już była wywoływana przy tworzeniu tokena w kroku 5). Wartości różnią się o ułamek sekundy, co jest nieszkodliwe, ale to pewna niespójność implementacyjna (wartość `expires_in` jest przybliżeniem).

**Co by było, gdyby `EmailAlreadyRegistered` nie było sprawdzane wcześniej?** Unikalny indeks na kolumnie `email` i tak by zablokował duplikat, ale z błędem z poziomu bazy (np. `IntegrityError`), który nie jest czytelny. Wczesna kontrola daje elegancki, domenowy wyjątek i nie generuje niepotrzebnego hashowania.

### 4.8 Plik: `src/application/use_cases/login.py`

**Cel:** use case "Logowanie" — weryfikacja tożsamości i wydanie tokenów.

```python
"""Login use case for Applicant Service."""

from __future__ import annotations

from datetime import datetime, timezone

from src.application.dto import LoginRequest, TokenResponse
from src.application.ports.password_hasher import PasswordHasher
from src.application.ports.repository import ApplicantRepository, RefreshTokenRepository
from src.application.ports.token_service import TokenService
from src.domain.entities import RefreshToken
from src.domain.exceptions import InvalidCredentials
```

- Konstruktor identyczny jak w `RegisterUseCase` (cztery zależności). To nie przypadek — wspólny "kształt" use case'ów ułatwia czytanie kodu.

```python
    async def execute(self, request: LoginRequest) -> TokenResponse:
        """Execute login."""
        applicant = await self._applicant_repo.get_by_email(request.email)
        if applicant is None:
            raise InvalidCredentials()
```

- Pobieramy klienta po e-mailu. **`get_by_email`** zwraca `None`, gdy nie ma takiego e-maila.
- **`raise InvalidCredentials()`** — ten sam wyjątek co przy złym haśle! Celowo. (Sekcja 7.6 o anty-enumeracji.)

```python
        if not self._password_hasher.verify(request.password, applicant.password_hash):
            raise InvalidCredentials()
```

- **`self._password_hasher.verify(hasło, hash_z_bazy)`** — synchroniczna weryfikacja. Zwraca `True`, gdy hash pasuje do hasła.
- Jeśli nie pasuje → ten sam wyjątek `InvalidCredentials`.

**Dlaczego nie rozróżniamy "user not found" i "wrong password"?** Szczegółowo w 7.6. Skrót: nie chcemy, aby atakujący mógł stwierdzić, czy dany e-mail istnieje w systemie (broń do enumeracji kont).

```python
        access_token = self._token_service.create_access_token(applicant.id, applicant.email)
        refresh_token = self._token_service.create_refresh_token(applicant.id)

        refresh_token_hash = self._password_hasher.hash(refresh_token)
        refresh_token_entity = RefreshToken(
            applicant_id=applicant.id,
            token_hash=refresh_token_hash,
            expires_at=self._token_service.get_refresh_token_expiry(),
        )
        await self._refresh_token_repo.save(refresh_token_entity)

        return TokenResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=int(
                (
                    self._token_service.get_access_token_expiry() - datetime.now(timezone.utc)
                ).total_seconds()
            ),
        )
```

- Logowanie tworzy **nowy refresh token** (obok access tokena) i zapisuje go. **Dlaczego nowy przy każdym logowaniu?** To dobra praktyka — każda sesja logowania ma własny token; wylogowanie jednej sesji nie psuje innych. Stare tokeny pozostają ważne (nie są unieważniane przy logowaniu) — to kompromis MVP.
- Reszta kodu (haszowanie tokena, `expires_in`) jest identyczna jak w rejestracji — to fragment wspólny, który mógłby zostać wydzielony do metody pomocniczej (refaktoring — patrz FAQ).

**Potencjalna pułapka:** każdy `POST /login` dodaje kolejny wiersz do `refresh_tokens`. Bez czyszczenia tabela rośnie. W produkcji należałoby dodać job usuwający wygasłe tokeny (TTL/cleanup). Wspominamy o tym w sekcji 8 i FAQ.

### 4.9 Plik: `src/application/use_cases/refresh_token.py`

**Cel:** use case "Odświeżenie tokena" — rotacja refresh tokena. To najbardziej skomplikowany use case.

```python
"""Refresh token use case for Applicant Service."""

from __future__ import annotations

from datetime import datetime, timezone

from src.application.dto import RefreshTokenRequest, TokenResponse
from src.application.ports.password_hasher import PasswordHasher
from src.application.ports.repository import ApplicantRepository, RefreshTokenRepository
from src.application.ports.token_service import TokenService
from src.domain.entities import RefreshToken
from src.domain.exceptions import InvalidRefreshToken, RefreshTokenRevoked
```

```python
    async def execute(self, request: RefreshTokenRequest) -> TokenResponse:
        """Execute token refresh."""
        now = datetime.now(timezone.utc)
```

- **`now`** — jeden wspólny moment czasu dla całej operacji. Używany do sprawdzenia ważności tokena i do porównań.

```python
        for token_entity in await self._refresh_token_repo.get_all_valid(now):
```

- **Punkt asynchroniczny:** pobieramy WSZYSTKIE ważne tokeny z bazy. Iterujemy po nich.
- **Dlaczego wszystkie?** Bo refresh token jest w bazie **haszowany** — nie można wykonać zapytania `WHERE token = 'xyz'`, gdyż czysty token nie jest przechowywany. Jedyne sensowne podejście: pobrać kandydujące tokeny i **weryfikować hash** po stronie aplikacji.
- **Niedoskonałość:** `get_all_valid(now)` zwraca WSZYSTKIE ważne tokeny WSZYSTKICH klientów. Przy dużej liczbie użytkowników to bardzo nieefektywne (tabela refresh_tokens może być ogromna). Lepsze rozwiązanie: `get_all_valid_for_applicant(applicant_id, now)` po wstępnym rozpoznaniu podmiotu — ale wtedy trzeba najpierw zdekodować JWT refresh tokena. Implementacja mogłaby dekodować refresh token (ma `sub`), a dopiero potem weryfikować hash. Obecne rozwiązanie jest poprawne funkcjonalnie, ale O(n) zamiast O(1) — do poprawy w v2 (zob. FAQ o skalowaniu).

```python
            if self._password_hasher.verify(request.refresh_token, token_entity.token_hash):
```

- Weryfikujemy, czy hash któregokolwiek tokena odpowiada przesłanemu tokenowi. `Argon2Hasher.verify` zwróci `True` dla dokładnie jednego (hash salt-owany).

```python
                if not token_entity.is_valid(now):
                    raise RefreshTokenRevoked()
```

- **Dlaczego druga kontrola ważności, skoro `get_all_valid` już filtrowała?** Warstwa obronna (defense in depth). W teorii `get_all_valid` zwraca tylko ważne tokeny; w praktyce druga kontrola chroni przed regresją implementacji repozytorium oraz przed błędami czasu. Koszt jest znikomy, a bezpieczeństwo wzrasta.
- Gdyby token okazał się nieważny (np. właśnie wygasł), rzucamy `RefreshTokenRevoked` — niech byle jaki token nie przechodził dalej.

```python
                token_entity.revoke()
                await self._refresh_token_repo.update(token_entity)
```

- **Rotacja:** unieważniamy stary token (ustawiamy `revoked_at`) i zapisujemy zmianę. W adapterze SQLAlchemy: `session.get(...)` → zmiana pól → `flush()`.
- **Czemu rotacja?** Gdyby stary token pozostał ważny, skradziony refresh token mógłby być używany w nieskończoność. Rotacja sprawia, że każdy token może być użyty raz; kolejne użycie tego samego tokena kończy się błędem (detekcja powtórnego użycia — ang. *replay detection*). (Sekcja 7.4.)

```python
                applicant = await self._applicant_repo.get_by_id(token_entity.applicant_id)
                if applicant is None:
                    raise InvalidRefreshToken()
```

- Pobieramy klienta przypisanego do tokena. Jeśli nie istnieje (np. konto usunięte) → `InvalidRefreshToken`. (Nie chcemy zdradzać przyczyny.)

```python
                new_access_token = self._token_service.create_access_token(
                    applicant.id, applicant.email
                )
                new_refresh_token = self._token_service.create_refresh_token(applicant.id)

                new_refresh_token_hash = self._password_hasher.hash(new_refresh_token)
                new_refresh_token_entity = RefreshToken(
                    applicant_id=applicant.id,
                    token_hash=new_refresh_token_hash,
                    expires_at=self._token_service.get_refresh_token_expiry(),
                )
                await self._refresh_token_repo.save(new_refresh_token_entity)
```

- Tworzymy **nowy** access token i **nowy** refresh token, haszujemy nowy refresh token i zapisujemy go. Klient dostaje parę (access, refresh).

```python
                return TokenResponse(
                    access_token=new_access_token,
                    refresh_token=new_refresh_token,
                    expires_in=int(
                        (
                            self._token_service.get_access_token_expiry()
                            - datetime.now(timezone.utc)
                        ).total_seconds()
                    ),
                )

        raise InvalidRefreshToken()
```

- Zwracamy nową parę. Jeśli pętla zakończy się bez znalezienia pasującego tokena → `raise InvalidRefreshToken()` na końcu metody. To "else" dla całej pętli.

**Ważna obserwacja — zamiana kolejności sprawdzeń:** w kodzie najpierw sprawdzamy `verify(hash)`, a potem `is_valid(now)`. Logika jest: znajdź token, którego hash pasuje; potem sprawdź, czy nie jest unieważniony. Ponieważ `get_all_valid` filtruje `revoked_at IS NULL`, token, który jest `revoked`, w ogóle nie trafi do pętli — zamiast tego na końcu dostaniemy `InvalidRefreshToken`. `RefreshTokenRevoked` pojawi się tylko w scenariuszu wygaśnięcia (expired) w momencie sprawdzenia, co może lekko mylić semantycznie. W testach jednostkowych celowo testujemy te ścieżki.

### 4.10 Plik: `src/application/use_cases/get_me.py`

**Cel:** use case "Pobierz mój profil" — najprostszy.

```python
"""GetMe use case for Applicant Service."""

from __future__ import annotations

from uuid import UUID

from src.application.dto import ApplicantResponse
from src.application.ports.repository import ApplicantRepository
from src.domain.exceptions import ApplicantNotFound


class GetMeUseCase:
    """Use case for getting current applicant profile."""

    def __init__(self, applicant_repo: ApplicantRepository) -> None:
        self._applicant_repo = applicant_repo

    async def execute(self, applicant_id: UUID) -> ApplicantResponse:
        """Execute get me."""
        applicant = await self._applicant_repo.get_by_id(applicant_id)
        if applicant is None:
            raise ApplicantNotFound(str(applicant_id))

        return ApplicantResponse(
            id=applicant.id,
            email=applicant.email,
            first_name=applicant.first_name,
            last_name=applicant.last_name,
            created_at=applicant.created_at,
        )
```

- **Konstruktor** przyjmuje tylko repozytorium klientów (nie potrzebuje hashera ani tokenów).
- **`execute(applicant_id: UUID)`** — argumentem jest `UUID` (wyciągnięty wcześniej z access tokena przez `get_current_applicant_id`). **Zauważ:** to jedyny use case, który przyjmuje bezpośrednio wartość, a nie DTO — bo "profil bieżącego użytkownika" nie ma ciała żądania.
- Jeśli klient nie istnieje → `ApplicantNotFound(str(applicant_id))`.
- Budujemy `ApplicantResponse` ręcznie (pole po polu). **Alternatywa:** `ApplicantResponse.model_validate(applicant)` z `from_attributes=True`. Ręczne budowanie jest jawniejsze, ale wymaga aktualizacji przy każdej zmianie pól.

**Czemu nie zwracamy `password_hash` w odpowiedzi?** Bo `ApplicantResponse` po prostu nie ma takiego pola — to naturalny sposób, by NIE wyciekać wrażliwych danych (przypadkowo hasz haseł mógłby zostać wykorzystany do ataku offline). Projektuj odpowiedzi tak, aby zawierały tylko to, co potrzebne (zasada "least privilege" na poziomie danych).

---

### 4.11 Plik: `src/infrastructure/persistence/models.py`

**Cel:** definiuje **modele ORM** (Object-Relational Mapping) SQLAlchemy. Modele mapują tabele bazy danych na klasy Pythona. To warstwa infrastruktury — celowo OSOBNA od encji domenowych.

```python
"""SQLAlchemy ORM models for Applicant Service."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import DateTime, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
```

- **`from sqlalchemy import DateTime, String, UniqueConstraint`** — typy kolumn SQL i narzędzie do definiowania ograniczeń unikalności.
- **`from sqlalchemy.dialects.postgresql import UUID as PG_UUID`** — specyficzny dla Postgresa typ UUID. Czyta się to jako `UUID` z dialektu postgresql (a nie z `uuid`), stąd alias `PG_UUID`, by nie kolidował z `UUID` z modułu `uuid`.
- **`from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column`** — nowoczesne (2.0) podejście do definiowania modeli. `DeclarativeBase` to baza klas; `Mapped[T]` to adnotacja typu kolumny; `mapped_column(...)` definiuje kolumnę.

#### Klasa `Base`

```python
class Base(DeclarativeBase):
    """Base class for all models."""
    pass
```

- `Base` to wspólna baza dla wszystkich modeli. Każda klasa dziedzicząca po `Base` jest rejestrowana w metadanych (`Base.metadata`), których Alembic używa do generowania migracji, a `create_all` — do tworzenia tabel.

#### Klasa `ApplicantModel`

```python
class ApplicantModel(Base):
    """Applicant table model."""

    __tablename__ = "applicants"

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid4
    )
```

- **`__tablename__ = "applicants"`** — nazwa tabeli w bazie.
- **`id: Mapped[UUID]`** — adnotacja: kolumna przechowuje `UUID`. `Mapped[T]` informuje SQLAlchemy o typie Pythona.
- **`mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)`**:
  - `PG_UUID(as_uuid=True)` — kolumna UUID w Postgresie; `as_uuid=True` oznacza: zwracaj obiekt `uuid.UUID` (a nie string) do Pythona.
  - `primary_key=True` — klucz główny.
  - `default=uuid4` — jeśli nie podano wartości, SQLAlchemy wygeneruje UUID. **Ważne:** tu SQLAlchemy sam generuje UUID przy INSERT (to `default` po stronie ORM/Pythona), chociaż w repozytorium przekazujemy `id` jawnie z encji.

```python
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
```

- **`String(255)`** — kolumna tekstowa o maksymalnej długości 255 (limit dla wygody i indeksów).
- **`unique=True`** — unikalna wartość; baza NIE pozwoli na dwa wiersze z tym samym e-mailem (druga linia obrony przed duplikatami, obok `exists_by_email`).
- **`nullable=False`** — kolumna wymagana (NOT NULL).
- **`index=True`** — utwórz indeks na tej kolumnie — przyspiesza zapytania `WHERE email = ?` (używane w logowaniu i rejestracji).
- **Razem `unique=True` + `index=True`** tworzy unikalny indeks — w Postgresie unikalność wymusza indeks, więc `index=True` jest redundantne (ale nieszkodliwe).

```python
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    first_name: Mapped[str] = mapped_column(String(100), nullable=False)
    last_name: Mapped[str] = mapped_column(String(100), nullable=False)
```

- `password_hash` — hash hasła (Argon2id). `String(255)` z zapasem (hash Argon2id ~97 znaków). **Kluczowe:** kolumna nazywa się `password_hash`, a nie `password` — bo przechowujemy HASH.
- `first_name`/`last_name` — `String(100)` (100 znaków wystarcza dla imienia/nazwiska, zgodnie z limitem Pydantic max_length=100).

```python
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
```

- **`DateTime(timezone=True)`** — kolumna daty/czasu ze strefą czasową (timestamptz w Postgresie). **`timezone=True`** zapewnia poprawne przechowywanie czasu UTC.
- **`default=lambda: datetime.now(timezone.utc)`** — wartość domyślna przy INSERT (po stronie Pythona).
- **`onupdate=lambda: datetime.now(timezone.utc)`** — przy każdej aktualizacji wiersza SQLAlchemy automatycznie ustawia nową wartość `updated_at`. To dzięki temu nie trzeba ręcznie wołać `update_timestamp()` z encji domenowej — SQLAlchemy zarządza tym "za kulisami".

#### Klasa `RefreshTokenModel`

```python
class RefreshTokenModel(Base):
    """Refresh token table model."""

    __tablename__ = "refresh_tokens"

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid4
    )
    applicant_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), nullable=False, index=True
    )
    token_hash: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    revoked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint("token_hash", name="uq_refresh_tokens_token_hash"),
    )
```

- **`id`** — klucz główny tokena (UUID).
- **`applicant_id`** — do jakiego klienta należy token. `index=True` → przyspiesza zapytania po kliencie (np. `revoke_all_for_applicant`). **Brak `ForeignKey`!** Szczegóły w 8.5.
- **`token_hash`** — hash refresh tokena. `unique=True` → dwa identyczne hashe nie mogą istnieć (teoretyczna ochrona przed kolizją duplikatów). `index=True` → szybkie wyszukiwanie po hashu (choć aktualny use case nie szuka po hashu bezpośrednio).
- **`__table_args__ = (UniqueConstraint("token_hash", name="uq_refresh_tokens_token_hash"),)`** — dodatkowe **nazwane** ograniczenie unikalności na `token_hash`. Dwa zapisy unikalności (raz w kolumnie `unique=True`, raz w `UniqueConstraint`) to duplikacja — w praktyce wystarczy jeden. `UniqueConstraint` z jawną nazwą jest przydatne przy migracjach (można je precyzyjnie odwołać), ale tutaj jest redundantne wobec `unique=True`.

**Różnica między encją domenową a modelem ORM:**
- Encja (`Applicant`) — czysta logika, brak zależności, testowalna offline.
- Model (`ApplicantModel`) — związany z SQLAlchemy i bazą.

Ta separacja jest zgodna z architekturą: warstwa domeny nie wie o SQLAlchemy. Repozytorium pośredniczy, mapując model ↔ encja (zob. `_to_entity`).

### 4.12 Plik: `src/infrastructure/persistence/repository.py`

**Cel:** implementuje porty repozytorium za pomocą SQLAlchemy (adaptery). To tu odbywa się mapowanie encja ↔ model oraz wykonywanie zapytań SQL.

```python
"""SQLAlchemy repository implementations for Applicant Service."""

from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from src.application.ports.repository import ApplicantRepository, RefreshTokenRepository
from src.domain.entities import Applicant, RefreshToken
from src.infrastructure.persistence.models import ApplicantModel, RefreshTokenModel
```

- **`from sqlalchemy import select`** — konstruktor wyrażenia SELECT (SQLAlchemy 2.0 style).
- **`from sqlalchemy.ext.asyncio import AsyncSession`** — asynchroniczna sesja bazy.
- **Adnotacja typu:** `SQLAlchemyApplicantRepository(ApplicantRepository)` — klasa dziedziczy po porcie i MUSI zaimplementować wszystkie jego abstrakcyjne metody, inaczej Python odmówi jej instancjonowania.

#### `SQLAlchemyApplicantRepository`

```python
class SQLAlchemyApplicantRepository(ApplicantRepository):
    """SQLAlchemy implementation of ApplicantRepository."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
```

- Konstruktor przyjmuje `AsyncSession` — jedno współdzielone połączenie-bazę-danych dla danego żądania. Repozytorium NIE tworzy sesji; dostaje ją z zewnątrz (wstrzykiwana przez `get_session` w `dependencies.py`). To realizuje wzorzec **Unit of Work** na poziomie sesji (sek. 6.4).

```python
    async def save(self, applicant: Applicant) -> None:
        model = ApplicantModel(
            id=applicant.id,
            email=applicant.email,
            password_hash=applicant.password_hash,
            first_name=applicant.first_name,
            last_name=applicant.last_name,
            created_at=applicant.created_at,
            updated_at=applicant.updated_at,
        )
        self._session.add(model)
        await self._session.flush()
```

- **Konstrukcja modelu** z pól encji (mapowanie encja → model). Wszystkie pola przepisane jawnie (łącznie z `id`, `created_at`, `updated_at` — bierzemy to, co wygenerowała encja).
- **`self._session.add(model)`** — dodaje obiekt do sesji (do "kolejki" zmian). **Synchroniczne** — tylko rejestruje obiekt w Unit of Work; nic nie leci do bazy.
- **`await self._session.flush()`** — **FLUSH**, nie `commit`! FLUSH wysyła do bazy zapytania INSERT (nadanie identyfikatorów, wyzwolenie constrtaintów), ale NIE kończy transakcji. Commit nastąpi później w `get_session()` (po zakończeniu całego żądania). Różnica — sekcja 8.3. (Dlaczego `await` — I/O.)

```python
    async def get_by_id(self, applicant_id: UUID) -> Optional[Applicant]:
        stmt = select(ApplicantModel).where(ApplicantModel.id == applicant_id)
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        if model is None:
            return None
        return self._to_entity(model)
```

- **`stmt = select(ApplicantModel).where(ApplicantModel.id == applicant_id)`** — budujemy wyrażenie. **WAŻNE:** `ApplicantModel.id == applicant_id` to NIE porównanie Pythona z `True`/`False` — SQLAlchemy nadpisuje operator `__eq__`, tworząc obiekt wyrażenia SQL (kolumna = wartość). To idiom ORM 2.0.
- **`await self._session.execute(stmt)`** — wykonanie zapytania (I/O). 
- **`result.scalar_one_or_none()`** — pobiera JEDNĄ wartość (pierwsza kolumna pierwszego wiersza) lub `None`, gdy wyniku nie ma; rzuca błąd, gdyby było więcej niż jeden wiersz (tu nie może być, bo `id` to klucz główny).
- Mapowanie model → encja.

```python
    async def get_by_email(self, email: str) -> Optional[Applicant]:
        stmt = select(ApplicantModel).where(ApplicantModel.email == email)
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        if model is None:
            return None
        return self._to_entity(model)
```

- Analogiczne do `get_by_id`, ale po kolumnie `email` (indeksowanej). Używane w logowaniu.

```python
    async def exists_by_email(self, email: str) -> bool:
        stmt = select(ApplicantModel.id).where(ApplicantModel.email == email)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None
```

- **`select(ApplicantModel.id)`** — wybieramy TYLKO kolumnę `id` (nie cały wiersz) — mniej danych pobranych z bazy.
- **`result.scalar_one_or_none() is not None`** — jeśli jest `id` → `True` (istnieje), jeśli `None` → `False`. 

```python
    def _to_entity(self, model: ApplicantModel) -> Applicant:
        return Applicant(
            id=model.id,
            email=model.email,
            password_hash=model.password_hash,
            first_name=model.first_name,
            last_name=model.last_name,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )
```

- Mapowanie model → encja. Metoda prywatna (`_`), pomocnicza.

#### `SQLAlchemyRefreshTokenRepository`

```python
class SQLAlchemyRefreshTokenRepository(RefreshTokenRepository):
    """SQLAlchemy implementation of RefreshTokenRepository."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, token: RefreshToken) -> None:
        model = RefreshTokenModel(
            id=token.id,
            applicant_id=token.applicant_id,
            token_hash=token.token_hash,
            expires_at=token.expires_at,
            created_at=token.created_at,
            revoked_at=token.revoked_at,
        )
        self._session.add(model)
        await self._session.flush()
```

- Analogiczny `save` jak dla klientów: mapa encja→model, `add`, `flush`.

```python
    async def update(self, token: RefreshToken) -> None:
        model = await self._session.get(RefreshTokenModel, token.id)
        if model is None:
            raise ValueError(f"Refresh token with id {token.id} not found")
        model.token_hash = token.token_hash
        model.expires_at = token.expires_at
        model.revoked_at = token.revoked_at
        await self._session.flush()
```

- **`await self._session.get(RefreshTokenModel, token.id)`** — pobranie obiektu po kluczu głównym (wykorzystuje pamięć podręczną sesji — identity map). 
- **`if model is None: raise ValueError(...)`** — jeśli nie znaleziono, rzuca `ValueError`. **Uwaga:** to NIE jest wyjątek domenowy — jeśli to przejdzie do FastAPI bez obsługi, da 500. W tym przepływie nie powinno się zdarzyć (bo `update` wołany jest na encji, która właśnie przyszła z bazy), ale obrona jest sensowna.
- Zmieniamy pola modelu (np. `revoked_at`) — to tak zwane "dirty tracking": SQLAlchemy sam wykryje zmiany.
- **`await self._session.flush()`** — wysłanie UPDATE do bazy.

```python
    async def get_by_hash(self, token_hash: str) -> Optional[RefreshToken]:
        stmt = select(RefreshTokenModel).where(RefreshTokenModel.token_hash == token_hash)
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        if model is None:
            return None
        return self._to_entity(model)
```

- Pobranie po hashu. Obecnie nieużywane w use case'ach.

```python
    async def revoke_all_for_applicant(self, applicant_id: UUID) -> None:
        stmt = select(RefreshTokenModel).where(
            RefreshTokenModel.applicant_id == applicant_id,
            RefreshTokenModel.revoked_at.is_(None),
        )
        result = await self._session.execute(stmt)
        for model in result.scalars().all():
            model.revoked_at = datetime.now()
```

- **`RefreshTokenModel.revoked_at.is_(None)`** — SQLAlchemy to `IS NULL`. (Nie piszemy `== None`, bo `==` dałby `= NULL`, co w SQL nic nie zwraca.)
- Pobieramy wszystkie nieunieważnione tokeny danego klienta i ustawiamy `revoked_at`. Zmiany trafią do bazy przy flush/commit.

```python
    async def get_all_valid(self, now: datetime) -> list[RefreshToken]:
        stmt = select(RefreshTokenModel).where(
            RefreshTokenModel.expires_at > now,
            RefreshTokenModel.revoked_at.is_(None),
        )
        result = await self._session.execute(stmt)
        return [self._to_entity(model) for model in result.scalars().all()]
```

- **`select(RefreshTokenModel).where(expires_at > now, revoked_at IS NULL)`** — pobiera wszystkie nieunieważnione i niewygasłe tokeny (WSZYSTKICH klientów — patrz uwaga w 4.9).
- **`result.scalars().all()`** — lista wierszy; `scalars()` wybiera pojedyncze obiekty (po pierwszej kolumnie).
- **`[self._to_entity(model) for model in ...]`** — **list comprehension** (składnia listy): dla każdego modelu z listy wyników tworzy encję i zbiera do listy.

**Analiza wzorca Repository tutaj:** każda metoda ukrywa szczegóły SQL. Use case woła `single obj`-poziomowe metody, nie wiedząc o `select(...)` czy `scalar_one_or_none()`. To czysta implementacja wzorca.

### 4.13 Plik: `src/infrastructure/security/argon2_hasher.py`

**Cel:** adapter hashowania haseł oparty o **Argon2id** (biblioteka `argon2-cffi`).

```python
"""Argon2id password hasher implementation."""

from __future__ import annotations

import argon2
from argon2 import PasswordHasher as Argon2PasswordHasher
from src.application.ports.password_hasher import PasswordHasher as PasswordHasherPort
```

- **`import argon2`** — moduł główny biblioteki (potrzebny do dostępu do `argon2.exceptions`).
- **`from argon2 import PasswordHasher as Argon2PasswordHasher`** — importujemy klasę `PasswordHasher` z argon2 i **aliastujemy** ją na `Argon2PasswordHasher` (żeby nie kolidować z naszym portem `PasswordHasher` poniżej).
- **`from src.application.ports.password_hasher import PasswordHasher as PasswordHasherPort`** — port, który implementujemy (alias, by rozróżnić).

```python
class Argon2Hasher(PasswordHasherPort):
    """Argon2id implementation of PasswordHasher."""

    def __init__(self) -> None:
        self._hasher = Argon2PasswordHasher(
            time_cost=3,
            memory_cost=65536,
            parallelism=4,
            hash_len=32,
            salt_len=16,
        )
```

- Klasa dziedziczy po porcie → musi zaimplementować `hash` i `verify`.
- **Konfiguracja Argon2:**
  - `time_cost=3` — liczba iteracji (przebiegów) algorytmu. Wyższe = wolniejsze i bezpieczniejsze, ale droższe CPU.
  - `memory_cost=65536` — zużycie pamięci w **kiB** (kibibajtach) = 64 MiB. Stąd nazwa "memory-hard" — algorytm wymaga dużo pamięci RAM, co utrudnia ataki na GPU/ASIC.
  - `parallelism=4` — liczba równoległych wątków.
  - `hash_len=32` — długość wyjściowego hash (32 bajty).
  - `salt_len=16` — długość losowej soli (16 bajtów).
  - Razem: kompromis między bezpieczeństwem a wydajnością, zgodny z rekomendacjami OWASP dla Argon2id.

```python
    def hash(self, password: str) -> str:
        return self._hasher.hash(password)
```

- Deleguje do `argon2.PasswordHasher.hash`. **Wynik** to **samoopisujący się ciąg** np.:
  ```
  $argon2id$v=19$m=65536,t=3,p=4$<sól base64>$<hash base64>
  ```
  Zawiera: algorytm (`argon2id`), wersję (`v=19`), parametry (`m`, `t`, `p`), sól i właściwy hash. Dzięki temu samo "pudełko" niesie całą informację potrzebną do weryfikacji — nie trzeba osobno przechowywać soli ani parametrów (sekcja 7.1).

```python
    def verify(self, password: str, password_hash: str) -> bool:
        try:
            self._hasher.verify(password_hash, password)
            return True
        except argon2.exceptions.VerifyMismatchError:
            return False
        except argon2.exceptions.HashingError:
            return False
```

- **`self._hasher.verify(password_hash, password)`** — UWAGA na kolejność argumentów: najpierw hash (przechowywany), potem hasło (podane). Przypadkowa zamiana = zawsze `False`.
- **`try/except`** — jeśli hash nie pasuje, argon2 rzuca `VerifyMismatchError`; inne błędy (np. zła wersja hasha) rzucają `HashingError`. Obie zamieniamy na `False`.
- **Czy wyjątek powinien być ukrywany?** Tak — weryfikacja ma zwracać bool, a nie eksplodować. Dasz atakującemu mniej informacji o wewnętrznych błędach formatu hasha. (Tradycyjna dobra praktyka.)

**Dlaczego Argon2id, a nie bcrypt?** Tabela porównawcza w sekcji 7.1. Skrót: Argon2id wygrywa w konkursie Password Hashing Competition (2015), jest odporny na ataki GPU (memory-hard), posiada konfigurowalną pamięć i równoległość, a wariant `id` łączy odporność na ataki side-channel (wariant `i`) i GPU (wariant `d`), czyli jest **hybrydą** skrojoną pod typowe scenariusze haseł.

### 4.14 Plik: `src/infrastructure/security/jwt_service.py`

**Cel:** adapter obsługujący JWT z podpisem **RS256** (asymetryczny RSA). Biblioteka: `python-jose`.

```python
"""RS256 JWT token service implementation."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

from jose import jwt
from jose.exceptions import JWTError
from src.application.ports.token_service import TokenService
```

- **`import os`** — dostęp do zmiennych środowiskowych (`os.getenv`).
- **`from pathlib import Path`** — nowoczesna klasa ścieżek (zamiast stringów); `Path` oferuje czytelne operacje (`/` skleja ścieżki, `read_bytes()` czyta plik).
- **`from jose import jwt`** — funkcje `encode`/`decode` z python-jose.
- **`from jose.exceptions import JWTError`** — klasa wyjątków JWT.

```python
class JWTService(TokenService):
    """RS256 implementation of TokenService."""

    ALGORITHM = "RS256"
    ACCESS_TOKEN_EXPIRE_MINUTES = 15
    REFRESH_TOKEN_EXPIRE_DAYS = 7
```

- **Stałe klasy** (upper-case): algorytm `RS256`, 15 minut dla access tokena, 7 dni dla refresh tokena.

```python
    def __init__(
        self,
        private_key_path: str | None = None,
        public_key_path: str | None = None,
    ) -> None:
        keys_dir = Path(os.getenv("KEYS_DIR", "/app/keys"))
        private_key_path = private_key_path or str(keys_dir / "private_key.pem")
        public_key_path = public_key_path or str(keys_dir / "public_key.pem")
        self._private_key = Path(private_key_path).read_bytes()
        self._public_key = Path(public_key_path).read_bytes()
```

- **`str | None = None`** — parametr opcjonalny (Python 3.10+ składnia union; mimo `requires-python >=3.9` — patrz uwaga o `from __future__ import annotations`, które pozwala tak pisać także na 3.9).
- **`Path(os.getenv("KEYS_DIR", "/app/keys"))`** — katalog kluczy; domyślnie `/app/keys` (w Dockerze); można nadpisać przez `KEYS_DIR`.
- **`private_key_path or str(keys_dir / "private_key.pem")`** — jeśli ścieżka nie została podana jako argument, użyjemy domyślnej (`<KEYS_DIR>/private_key.pem`). Operator `or` zwraca pierwsze wartościowe (truthy) wyrażenie.
- **`.read_bytes()`** — wczytuje klucze z plików do bajtów (python-jose przyjmuje bajty lub PEM string). **Uwaga na bezpieczeństwo plików** — patrz 7.3.

```python
    def create_access_token(self, applicant_id: UUID, email: str) -> str:
        now = datetime.now(timezone.utc)
        expire = now + timedelta(minutes=self.ACCESS_TOKEN_EXPIRE_MINUTES)
        payload = {
            "sub": str(applicant_id),
            "email": email,
            "type": "access",
            "iat": int(now.timestamp()),
            "exp": int(expire.timestamp()),
        }
        return jwt.encode(payload, self._private_key, algorithm=self.ALGORITHM)
```

- **`now`** — bieżący czas UTC.
- **`expire = now + timedelta(minutes=15)`** — moment wygaśnięcia.
- **Payload (claims)**:
  - `"sub"` (subject) — identyfikator podmiotu; tu string UUID klienta.
  - `"email"` — adres e-mail (do szybkiego odczytu bez sięgania do bazy).
  - `"type": "access"` — typ tokena (odróżnia access od refresh).
  - `"iat"` (issued-at) — czas wystawienia jako **timestamp Unix** (int sekundy).
  - `"exp"` (expiry) — czas wygaśnięcia jako timestamp.
- **`jwt.encode(payload, self._private_key, algorithm="RS256")`** — podpisanie PRYWATNYM kluczem. Wynik: JWT (trzy części base64url połączone kropkami): `header.payload.signature`.

```python
    def create_refresh_token(self, applicant_id: UUID) -> str:
        now = datetime.now(timezone.utc)
        expire = now + timedelta(days=self.REFRESH_TOKEN_EXPIRE_DAYS)
        payload = {
            "sub": str(applicant_id),
            "type": "refresh",
            "iat": int(now.timestamp()),
            "exp": int(expire.timestamp()),
        }
        return jwt.encode(payload, self._private_key, algorithm=self.ALGORITHM)
```

- Podobny do access tokena, ale: `type: "refresh"`, czas życia 7 dni, **bez `email`** (refresh token nie potrzebuje e-maila).

```python
    def decode_access_token(self, token: str) -> tuple[UUID, str]:
        try:
            payload = jwt.decode(token, self._public_key, algorithms=[self.ALGORITHM])
            if payload.get("type") != "access":
                raise JWTError("Invalid token type")
            applicant_id = UUID(payload["sub"])
            email = payload["email"]
            return applicant_id, email
        except (JWTError, KeyError, ValueError) as e:
            raise JWTError(f"Invalid access token: {e}") from e
```

- **`jwt.decode(token, self._public_key, algorithms=["RS256"])`** — weryfikacja podpisu PRYWATNYM? Nie — **publicznym** kluczem. `decode` również sprawdza `exp` (wygaśnięcie) i podpis. `algorithms=[...]` wymusza białą listę algorytmów (ważne — zapobiega atakom algorytm confusion, np. podanie `HS256`).
- **`if payload.get("type") != "access": raise JWTError(...)`** — upewniamy się, że to NAPRAWDĘ access token, a nie refresh (jeden stos kluczy = musimy rozróżniać po `type`).
- **`UUID(payload["sub"])`** — konwersja string → UUID; może rzucić `ValueError` przy niepoprawnym formacie.
- **`except (JWTError, KeyError, ValueError) as e`** — łapiemy szeroki zestaw błędów i **opakowujemy** w `JWTError` z ujednoliconym komunikatem `"Invalid access token: ..."`, zachowując oryginalny wyjątek w łańcuchu przez **`from e`**. Powód: nie chcemy wyciekać wewnętrznych szczegółów do respondenta.

```python
    def get_access_token_expiry(self) -> datetime:
        return datetime.now(timezone.utc) + timedelta(minutes=self.ACCESS_TOKEN_EXPIRE_MINUTES)

    def get_refresh_token_expiry(self) -> datetime:
        return datetime.now(timezone.utc) + timedelta(days=self.REFRESH_TOKEN_EXPIRE_DAYS)
```

- Zwracają moment wygaśnięcia "od teraz". Używane do wyliczenia `expires_in` i `expires_at` encji `RefreshToken`.

**Czemu `exp` jako int, a nie datetime?** Standard JWT wymaga `exp` jako **NumericDate** (liczba sekund od 1970-01-01, Unix timestamp). `int(now.timestamp())` daje taką liczbę. `datetime` nie byłoby rozumiane przez dekoder.

---

### 4.15 Plik: `src/infrastructure/security/keygen.py`

**Cel:** narzędzie CLI do wygenerowania pary kluczy RSA (RS256). Uruchamiane ręcznie (raz) przed startem serwisu. Biblioteka: `cryptography`.

```python
#!/usr/bin/env python3
"""Generate RSA key pair for RS256 JWT signing.

Run once to generate keys:
    python -m src.infrastructure.security.keygen

Keys are saved to KEYS_DIR (default: ./keys/):
- private_key.pem (Applicant Service only)
- public_key.pem (shared with Gateway via volume)
"""

from __future__ import annotations

import os
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
```

- **`#!/usr/bin/env python3`** — shebang: wskazuje interpreter przy uruchamianiu jako skrypt.
- **`from cryptography.hazmat.primitives import serialization`** — kodowanie/odczytywanie kluczy (serializacja).
- **`from cryptography.hazmat.primitives.asymmetric import rsa`** — generatory kluczy RSA.

```python
KEYS_DIR = Path(os.getenv("KEYS_DIR", "./keys"))
PRIVATE_KEY_PATH = KEYS_DIR / "private_key.pem"
PUBLIC_KEY_PATH = KEYS_DIR / "public_key.pem"
```

- Ścieżki kluczy. `KEYS_DIR` domyślnie `./keys` (lokalnie), `Path / "..."` skleja ścieżki.

```python
def generate_keys() -> None:
    """Generate RSA 2048-bit key pair."""
    KEYS_DIR.mkdir(parents=True, exist_ok=True)

    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
```

- **`KEYS_DIR.mkdir(parents=True, exist_ok=True)`** — utwórz katalog (rekurencyjnie `parents=True`; nie błąd, jeśli istnieje `exist_ok=True`).
- **`rsa.generate_private_key(public_exponent=65537, key_size=2048)`** — generowanie klucza RSA:
  - `public_exponent=65537` — standardowy, bezpiczy wykładnik publiczny (Fermat prime).
  - `key_size=2048` — 2048 bitów (obecny standard minimum dla RSA). Więcej bitów (4096) = bezpieczniej, ale wolniej; 2048 jest uznawane za bezpieczne przez NIST na dziś.

```python
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
```

- **`private_bytes(...)`** — serializacja klucza prywatnego do **PEM** (format zaczynający się od `-----BEGIN PRIVATE KEY-----`).
  - `PrivateFormat.PKCS8` — PKCS#8, nowoczesny, elastyczny standard (w przeciwieństwie do starszego PKCS#1 `-----BEGIN RSA PRIVATE KEY-----`).
  - **`NoEncryption()`** — klucz NIE jest chroniony hasłem. **Uwaga bezpieczeństwa:** serwis musi czytać klucz bez podawania hasła przy każdym starcie, dlatego klucz jest bez szyfrowania — zabezpieczeniem są **uprawnienia pliku** (0600), a nie hasło.

```python
    public_key = private_key.public_key()
    public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    PRIVATE_KEY_PATH.write_bytes(private_pem)
    PUBLIC_KEY_PATH.write_bytes(public_pem)

    # Secure permissions
    PRIVATE_KEY_PATH.chmod(0o600)
    PUBLIC_KEY_PATH.chmod(0o644)

    print("Generated keys:")
    print(f"  Private: {PRIVATE_KEY_PATH}")
    print(f"  Public:  {PUBLIC_KEY_PATH}")
```

- **`private_key.public_key()`** — z klucza prywatnego wyprowadzamy klucz publiczny (RSA matematycznie na to pozwala).
- **`public_bytes(..., SubjectPublicKeyInfo)`** — serializacja klucza publicznego do PEM (`-----BEGIN PUBLIC KEY-----`).
- **`write_bytes`** — zapis plików.
- **`chmod(0o600)`** — klucz prywatny: tylko właściciel może czytać/pisać (uprawnienia 600). **`chmod(0o644)`** — klucz publiczny: czytelny przez wszystkich (644). To kluczowe dla bezpieczeństwa (patrz 7.3).
- **`print(...)`** — informacja o wygenerowanych plikach.

```python
if __name__ == "__main__":
    generate_keys()
```

- **`if __name__ == "__main__":`** — idiom Pythona. Wykonuje `generate_keys()` TYLKO, gdy plik jest uruchamiany bezpośrednio (`python -m src...keygen` lub `python keygen.py`), a NIE gdy jest importowany jako moduł.
- **Dlaczego to ważne?** Gdyby ten plik był zaimportowany w innym miejscu, nie chcielibyśmy przypadkowo generować kluczy. To standardowa brama dla skryptów CLI.

**Jak to uruchomić?** `python -m src.infrastructure.security.keygen` z katalogu `services/applicant`.

### 4.16 Plik: `src/infrastructure/database.py`

**Cel:** zarządzanie silnikiem (engine) bazy, fabryką sesji oraz cyklem życia sesji dla każdego żądania. Realizuje wzorzec **Unit of Work** i wstrzykiwanie transakcji.

```python
"""Database session management for Applicant Service."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
```

- **`from collections.abc import AsyncGenerator`** — typ dla funkcji generujących asynchronicznie (używany w `get_session`).
- **`create_async_engine`** — tworzy silnik asynchroniczny.
- **`async_sessionmaker`** — fabryka sesji.
- **`NullPool`** — pula połączeń, w której KAŻDE połączenie jest zamykane po użyciu (brak współdzielenia). Używane w MVP/serwisach bez stanu.

#### Klasa `Database`

```python
class Database:
    def __init__(self, url: str) -> None:
        self._engine = create_async_engine(url, poolclass=NullPool)
        self._session_factory = async_sessionmaker(
            self._engine, class_=AsyncSession, expire_on_commit=False
        )

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        return self._session_factory
```

- **`create_async_engine(url, poolclass=NullPool)`** — silnik łączący się z bazą przy użyciu adresu URL (np. `postgresql+asyncpg://...`). `NullPool` — każde połączenie żyje krótko (bez puli), co w mikroserwisach bezstanowych jest rozsądne i upraszcza zarządzanie.
- **`async_sessionmaker(self._engine, class_=AsyncSession, expire_on_commit=False)`** — fabryka tworząca sesje. **`expire_on_commit=False`** — po `commit` nie chcesz, żeby atrybuty obiektów zostały "wygaszone" (co wymuszałoby ponowne zapytania). Komentarz: to częsty wybór dla wydajności i wygody.
- **`@property session_factory`** — wystawia fabrykę sesji jako tylko-do-odczytu.

```python
    async def create_all(self) -> None:
        from src.infrastructure.persistence.models import Base
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def close(self) -> None:
        await self._engine.dispose()
```

- **`create_all()`** — tworzy wszystkie tabele na podstawie metadanych modeli. Używane w `init_database` (wygodne w MVP, ale do produkcji — migracje Alembic).
- **`from ... import Base`** — lokalny import wewnątrz funkcji (unikanie cyklicznych importów na starcie).
- **`async with self._engine.begin() as conn`** — otwiera połączenie i transakcję.
- **`conn.run_sync(Base.metadata.create_all)`** — `run_sync` uruchamia funkcję **synchroniczną** (create_all jest synchroniczna) w kontekście połączenia asynchronicznego.
- **`close()` → `await self._engine.dispose()`** — sprzątanie połączeń przy zamknięciu aplikacji.

#### Wzorzec Singletona i Lazy init

```python
_db: Optional[Database] = None


def get_database() -> Database:
    assert _db is not None, "Database not initialized"
    return _db
```

- **Zmienna modułowa `_db: Optional[Database] = None`** — przechowuje pojedynczą instancję bazodanową (quasi-**Singleton**).
- **`get_database()`** — zwraca instancję lub rzuca `AssertionError`, jeśli nie została zainicjalizowana. To prosta forma **Lazy initialization + Singleton** (instancja tworzona dokładnie raz w `init_database`).

```python
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for database session."""
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

- **`async def get_session()`** — funkcja-generator asynchroniczny (ma `yield`).
- **`async with db.session_factory() as session`** — otwiera sesję (kontekst).
- **`yield session`** — **oddaje sesję konsumentowi** (np. repozytorium). `Depends(get_session)` wstrzyknie wartość z `yield` do endpointu. Kod po `yield` wykona się PO zakończeniu tego requestu (to jest ich "epilog").
- **`await session.commit()`** — **COMMIT transakcji** po pomyślnym zakończeniu całego żądania (to tutaj podejmujemy ostateczną decyzję o zapisie — patrz różnica flush vs commit w 8.3).
- **`except Exception: await session.rollback(); raise`** — jeśli cokolwiek w żądaniu wyrzuci wyjątek, cofamy transakcję (`rollback`) i **ponownie rzucamy** wyjątek dalej (`raise` bez argumentu zachowuje oryginał), aby FastAPI mogło go obsłużyć.
- **`finally: await session.close()`** — bezwarunkowe zamknięcie sesji (zwolnienie połączenia).

**To jest serce transakcyjności requestu:** cały endpoint + use case + repozytoria żyją w JEDNEJ sesji/transakcji. Jeśli coś pójdzie nie tak — wszystko jest cofnięte (spójność, patrz 8.4).

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

- **`global _db`** — informuje Pythona, że modyfikujemy zmienną globalną modułu (inaczej Python potraktowałby `_db` jako lokalną). Używane w `init_database` i `close_database`.
- `init_database` tworzy instancję `Database` i tworzy tabele; `close_database` zamyka silnik i czyści referencję.

### 4.17 Plik: `src/api/dependencies.py`

**Cel:** **fabryki zależności (Dependency Injection)** dla FastAPI — łączą infrastrukturę z use case'ami. To "kompozycyjny korzeń" aplikacji.

```python
"""FastAPI dependencies for Applicant Service."""

from __future__ import annotations

from typing import Optional
from uuid import UUID

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from src.application.ports.token_service import TokenService
from src.application.use_cases.get_me import GetMeUseCase
from src.application.use_cases.login import LoginUseCase
from src.application.use_cases.refresh_token import RefreshTokenUseCase
from src.application.use_cases.register import RegisterUseCase
from src.infrastructure.database import get_session
from src.infrastructure.persistence.repository import (
    SQLAlchemyApplicantRepository,
    SQLAlchemyRefreshTokenRepository,
)
from src.infrastructure.security.argon2_hasher import Argon2Hasher
from src.infrastructure.security.jwt_service import JWTService
```

- **`Depends`** — funkcja FastAPI/Starlette do deklarowania zależności w sygnaturze endpointu.
- **`Header`** — ekstraktor nagłówka HTTP.
- **`HTTPException`, `status`** — budowa odpowiedzi błędów HTTP z poprawnymi kodami.
- **Importy z `infrastructure`** — to jedyna warstwa, gdzie "wiemy", która implementacja idzie w które miejsce.

#### Lazy-Singleton dla ciężkich obiektów

```python
_token_service: Optional[TokenService] = None
_argon2_hasher: Optional[Argon2Hasher] = None


def get_token_service() -> TokenService:
    global _token_service
    if _token_service is None:
        _token_service = JWTService()
    return _token_service


def get_password_hasher() -> Argon2Hasher:
    global _argon2_hasher
    if _argon2_hasher is None:
        _argon2_hasher = Argon2Hasher()
    return _argon2_hasher
```

- `JWTService` (czyta klucze z dysku) i `Argon2Hasher` (tworzy obiekt z parametrami) są ciężkie — tworzymy je **raz** (Singleton), przy pierwszym użyciu (Lazy).
- **`global`** — modyfikacja zmiennej modułowej.
- **`if _token_service is None: ...`** — sprawdzenie, czy już utworzono; jeśli nie — utwórz i zapisz.

#### Fabryki repozytoriów (session przekazywana przez Depends)

```python
def get_applicant_repo(
    session: AsyncSession = Depends(get_session),
) -> SQLAlchemyApplicantRepository:
    return SQLAlchemyApplicantRepository(session)


def get_refresh_token_repo(
    session: AsyncSession = Depends(get_session),
) -> SQLAlchemyRefreshTokenRepository:
    return SQLAlchemyRefreshTokenRepository(session)
```

- **`session: AsyncSession = Depends(get_session)`** — ta funkcja **wymaga** sesji; FastAPI rozwiąże `get_session` (otwierając sesję bazodanową) i wstrzyknie ją.
- Kolejność rozwiązywania: `get_session` jest uruchamiane najpierw, potem `get_applicant_repo` buduje repozytorium na tej sesji.
- **Kaskada Depends** — zobaczę to przy ścieżce wywołania (sekcja 5).

#### Fabryki use case'ów

```python
def get_register_use_case(
    applicant_repo: SQLAlchemyApplicantRepository = Depends(get_applicant_repo),
    refresh_token_repo: SQLAlchemyRefreshTokenRepository = Depends(get_refresh_token_repo),
    password_hasher: Argon2Hasher = Depends(get_password_hasher),
    token_service: TokenService = Depends(get_token_service),
) -> RegisterUseCase:
    return RegisterUseCase(applicant_repo, refresh_token_repo, password_hasher, token_service)


def get_login_use_case(...) -> LoginUseCase:
    return LoginUseCase(applicant_repo, refresh_token_repo, password_hasher, token_service)


def get_refresh_token_use_case(...) -> RefreshTokenUseCase:
    return RefreshTokenUseCase(applicant_repo, refresh_token_repo, password_hasher, token_service)


def get_get_me_use_case(
    applicant_repo: SQLAlchemyApplicantRepository = Depends(get_applicant_repo),
) -> GetMeUseCase:
    return GetMeUseCase(applicant_repo)
```

- Każda fabryka deklaruje swoje zależności przez `Depends(...)`; FastAPI rozstrzyga je **rekurencyjnie** (kolejność: `get_session` → repo → use case).
- **To jest kompozycyjny korzeń:** tu "sklejamy" porty z adapterami. Use case dostaje `SQLAlchemyApplicantRepository` (implementacja), mimo że w swojej sygnaturze oczekuje `ApplicantRepository` (interfejsu) — to działa, bo klasa implementuje interfejs.

#### Autoryzacja: `get_current_applicant_id`

```python
async def get_current_applicant_id(
    authorization: Optional[str] = Header(None, alias="Authorization"),
    token_service: TokenService = Depends(get_token_service),
) -> UUID:
    """Extract and validate applicant ID from Bearer token."""
    if authorization is None or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid Authorization header",
        )

    token = authorization[7:]  # Remove "Bearer "
    try:
        applicant_id, _ = token_service.decode_access_token(token)
        return applicant_id
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token: {e}",
        )
```

- **`authorization: Optional[str] = Header(None, alias="Authorization")`** — wyciąga nagłówek `Authorization`; `None` jeśli go brak.
- **`authorization.startswith("Bearer ")`** — sprawdzenie schematu (Bearer). 
- **`raise HTTPException(401, "Missing or invalid Authorization header")`** — brak/niepoprawny nagłówek → 401 (Unauthorized).
- **`authorization[7:]`** — wycina `"Bearer "` (7 znaków), zostawiając sam token.
- **`applicant_id, _ = token_service.decode_access_token(token)`** — **rozpakowanie krotki** `(applicant_id, email)`: pierwsza wartość do `applicant_id`, druga do `_` (zmienna "śmieciowa"). Weryfikacja podpisu, wygaśnięcia i typu — wszystko wewnątrz `decode_access_token`.
- **`except Exception`** — każde `Invalid token: <msg>` → 401. (Szeroki `Exception` — prostota; w produkcji lepiej łapać konkretne typy.)
- Zwraca `UUID` — identyfikator zalogowanego klienta, używany przez `GetMeUseCase`.

**Dlaczego autoryzacja jest tu, w dependencies, a nie w use case?** To sprawa transportowa (HTTP) — wyciągnięcie tożsamości z nagłówka. Use case `GetMe` przyjmuje już gotowy `applicant_id`. To zgodne z architekturą: warstwa API zajmuje się HTTP, aplikacja — logiką.

### 4.18 Plik: `src/api/routes/auth.py`

**Cel:** definicja endpointów auth (register, login, refresh) jako **APIRouter**.

```python
"""Auth routes for Applicant Service."""

from __future__ import annotations

from fastapi import APIRouter, Depends, status
from fastapi.routing import APIRouter as APIRouterType
from src.api.dependencies import (
    get_login_use_case,
    get_refresh_token_use_case,
    get_register_use_case,
)
from src.application.dto import LoginRequest, RefreshTokenRequest, RegisterRequest, TokenResponse
from src.application.use_cases.login import LoginUseCase
from src.application.use_cases.refresh_token import RefreshTokenUseCase
from src.application.use_cases.register import RegisterUseCase

router: APIRouterType = APIRouter(prefix="/auth", tags=["auth"])
```

- **`from fastapi.routing import APIRouter as APIRouterType`** — import klasy `APIRouter` z `fastapi.routing` i alias na `APIRouterType` (żeby móc użyć jako adnotacji typu zmiennej, nie kolidując z wartością `router`).
- **`router: APIRouterType = APIRouter(prefix="/auth", tags=["auth"])`** — tworzymy router z prefiksem `/auth` i tagiem `auth` (tagi pomagają w dokumentacji Swagger/OpenAPI).
- **`tags=["auth"]`** — grupowanie w docs.

#### Endpoint `register`

```python
@router.post(
    "/register",
    response_model=TokenResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new applicant",
)
async def register(
    request: RegisterRequest,
    use_case: RegisterUseCase = Depends(get_register_use_case),
) -> TokenResponse:
    """Register a new applicant and return access + refresh tokens."""
    return await use_case.execute(request)
```

- **Dekorator `@router.post("/register", ...)`**:
  - `/register` — ścieżka względna; po połączeniu z prefiksem `/auth` i globalnym `/api/v1` da pełną ścieżkę `/api/v1/auth/register`.
  - `response_model=TokenResponse` — FastAPI serializuje wynik do `TokenResponse` (i dokumentuje w OpenAPI).
  - `status_code=201` — CREATED, bo rejestracja tworzy zasób.
  - `summary` — opis w docs.
- **`request: RegisterRequest`** — Pydantic automatycznie waliduje body JSON zgodnie z `RegisterRequest`.
- **`use_case: RegisterUseCase = Depends(get_register_use_case)`** — wstrzyknięcie use case'a (uruchomi całą kaskadę zależności).
- **`await use_case.execute(request)`** — wykonanie.

#### Endpoint `login`

```python
@router.post(
    "/login",
    response_model=TokenResponse,
    summary="Login and get access + refresh tokens",
)
async def login(
    request: LoginRequest,
    use_case: LoginUseCase = Depends(get_login_use_case),
) -> TokenResponse:
    """Login with email and password, return access + refresh tokens."""
    return await use_case.execute(request)
```

- Analogiczny do register, ale domyślny status 200.

#### Endpoint `refresh`

```python
@router.post(
    "/refresh",
    response_model=TokenResponse,
    summary="Refresh access token using refresh token",
)
async def refresh_token(
    request: RefreshTokenRequest,
    use_case: RefreshTokenUseCase = Depends(get_refresh_token_use_case),
) -> TokenResponse:
    """Rotate refresh token and return new access + refresh tokens."""
    return await use_case.execute(request)
```

- Odświeżanie tokena przyjmuje `RefreshTokenRequest` (zawierający `refresh_token`).

**Ważna obserwacja (słaby punkt):** ani ten router, ani żaden handler globalny NIE mapuje wyjątków domenowych (`EmailAlreadyRegistered`, `InvalidCredentials`, itd.) na kody HTTP. Wystąpienie takiego wyjątku w use case **przebije się** aż do domyślnego handlera FastAPI i zakończy się `500 Internal Server Error` zamiast czytelnego `409`, `401` itd. To obszar do poprawy (patrz FAQ "jak dodać handler wyjątków").

### 4.19 Plik: `src/api/routes/me.py`

**Cel:** endpoint `/me` — profil zalogowanego użytkownika.

```python
"""Me routes for Applicant Service."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends
from fastapi.routing import APIRouter as APIRouterType
from src.api.dependencies import get_current_applicant_id, get_get_me_use_case
from src.application.dto import ApplicantResponse
from src.application.use_cases.get_me import GetMeUseCase

router: APIRouterType = APIRouter(prefix="/me", tags=["me"])


@router.get(
    "",
    response_model=ApplicantResponse,
    summary="Get current applicant profile",
)
async def get_me(
    applicant_id: UUID = Depends(get_current_applicant_id),
    use_case: GetMeUseCase = Depends(get_get_me_use_case),
) -> ApplicantResponse:
    """Get current authenticated applicant's profile."""
    return await use_case.execute(applicant_id)
```

- **`@router.get("", ...)`** — ścieżka pusta `""` — po połączeniu z prefiksem `/me` daje `/me`. (Dlaczego nie `"/"`? Zależało, by końcowa ścieżka była dokładnie `/api/v1/me` bez slash'u.)
- **`applicant_id: UUID = Depends(get_current_applicant_id)`** — **przed** wywołaniem use case'a rozwiązana jest autoryzacja: weryfikacja Bearer tokena i wyciągnięcie `applicant_id`. Jeśli token zły/brak → 401.
- **`use_case: GetMeUseCase = Depends(get_get_me_use_case)`** — wstrzyknięcie use case'a (i przez to sesji DB).
- **`return await use_case.execute(applicant_id)`** — profil bieżącego użytkownika.

---

### 4.20 Plik: `src/main.py`

**Cel:** punkt wejścia aplikacji FastAPI. Tworzy aplikację, konfiguruje logging, zarządza cyklem życia (lifespan), rejestruje routery i healthchecki.

```python
"""FastAPI application for Applicant Service."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from crediguard_observability import configure_logging, get_logger
from fastapi import FastAPI
from src.api.routes.auth import router as auth_router
from src.api.routes.me import router as me_router
from src.infrastructure.database import close_database, init_database
```

- **`from contextlib import asynccontextmanager`** — dekorator tworzący asynchroniczny menedżer kontekstu z funkcji-generatora.
- **`from crediguard_observability import configure_logging, get_logger`** — funkcje ze współdzielonej biblioteki `libs/observability` (skonfigurowane w `pyproject.toml` przez `mypy_path`).
- **`from src.api.routes.auth import router as auth_router`** — import routera z aliasem.

```python
# Configure structured logging
configure_logging("applicant-service")
logger = get_logger()
```

- **`configure_logging("applicant-service")`** — uruchamia strukturalny (JSON) logging ze współdzielonej biblioteki, z nazwą serwisu. (Funkcja ta ustawia `structlog`, patrz 4.23.)
- **`logger = get_logger()`** — pobiera logger do użycia w tym module.

#### Cykl życia aplikacji (lifespan)

```python
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    import os

    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://applicant_svc:applicant_dev_pw@localhost:5433/applicant_db",
    )
    await init_database(database_url)
    logger.info("Applicant service started", database_url=database_url)
    yield
    await close_database()
    logger.info("Applicant service stopped")
```

- **`@asynccontextmanager`** — zamienia funkcję w asynchroniczny menedżer kontekstu (współpracujący z `async with`).
- **`async def lifespan(app)`** — funkcja uruchamiana przy starcie i zamknięciu aplikacji. Jest to nowoczesny mechanizm FastAPI (zastępuje stare `@app.on_event("startup")`).
- **`database_url = os.getenv("DATABASE_URL", "postgresql+asyncpg://...")`** — adres bazy ze zmiennej środowiskowej, z domyślną wartością developerską.
- **`await init_database(database_url)`** — inicjalizacja DB (tworzy silnik + tabele, patrz `database.py`).
- **`logger.info("Applicant service started", database_url=...)`** — strukturalny log z polem `database_url`.
- **`yield`** — w tym miejscu aplikacja "pracuje" (obsługuje żądania). Wszystko przed `yield` — start; po `yield` — zamknięcie.
- **`await close_database()`** — sprzątanie (dispose silnika) przy zamknięciu.
- **`logger.info("Applicant service stopped")`** — log końcowy.

#### Aplikacja i rejestracja routerów

```python
app = FastAPI(
    title="CrediGuard Applicant Service",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(auth_router, prefix="/api/v1")
app.include_router(me_router, prefix="/api/v1")
```

- **`FastAPI(title=..., version=..., lifespan=lifespan)`** — tworzy aplikację z metadanymi i podłączonym lifespandem.
- **`app.include_router(auth_router, prefix="/api/v1")`** — dołącza router auth z globalnym prefiksem `/api/v1`. Ostateczne ścieżki:
  - `POST /api/v1/auth/register`
  - `POST /api/v1/auth/login`
  - `POST /api/v1/auth/refresh`
  - `GET /api/v1/me`

#### Healthchecki

```python
@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready", tags=["health"])
async def ready() -> dict[str, str]:
    return {"status": "ready"}
```

- **`/health`** — liveness probe (k8s/Docker): serwis żyje? Zawsze `{"status": "ok"}`.
- **`/ready`** — readiness probe: serwis gotowy? Zwraca `{"status": "ready"}`. (Obecnie nie sprawdza realnej gotowości bazy — to uproszczenie.)

### 4.21 Plik: `src/api/schemas.py`

**Cel:** (patrz uwaga o duplikacji) — zawiera schematy Pydantic bardzo podobne do `dto.py`, plus dodatkowy `ErrorResponse`.

```python
"""Pydantic schemas for Applicant Service API."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)
    first_name: str = Field(min_length=1, max_length=100)
    last_name: str = Field(min_length=1, max_length=100)


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class RefreshTokenRequest(BaseModel):
    refresh_token: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int


class ApplicantResponse(BaseModel):
    id: UUID
    email: EmailStr
    first_name: str
    last_name: str
    created_at: datetime

    class Config:
        from_attributes = True


class ErrorResponse(BaseModel):
    detail: str
```

- Elementy `RegisterRequest`, `LoginRequest`, `RefreshTokenRequest`, `TokenResponse`, `ApplicantResponse` są **duplikatami** klas z `application/dto.py`.
- **Jedyna unikalna klasa:** `ErrorResponse` — `{"detail": str}`, standardowy kształt błędu FastAPI (odpowiada domyślnemu `HTTPException`).
- **Dlaczego duplikacja?** Prawdopodobnie historyczne rozdzielenie "schematów API" od "DTO aplikacji". W czystej architekturze lepiej trzymać JEDNO źródło prawdy (np. używać `application/dto` także w API, lub przenieść tu wszystko i zmienić importy w use case'ach). To kandydat do refaktoringu (patrz FAQ/dobre praktyki).

### 4.22 Pliki `__init__.py`

**Cel:** oznaczają katalogi jako pakiety i (często) re-exportują publiczne API.

**Przykład `src/domain/__init__.py`:**
```python
"""Domain layer for Applicant Service."""

from .entities import Applicant, RefreshToken
from .exceptions import (
    ApplicantDomainError,
    ApplicantNotFound,
    EmailAlreadyRegistered,
    InvalidCredentials,
    InvalidRefreshToken,
    RefreshTokenRevoked,
)

__all__ = [
    "Applicant",
    "RefreshToken",
    "ApplicantDomainError",
    "ApplicantNotFound",
    "EmailAlreadyRegistered",
    "InvalidCredentials",
    "InvalidRefreshToken",
    "RefreshTokenRevoked",
]
```

- **`.` w `from .entities import ...`** — import **względny** (od katalogu bieżącego pakietu). Pozwala przenieść pakiet bez zmiany ścieżek bezwzględnych.
- **`__all__`** — lista nazw eksportowanych przy `from src.domain import *`; dokumentuje publiczne API i steruje narzędziami.

**Pozostałe `__init__.py`** (application, ports, use_cases, infrastructure, persistence, security, api) robią analogiczne re-exporty. `tests/` celowo NIE ma `__init__.py` (pytest nie wymaga ich, a ich brak upraszcza kolekcję).

### 4.23 Plik: `libs/observability/src/crediguard_observability/logging.py`

**Cel:** współdzielona konfiguracja **ustrukturyzowanego logowania JSON** (structlog) dla wszystkich serwisów CrediGuard (spec §9.2).

```python
"""Structured JSON logging shared across CrediGuard services (spec §9.2)."""

from __future__ import annotations

import logging
import sys

import structlog


def configure_logging(service_name: str, *, level: int = logging.INFO) -> None:
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    structlog.contextvars.bind_contextvars(service=service_name)
```

- **`basicConfig`** — konfiguruje bazowe logowanie (stdout, poziom INFO).
- **`structlog.configure(processors=[...])`** — łańcuch **procesorów** przetwarzających wpisy:
  - `merge_contextvars` — dołącz kontekst (zmienne powiązane przez contextvars, np. `correlation_id`, `service`).
  - `add_log_level` — dodaj poziom logowania (INFO, ERROR...).
  - `TimeStamper(fmt="iso")` — dodaj znacznik czasu ISO.
  - `StackInfoRenderer` + `format_exc_info` — obsługa śladów stosu i wyjątków.
  - `JSONRenderer()` — **finalny renderer: wypisuje JSON**.
- **`wrapper_class=structlog.make_filtering_bound_logger(level)`** — logger filtrujący po poziomie.
- **`context_class=dict`, `logger_factory=PrintLoggerFactory()`** — proste impementacje.
- **`cache_logger_on_first_use=True`** — cache'owanie loggerów dla wydajności.
- **`structlog.contextvars.bind_contextvars(service=service_name)`** — przypisuje nazwę serwisu do wszystkich logów w tym procesie.

```python
def get_logger(*args: object, **kwargs: object) -> structlog.types.FilteringBoundLogger:
    return structlog.get_logger(*args, **kwargs)
```

- Prosty getter loggera (zwraca `FilteringBoundLogger`).

**Efekt:** `logger.info("started", db="x")` wyprodukuje linię JSON podobną do:
```json
{"service": "applicant-service", "event": "started", "db": "x", "level": "info", "timestamp": "2026-08-26T..."}
```

### 4.24 Plik: `libs/observability/src/crediguard_observability/correlation.py`

**Cel:** **middleware** propagujące `X-Correlation-ID` (spec §9.1) — czyta nagłówek z żądania (lub generuje), wiąże w kontekście structlog i zwraca w odpowiedzi.

```python
"""Correlation-ID propagation (spec §9.1)..."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, MutableMapping
from typing import Any
from uuid import uuid4

import structlog

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]

HEADER_NAME = b"x-correlation-id"
```

- **ASGI type aliases** — bo middleware jest czystym ASGI (nie zależy od frameworka): `Scope`, `Receive`, `Send`, `ASGIApp`.
- **`HEADER_NAME = b"x-correlation-id"`** — nagłówek (bajty, bo tak ASGI przekazuje nagłówki).

```python
class CorrelationIdMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        correlation_id = headers.get(HEADER_NAME, b"").decode() or str(uuid4())

        async def send_with_correlation_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                message.setdefault("headers", [])
                message["headers"].append((HEADER_NAME, correlation_id.encode()))
            await send(message)

        structlog.contextvars.bind_contextvars(correlation_id=correlation_id)
        try:
            await self.app(scope, receive, send_with_correlation_id)
        finally:
            structlog.contextvars.unbind_contextvars("correlation_id")
```

- **`__call__`** — obsługa żądania ASGI.
- **`if scope["type"] != "http": ... return`** — tylko żądania HTTP; inne (np. WebSocket) przepuszczamy bez zmian.
- **`dict(scope.get("headers", []))`** — lista nagłówków (pary bajt-klucz: bajt-wartość) zamieniona na słownik.
- **`headers.get(HEADER_NAME, b"").decode() or str(uuid4())`** — pobierz nagłówek, zdekoduj; jeśli pusty → wygeneruj nowy UUID (fallback).
- **`send_with_correlation_id`** — dekorator funkcja `send`: przy nagłówku odpowiedzi (`http.response.start`) dołącza nasz nagłówek.
- **`structlog.contextvars.bind_contextvars(correlation_id=...)`** — wiąże `correlation_id` w kontekście, by trafił do każdego logu w tym żądaniu.
- **`try/finally` z `unbind_contextvars`** — po przetworzeniu żądania czyścimy kontekst (nie chcemy wycieku między żądaniami).

### 4.25 Plik: `alembic/env.py`

**Cel:** środowisko wykonania migracji Alembic (konfiguracja połączenia, rejestracja metadanych, obsługa trybu async).

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

- **`fileConfig`** — konfiguruje logging zgodnie z `alembic.ini` (sekcje `[loggers]` itd.).
- **`target_metadata = Base.metadata`** — metadane modeli; Alembic porównuje schemat z `Base.metadata` i generuje operacje (to ważne: env musi importować modele, żeby wiedzieć o tabelach).

```python
def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()
```

- Tryb **offline**: generowanie SQL bez połączenia (do ręcznego przeglądu/uruchomienia). `literal_binds=True` — wstawia wartości do SQL.

```python
def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()
```

- Tryb **online**: uruchamia migracje na podanym połączeniu.

```python
async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()
```

- **`async_engine_from_config(...)`** — tworzy asynchroniczny silnik z konfiguracji (sekcja `[alembic]` w `alembic.ini`, prefiks `sqlalchemy.`).
- **`async with connectable.connect()`** — otwiera połączenie asynchroniczne.
- **`await connection.run_sync(do_run_migrations)`** — `run_sync` uruchamia **synchroniczną** funkcję `do_run_migrations` (Alembic jest synchroniczny) na połączeniu async.
- **`await connectable.dispose()`** — sprzątanie.

```python
if context.is_offline_mode():
    run_migrations_offline()
else:
    import asyncio
    asyncio.run(run_async_migrations())
```

- **`context.is_offline_mode()`** — rozróżnienie trybu. W trybie online uruchamiamy asynchronicznie przez **`asyncio.run(run_async_migrations())`** — top-level event loop dla procesu migracji.

### 4.26 Plik: `alembic/versions/0001_initial.py`

**Cel:** pierwsza migracja tworząca tabele `applicants` i `refresh_tokens`.

```python
"""Initial migration: create applicants and refresh_tokens tables.

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

- **`revision = "0001"`** — identyfikator migracji.
- **`down_revision = None`** — poprzednia migracja (żadna — to pierwsza).
- **`branch_labels`, `depends_on`** — zaawansowane pola (brak).

```python
def upgrade() -> None:
    op.create_table(
        "applicants",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("email", sa.String(255), unique=True, nullable=False, index=True),
        sa.Column("password_hash", sa.String(255), nullable=False),
        sa.Column("first_name", sa.String(100), nullable=False),
        sa.Column("last_name", sa.String(100), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
```

- **`op.create_table(...)`** — tworzy tabelę. Definicje kolumn odpowiadają modelom ORM.
- **`postgresql.UUID(as_uuid=True)`** — kolumna UUID Postgresa.

```python
    op.create_table(
        "refresh_tokens",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("applicant_id", postgresql.UUID(as_uuid=True), nullable=False, index=True),
        sa.Column("token_hash", sa.String(255), unique=True, nullable=False, index=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("refresh_tokens")
    op.drop_table("applicants")
```

- **`downgrade`** — odwraca migrację: usuwa tabele (kolejność ważna — najpierw `refresh_tokens` zależna, potem `applicants`).

**Uwaga o spójności:** migracja jest ręcznie napisana (odpowiada modelom). W idealnym procesie generuje się migracje z `alembic revision --autogenerate` (ale tu, przy MVP ręczne jest OK).

---

## 5. Ścieżki wywołań endpointów

W tej sekcji przeprowadzamy przez **pełną, krok po kroku** drogę żądania HTTP przez cały serwis — od curl po odpowiedź JSON — dla każdego z czterech endpointów. Zaznaczamy punkty `await`, miejsca wstrzykiwania zależności i przechwytywania błędów.

Wspólne założenia:
- Serwis działa na `http://localhost:8001`.
- Klucze RSA wygenerowane (`keygen.py`).
- Baza uruchomiona, migracje wykonane.
- Używamy `curl`.

---

### 5.1 Endpoint `/api/v1/auth/register`

#### Krok 1: Żądanie HTTP (curl)

```bash
curl -X POST http://localhost:8001/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"password123","first_name":"John","last_name":"Doe"}'
```

- `-X POST` — metoda HTTP.
- `-H "Content-Type: application/json"` — mówimy serwerowi, że body to JSON.
- `-d '{...}'` — dane żądania w JSON.

#### Krok 2: FastAPI odbiera żądanie

- Uvicorn (serwer ASGI) odbiera TCP, parsuje HTTP i przekazuje do aplikacji `app` (z `src/main.py`).
- **Middleware `CorrelationIdMiddleware`** (jeśli podłączony) czyta/generuje `X-Correlation-ID` i wiąże w kontekście.
- FastAPI dopasowuje ścieżkę `/api/v1/auth/register` do dekoratora `@router.post("/register", ...)` w `src/api/routes/auth.py:31`. Prefiksy: `/api/v1` (globalny) + `/auth` (router) + `/register` (dekorator) = `/api/v1/auth/register`.
- Wywołana zostaje funkcja **`async def register(request: RegisterRequest, use_case: RegisterUseCase = Depends(get_register_use_case)) -> TokenResponse`**.

#### Krok 3: Walidacja danych przez Pydantic

- FastAPI widzi `request: RegisterRequest` → waliduje JSON zgodnie z `RegisterRequest` (`dto.py`):
  - `email` musi być poprawnym adresem (`EmailStr`).
  - `password` od 8 do 128 znaków.
  - `first_name`, `last_name` od 1 do 100 znaków.
- **Przykład błędu** walidacji (niepoprawny e-mail):
  ```json
  {
    "detail": [
      {"type": "value_error", "loc": ["body", "email"],
       "msg": "value is not a valid email address", "input": "test"}
    ]
  }
  ```
  z kodem **422**.

#### Krok 4: Wstrzykiwanie zależności (Depends)

FastAPI rozwiązuje `Depends(get_register_use_case)` rekurencyjnie:

1. `get_register_use_case` potrzebuje:
   - `get_applicant_repo` → potrzebuje `get_session`:
     - **`get_session()`** (w `database.py`) → `async with db.session_factory() as session` → **otwiera sesję DB** → `yield session`.
   - `get_refresh_token_repo` → ta sama sesja (FastAPI **cache'uje** zależność zależnie od cache; w tym wypadku `get_session` może być użyta raz, potem ponownie — FastAPI domyślnie cache'uje zależności w zakresie requestu, więc sesja jest współdzielona).
   - `get_password_hasher` → zwraca **singleton** `Argon2Hasher()`.
   - `get_token_service` → zwraca **singleton** `JWTService()` (czyta klucze).
2. Następnie budowane są `SQLAlchemyApplicantRepository(session)` i `SQLAlchemyRefreshTokenRepository(session)`.
3. Na końcu konstruowany jest `RegisterUseCase(...)` z tymi czterema komponentami.

**Kolejność:** `get_session` → repo → hasher/token service (singletony) → use case.

#### Krok 5: Wywołanie `execute()`

- `return await use_case.execute(request)` — przekazujemy `RegisterRequest`.
- `await` — punkt asynchroniczny: sterowanie wchodzi do use case'a.

#### Krok 6: Wewnątrz use case'a — operacje

1. **`await self._applicant_repo.exists_by_email(request.email)`** — kontrola duplikatu.
2. **`self._password_hasher.hash(request.password)`** — hashowanie Argon2id (SYNC).
3. **Konstrukcja `Applicant(...)`** — encja (UUID, created_at, updated_at automatycznie).
4. **`await self._applicant_repo.save(applicant)`** — zapis (INSERT po flush).
5. **`self._token_service.create_access_token(id, email)`** — podpisanie access tokena (SYNC).
6. **`self._token_service.create_refresh_token(id)`** — podpisanie refresh tokena (SYNC).
7. **`self._password_hasher.hash(refresh_token)`** — hashowanie refresh tokena (SYNC).
8. **Konstrukcja `RefreshToken(...)`** + **`await self._refresh_token_repo.save(...)`** — zapis encji tokena.
9. Obliczenie `expires_in`.

#### Krok 7: Komunikacja z bazą (SQL)

Przykładowe zapytania generowane przez SQLAlchemy:

- **`exists_by_email`**:
  ```sql
  SELECT applicants.id
  FROM applicants
  WHERE applicants.email = 'test@example.com'
  LIMIT 1;
  ```
- **`save` (INSERT applicant)** po `flush()`:
  ```sql
  INSERT INTO applicants (id, email, password_hash, first_name, last_name, created_at, updated_at)
  VALUES ($1, $2, $3, $4, $5, $6, $7);
  ```
- **`save` (INSERT refresh_token)** po `flush()`:
  ```sql
  INSERT INTO refresh_tokens (id, applicant_id, token_hash, expires_at, created_at, revoked_at)
  VALUES ($1, $2, $3, $4, $5, $6);
  ```

Wszystkie te operacje wykonują się w **JEDNEJ transakcji** (tej samej sesji).

#### Krok 8: Obsługa błędów

- **`EmailAlreadyRegistered`** — wyrzucone, gdy e-mail istnieje. **Aktualny słaby punkt:** NIE jest mapowane na HTTP przez żaden handler → spowoduje `500 Internal Server Error` (poprawka opisana w FAQ).
- **Błąd bazy** (np. `IntegrityError` na unikalnym e-mailu) — propaguje do `get_session`, które robi `rollback`.

#### Krok 9: Przygotowanie odpowiedzi

- Use case zwraca `TokenResponse(access_token=..., refresh_token=..., token_type="bearer", expires_in=...)`.

#### Krok 10: Serializacja i wysłanie

- FastAPI serializuje `TokenResponse` do JSON (z `response_model=TokenResponse`).
- Status: **201 Created** (ustawiony w dekoratorze).
- Przykładowa odpowiedź:
  ```json
  {
    "access_token": "eyJhbGciOiJSUzI1NiIs...",
    "refresh_token": "eyJhbGciOiJSUzI1NiIs...",
    "token_type": "bearer",
    "expires_in": 899
  }
  ```

**Punkty asynchroniczne (`await`):** `exists_by_email`, `save` (klient), `save` (token). Reszta jest synchroniczna (hashing, tokeny — CPU).

---

### 5.2 Endpoint `/api/v1/auth/login`

#### Krok 1: Żądanie HTTP

```bash
curl -X POST http://localhost:8001/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"password123"}'
```

#### Krok 2: Route resolution

- Dekorator `@router.post("/login", ...)` w `auth.py` → pełna ścieżka `/api/v1/auth/login`.
- Funkcja `async def login(request: LoginRequest, use_case: LoginUseCase = Depends(get_login_use_case))`.

#### Krok 3: Walidacja

- `LoginRequest`: `email` (EmailStr), `password` (str — bez limitu długości).

#### Krok 4: Wstrzykiwanie

- Analogiczne jak w register: `get_login_use_case` kaskadowo → `get_session` (sesja DB) → repo → hasher → token service → `LoginUseCase`.

#### Krok 5: `execute()`

- `await use_case.execute(request)`.

#### Krok 6: Operacje w use case

1. **`await self._applicant_repo.get_by_email(request.email)`** — znajdź klienta.
2. **`if applicant is None: raise InvalidCredentials()`** — anty-enumeracja.
3. **`self._password_hasher.verify(request.password, applicant.password_hash)`** — weryfikacja hasła (SYNC). Jeśli `False` → `InvalidCredentials()`.
4. Generowanie access tokena i refresh tokena (SYNC).
5. Haszowanie refresh tokena, konstrukcja `RefreshToken`, **`await save`**.
6. Obliczenie `expires_in`.

#### Krok 7: SQL

- **`get_by_email`**:
  ```sql
  SELECT applicants.id, applicants.email, applicants.password_hash,
         applicants.first_name, applicants.last_name,
         applicants.created_at, applicants.updated_at
  FROM applicants
  WHERE applicants.email = 'test@example.com'
  LIMIT 1;
  ```
- **`save` (INSERT refresh_token)** jak w rejestracji.

#### Krok 8: Błędy

- `InvalidCredentials` (brak użytkownika lub złe hasło) — **nie mapowane** → 500 (słaby punkt).
- Błędy bazy → rollback w `get_session`.

#### Krok 9-10: Odpowiedź

- `TokenResponse` → JSON, status **200**.
  ```json
  {
    "access_token": "eyJ...",
    "refresh_token": "eyJ...",
    "token_type": "bearer",
    "expires_in": 899
  }
  ```

**Punkty async:** `get_by_email`, `save`.

---

### 5.3 Endpoint `/api/v1/auth/refresh`

#### Krok 1: Żądanie HTTP

```bash
curl -X POST http://localhost:8001/api/v1/auth/refresh \
  -H "Content-Type: application/json" \
  -d '{"refresh_token":"<pobrany_wcześniej_refresh_token>"}'
```

#### Krok 2: Route resolution

- Dekorator `@router.post("/refresh", ...)` → `/api/v1/auth/refresh`.
- Funkcja `async def refresh_token(request: RefreshTokenRequest, use_case: RefreshTokenUseCase = Depends(get_refresh_token_use_case))`.

#### Krok 3: Walidacja

- `RefreshTokenRequest`: jedno obowiązkowe pole `refresh_token`.

#### Krok 4: Wstrzykiwanie

- `get_refresh_token_use_case` → sesja → repo → hasher → token service → `RefreshTokenUseCase`.

#### Krok 5: `execute()`

- `await use_case.execute(request)`.

#### Krok 6: Operacje w use case (rotacja)

1. `now = datetime.now(timezone.utc)`.
2. **`await self._refresh_token_repo.get_all_valid(now)`** — pobranie WSZYSTKICH ważnych tokenów.
3. Pętla `for token_entity in ...` — weryfikuje hash: **`self._password_hasher.verify(request.refresh_token, token_entity.token_hash)`** (SYNC).
4. Gdy hash pasuje:
   - **`token_entity.is_valid(now)`** — jeśli nie → `RefreshTokenRevoked`.
   - **`token_entity.revoke()`** — unieważnienie.
   - **`await self._refresh_token_repo.update(token_entity)`** — zapis zmiany.
   - **`await self._applicant_repo.get_by_id(token_entity.applicant_id)`** — pobranie klienta; jeśli `None` → `InvalidRefreshToken`.
   - Generowanie **nowego** access i refresh tokena (SYNC).
   - Haszowanie nowego refresh tokena, konstrukcja `RefreshToken`, **`await self._refresh_token_repo.save(...)`**.
   - Obliczenie `expires_in`, `return TokenResponse`.
5. Jeśli pętla nic nie dopasowała → **`raise InvalidRefreshToken()`**.

#### Krok 7: SQL

- **`get_all_valid`**:
  ```sql
  SELECT * FROM refresh_tokens
  WHERE refresh_tokens.expires_at > $1
    AND refresh_tokens.revoked_at IS NULL;
  ```
- **`update`** (ustawienie `revoked_at`):
  ```sql
  UPDATE refresh_tokens SET revoked_at = $2
  WHERE refresh_tokens.id = $1;
  ```
- **`get_by_id`**:
  ```sql
  SELECT ... FROM applicants WHERE applicants.id = $1 LIMIT 1;
  ```
- **`save`** (INSERT nowego refresh tokena).

#### Krok 8: Błędy

- `InvalidRefreshToken` / `RefreshTokenRevoked` — **nie mapowane** → 500 (słaby punkt).
- Błędy bazy → rollback.

#### Krok 9-10: Odpowiedź

- Nowa para tokenów, status **200**.
  ```json
  {
    "access_token": "eyJ100...",
    "refresh_token": "eyJ200...",
    "token_type": "bearer",
    "expires_in": 899
  }
  ```

**Punkty async:** `get_all_valid`, `update`, `get_by_id`, `save`.

---

### 5.4 Endpoint `/api/v1/me`

#### Krok 1: Żądanie HTTP

```bash
curl http://localhost:8001/api/v1/me \
  -H "Authorization: Bearer <access_token>"
```

- Ten endpoint wymaga nagłówka `Authorization: Bearer <token>`.

#### Krok 2: Route resolution + autoryzacja

- Dekorator `@router.get("", ...)` w `me.py` → pełna ścieżka `/api/v1/me`.
- Funkcja `async def get_me(applicant_id: UUID = Depends(get_current_applicant_id), use_case: GetMeUseCase = Depends(get_get_me_use_case))`.
- **Najpierw** rozwiązana jest `get_current_applicant_id`:
  - Czyta nagłówek `Authorization` (przez `Header(None, alias="Authorization")`).
  - Sprawdza `startswith("Bearer ")`; jeśli nie → **HTTPException 401**.
  - Wycina token: `authorization[7:]`.
  - **`token_service.decode_access_token(token)`** — weryfikacja podpisu (publiczny klucz), `exp`, `type == "access"`, konwersja `sub` → UUID.
  - Zwraca `applicant_id`.

#### Krok 3: Walidacja

- Brak body; autoryzacja już "walidowana" (token).

#### Krok 4: Wstrzykiwanie

- `get_current_applicant_id` (token service singleton).
- `get_get_me_use_case` → `get_applicant_repo` → `get_session` (sesja DB).

#### Krok 5: `execute()`

- `await use_case.execute(applicant_id)` (przekazujemy UUID z tokena).

#### Krok 6: Operacje w use case

1. **`await self._applicant_repo.get_by_id(applicant_id)`** — pobranie profilu.
2. Jeśli `None` → `raise ApplicantNotFound(str(applicant_id))`.
3. Budowa `ApplicantResponse.id/email/first_name/last_name/created_at`.

#### Krok 7: SQL

- `get_by_id`:
  ```sql
  SELECT ... FROM applicants WHERE applicants.id = $1 LIMIT 1;
  ```

#### Krok 8: Błędy

- **401** — zły/brakujący token (HTTPException w `get_current_applicant_id`).
- `ApplicantNotFound` — **nie mapowane** → 500 (słaby punkt).

#### Krok 9-10: Odpowiedź

- `ApplicantResponse` → JSON, status **200**.
  ```json
  {
    "id": "3f2a5e2a-...", 
    "email": "test@example.com",
    "first_name": "John",
    "last_name": "Doe",
    "created_at": "2026-08-26T12:00:00Z"
  }
  ```

**Punkty async:** `get_by_id`. (Autoryzacja — synchroniczna, JWT.)

---

## 6. Koncepcje techniczne

Ta sekcja wyjaśnia — prostym językiem, z przykładami — wszystkie konstrukcje Pythona, dekoratory, typy i wzorce projektowe użyte w projekcie.

### 6.1 Słowa kluczowe i konstrukcje Pythona

Poniższa tabela zbiera elementy pojawiające się w kodzie wraz z wyjaśnieniem i miejscem użycia.

| Konstrukcja | Co to jest | Przykład użycia |
|-------------|------------|-----------------|
| `def` | Definicja funkcji lub metody | `def full_name(self)` |
| `class` | Definicja klasy | `class Applicant:` |
| `async` | Naznacza funkcję jako **asynchroniczną** (zwraca korutynę) | `async def execute(...)` |
| `await` | Wstrzymuje wykonanie, aż korutyna się zakończy (oddaje sterowanie event loop) | `await self._repo.save(...)` |
| `yield` | W funkcji-generatorze zwraca wartość i **pauzuje**; wznowi po `next()`/zużyciu | `yield session` w `get_session` |
| `import` / `from ... import` | Dołączanie modułów/nazw | `from uuid import UUID` |
| `raise` | Wyrzuca wyjątek | `raise InvalidCredentials()` |
| `try / except` | Przechwytywanie wyjątków | w `Argon2Hasher.verify` |
| `with` | Menedżer kontekstu (czyści zasoby) | `with pytest.raises(...)` |
| `global` | Modyfikacja zmiennej globalnej modułu | w `init_database` |
| `property` | Dekorator: metoda jako atrybut | `@property def full_name` |
| `dataclass` | Dekorator: automatyczny `__init__`, `__repr__`, `__eq__` | `@dataclass class Applicant` |
| `field` | Funkcja do ustawiania domyślnej fabryki dla pola | `field(default_factory=uuid4)` |
| `lambda` | Anonimowa (bezimienna) funkcja jedno-linijkowa | `lambda: datetime.now(timezone.utc)` |
| `super()` | Odniesienie do metody klasy nadrzędnej | `super().__init__(...)` |
| `self` | Referencja do instancji w metodzie | `self._session` |
| `@abstractmethod` | Oznacza metodę abstrakcyjną (musi być zaimplementowana) | w portach |
| `@asynccontextmanager` | Zamienia generator w asynchroniczny menedżer kontekstu | `lifespan` |
| `if __name__ == "__main__"` | Uruchom tylko, gdy plik startowany, nie importowany | w `keygen.py` |
| `from __future__ import annotations` | Leniwa ewaluacja adnotacji (PEP 563) | prawie każdy plik |

#### 6.1.1 `async` / `await` — programowanie asynchroniczne

Python ma **event loop** (pętlę zdarzeń), który obsługuje wiele współbieżnych zadań w jednym wątku. Gdy zadanie czeka na I/O (baza danych, HTTP, plik), `await` **oddaje sterowanie** pętli, która zajmuje się innymi zadaniami. Dzięki temu serwer obsłuży wiele żądań jednocześnie, nie blokując się na żadnym.

- **`async def`** — deklaruje korutynę. Jej wykonanie zaczyna się dopiero po umieszczeniu w event loop (np. przez `await`).
- **`await korutyna`** — czeka na wynik, ale BEZ blokowania całego procesu.

> **Analogia:** restauracja z jednym kelnerem (event loop). Kelner przyjmuje zamówienia (I/O) u wielu stolików na zmianę, zamiast czekać przy jednym, aż klient przemyśli. Kiedy klient czeka na kuchnię (baza), kelner obsługuje innych.

W projekcie `await` pojawia się głównie przy operacjach **na bazie** (`exists_by_email`, `save`, `get_by_id`, `flush`, `execute`, `commit`).

#### 6.1.2 Blokujący vs nieblokujący — sync vs async

| Aspekt | Synchroniczny (`def`) | Asynchroniczny (`async def`) |
|--------|----------------------|------------------------------|
| Wykonanie | Blokuje bieżący wątek do końca | Oddaje sterowanie w `await` |
| I/O | Czeka, nic innego nie robi | Pozwala obsługiwać inne zadania |
| Kiedy używać | Obliczenia CPU (szybkie lub celowe) | Operacje I/O (baza, sieć, pliki) |
| Przykład w kodzie | `argon2.hash`, `jwt.encode/decode` | `session.execute`, `session.flush` |

**Uwaga o hachowaniu:** `argon2.hash` jest **CPU-bound** i blokuje event loop. W obecnym MVP jest to akceptowalne (jedno żądanie = jeden hash), ale przy dużym ruchu hashowanie powinno być przeniesione do osobnego wątku (`asyncio.to_thread`) lub procesu roboczego, aby nie blokować obsługi innych żądań. To kandydat do optymalizacji (patrz FAQ "jak skalować").

#### 6.1.3 `yield` i funkcje-generatory

Funkcja z `yield` jest **generatorem**: jej wykonanie można pauzować i wznawiać. `get_session` jest **asynchronicznym generatorem** (ang. async generator). FastAPI używa tej zdolności, aby wstrzyknąć wartość z `yield` (sesję) do kodu endpointu, a po zakończeniu żądania wznowić generator i wykonać kod po `yield` (commit/rollback/close). To kluczowy mechanizm zarządzania transakcją (patrz 8.4).

#### 6.1.4 `dataclass` i `field`

`@dataclass` generuje `__init__`, `__repr__`, `__eq__`. `field(default_factory=...)` pozwala ustawić **funkcję fabrykującą** wartości domyślne — wywoływaną dla KAŻDEJ instancji osobno. To kluczowe dla `id`, `created_at`, `updated_at`: każda encja ma własny UUID i własny czas.

**Błąd do uniknięcia:** `id: UUID = uuid4()` (bez `field`) policzyłoby default RAZ przy definicji klasy → wszystkie encje dzieliłyby ten sam UUID.

#### 6.1.5 `from __future__ import annotations`

Włącza **leniwe** (opóźnione) traktowanie adnotacji jako ciągów znaków. Zalety:
- Można używać typów zadeklarowanych później w tym samym pliku.
- Można pisać `str | None`, `list[str]`, `tuple[UUID, str]` nawet na Pythonie 3.9.
- mypy strict je rozumie dzięki dołączeniu do kontekstu.

**Alternatywa:** importować `Optional`, `List`, `Tuple` z `typing`. Ale to więcej kodu i mniej idiomów. Dlatego wybrano `from __future__ import annotations`.

#### 6.1.6 `if __name__ == "__main__"`

Ten blok wykonuje się **tylko**, gdy plik jest uruchamiany bezpośrednio (np. `python keygen.py`), a **nie** gdy jest importowany jako moduł. Dzięki temu `import keygen` nie spowoduje przypadkowego wygenerowania kluczy. To standardowy idiom dla skryptów CLI.

### 6.2 Dekoratory używane w projekcie

| Dekorator | Co robi | Dlaczego |
|-----------|---------|----------|
| `@dataclass` | Generuje `__init__`, `__repr__`, `__eq__` | Zwięzłe definicje encji |
| `@property` | Metoda dostępna jako atrybut bez `()` | `full_name` jako logiczny atrybut |
| `@abstractmethod` | Wymusza implementację metody w podklasie | Budowa portów (interfejsów) |
| `@asynccontextmanager` | Określa asynchroniczny menedżer kontekstu | Cykl życia aplikacji (`lifespan`) |
| `@router.post` / `@router.get` | Rejestruje funkcję jako endpoint HTTP | Definicja API |
| `@app.get` | Rejestruje healthcheck | `/health`, `/ready` |
| `@pytest.mark.asyncio` | Mówi pytest-asyncio, że test jest asynchroniczny | Testy `async def` |

#### 6.2.1 `@abstractmethod` — jak działa port?

Port to **interfejs**: klasa abstrakcyjna z metodami bez ciała (lub z samymi docstringami). Podklasa MUSI je zaimplementować. Próba utworzenia instancji klasy abstrakcyjnej kończy się błędem `TypeError`. Dzięki temu:
- Use case wie, czego oczekiwać (kontrakt).
- Wymiana adaptera (np. Postgres → inne repo) nie zmienia logiki.

#### 6.2.2 `@router.post("/register", response_model=..., status_code=201)`

Ten dekorator wykonuje następujące zadania:
- Rejestruje funkcję w routerze pod metodą `POST` i ścieżką `/register`.
- `response_model` — gwarantuje serializację wyniku do danego modelu.
- `status_code` — stały kod odpowiedzi.

**Alternatywa:** Flask (`@app.route("/register", methods=["POST"])`). FastAPI daje automatycznie walidację Pydantic, OpenAPI/docs i lepsze wsparcie async — dlatego wybrano FastAPI, a nie Flask.

#### 6.2.3 `@pytest.mark.asyncio`

Informuje plugin `pytest-asyncio`, żeby uruchomił daną funkcję testową w asynchronicznym event loop. Bez tego dekoratora test `async def` nie wykona się prawidłowo (albo wcale się nie uruchomi).

### 6.3 Typy i klasy z bibliotek zewnętrznych

| Typ/Klasa | Biblioteka | Cel |
|-----------|-----------|-----|
| `BaseModel` | Pydantic | Baza modeli walidujących i serializowalnych |
| `EmailStr` | Pydantic (email-validator) | Walidacja adresu e-mail |
| `Field` | Pydantic | Metadane walidacyjne (długość itd.) |
| `AsyncSession` | SQLAlchemy | Asynchroniczna sesja bazy |
| `Mapped`, `mapped_column`, `DeclarativeBase` | SQLAlchemy | Definiowanie modeli (2.0 style) |
| `select` | SQLAlchemy | Budowanie zapytań SELECT |
| `DateTime`, `String`, `UniqueConstraint` | SQLAlchemy | Typy kolumn i ograniczenia |
| `PG_UUID` | SQLAlchemy dialects postgresql | Typ UUID Postgresa |
| `async_sessionmaker`, `create_async_engine`, `NullPool` | SQLAlchemy | Silnik/fabryka sesji/pula |
| `JWError` (`JWTError`) | python-jose | Wyjątek JWT |
| `APIRouter`, `Depends`, `Header`, `HTTPException`, `status` | FastAPI/Starlette | Trasowanie, DI, autoryzacja, błędy HTTP |
| `UUID`, `uuid4` | stdlib `uuid` | Identyfikatory |
| `datetime`, `timedelta`, `timezone` | stdlib `datetime` | Czas i strefy |
| `Path` | stdlib `pathlib` | Ścieżki plików |
| `os` | stdlib | Zmienne środowiskowe |
| `argon2` | argon2-cffi | Haszowanie Argon2id |
| `jwt` | python-jose | Kodowanie/dekodowanie JWT |
| `serialization`, `rsa` | cryptography | Serializacja/generacja kluczy |
| `ABC`, `abstractmethod` | stdlib `abc` | Klasy abstrakcyjne |
| `AsyncMock`, `MagicMock` | stdlib `unittest.mock` | Atrapy w testach |
| `AsyncGenerator` | stdlib `collections.abc` | Typ generatora async |

#### 6.3.1 Pydantic `BaseModel` — po co?

Pydantic nie tylko trzyma dane — **waliduje je przy tworzeniu** i **serializuje do JSON**. Gdy klient wyśle `{"email": "niepoprawny"}`, Pydantic rzuci błąd walidacji, który FastAPI zamienia na `422`. To "brama obronna" systemu.

#### 6.3.2 `EmailStr` vs `str`

`str` — dowolny tekst. `EmailStr` — tekst, który **musi** być poprawnym adresem e-mail (walidacja regex/syntax). Zastosowanie `EmailStr` w DTO przenosi walidację formatu na **granicę systemu** (wczesna walidacja), zamiast odkładać ją na logikę domeny.

#### 6.3.3 SQLAlchemy 2.0 style — `Mapped` i `mapped_column`

Nowoczesny (2.0) sposób definiowania kolumn:
```python
id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
```
- `Mapped[UUID]` — typ kolumny po stronie Pythona.
- `mapped_column(...)` — pełna definicja kolumny (typ SQL, ograniczenia).

**Alternatywa (stary styl 1.x):** `id = Column(UUID, primary_key=True)`. Nowy styl jest typowany i czytelny, preferowany przez SQLAlchemy 2.0.

### 6.4 Wzorce projektowe

| Wzorzec | Co to (prostym językiem) | Analogia | Gdzie w kodzie |
|---------|--------------------------|----------|----------------|
| **Repository** | Udaje kolekcję encji w pamięci; ukrywa bazę | Kartoteka w bibliotece — "znajdź książkę po tytule", nie wiesz, w którym regale | `repository.py` (adapter), użycase używa metod zamiast SQL |
| **Port-Adapter (Hexagonal)** | Interfejs (port) + konkretne implementacje (adaptery); zależność od abstrakcji | Gniazdko (port) + różne wtyczki (adaptery) | Porty w `application/ports`, adaptery w `infrastructure` |
| **Use Case** | Pojedyncza operacja biznesowa; orkiestracja portów | Procedura "przyjmij zamówienie" | `application/use_cases/*` |
| **Dependency Injection** | Zależności przekazywane z zewnątrz, nie tworzone wewnątrz | Restauracja dostaje świeże składniki od dostawcy | Konstruktory use case'ów; `dependencies.py` |
| **Singleton** | Jedna instancja obiektu w procesie | Jedyny klucz główny do sejfu | `_token_service`, `_argon2_hasher`, `_db` |
| **Lazy initialization** | Tworzenie obiektu dopiero przy pierwszym użyciu | Włączenie piekarnika dopiero, gdy trzeba | w `get_token_service()` itd. |
| **Unit of Work** | Grupuje zmiany w jedną transakcję do wspólnego commita | Lista zakupów — płacisz raz na końcu | `get_session` (sesja + commit/rollback) |

#### 6.4.1 Dependency Injection — głębiej

W kodzie zależności są przekazywane **przez konstruktor** (Constructor Injection): `RegisterUseCase(applicant_repo, refresh_token_repo, password_hasher, token_service)`. Kontener DI (FastAPI `Depends`) buduje cały graf obiektów. Zalety:
- **Testowalność** — testy tworzą use case z mockami zamiast realnej bazy.
- **Luźne sprzężenie** — podmiana implementacji bez zmiany logiki.
- **Łatwe porządkowanie** — centralny "kompozycyjny korzeń" w `dependencies.py`.

#### 6.4.2 Port-Adapter — analogia gniazdka

Port definiuje kontrakt: "cokolwiek zapisuje klientów, musi umieć `save/get_by_id`". Adapter to konkretna wtyczka — tu `SQLAlchemyApplicantRepository`. Gdybyśmy zdecydowali się na MongoDB, napisalibyśmy drugi adapter (`MongoApplicantRepository`) — use case'y pozostałyby bez zmian.

#### 6.4.3 Unit of Work — cykl życia sesji

W `get_session`:
1. Otwarcie sesji (początek transakcji).
2. `yield` — przekazanie sesji do repozytoriów/use case'u.
3. Po zakończeniu: `commit` (jeśli bez błędów) lub `rollback` (jeśli wyjątek).
4. `close` — zamknięcie.

Dzięki temu wszystkie zapisy w trakcie jednego żądania są **atomowe** — albo wszystkie się udają, albo żaden. (Szczegóły w 8.4.)

---

## 7. Bezpieczeństwo

Ta sekcja wyjaśnia decyzje bezpieczeństwa projektu i ich uzasadnienie techniczne.

### 7.1 Argon2id — dlaczego ten algorytm i jak działa

**Argon2** to zwycięzca **Password Hashing Competition** (2015) — konkursu mającego wyłonić najlepszy algorytm hashowania haseł. Ma trzy warianty:
- **Argon2d** — odporny na ataki GPU, ale podatny na timing/side-channel.
- **Argon2i** — odporny na side-channel, ale słabszy na GPU.
- **Argon2id** — **hybryda**, która łączy zalety obu: najpierw działa jak Argon2i (walka z side-channel), potem jak Argon2d (walka z GPU). To wariant **rekomendowany dla haseł** — i to on jest wybrany (domyślnie przez `argon2-cffi` `PasswordHasher`).

**Parametry w kodzie (`Argon2Hasher`):**
- `time_cost=3` — liczba iteracji (kolejnych przebiegów). Wyższy = wolniejszy i bezpieczniejszy (zużycie CPU).
- `memory_cost=65536` — zużycie pamięci w **kiB** = **64 MiB**. To czyni algorytm **memory-hard**: wymaga dużej pamięci RAM, co drastycznie utrudnia ataki na tanie GPU/ASIC (one mają mało pamięci), które znakomicie rozprawiają się z prostymi hash.
- `parallelism=4` — liczba równoległych wątków (wykorzystanie wielu rdzeni).
- `hash_len=32` — długość wyjściowego hash (32 bajty).
- `salt_len=16` — 16 bajtów **losowej soli**.

**Sól (salt):** to losowa wartość dołączana do hasła PRZED hashowaniem. Dzięki niej:
- Dwa identyczne hasła różnych użytkowników dają **różne hashe**.
- Atakujący nie może użyć **tablic tęczowych** (prekompilowanych map hash→hasło).
- Każda sól wymaga osobnego ataku brute-force.

**Dlaczego hasze są "samoopisujące"?** Wyjście Argon2 (np.):
```
$argon2id$v=19$m=65536,t=3,p=4$c2FsdA$NmVtWVlhWVl...
```
Zawiera: wersję algorytmu, parametry, sól (base64) i właściwy hash. Dzięki temu do weryfikacji nie trzeba osobno przechowywać parametrów ani soli — wszystko jest w jednym ciągu. To również pozwala **podnieść parametry w przyszłości** bez migracji starych haszy: po weryfikacji można "przehasować" nowym, mocniejszym ustawieniem.

#### Tabela: bcrypt vs Argon2id

| Cecha | bcrypt | Argon2id |
|-------|--------|----------|
| Pochodzenie | 1999, powiązany z Blowfish | Zwycięzca PHC 2015 |
| Memory-hard | Częściowo (ograniczona pamięć) | Tak, w pełni konfigurowalna (memy) |
| Odporność na GPU | Średnia | Duża (wymaga dużo RAM) |
| Konfiguracja | Tylko koszt (cost) | czas, pamięć, równoległość |
| Warianty | — | d, i, id |
| Rekomendacja | Przestarzała, ale wciąż OK | **Obecny standard OWASP** |

Wniosek: wybór Argon2id to **podniesienie standardu bezpieczeństwa** ponad bcrypt, kosztem nieco większego zużycia zasobów (co w MVP jest akceptowalne i konfigurowalne).

### 7.2 RS256 vs HS256 — dlaczego asymetrycznie

| Cecha | HS256 (symetryczny) | RS256 (asymetryczny, RSA) |
|-------|---------------------|---------------------------|
| Klucz | Jeden wspólny **sekretny** klucz do podpisu i weryfikacji | **Para**: prywatny (podpis) + publiczny (weryfikacja) |
| Uwierzytelnienie | Każdy, kto zna sekret, może i podpisać, i zweryfikować | Podpisać może tylko posiadacz klucza prywatnego |
| Dystrybucja | Sekret musi być współdzielony między wszystkimi usługami | Klucz publiczny może być rozdawany swobodnie |
| Bezpieczeństwo w mikroserwisach | Ryzyko wycieku wspólnego sekretu | Klucz prywatny trzyma TYLKO jeden serwis |
| Wydajność | Szybszy (obliczenia symetryczne) | Wolniejszy (matematyka RSA) |
| Użycie w projekcie | — (nie użyty) | **Wybor dla CrediGuard** |

**Dlaczego RS256 jest lepszy w mikroserwisach?**
1. **Podział ról:** tylko Applicant Service ma klucz **prywatny** (do podpisywania). Gateway i inne serwisy mają tylko klucz **publiczny** do weryfikacji. Nawet jeśli ktoś ukradnie klucz publiczny, nie może podpisać nowych tokenów.
2. **Rozproszenie bez ryzyka:** klucz publiczny można rozdać wszystkim usługom (przez wolumen/objętość Docker, zmienną środowiskową, centralny keyczar itd.) bez obawy o bezpieczeństwo.
3. **Jeśli jeden serwis zostanie skompromitowany** (straci sekret HS256), atakujący może fałszować tokeny WSZĘDZIE. Przy RS256 — tylko jeśli wpadnie klucz prywatny (wąska, jednopunktowa powierzchnia).

**Gdzie leżą klucze?** W katalogu `KEYS_DIR` (domyślnie `/app/keys` w Dockerze, `./keys` lokalnie) jako pliki PEM:
- `private_key.pem` — tylko w Applicant Service.
- `public_key.pem` — montowany do Gateway przez wolumen.

**Uprawnienia plików:** `keygen.py` ustawia `0o600` (rw-------) dla klucza prywatnego — tylko właściciel może go czytać. Klucz publiczny: `0o644`.

**Uwaga ryzyka altymconfusion:** `jwt.decode(..., algorithms=["RS256"])` wymusza białą listę algorytmów. Gdybyśmy tego nie zrobili, atakujący mógłby próbować podmienić algorytm na `HS256` i podpisać token **publicznym kluczem** jako sekretem (znany atak "algorytm confusion"). Biała lista to kluczowa obrona.

### 7.3 Ochrona klucza prywatnego

- **Klucz prywatny NIGDY nie powinien trafić do repozytorium** (zob. `keys/` w `services/applicant` — pamiętaj o `.gitignore`).
- Uprawnienia `0600` w systemach Unix ograniczają odczyt do właściciela (użytkownika kontenera).
- Klucz jest **bez szyfrowania** (`NoEncryption()` w keygen) — bo serwis musi go czytać automatycznie przy starcie; bezpieczeństwo zapewniają uprawnienia plików i izolacja kontenera, a nie hasło.
- W produkcji typowo używa się menedżera sekretów (Vault, KMS) — klucz może być pobierany jako zmienna środowiskowa/bajty z bezpiecznego magazynu.

### 7.4 Rotacja refresh tokena

**Rotacja** = przy każdym użyciu refresh tokena, **stary jest unieważniany**, a **nowy wydawany**. W kodzie: `RefreshTokenUseCase`:
1. Znajduje token, którego hash pasuje.
2. `token_entity.revoke()` (ustawia `revoked_at`).
3. `update()` — zapisuje unieważnienie.
4. Generuje NOWY refresh token i zapisuje go.

**Dlaczego to ochrania przed replay atakiem (powtórnym użyciem)?** Jeśli atakujący skradł refresh token, może go użyć raz — ale wtedy stary zostaje unieważniony i klient dostaje nowy. Jeśli atakujący spróbuje użyć **skradzionego (starego)** tokena ponownie, system nie znajdzie pasującego ważnego tokena (bo był zrotowany/unieważniony) → błąd. Co więcej, w idealnej implementacji ponowne użycie starego tokena sygnalizuje kradzież (można wtedy unieważnić WSZYSTKIE tokeny użytkownika).

**Ograniczenie implementacji:** `test_refresh_token_rotation` potwierdza, że drugie użycie starego tokena kończy się błędem. To właśnie mechanizm ochrony przed replay.

### 7.5 Hashowanie refresh tokena przed zapisem (storage hash)

Refresh token **NIE jest przechowywany w bazie w czystej postaci**. Zamiast tego zapisujemy jego **hash Argon2id** (`token_hash`).

**Dlaczego?** Refresh token jest długożyjący (7 dni) i uprawnia do uzyskania nowego access tokena — to praktycznie "hasło sesji". Gdyby baza (lub kopia zapasowa/wyniki logów) wyciekła:
- Bez hashowania: atakujący ma działające tokeny → może podszyć się pod każdego użytkownika.
- Z hashowaniem: atakujący ma bezużyteczne hashe (podobnie jak z hasłami — nie da się odtworzyć tokena, a hashowanie przy każdym użyciu jest na tyle kosztowne, że atak offline jest nieopłacalny).

Hash przechowywany jest w kolumnie `token_hash` z unikalnym indeksem. Weryfikacja przy refresh: iterujemy po ważnych tokenach i sprawdzamy `argon2.verify(token, token_hash)`.

### 7.6 Jednolity błąd logowania — anty-enumeracja

W `LoginUseCase`:
```python
if applicant is None:
    raise InvalidCredentials()
if not self._password_hasher.verify(...):
    raise InvalidCredentials()
```

Oba przypadki (brak użytkownika vs złe hasło) rzucają **ten sam wyjątek** z tym samym komunikatem: `"Invalid email or password"`.

**Dlaczego to ważne?** Gdyby serwis odpowiadał inaczej (np. `404 User not found` vs `401 Wrong password`), atakujący mógłby **enumerować konta**: testować listę e-maili i sprawdzać, które istnieją. To podszyłoby phishing/targeting. Ujednolicenie komunikatu sprawia, że atakujący nie ma skąd wiedzieć, czy e-mail istnieje — obie ścieżki wyglądają identycznie.

**Dodatkowa ochrona (w idealnej implementacji):** identyczny **czas** obu ścieżek (timing attack) — gdy użytkownik nie istnieje, nie ma sensu mierzyć się z kosztownym hashowaniem; wówczas wykonuje się tzw. "dummy hash" dla wyrównania czasu. (W obecnym MVP ten aspekt nie jest jeszcze zoptymalizowany.)

### 7.7 JWT claims — `sub`, `iat`, `exp`, `type`, `email`

Claims to pola w ładunku (payload) tokena. Standard JWT (RFC 7519) definiuje kilka "zarejestrowanych" claims:

| Claim | Znaczenie | Czy wymagany | W projekcie |
|-------|-----------|--------------|-------------|
| `sub` | **Subject** — identyfikator podmiotu (tu: UUID klienta) | Tak (rekomendowany) | `"sub": str(applicant_id)` |
| `iat` | **Issued At** — czas wystawienia (Unix timestamp) | Nie | `iat: int(now.timestamp())` |
| `exp` | **Expiration** — czas wygaśnięcia (Unix timestamp) | Tak | `exp: int(expire.timestamp())` |
| `nbf` | Not Before (działa od...) | Nie | nie użyty |
| `iss` | Issuer (kto podpisał) | Nie | nie użyty |
| `aud` | Audience (dla kogo przeznaczony) | Nie | nie użyty |

**Dlaczego są potrzebne?**
- `sub` — pozwala jednoznacznie wskazać, kogo dotyczy token; bez niego weryfikator nie wiedziałby, czyim jest.
- `iat` — daje audyt / czas wystawienia.
- `exp` — **krytyczne dla bezpieczeństwa**: token ma ograniczony czas życia. `jwt.decode` automatycznie sprawdza `exp` i odrzuca token po jego upływie.
- `type` (własne claim) — rozróżnia access token od refresh tokena, co jest kluczowe, bo oba podpisane są tym samym kluczem.
- `email` (własne claim, tylko w access token) — pozwala na szybki odczyt e-maila bez sięgania do bazy (używane np. przy logice businessowej lub w innych serwisach na podstawie tokena).

**Uwaga bezpieczeństwa:** JWT payload jest **podpisany**, ale **nie zaszyfrowany** — można go odczytać (base64url). Dlatego nigdy nie wkładamy do tokena danych wrażliwych (tylko `sub`, `email`, `type`, daty).

---

## 8. Baza danych i SQLAlchemy

### 8.1 Modele i ich kolumny

#### Tabela `applicants`

| Kolumna | Typ | Opis | Ograniczenia |
|---------|-----|------|--------------|
| `id` | `UUID` | Identyfikator klienta | `primary_key`, default `uuid4` |
| `email` | `String(255)` | Adres e-mail | `unique`, `not null`, `index` |
| `password_hash` | `String(255)` | Hash hasła Argon2id | `not null` |
| `first_name` | `String(100)` | Imię | `not null` |
| `last_name` | `String(100)` | Nazwisko | `not null` |
| `created_at` | `DateTime(tz)` | Data utworzenia | `not null`, default now |
| `updated_at` | `DateTime(tz)` | Data aktualizacji | `not null`, default now, `onupdate` now |

- **Dlaczego `UUID` zamiast auto-increment `INTEGER`?** UUID nie wymaga centralnego licznika, jest globalnie unikalny (łatwiej łączyć z innymi serwisami, bezpieczniejszy przed zgadywaniem ID, dobry dla rozproszonych systemów). Minus: większy index.
- **`String(255)` dla e-maila** — wystarczający zapas (po standardowych limitach); `unique` chroni przed duplikatami.
- **`DateTime(timezone=True)`** — czas ze strefą (timestamptz), przechowuje z poprawnością co do UTC.

#### Tabela `refresh_tokens`

| Kolumna | Typ | Opis | Ograniczenia |
|---------|-----|------|--------------|
| `id` | `UUID` | Identyfikator tokena | `primary_key`, default `uuid4` |
| `applicant_id` | `UUID` | Właściciel tokena | `not null`, `index` |
| `token_hash` | `String(255)` | Hash refresh tokena | `unique`, `not null`, `index` |
| `expires_at` | `DateTime(tz)` | Moment wygaśnięcia | `not null` |
| `created_at` | `DateTime(tz)` | Data utworzenia | `not null`, default now |
| `revoked_at` | `DateTime(tz)` | Moment unieważnienia | nullable (NULL = aktywny) |
| `token_hash` (UniqueConstraint) | — | jawne ograniczenie | `UniqueConstraint("token_hash", name="uq_refresh_tokens_token_hash")` |

- **`revoked_at` nullable** — `NULL` oznacza "token aktywny"; nie-null = unieważniony. To podejście "soft delete" / logiczne flagi zamiast fizycznego usuwania.
- **Duplikacja unikalności** (`unique=True` na kolumnie + osobny `UniqueConstraint`) — zbędne zdublowanie; w praktyce wystarczy jeden zapis.

### 8.2 `flush()` vs `commit()` — kluczowa różnica

| Operacja | Co robi | Kiedy w kodzie |
|----------|---------|----------------|
| `flush()` | Wysyła bieżące zmiany do bazy (INSERT/UPDATE), ale **nie kończy transakcji**. Nadaje identyfikatory, narzuca constraints. | w metodach repozytorium (`save`, `update`) |
| `commit()` | **Zatwierdza transakcję** — czyni zmiany trwałe (COMMIT). | w `get_session` po zakończeniu żądania |

**Dlaczego w repozytorium `flush()`, a w `get_session` `commit()`?**
- Repozytorium wykonuje tylko **etap**. Wiele operacji (INSERT klienta, INSERT tokena) dzieje się w tej samej sesji. `flush()` wysyła je do bazy, dzięki czemu np. `IntegrityError` (konflikt) pojawia się TU, a nie dopiero na końcu. Ale to nie jest ostateczne zatwierdzenie.
- `commit()` dzieje się **raz**, na końcu żądania, w `get_session` — po udanym przebiegu całego use case'a. Dzięki temu całość jest zaklepana jako jedna atomowa transakcja.

**Gdyby repozytorium robiło `commit()`:** każdy `save` zatwierdzałby osobno — nie dałoby się wycofać całego żądania, a poszczególne operacje nie byłyby atomowe. Dlatego to sesja (Unit of Work) zarządza commitem.

### 8.3 Jak działa transakcja na poziomie requestu

Cykl w `get_session`:
1. `async with db.session_factory() as session` — otwarcie sesji = start transakcji.
2. `yield session` — use case wykonuje operacje (INSERT-y, UPDATE-y), ale bez commita.
3. Po zakończeniu (bez wyjątku): `await session.commit()` — zatwierdzenie.
4. Jeśli wyjątek: `await session.rollback()` + `raise` — wycofanie wszystkich zmian.
5. `finally: await session.close()` — zamknięcie.

**Dlaczego to ważne (spójność)?** Jeśli np. zapis klienta się powiedzie, ale zapis tokena nie — bez atomowości mielibyśmy klienta bez tokena (niezdatny stan). Dzięki wspólnej transakcji oba "albo oba, albo żaden" (ACID: Atomicity). Przy błędzie wszystko jest cofnięte, baza nie zostaje w niespójnym stanie.

Mermaid — cykl życia sesji (patrz sekcja 12).

### 8.4 Brak klucza obcego między `refresh_tokens` a `applicants`

W modelu `RefreshTokenModel` kolumna `applicant_id` jest zwykłą kolumną UUID **bez `ForeignKey`**. Dlaczego?

**Zasada "database-per-service" (baza na serwis):** w mikroserwisach każdy serwis ma WŁASNĄ bazę danych i nie ma umów na poziomy schematu z innymi serwisami. Klucz obcy wymagałby, żeby `applicants` (właściciel) i `refresh_tokens` były w tej samej bazie **i** znały się nawzajem. W architekturze rozproszonej:
- Dojrzałe relacje nie są wymuszane przez FK; integralność dba aplikacja (tutaj: `get_by_id` w use case, sprawdzenie istnienia).
- Umożliwia to niezależne skalowanie i ewolucję schematów.
- Twarda reguła projektu: "Serwisy NIE czytają cudzych baz".

Z tego powodu `applicant_id` jest **logicznie** powiązane (to adres UUID klienta), ale nie ma fizycznego constrainta FK. Konsekwencje: brak kaskadowych usuwów; usunięcie klienta nie usuwa automatycznie tokenów (trzeba zadbać o to w kodzie — np. metoda `revoke_all_for_applicant`).

### 8.5 Przykładowe zapytania SQL (podsumowanie)

Zebrane operacje SQL generowane przez SQLAlchemy (asyncpg parametryzuje `$1`...):

- **exists_by_email / get_by_email** (fragment):
  ```sql
  SELECT applicants.id FROM applicants WHERE applicants.email = $1 LIMIT 1;
  ```
- **INSERT applicants:**
  ```sql
  INSERT INTO applicants (id, email, password_hash, first_name, last_name, created_at, updated_at)
  VALUES ($1, $2, $3, $4, $5, $6, $7);
  ```
- **get_all_valid:**
  ```sql
  SELECT refresh_tokens.* FROM refresh_tokens
  WHERE refresh_tokens.expires_at > $1 AND refresh_tokens.revoked_at IS NULL;
  ```
- **UPDATE refresh_token (revoke):**
  ```sql
  UPDATE refresh_tokens SET revoked_at = $2 WHERE refresh_tokens.id = $1;
  ```
- **get_by_id (applicant):**
  ```sql
  SELECT applicants.* FROM applicants WHERE applicants.id = $1 LIMIT 1;
  ```

---

## 9. Testy

Testy dzielą się na:
- **Jednostkowe** (`tests/unit`) — bez bazy, z mockami; szybkie, izolowane.
- **Integracyjne** (`tests/integration`) — z realnym Postgresem przez testcontainers.

Zaczynamy od wyjaśnienia narzędzi: `pytest`, fixtures, `AsyncMock`, `MagicMock`, `pytest.mark.asyncio`.

### 9.1 Narzędzia testowe

- **pytest** — środowisko do pisania i uruchamiania testów. Funkcje `test_*` w plikach `test_*.py` są automatycznie wykrywane.
- **`pytest.mark.asyncio`** — dekorator mówiący pluginowi pytest-asyncio, żeby test `async def` uruchomił w event loop.
- **`@pytest.fixture`** — funkcja tworząca wartość wielokrotnego użytku (np. mocka, encję, use case). Pytest wstrzykuje fixture przez **nazwę parametru** funkcji testowej.
- **`AsyncMock`** — mock dla metod **asynchronicznych**: jego wywołanie zwraca awaitable; `await mock.verify(...)` działa. Metody takie jak `repo.save` są `async`, więc używamy `AsyncMock`.
- **`MagicMock`** — mock dla metod **synchronicznych** (np. `hasher.hash`, `token_service.create_access_token`). Pozwala ustawić `return_value` i asertować wywołania.
- **`pytest.raises(...)`** — menedżer kontekstu sprawdzający, że wyrzucono dany wyjątek.

**Dlaczego testy jednostkowe są izolowane od bazy?**
- **Szybkość** — mock jest natychmiastowy; nie czekamy na sieć.
- **Niezależność** — nie wymagamy działającego Postgresa; testy działają w CI/offline.
- **Determinizm** — kontrolujemy zwracane wartości mocków.
- **Ukierunkowanie** — testujemy LOGIKĘ use case'a, a nie integrację (to rola testów integracyjnych).

### 9.2 Jak działają mocki — dogłębnie (`AsyncMock` vs `MagicMock`)

Aby w pełni zrozumieć testy, warto wiedzieć, jak działają atrapy (mocks) z `unittest.mock`.

#### `MagicMock`
- Dynamiczny obiekt, którego **każdy atrybut/metoda** jest samodzielnym mockiem.
- Wywołanie zwraca kolejny `MagicMock`, chyba że ustawimy `return_value`.
- Przeznaczony dla **synchronicznych** metod (bo zwraca wartość bezpośrednio).

**Przykład:**
```python
hasher = MagicMock()
hasher.hash = MagicMock(return_value="hashed_password")
print(hasher.hash("password123"))  # "hashed_password"
hasher.hash.assert_called_once_with("password123")
```
- `MagicMock(return_value=...)` — ustawia, co ma zwracać.
- `assert_called_once_with(...)` — sprawdza, że metoda została wywołana dokładnie raz z tymi argumentami.
- `call_count` — ile razy wywołano.

#### `AsyncMock`
- Podklasa `MagicMock`, ale **wywołalna asynchronicznie**: `await mock(...)` działa.
- Przeznaczony dla **asynchronicznych** metod (np. `repo.save`, `repo.get_by_email`), które w use case'ie są wywoływane przez `await`.

**Przykład:**
```python
repo = AsyncMock()
repo.exists_by_email = AsyncMock(return_value=False)
result = await repo.exists_by_email("test@example.com")  # False
```

**Co by było, gdyby użyć niewłaściwego mocka?**
- Gdyby `save` (async) był `MagicMock` zamiast `AsyncMock`, to `await repo.save(...)` rzuciłoby `TypeError: object MagicMock can't be used in 'await' expression`.
- Gdyby `hash` (sync, używane przez zwykłe wywołanie) był `AsyncMock`, to `self._password_hasher.hash(password)` zwróciłoby korutynę zamiast stringa → późniejsze operacje (np. konstrukcja encji) dostałyby zły typ.

#### Dlaczego w testach używamy mocków zamiast realnych obiektów?
- **Prędkość:** brak sieci, brak bazy, brak kosztownego hashowania.
- **Kontrola:** możemy wymusić każdy scenariusz (np. `get_by_email` → `None`).
- **Izolacja:** testujemy wyłącznie logikę use case'a, nie infrastrukturę.
- **Determinizm:** nie zależymy od zegara, losowości UUID, czy stanu bazy.

#### Fixtures — jak działają?

`@pytest.fixture` definiuje wartość wielokrotnego użytku. Pytest **wstrzykuje** fixture do funkcji testowej przez nazwę parametru:

```python
@pytest.fixture
def mock_applicant_repo(self):
    return AsyncMock()

async def test_x(self, mock_applicant_repo):
    repo = mock_applicant_repo  # pytest podał wartość
```

- Fixtures mogą zależeć od innych fixtures (`use_case` zależy od wszystkich `mock_*`).
- Domyślnie fixtures są **funkcyjne** (nowa instancja na każdy test) — domyślnie `scope="function"`.
- `scope="session"` oznacza jedną instancję na całą sesję (używane w `conftest.py` testów integracyjnych — np. kontener Postgresa startuje RAZ).

### 9.3 `tests/unit/domain/test_entities.py`

**Cel:** testuje czyste encje domenowe (`Applicant`, `RefreshToken`).

#### `TestApplicant.test_create_applicant`
- Tworzy encję `Applicant(...)`.
- Asercje: pola pasują; `full_name == "John Doe"`; `id is not None` (UUID wygenerowany); `created_at`/`updated_at` to `datetime`.

**Co testowane:** poprawność dataclass, właściwości `full_name`, domyślne fabryki (`id`, `created_at`).

#### `TestApplicant.test_update_timestamp`
- Tworzy encję, zapisuje `old_updated`, robi `time.sleep(0.01)` (mała przerwa, by znacznik się zmienił), woła `update_timestamp()`.
- Asercja: `updated_at > old_updated`.

**Mocks:** brak (czysta domena). `time.sleep` opóźnia wykonanie ~0.01s — wystarczy, by różnica była widoczna.

#### `TestRefreshToken`
- `test_create_refresh_token` — tworzy token; sprawdza `applicant_id`, `token_hash`, `revoked_at is None`.
- `test_is_valid_when_valid` — token z `expires_at` w przyszłości → `is_valid() is True`.
- `test_is_valid_when_expired` — token z `expires_at` w przeszłości → `False`.
- `test_is_valid_when_revoked` — token po `revoke()` → `False`, `revoked_at is not None`.
- `test_revoke` — po `revoke()`, `revoked_at` ustawione i `is_valid() is False`.

**Co testowane:** logika `is_valid` (wygasnięcie + unieważnienie) i `revoke`.

### 9.4 `tests/unit/application/test_register.py`

**Cel:** testuje `RegisterUseCase`.

#### Fixtures
- `mock_applicant_repo` — `AsyncMock`; ustawia `exists_by_email` na `return_value=False`, `save` bez wartości.
- `mock_refresh_token_repo` — `AsyncMock`; `save` bez wartości.
- `mock_password_hasher` — `MagicMock`; `hash` zwraca `"hashed_password"`.
- `mock_token_service` — `MagicMock`; `create_access_token` → `"access_token_123"`, `create_refresh_token` → `"refresh_token_123"`, `get_access_token_expiry`/`get_refresh_token_expiry` bez ustawionych wartości.
- `use_case` — buduje `RegisterUseCase` z powyższych mocków.

**Dlaczego mocki?** Chcemy sprawdzić LOGIKĘ use case'a (sekwencję wywołań), a nie realną bazę/krypto. Mocki dają pełną kontrolę.

#### `test_register_success`
- Tworzy `RegisterRequest` i woła `await use_case.execute(request)`.
- Asercje:
  - wynik jest `TokenResponse`.
  - `access_token == "access_token_123"`, `refresh_token == "refresh_token_123"`, `token_type == "bearer"`.
  - `exists_by_email.assert_called_once_with("test@example.com")`.
  - `hash.call_count == 2` (hash hasła + hash refresh tokena) i `hash.assert_any_call("password123")`.
  - `save` (applicant) wywołane raz; `create_access_token`/`create_refresh_token` po razie; `save` (refresh) raz.

**Co testowane:** pełna sukces ścieżka rejestracji i poprawna kolejność wywołań.

#### `test_register_email_already_exists`
- Ustawia `exists_by_email` na `return_value=True`.
- Woła `execute` w `pytest.raises(EmailAlreadyRegistered)`.
- Asercje: `exc_info.value.email == "existing@example.com"`; `save` NIE wywołane.

**Co testowane:** ścieżka błędu — duplikat e-maila, brak zapisu.

### 9.5 `tests/unit/application/test_login.py`

**Cel:** testuje `LoginUseCase`.

#### `test_login_success`
- `get_by_email` → `applicant`; `verify` → `True`.
- Asercje: `TokenResponse` z `"access_token_123"`/`"refresh_token_123"`; `get_by_email.assert_called_once_with(...)`; `verify.assert_called_once_with("password123", "hashed_password")`; `refresh save` raz.

#### `test_login_user_not_found`
- `get_by_email` → `None`.
- `pytest.raises(InvalidCredentials)`. (Brak użytkownika → ten sam błąd co złe hasło — anty-enumeracja.)

#### `test_login_invalid_password`
- `get_by_email` → `applicant`; `verify` → `False`.
- `pytest.raises(InvalidCredentials)`.

**Co testowane:** sukces + obie ścieżki błędu (brak użytkownika / złe hasło) → ten sam wyjątek.

### 9.6 `tests/unit/application/test_refresh_token.py`

**Cel:** testuje `RefreshTokenUseCase` (rotację).

- Fixtures: `mock_applicant_repo`, `mock_refresh_token_repo`, `mock_password_hasher` (`verify`→True, `hash`→nowy hash), `mock_token_service`, `applicant`, `valid_refresh_token` (ważny token z `expires_at` w przyszłości).

#### `test_refresh_success`
- `get_all_valid` → `[valid_refresh_token]`; `get_by_id` → `applicant`.
- Asercje: wynik `TokenResponse`; `get_all_valid` raz; `verify("valid_refresh_token", "hashed_refresh_token")`; **`valid_refresh_token.revoked_at is not None`** (stary unieważniony — rotacja); `get_by_id(applicant.id)`; `save` (nowy) raz.

#### `test_refresh_token_not_found`
- `get_all_valid` → `[]`.
- `pytest.raises(InvalidRefreshToken)`.

#### `test_refresh_token_expired`
- `get_all_valid` → `[expired_token]`.
- `pytest.raises(RefreshTokenRevoked)`.

#### `test_refresh_token_already_revoked`
- Token po `revoke()` → `pytest.raises(RefreshTokenRevoked)`.

#### `test_refresh_applicant_not_found`
- `get_all_valid` → `[valid_refresh_token]`; `get_by_id` → `None`.
- `pytest.raises(InvalidRefreshToken)`.

**Co testowane:** rotacja, unieważnienie starego, oraz wszystkie ścieżki błędów (brak tokena, wygasły, unieważniony, brak użytkownika).

### 9.7 `tests/unit/application/test_get_me.py`

**Cel:** testuje `GetMeUseCase`.

- `test_get_me_success`: `get_by_id` → `applicant`; asercje na polach `ApplicantResponse`; `get_by_id.assert_called_once_with(applicant.id)`.
- `test_get_me_not_found`: `get_by_id` → `None`; `pytest.raises(ApplicantNotFound)`; `exc_info.value.applicant_id == str(applicant_id)`.

### 9.8 Testy integracyjne (`tests/integration`)

**Cel:** weryfikują współpracę komponentów z REALNYM Postgresem (via testcontainers) i realnym kryptografią (Argon2, JWT).

#### `conftest.py`

- **`postgres_container`** (fixture `scope="session"`): startuje kontener `postgres:16-alpine` przez `testcontainers`; zwraca obiekt kontenera. Działa raz na całą sesję.
- **`database_url`**: `postgres_container.get_connection_url(driver="asyncpg")` — adres bazy z driverem asyncpg.
- **`engine`** (`scope="session"`, `async`): tworzy silnik async (NullPool); tworzy tabele przez `run_sync(Base.metadata.create_all)`; `yield engine`; `dispose`.
- **`session`** (per test, `async`): tworzy sesję przez `async_sessionmaker`; `yield session`.

**Uwaga:** ten setup wykorzystuje `create_all` (nie migracje) — prostsze do testów.

#### `test_auth_flow.py`

Buduje realne obiekty (repo, hasher, token_service) i woła use case'y bezpośrednio (nie przez HTTP), z realną sesją i kluczami.

- **`test_full_auth_flow`**: register → login → get_me → refresh → get_me po odświeżeniu. Sprawdza tokeny i rotację.
  - Uwaga: wymaga kluczy pod `/app/keys/...` — ścieżki twardo wpisane (słabe założenie — patrz FAQ).
- **`test_duplicate_registration_fails`**: drugi register z tym samym e-mailem → wyjątek zawierający "already registered".
- **`test_wrong_password_fails`**: login ze złym hasłem → wyjątek z "invalid".
- **`test_refresh_token_rotation`**: dokładnie testuje, że stary token po rotacji nie działa, a nowy działa.

**Dlaczego testy integracyjne są osobno?** Wymagają Docker + sieci; są wolniejsze. Dlatego w README uruchamia się je osobno (`pytest tests/integration`), a jednostkowe szybko (`pytest tests/unit`).

**Jak uruchomić testy?**
```bash
pytest tests/unit -v      # jednostkowe
pytest tests/integration -v  # integracyjne (wymaga Docker)
pytest -v                 # wszystkie
```

---

## 10. Konfiguracja i uruchamianie

### 10.1 Szybki start — krok po kroku

Z sekcji "Quick Start" w `README.md`:

```bash
make infra-up          # 1. Start infrastruktury
cd services/applicant  # 2. Wejdź do serwisu
cp .env.example .env   # 3. Przygotuj zmienne środowiskowe
pip install -e ".[dev]"    # 4. Zainstaluj zależności
python -m src.infrastructure.security.keygen  # 5. Wygeneruj klucze RSA
alembic upgrade head    # 6. Wykonaj migracje
uvicorn src.main:app --reload --port 8001  # 7. Uruchom serwis
```

1. **`make infra-up`** — z katalogu głównego repo. Uruchamia przez Docker Compose infrastrukturę: Postgres, Redis, Kafka, MinIO, Jaeger. **Dlaczego?** Serwis potrzebuje bazy (Postgres). (Redis/Kafka/MinIO/Jaeger są potrzebne innym serwisom/specyfikacjom, ale w tym projekcie aplikacja korzysta głównie z Postgresa.)

2. **`cd services/applicant`** — przejście do katalogu serwisu.

3. **`cp .env.example .env`** — skopiowanie wzorca do `.env`. **Co zawiera `.env.example`?**
   - `DATABASE_URL=postgresql+asyncpg://applicant_svc:applicant_dev_pw@localhost:5433/applicant_db`
   - `JWT_PRIVATE_KEY_PATH` / `JWT_PUBLIC_KEY_PATH`
   - `ACCESS_TOKEN_EXPIRE_MINUTES=15`, `REFRESH_TOKEN_EXPIRE_DAYS=7`
   - `ARGON2_TIME_COST=3`, `ARGON2_MEMORY_COST=65536`, `ARGON2_PARALLELISM=4`

   **Uwaga:** w kodzie `JWTService` używa zmiennej `KEYS_DIR`, a nie `JWT_PRIVATE_KEY_PATH`/`JWT_PUBLIC_KEY_PATH`. Ponadto parametry Argon2 i czasy tokenów są w kodzie **zakodowane na stałe** (klasy `JWTService` / `Argon2Hasher`), mimo że `.env.example` sugeruje, iż są konfigurowalne. To niespójność dokumentacji vs implementacji — warto ujednolicić.

4. **`pip install -e ".[dev]"`** — instaluje pakiet w trybie editable (rozwój) wraz z dependencyjami dev (pytest, ruff, mypy itd.). `-e .` = praca bez reinstalacji po każdej zmianie.

5. **`python -m src.infrastructure.security.keygen`** — **generuje klucze RSA** (private+public PEM). **Dlaczego potrzebne?** Serwis podpisuje JWT algorytmem **RS256**, który wymaga pary kluczy:
   - Prywatny — do podpisywania tokenów w tym serwisie.
   - Publiczny — do weryfikacji w Gateway i innych serwisach.
   Bez kluczy `JWTService` rzuci błąd przy starcie/próbie stworzenia tokena.

6. **`alembic upgrade head`** — wykonuje migracje bazy (stosuje `0001_initial` tworzącą tabele). **Dlaczego migracje, a nie `create_all`?** Migracje to wersjonowana, powtarzalna zmiana schematu (lepsza kontrola w produkcji); `create_all` z `database.py` jest wygodne do dev, ale w produkcji używa się migracji.

7. **`uvicorn src.main:app --reload --port 8001`** — uruchamia serwer ASGI (Uvicorn) na `src.main:app`, port 8001. `--reload` — auto-restart przy zmianie kodzie (dev).

### 10.2 Zmienne środowiskowe

| Zmienna | Domyślna | Znaczenie |
|---------|----------|-----------|
| `DATABASE_URL` | `postgresql+asyncpg://...` | Adres bazy (asyncpg driver) |
| `KEYS_DIR` | `/app/keys` (lub `./keys`) | Katalog kluczy PEM |
| `JWT_PRIVATE_KEY_PATH` / `JWT_PUBLIC_KEY_PATH` | (w .env.example) | Ścieżki kluczy (obecnie nieużywane wprost w kodzie — patrz uwaga wyżej) |
| `ACCESS_TOKEN_EXPIRE_MINUTES` | 15 (w kodzie) | Czas życia access tokena |
| `REFRESH_TOKEN_EXPIRE_DAYS` | 7 (w kodzie) | Czas życia refresh tokena |
| `ARGON2_TIME_COST/MEMORY_COST/PARALLELISM` | 3/65536/4 (w kodzie) | Parametry Argon2id |

**Zasada projektu:** sekrety tylko przez środowisko; nowa zmienna ⇒ aktualizacja `.env.example`.

### 10.3 Jak działa `keygen.py` (głębiej)

Opisane już w 4.15. Podsumowanie kluczowych punktów:
- `cryptography` generuje klucz RSA 2048 bitów.
- Serializacja do PEM (PKCS8 dla prywatnego, SubjectPublicKeyInfo dla publicznego).
- Zapis plików z uprawnieniami: private `0600`, public `0644`.
- `if __name__ == "__main__"` — brama do uruchomienia jako CLI.

### 10.4 Jak działa `alembic/env.py` (głębiej)

Opisane w 4.25. Podsumowanie:
- Importuje metadane modeli (`Base.metadata`) — Alembic wie o tabelach.
- Tryb offline: generuje SQL bez połączenia.
- Tryb online (async): `async_engine_from_config(...)`, połączenie async, `run_sync(do_run_migrations)`, `asyncio.run(run_async_migrations())`.

`asyncio.run()` uruchamia nowy event loop dla całego procesu migracji — to standardowy wzorzec dla async CLI (bo nie ma tu działającego serwera, tylko stary tryb skryptowy).

---

## 11. Style i dobre praktyki

### 11.1 Przykłady czystej architektury

- **Odwrócenie zależności (Dependency Inversion):** use case'y zależą od **portów** (abstract), a nie konkretów. Adaptery (`SQLAlchemyApplicantRepository`, `Argon2Hasher`, `JWTService`) implementują porty. Kierunek zależności: zewnętrzne → wewnętrzne.
- **Single Responsibility (SRP):** każdy plik ma jedno zadanie: `entities.py` — encje, `exceptions.py` — wyjątki, `register.py` — rejestracja, `repository.py` — persystencja itd.
- **Open/Closed (OCP):** system otwarty na rozszerzanie (nowe adaptery, nowe use case'y), zamknięty na modyfikację istniejącej logiki — bo logika zależy od interfejsów, nie konretów.
- **Dependency Injection:** kompozycja przez konstruktor + kontener FastAPI.
- **Repository + Unit of Work:** abstrakcja dostępu do danych + wspólna transakcja.

### 11.2 `from __future__ import annotations`

Leniwe typowanie — patrz 6.1.5. Daje kompatybilność z Pythonem 3.9, pozwala na natywne generyki i czystsze adnotacje. Bez tego zapisy jak `list[RefreshToken]` czy `str | None` na 3.9 by się nie skompilowały.

### 11.3 `timezone.utc` zamiast `datetime.UTC`

`datetime.UTC` (alias `timezone.utc`) pojawił się w **Pythonie 3.11**. Projekt wspiera Python 3.9 (`requires-python = ">=3.9"`), więc użyto `timezone.utc`, dostępnego od 3.2. To decyzja o **kompatybilności wstecznej**.

### 11.4 `EmailStr` zamiast `str` w DTO

Walidacja formatu na granicy — patrz 6.3.2. Dzięki temu niepoprawne e-maile są odrzucane przed logiką biznesową, a błąd jest czytelny (422).

### 11.5 Obszary do poprawy (wnioski z analizy)

1. **Brak mapowania wyjątków domenowych na HTTP** — wyjątki (`EmailAlreadyRegistered`, `InvalidCredentials`, itd.) nie są łapane; zwracają 500 zamiast 409/401. **Rozwiązanie:** globalny `exception_handler` dla `ApplicantDomainError` (patrz FAQ).
2. **Duplikacja `dto.py` vs `schemas.py`** — te same klasy w dwóch miejscach. **Rozwiązanie:** jedno źródło prawdy.
3. **`get_all_valid` skanuje WSZYSTKIE tokeny** — przy skali należy przejść na wyszukiwanie per-applicant (najpierw zdekoduj `sub` z refresh tokena).
4. **Niespójność `.env.example`** — klucze/parametry zadeklarowane w env, ale na stałe w kodzie.
5. **Testy integracyjne z twardymi ścieżkami `/app/keys/...`** — kruche poza Dockerem.
6. **Blokujące hashowanie Argon2** w event loop — optymalizacja przez `to_thread`/worker.

### 11.6 Wskazówki stylistyczne obecne w kodzie

- Konwencja nazewnicza: prywatne atrybuty `_nazwa`; klasy `PascalCase`; funkcje/zmienne `snake_case`; stałe `UPPER_CASE`.
- Docstringi opisują cel każdej klasy/funkcji.
- Adnotacje typów **wszędzie** (wymóg `mypy --strict`).
- `from __future__ import annotations` w nagłówku prawie każdego pliku.
- Proste, czytelne konstrukcje; unikanie magii.

---

## 12. Dodatki

### 12.1 Diagramy Mermaid

#### 12.1.1 Architektura warstwowa (ze wskazaniem kierunku zależności)

```mermaid
flowchart LR
    subgraph Zew["Zewnętrzne"]
        HTTP["HTTP / Klient"]
        DB[(PostgreSQL)]
        KEYS["Klucze RSA"]
    end

    subgraph API["API"]
        R["routes + dependencies"]
    end

    subgraph INFRA["Infrastructure"]
        P["persistence/repository.py"]
        S["security (argon2, jwt, keygen)"]
    end

    subgraph APP["Application"]
        UC["use cases"]
        PORTS["ports (interfejsy)"]
        DTO["dto"]
    end

    subgraph DOM["Domain"]
        ENT["entities"]
        EXC["exceptions"]
    end

    HTTP --> R
    R --> APP
    R --> INFRA
    APP --> DOM
    INFRA --> APP
    INFRA --> DOM
    P --> DB
    S --> KEYS

    style DOM fill:#dff,stroke:#66c
    style APP fill:#dfd,stroke:#6c6
```

(Zależności zawsze pod prąd: `api → infrastructure → application → domain`.)

#### 12.1.2 Przepływ rejestracji (register)

```mermaid
sequenceDiagram
    participant C as Klient (curl)
    participant F as FastAPI
    participant UC as RegisterUseCase
    participant H as Argon2Hasher
    participant T as JWTService
    participant R as Repository
    participant DB as Baza

    C->>F: POST /api/v1/auth/register {email,password,...}
    F->>F: Walidacja Pydantic (RegisterRequest)
    F->>F: Wstrzykiwanie Depends (session, repo, hasher, token)
    F->>UC: execute(request)
    UC->>R: exists_by_email(email)
    R->>DB: SELECT id FROM applicants WHERE email=?
    DB-->>R: brak
    R-->>UC: False
    UC->>H: hash(password)
    H-->>UC: password_hash
    UC->>UC: twórz Applicant()
    UC->>R: save(applicant)
    R->>DB: INSERT applicants; flush
    UC->>T: create_access_token(id,email)
    T-->>UC: access_token
    UC->>T: create_refresh_token(id)
    T-->>UC: refresh_token
    UC->>H: hash(refresh_token)
    UC->>R: save(refresh_token_entity)
    R->>DB: INSERT refresh_tokens; flush
    UC-->>F: TokenResponse
    F->>F: commit sesji (get_session epilog)
    F-->>C: 201 {access_token, refresh_token, ...}
```

#### 12.1.3 Przepływ logowania (login)

```mermaid
sequenceDiagram
    participant C as Klient
    participant F as FastAPI
    participant UC as LoginUseCase
    participant R as Repository
    participant H as Argon2Hasher
    participant T as JWTService
    participant DB as Baza

    C->>F: POST /api/v1/auth/login {email,password}
    F->>F: Walidacja (LoginRequest) + Depends
    F->>UC: execute(request)
    UC->>R: get_by_email(email)
    R->>DB: SELECT ... FROM applicants WHERE email=?
    DB-->>R: applicant (albo brak)
    R-->>UC: applicant | None
    alt brak klienta
        UC-->>F: raise InvalidCredentials
    else
        UC->>H: verify(password, hash)
        alt nie pasuje
            UC-->>F: raise InvalidCredentials
        else
            UC->>T: create_access_token / create_refresh_token
            UC->>H: hash(refresh_token)
            UC->>R: save(refresh_token)
            UC-->>F: TokenResponse
        end
    end
    F-->>C: 200 tokeny
```

#### 12.1.4 Przepływ odświeżania tokena (refresh / rotacja)

```mermaid
sequenceDiagram
    participant C as Klient
    participant F as FastAPI
    participant UC as RefreshTokenUseCase
    participant R as Repository
    participant H as Argon2Hasher
    participant T as JWTService
    participant DB as Baza

    C->>F: POST /api/v1/auth/refresh {refresh_token}
    F->>F: Walidacja (RefreshTokenRequest) + Depends
    F->>UC: execute(request)
    UC->>R: get_all_valid(now)
    R->>DB: SELECT * FROM refresh_tokens WHERE expires_at>now AND revoked_at IS NULL
    DB-->>R: tokeny
    R-->>UC: [tokens]
    loop każdy token
        UC->>H: verify(refresh_token, token_hash)
        alt pasuje hash
            UC->>UC: token.is_valid? revoke()
            UC->>R: update(token)  # revoked_at
            UC->>R: get_by_id(applicant_id)
            UC->>T: create_new_access_token / new_refresh_token
            UC->>H: hash(new_refresh_token)
            UC->>R: save(new_refresh_token)
            UC-->>F: TokenResponse (nowe tokeny)
        end
    end
    F-->>C: 200 (lub błąd, jeśli nic nie pasowało)
```

#### 12.1.5 Cykl życia sesji bazy (get_session)

```mermaid
sequenceDiagram
    participant F as FastAPI
    participant D as Database/Session
    participant UC as UseCase/Repo
    participant DB as Baza

    F->>D: get_session() otwiera sesję (transakcja start)
    D-->>F: yield session (wstrzyknięcie)
    F->>UC: wykonaj (inserty/update'y, flush -> baza)
    alt sukces
        F->>D: commit()
        D->>DB: COMMIT
    else wyjątek
        F->>D: rollback()
        D->>DB: ROLLBACK
    end
    D->>D: close()  (finally)
```

### 12.2 Tabele porównawcze

#### sync vs async

| Aspekt | sync | async |
|--------|------|-------|
| Model | blokujący wątek | event loop / korutyny |
| I/O | czeka | nie blokuje innych zadań |
| Wydajność przy wielu I/O | niska (jeden wątek) | wysoka |
| Użycie | CPU, szybkie op | I/O (baza, sieć) |
| W kodzie | `argon2.hash`, `jwt.encode` | `session.execute`, `flush` |

#### HS256 vs RS256

| | HS256 | RS256 |
|---|-------|-------|
| Typ | symetryczny | asymetryczny (RSA) |
| Klucz | jeden sekret | para (private/public) |
| Dystrybucja | sekret wspólny | publiczny swobodny |
| Bezpieczeństwo wycieku sekretu | krytyczne | tylko private |
| Wydajność | szybki | wolniejszy |
| Użycie | brak | **wybrane** |

#### bcrypt vs Argon2id

| | bcrypt | Argon2id |
|---|--------|----------|
| Rok | 1999 | 2015 (PHC) |
| Memory-hard | częściowo | w pełni konfig |
| Parametry | cost | time/memory/parallel |
| Odporność GPU | średnia | wysoka |
| Rekomendacja | przestarzała | **standard OWASP** |

#### Access token vs Refresh token

| | Access token | Refresh token |
|---|---|---|
| Żywotność | krótka (15 min) | długa (7 dni) |
| Cel | autoryzacja API | uzyskanie nowego access tokena |
| Przechowywanie | u klienta (in-memory/secure) | u klienta; w bazie tylko HASH |
| Rotacja | nie | tak (po użyciu unieważniany) |
| Zawartość claims | sub+email+type+exp | sub+type+exp |

### 12.3 Glosariusz

| Pojęcie | Proste wyjaśnienie |
|---------|--------------------|
| **JWT** | JSON Web Token — podpisany ciąg, który "poświadcza" tożsamość |
| **Access token** | Krótkożyjący token do autoryzacji API |
| **Refresh token** | Długożyjący token do odnowienia access tokena |
| **Claims** | Pola w ładunku JWT (sub, exp, ...) |
| **Argon2id** | Nowoczesny algorytm hashowania haseł (memory-hard) |
| **Salt** | Losowa wartość uniemożliwiająca tablice tęczowe |
| **ORM** | Object-Relational Mapping — mapowanie tabel na obiekty Pythona |
| **DI** | Dependency Injection — wstrzykiwanie zależności z zewnątrz |
| **Port** | Interfejs (abstrakcyjny kontrakt) |
| **Adapter** | Konkretna implementacja portu |
| **Use Case** | Pojedyncza operacja biznesowa |
| **Repository** | Abstrakcja dostępu do danych (udaje kolekcję) |
| **Unit of Work** | Grupowanie zmian w jedną transakcję |
| **Lazy initialization** | Tworzenie obiektu przy pierwszym użyciu |
| **Singleton** | Jedna instancja w procesie |
| **Clean Architecture** | Architektura z warstwami i zasadą zależności |
| **Dependency Inversion** | Zależność od abstrakcji, nie konretów |
| **Pydantic** | Biblioteka walidacji/serializacji modeli w Pythonie |
| **FastAPI** | Nowoczesny framework web oparty o Pydantic + async |
| **SQLAlchemy** | ORM dla Pythona (2.0 async) |
| **Alembic** | Narzędzie do migracji schematu bazy |
| **asyncpg** | Asynchroniczny driver PostgreSQL |
| **Uvicorn** | Serwer ASGI dla FastAPI |
| **Eastloop / event loop** | Pętla zdarzeń obsługująca współbieżne async |
| **Transaction** | Atomowy zbiór operacji na bazie (commit/rollback) |
| **Rollback** | Wycofanie transakcji przy błędzie |
| **testcontainers** | Narzędzie uruchamiające kontenery do testów |

### 12.4 FAQ

#### "Czy to jest bezpieczne na produkcję?"

**Odpowiedź:** podstawa (Argon2id, RS256, rotacja, hashowanie refresh tokenów) jest DOBRA. Jednak przed produkcją należy:
1. Dodać **globalny handler wyjątków domenowych** → spójne kody HTTP (obecnie zwracają 500).
2. Uwierzytelnić **zmienne środowiskowe** (parametry Argon2/tokenów zamiast hardcode).
3. Zabezpieczyć **klucz prywatny** (nie commituj go; w produkcji menedżer sekretów/KMS).
4. Zweryfikować **uprawnienia plików** i kontenera.
5. Rozważyć **rate limiting** na /login oraz /refresh (ochrona przed brute-force).
6. Dodać **czyszczenie wygasłych refresh tokenów** (sprzątanie tabeli).

#### "Jak to przetestować ręcznie?"

Użyj `curl` (sekcja 5) lub Swagger pod `http://localhost:8001/docs`:
1. Register → otrzymujesz access + refresh.
2. Login → nowe tokeny.
3. GET /me z `Authorization: Bearer <access_token>`.
4. POST /refresh z refresh_token → nowa para.

#### "Co zrobić, gdy zmienię model bazy danych?"

1. Zmień model w `models.py`.
2. `alembic revision --autogenerate -m "opis"` — wygeneruj migrację (lub napisz ręcznie).
3. Przejrzyj wygenerowany plik w `alembic/versions/`.
4. `alembic upgrade head` — zastosuj.
5. Zaktualizuj testy integracyjne (jeśli zależą od schematu).

#### "Jak dodać nowy endpoint?"

1. Dodaj DTO w `application/dto.py` (lub `api/schemas.py` — lepiej jedno źródło).
2. Utwórz use case w `application/use_cases/` (implementujesz przez porty).
3. Dodaj fabrykę w `api/dependencies.py` (jeśli potrzebne nowe zależności).
4. Dodaj route w `api/routes/` (lub nowy router) i dołącz go w `main.py`.
5. Napisz testy (jednostkowe use case'a + ewentualnie integracyjne).

#### "Jak skalować ten serwis?"

- **Poziomo (horyzontalnie):** uruchom wiele instancji za load balancerem (bezstanowe poza bazą; pula NullPool nie współdzieli połączeń; sesje per-request).
- **Baza:** skaluj Postgresa (repliki reads/writes). Rozważ indeksy / partycjonowanie `refresh_tokens`.
- **Zoptymalizuj `get_all_valid`** → wyszukiwanie per-applicant (najpierw dekoduj `sub` z refresh tokena), by uniknąć skanowania całej tabeli.
- **Hashing** — wynieś Argon2 do osobnych wątków/workera, by nie blokować event loop.
- **Czyszczenie** — dodaj TTL/usuwanie wygasłych tokenów.
- **Rate limiting** na auth; **cache/Redis** na gorące ścieżki (jeśli potrzeba).

#### "Dlaczego jest duplikacja `dto.py` i `schemas.py`?"

Prawdopodobnie historyczne rozdzielenie "DTO aplikacji" od "schematów API". W clean architecture dobrze mieć JEDNO źródło prawdy. Rekomendacja: przenieść wszystkie schematy do `application/dto.py` i usuwać `api/schemas.py` (lub odwrotnie — ale wtedy use case'y nie powinny zależeć od warstwy API).

#### "Dlaczego niektóre wyjątki zwracają 500 zamiast 401/409?"

Brak globalnego handlera. **Rozwiązanie** (do wdrożenia):

```python
from fastapi import Request
from fastapi.responses import JSONResponse
from src.domain.exceptions import (
    ApplicantDomainError, EmailAlreadyRegistered, InvalidCredentials,
    ApplicantNotFound, InvalidRefreshToken, RefreshTokenRevoked,
)

_HTTP_MAP = {
    EmailAlreadyRegistered: 409,
    InvalidCredentials: 401,
    ApplicantNotFound: 404,
    InvalidRefreshToken: 401,
    RefreshTokenRevoked: 401,
}

async def domain_exception_handler(request: Request, exc: ApplicantDomainError):
    status = _HTTP_MAP.get(type(exc), 500)
    return JSONResponse(status_code=status, content={"detail": str(exc)})

# w main.py:
# app.add_exception_handler(ApplicantDomainError, domain_exception_handler)
```

Dzięki temu błędy domenowe będą mapowane na czytelne kody HTTP.

---

## 13. Podsumowanie — kluczowe decyzje architektoniczne

1. **Clean Architecture** podzielona na 4 warstwy (domain / application / infrastructure / api) z zasadą zależności skierowaną do wewnątrz — zapewnia testowalność i zamienność komponentów.
2. **Porty i adaptery** — logika biznesowa zależy od interfejsów; infrastruktura (SQLAlchemy, Argon2, JWT) stanowi adaptery.
3. **Dependency Injection** przez konstruktor + kontener FastAPI (`Depends`) — centralny "kompozycyjny root" w `dependencies.py`.
4. **Unit of Work** w `get_session` — jedna transakcja na request (commit/rollback/close), pełna atomowość.
5. **Security by design:** Argon2id do haseł, RS256 asymetryczny dla JWT, hashowanie refresh tokenów, rotacja (unieważnienie + nowy), anty-enumeracja przez jednolity błąd logowania.
6. **Asynchronouszny stos** (FastAPI + asyncpg + SQLAlchemy async) — wysokie wykorzystanie event loop przy I/O.
7. **Testy:** jednostkowe (mockowane, szybkie, izolowane) + integracyjne (testcontainers, realny Postgres) — pokrywające logikę i pełny przepływ auth.
8. **Konfiguracja:** zmienne środowiskowe, klucze RSA z keygen, migracje Alembic, strukturalny logging JSON z `libs/observability`.

**Znane obszary do poprawy:** mapowanie wyjątków domenowych na HTTP, duplikacja DTO, efektywność `get_all_valid`, spójność `.env`, blokujące hashowanie, twarde ścieżki kluczy w testach integracyjnych.

To kompletny, produkcyjnie zorientowany fundament `Applicant Service` w architekturze CrediGuard.

---

*Koniec przewodnika. Dokument opracowany na podstawie pełnej analizy kodu źródłowego serwisu `services/applicant`.*

