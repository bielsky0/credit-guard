# Etap 1 — Applicant Service + Auth: Dokumentacja Implementacji

## Materiał do nauki Pythona — linijka po linijce, ze ścieżkami wywołań

| | |
|---|---|
| **Serwis** | `services/applicant` (Applicant Service) |
| **Port** | `8001` |
| **Co robi** | Rejestracja, logowanie, wydawanie JWT (RS256), refresh token |
| **Cel tego dokumentu** | Wytłumaczyć **każdy plik, każdą linię**: co się wywołuje, czym jest każda konstrukcja Pythona, dlaczego tak, a nie inaczej |
| **Źródło prawdy** | `docs/SPECYFIKACJA.md` §4.2 i §11 Etap 1 |

---

## 0. Szybki start (jak uruchomić)

```bash
# 1. Start infrastruktury (Postgres, Redis, Kafka, MinIO, Jaeger)
make infra-up

# 2. Utwórz topiki Kafka (na później, nie potrzebne w Etap 1)
make topics

# 3. Wygeneruj parę kluczy RSA (raz)
cd services/applicant
python3 -m src.infrastructure.security.keygen   # utworzy ./keys/

# 4. Zainstaluj zależności
python3 -m pip install -e ".[dev]"

# 5. Migracje bazy (Alembic)
python3 -m alembic upgrade head

# 6. Uruchom serwis (KEYS_DIR wskazuje lokalne klucze, nie /app/keys)
KEYS_DIR=./keys python3 -m uvicorn src.main:app --host 0.0.0.0 --port 8001
```

### Testowanie curl

```bash
# Rejestracja
curl -s -X POST http://localhost:8001/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"a@b.com","password":"password123","first_name":"Jan","last_name":"Kowalski"}'

# Login (zwraca access_token + refresh_token)
curl -s -X POST http://localhost:8001/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"a@b.com","password":"password123"}'

# Profil (Bearer <access_token>)
curl -s http://localhost:8001/api/v1/me -H "Authorization: Bearer <access_token>"

# Odświeżenie tokena (rotacja)
curl -s -X POST http://localhost:8001/api/v1/auth/refresh \
  -H "Content-Type: application/json" \
  -d '{"refresh_token":"<refresh_token>"}'
```

---

## 1. Struktura projektu i ścieżka nauki

```
services/applicant/
├── src/
│   ├── domain/                     # WARSTWA 1 — czysty Python, zero frameworków
│   │   ├── entities.py             #   Encje: Applicant, RefreshToken
│   │   ├── exceptions.py           #   Wyjątki domenowe
│   │   └── __init__.py
│   ├── application/                # WARSTWA 2 — logika biznesowa (use case'y)
│   │   ├── ports/                  #   Interfejsy (porty) — DB, hasher, JWT
│   │   │   ├── repository.py
│   │   │   ├── password_hasher.py
│   │   │   └── token_service.py
│   │   ├── use_cases/              #   Use case'y (operacje biznesowe)
│   │   │   ├── register.py
│   │   │   ├── login.py
│   │   │   ├── refresh_token.py
│   │   │   └── get_me.py
│   │   └── dto.py                  #   DTO (Data Transfer Objects)
│   ├── infrastructure/             # WARSTWA 3 — implementacje portów
│   │   ├── persistence/            #   SQLAlchemy (ORM)
│   │   │   ├── models.py
│   │   │   └── repository.py
│   │   ├── security/               #   Kryptografia
│   │   │   ├── argon2_hasher.py
│   │   │   ├── jwt_service.py
│   │   │   └── keygen.py
│   │   └── database.py             #   Sesje bazodanowe
│   ├── api/                        # WARSTWA 4 — FastAPI (HTTP)
│   │   ├── dependencies.py         #   Kontener DI (wstrzykiwanie zależności)
│   │   ├── schemas.py
│   │   └── routes/
│   │       ├── auth.py
│   │       └── me.py
│   └── main.py                     # Wejście — tworzy aplikację FastAPI
├── alembic/
│   ├── env.py
│   └── versions/0001_initial.py
├── tests/
│   └── unit/
│       ├── domain/test_entities.py
│       └── application/{test_register,test_login,test_refresh_token,test_get_me}.py
├── pyproject.toml
├── alembic.ini
├── Dockerfile
└── keys/                           # Wygenerowane klucze RSA (nie commitować!)
```

### Zasada zależności (Clean Architecture)

```
api → application → domain
infrastructure → application (porty)
domain → (nic! tylko stdlib)
```

Kluczowa reguła: **warstwa wewnętrzna nie wie o zewnętrznej**.
- `domain` importuje tylko stdlib (żadnych FastAPI/SQLAlchemy).
- `application` zna tylko **interfejsy** (porty), nie ich implementacje.
- `infrastructure` dostarcza implementacje portów.
- `api` tylko łączy to wszystko (wstrzykiwanie zależności).

### Zalecana ścieżka nauki

1. `domain/entities.py` — czysty Python, bez frameworków (najłatwiejsze)
2. `domain/exceptions.py`
3. `application/ports/*` — interfejsy (co, NIE jak)
4. `application/use_cases/register.py` — logika łącząca porty
5. `application/use_cases/{login,refresh_token,get_me}.py`
6. `infrastructure/persistence/models.py` + `repository.py` — implementacje
7. `infrastructure/security/*` — hashowanie + JWT
8. `infrastructure/database.py` — sesje
9. `api/dependencies.py` — spinanie (DI)
10. `api/routes/*` — HTTP
11. `main.py` — tworzenie aplikacji
12. `alembic/env.py` + migracja — baza danych

---

## 2. Architektura — przepływ danych i wywołań

### 2.1 Diagram przepływu rejestracji (Mermaid)

```mermaid
sequenceDiagram
    participant Client
    participant Route as api/routes/auth.py
    participant DI as api/dependencies.py
    participant UC as application/use_cases/register.py
    participant Port as application/ports/repository.py
    participant Repo as infrastructure/repository.py
    participant DB as PostgreSQL
    participant Hasher as Argon2Hasher
    participant JWT as JWTService

    Client->>Route: POST /api/v1/auth/register {json}
    Route->>DI: Depends(get_register_use_case)
    DI->>UC: RegisterUseCase(repo, token_repo, hasher, jwt)
    UC->>Port: applicant_repo.exists_by_email(email)
    Port->>Repo: SQLAlchemyApplicantRepository.exists_by_email()
    Repo->>DB: SELECT id FROM applicants WHERE email=$1
    DB-->>Repo: (brak rekordu)
    Repo-->>Port: False
    Port-->>UC: False
    UC->>Hasher: hash(password)  # Argon2id
    Hasher-->>UC: password_hash
    UC->>UC: Applicant(...)  # powstaje encja
    UC->>Port: applicant_repo.save(applicant)
    Port->>Repo: save()
    Repo->>DB: INSERT INTO applicants ...
    UC->>JWT: create_access_token(id, email)
    JWT-->>UC: access_token
    UC->>JWT: create_refresh_token(id)
    UC->>Hasher: hash(refresh_token)  # hash tokena do DB
    UC->>Port: refresh_token_repo.save(entity)
    UC-->>Route: TokenResponse(access, refresh, expires_in)
    Route-->>Client: 201 Created
```

### 2.2 Co się wywołuje po kolei (perspektywa użytkownika)

```
curl POST /api/v1/auth/register
        │
        ▼
uvicorn ─── src.main:app  (ładuje FastAPI)
        │
        ▼
FastAPI dopasowuje ścieżkę → routes/auth.py:register()
        │
        ▼
Depends(get_register_use_case)
        │  rozwiązuje zależności (patrz §9 dependencies.py):
        │   ├─ get_applicant_repo → SQLAlchemyApplicantRepository(session)
        │   ├─ get_refresh_token_repo → SQLAlchemyRefreshTokenRepository(session)
        │   ├─ get_password_hasher → Argon2Hasher()  (globalny singleton)
        │   └─ get_token_service → JWTService()      (globalny singleton)
        ▼
RegisterUseCase.execute(request: RegisterRequest)
        │
        ├─ 1. exists_by_email(email)  ──► repozytorium ──► SELECT
        ├─ 2. hash(password)           ──► Argon2Hasher
        ├─ 3. Applicant(email, hash, ...)   ← tworzy ENTITY (dataclass)
        ├─ 4. save(applicant)          ──► repozytorium ──► INSERT
        ├─ 5. create_access_token()    ──► JWTService (RS256)
        ├─ 6. create_refresh_token()   ──► JWTService
        ├─ 7. hash(refresh_token)      ──► Argon2Hasher
        ├─ 8. RefreshToken(...)        ← tworzy entity tokena
        ├─ 9. save(refresh_token)      ──► repozytorium ──► INSERT
        └─ 10. zwróć TokenResponse
        │
        ▼
FastAPI serializuje TokenResponse → JSON  (response_model=TokenResponse)
        │
        ▼
201 Created {access_token, refresh_token, token_type, expires_in}
```

Kluczowa obserwacja: **use case nie wie, że baza to PostgreSQL** — zna tylko
interfejs `ApplicantRepository`. To jest *Dependency Inversion*.

---

## 3. Słownik konstrukcji Pythona użytych w projekcie

| Konstrukcja | Gdzie | Wyjaśnienie |
|-------------|-------|-------------|
| `from __future__ import annotations` | wszystkie pliki | PEP 563 — typowanie "leniwe"; pozwala używać `list[str]`, `X \| None` w Pythonie 3.9 |
| `@dataclass` | `entities.py` | Automatycznie generuje `__init__`, `__repr__`, `__eq__` dla klasy |
| `field(default_factory=...)` | `entities.py` | Prawidłowy sposób na domyślne wartości *obiektów* (unika współdzielonego `[]`/`None`) |
| `datetime.now(timezone.utc)` | wszędzie | Czas *świadomy strefy* (aware) — unika błędu naive vs aware |
| `Optional[X]` | wszędzie | To samo co `X \| None`, ale działa w Pythonie 3.9 |
| `@property` | `entities.py` | Metoda dostępna jak atrybut: `obj.full_name` zamiast `obj.full_name()` |
| `CCC` + `@abstractmethod` | `ports/` | Abstrakcyjna klasa bazowa; wymusza implementację metod w podklasach |
| `async def` / `await` | wszędzie | Programowanie asynchroniczne; `await` czeka na zadanie bez blokowania |
| `AsyncGenerator` | `database.py`, `main.py` | Typ dla async generatorów (funkcji z `yield`) |
| `@asynccontextmanager` | `database.py`, `main.py` | Async menedżer kontekstu; `yield` = granica setup/teardown |
| `BaseModel` / Pydantic | `dto.py`, `schemas.py` | Walidacja + serializacja danych, automatyczny OpenAPI |
| `EmailStr` | `dto.py` | Waliduje format adresu e-mail |
| `Field(min_length=...)` | `dto.py` | Reguły walidacji per pole |
| `timezone.utc` | wszędzie | Stała strefy UTC (w 3.11+ jest też `datetime.UTC`) |
| `Depends(...)` | `dependencies.py`, routes | Wstrzykiwanie zależności w FastAPI |
| `APIRouter()` | routes | Rozdziela endpointy na moduły |
| `@router.post(...)` | routes | Dekorator rejestrujący endpoint |
| `response_model=...` | routes | Jaki schemat Pydantic zwrócić (serializacja) |
| `global` | `dependencies.py` | Porusza zmienną modułową (singleton) |
| `status.HTTP_201_CREATED` | routes | Stałe kodów HTTP |
| `async with` | `database.py` | Async menedżer kontekstu — automatycznie zamyka sesję |
| `list[X]` | ports | Generyczna lista (od 3.9 wbudowane) |
| `tuple[UUID, str]` | porty | Tuple z adnotacjami typów |
| `Optional[datetime] = None` | `entities.py` | Parametr opcjonalny |

Dla każdego pliku poniżej są szczegółowe "Pitfall / Our Solution / Why" oraz tabele
"Python Concepts".

---

---

# CZĘŚĆ A — WARSTWA DOMAIN (czysty Python)

## A1. `src/domain/entities.py` — Encje domenowe

### Cel
Niezmienne (w sensie reguł) obiekty biznesowe. **Nie importują żadnego frameworka** —
ani FastAPI, ani SQLAlchemy. To gwarancja, że logika domenowa da się testować
w izolacji i że nie cieknie do warstwy zależności.

### Co ten plik definiuje
- `Applicant` — kandydat/klient (aggregate root w DDD)
- `RefreshToken` — encja tokena odświeżającego (przechowywany jako hash)

### Kto to wywołuje
- `RegisterUseCase.execute()` — tworzy `Applicant(...)` i `RefreshToken(...)`
- `LoginUseCase.execute()` — tworzy `RefreshToken(...)`
- `RefreshTokenUseCase.execute()` — tworzy `RefreshToken(...)`, woła `is_valid()` i `revoke()`
- `GetMeUseCase.execute()` — czyta pola `Applicant`

---

### Linijka po linijce

```python
# ── Linia 1: docstring modułu — opisuje, co robi ten plik ──
"""Domain entities for Applicant Service."""

# ── Linia 3: PEG 563 — "leniwe" typowanie ──
from __future__ import annotations
# Bez tego Python opóźniałby ewaluację typów → w 3.9 nie dałoby się pisać
# np. `revoked_at: Optional[datetime]`. Dzięki temu adnotacje są stringami
# i Pythona nie obchodzi, czy typ istnieje w momencie definicji klasy.

# ── Linie 5-8: importy ──
from dataclasses import dataclass, field
#   dataclass = dekorator generujący __init__/__repr__/__eq__
#   field = funkcja do precyzyjnego ustawiania pola (np. default_factory)
from datetime import datetime, timezone
#   datetime = klasa czasu; timezone.utc = stała strefy UTC (obiekt tzinfo)
from typing import Optional
#   Optional[X] ≡ Union[X, None] ≡ X | None (działa w 3.9)
from uuid import UUID, uuid4
#   UUID = typ (adnotacja); uuid4() = funkcja zwracająca losowy UUID v4


# ── Linia 11: dekorator dataclass ──
@dataclass
class Applicant:
    # @dataclass po cichu generuje:
    #   __init__(self, email, password_hash, first_name, last_name, id=..., ...)
    #   __repr__(self)  → "Applicant(email='...', ...)"
    #   __eq__(self, other)  → porównuje wszystkie pola

    """Applicant aggregate root."""
    # "Aggregate root" (DDD) = główna encja, przez którą operujemy na grafu.

    # ── Linie 15-18: wymagane pola (bez wartości domyślnej = trzeba podać) ──
    email: str
    #   Adnotacja `str` to tylko "obietnica" dla mypy/IDE. Walidacja FORMATU
    #   (czy to poprawny e-mail) dzieje się na granicy API (Pydantic EmailStr).
    password_hash: str
    #   Przechowujemy HASH hasła, NIGDY hasła w plaintext! Hash tworzy Argon2.
    first_name: str
    last_name: str

    # ── Linia 19: pole z automatycznym generowaniem UUID ──
    id: UUID = field(default_factory=uuid4)
    #   default_factory=OCZEKUJE_FUNKCJI → wywołuje ją dla KAŻDEJ instancji.
    #   ⛔ BŁĄD: `default=uuid4()` → wywołałoby funkcję RAZ przy definicji klasy,
    #   więc WSZYSTKIE instancje dostałyby ten sam UUID!

    # ── Linie 20-21: znaczniki czasu (timezone-aware) ──
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    #   lambda = anonimowa funkcja opóźniona (wykonywana przy tworzeniu instancji,
    #   nie przy ładowaniu modułu). timezone.utc ⇒ czas jest "aware" (ma tzinfo),
    #   więc porównania i odejmowania są bezpieczne.

    # ── Linie 23-25: metoda aktualizacji czasu ──
    def update_timestamp(self) -> None:
        """Update the updated_at timestamp."""
        self.updated_at = datetime.now(timezone.utc)
    #   self = bieżąca instancja (implizytnie przekazywana, nie podajemy).
    #   Ta metoda MUTUJE obiekt (dataclass jest mutable/zmienna).

    # ── Linie 27-30: właściwość (property) ──
    @property
    def full_name(self) -> str:
        """Return full name."""
        return f"{self.first_name} {self.last_name}"
    #   @property pozwala czytać `applicant.full_name` BEZ nawiasów.
    #   Tu nie ma settera → pole jest tylko do odczytu (read-only computed).
    #   f-string → formatowanie: f"{pierwsze} {drugie}"


# ============ Druga encja ============

# ── Linia 33 ──
@dataclass
class RefreshToken:
    """Refresh token entity (stored hashed)."""

    # ── Linie 37-39: wymagane ──
    applicant_id: UUID          # do kogo należy token
    token_hash: str             # HASH tokena (nigdy goły token!)
    expires_at: datetime        # kiedy wygasa

    # ── Linie 40-42 ──
    id: UUID = field(default_factory=uuid4)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    revoked_at: Optional[datetime] = None
    #   revoked_at = kiedy unieważniono; None = nadal aktywny.
    #   Optional[datetime] = None → domyślnie "nie unieważniony".

    # ── Linie 44-48: walidacja stanu ──
    def is_valid(self, now: Optional[datetime] = None) -> bool:
        """Check if token is valid (not expired, not revoked)."""
        if now is None:
            now = datetime.now(timezone.utc)
        return self.revoked_at is None and self.expires_at > now
    #   Sprawdza: NIE został unieważniony ORAZ nie wygasł.
    #   Parametr `now` pozwala wstrzyknąć czas w testach (determinizm!).
    #   `self.revoked_at is None` – porównanie z singletonem None (nie ==).
    #   `and` – krótka ocena: jeśli pierwszy warunek False, drugiego nie liczy.

    # ── Linie 50-52: unieważnienie ──
    def revoke(self) -> None:
        """Mark token as revoked."""
        self.revoked_at = datetime.now(timezone.utc)
    #   Ustawia revoked_at na "teraz". Później repozytorium zapisze to do DB.
```

---

### Python Concepts Used Here

| Koncepcja | Linia | Wyjaśnienie |
|-----------|-------|-------------|
| `@dataclass` | 11, 33 | Generuje boilerplate (`__init__`, `__repr__`, `__eq__`) |
| `field(default_factory=...)` | 19-21, 40-41 | Wywołuje funkcję per instancja — unika mutowalnej wartości domyślnej |
| `lambda` | 20-21, 41 | Anonimowa funkcja — defered execution |
| `@property` | 27 | Czytanie jako atrybut; computed field |
| `f-string` | 30 | Formatowanie łańcucha |
| `timezone.utc` | 20, 21, 25, 47, 52 | Świadomy strefy czas | 
| `Optional[X]` | 42, 44 | Połączenie `X` i `None` |
| `parameter default = None` | 44 | Wstrzykiwanie czasu do testów |

---

### Pitfalls / Our Solution / Why

| ❌ Pitfall | ✅ Our Solution | 💡 Why |
|-----------|----------------|--------|
| `default=uuid4()` | `default_factory=uuid4` | Tak zapisane `uuid4()` odpala się raz; wszystkie obiekty miałyby ten sam UUID |
| `default=[]` (lista) | `default_factory=list` | Mutable default jest współdzielone między instancjami |
| `datetime.now()` (naive) | `datetime.now(timezone.utc)` | Naive vs aware datetime → błąd porównania/odejmowania |
| `datetime.UTC` alias | `timezone.utc` | `datetime.UTC` istnieje dopiero w 3.11 |
| `slots=True` w `@dataclass` | brak (3.9) | `slots` w dataclass dostępne od 3.10 |
| `X \| None` w adnotacji | `Optional[X]` | Operator `\|` dla typów od 3.10 |
| `revoked_at == None` | `revoked_at is None` | `is None` — porównanie tożsamości z singletonem; `==` może być przeładowane |

---

## A2. `src/domain/exceptions.py` — Wyjątki domenowe

### Cel
Hierarchia wyjątków specyficznych dla domeny. Pozwala warstwie aplikacji/api
rozpoznać konkretny błąd biznesowy (np. `EmailAlreadyRegistered`) i odpowiednio
go obsłużyć (np. zwrócić 409), bez sprawdzania stringów.

### Kto wywołuje (rzuca)
| Wyjątek | Gdzie rzucany | Kiedy |
|---------|---------------|-------|
| `EmailAlreadyRegistered` | `register.py:33` | e-mail już istnieje |
| `InvalidCredentials` | `login.py:34,37` | zły e-mail lub hasło |
| `RefreshTokenRevoked` | `refresh_token.py:37` | token już użyty/unieważniony |
| `InvalidRefreshToken` | `refresh_token.py:44,70` | zły/nieznany/wygasły token |
| `ApplicantNotFound` | `get_me.py:22` | nie znaleziono kandydata |

---

### Linijka po linijce

```python
# ── Linia 3 ──
from __future__ import annotations

# ── Linia 6: klasa bazowa ──
class ApplicantDomainError(Exception):
    """Base exception for applicant domain errors."""
#   Wszystkie wyjątki domenowe dziedziczą po tej jednej → łapiąc ją,
#   łapiesz wszystkie błędy domenowe naraz (polimorfizm).

# ── Linie 10-15 ──
class ApplicantNotFound(ApplicantDomainError):
    """Raised when applicant is not found."""

    def __init__(self, applicant_id: str) -> None:
        self.applicant_id = applicant_id          # zapamiętuje kontekst
        super().__init__(f"Applicant not found: {applicant_id}")
    #   Nadpisujemy __init__, żeby: (1) zapisać dane (self.applicant_id),
    #   (2) wywołać super().__init__(msg) z czytelną wiadomością.
    #   `super()` = obiekt klasy nadrzędnej (Exception).

# ── Linie 18-23 ──
class EmailAlreadyRegistered(ApplicantDomainError):
    """Raised when email is already registered."""

    def __init__(self, email: str) -> None:
        self.email = email
        super().__init__(f"Email already registered: {email}")

# ── Linie 26-30 ──
class InvalidCredentials(ApplicantDomainError):
    """Raised when login credentials are invalid."""

    def __init__(self) -> None:
        super().__init__("Invalid email or password")
    #   Nie chcemy zdradzać, czy złe jest hasło, czy e-mail (bezpieczeństwo!).
    #   Ta sama wiadomość dla obu → utrudnia zgadywanie kont.

# ── Linie 33-44 ── (analogiczne; bez przechowywania danych)
class InvalidRefreshToken(ApplicantDomainError):
    """Raised when refresh token is invalid or expired."""
    def __init__(self) -> None:
        super().__init__("Invalid or expired refresh token")

class RefreshTokenRevoked(ApplicantDomainError):
    """Raised when refresh token has been revoked (used already)."""
    def __init__(self) -> None:
        super().__init__("Refresh token has been revoked")
```

---

### Python Concepts Used Here

| Koncepcja | Linia | Wyjaśnienie |
|-----------|-------|-------------|
| Dziedziczenie klas | 6, 10, 18, 26, 33, 40 | `class X(Base)` — klasa dziedziczy metodami/atrybutami |
| `super().__init__(...)` | 15, 23, 30, 37, 44 | Wywołanie konstruktora klasy nadrzędnej |
| `self.attr = ...` | 14, 22 | Zapisanie stanu do instancji |

---

### Pitfalls / Our Solution / Why

| ❌ Pitfall | ✅ Our Solution | 💡 Why |
|-----------|----------------|--------|
| Rzucanie gołego `Exception` | Własna hierarchia `ApplicantDomainError` | API może precyzyjnie mapować na kody HTTP |
| Zdradzanie "Wrong password" | Jedno `InvalidCredentials` dla e-mail i hasła | Zapobiega enumeracji użytkowników |
| Complicated `__init__` bez super() | `super().__init__(msg)` | Utrzymuje poprawny `str(exc)` i traceback |

---

---

# CZĘŚĆ B — WARSTWA APPLICATION (logika biznesowa)

## B1. Porty — `src/application/ports/`

### Czym jest "port"?
Port to **interfejs** (kontrakt) — mówi *co* umiemy robić, nie *jak*.
Implementacja ("adapter") żyje w `infrastructure/`. Dzięki temu:
- use case zależy od interfejsu, a nie od konkretnej bazy → można podmienić DB
- testujemy użyciem fake'ów (jak w `tests/unit`)

### B1.1 `ports/repository.py` — repozytoria

```python
from __future__ import annotations

from abc import ABC, abstractmethod          # ABC = klasa abstrakcyjna
from datetime import datetime
from typing import Optional
from uuid import UUID

from src.domain.entities import Applicant, RefreshToken


class ApplicantRepository(ABC):
    """Port for applicant persistence."""

    @abstractmethod
    async def save(self, applicant: Applicant) -> None:
        """Save applicant."""
    #   @abstractmethod = wymusza implementację w podklasie.
    #   `async def` → metoda asynchroniczna (zwraca "przyszłość").
    #   -> None ≡ nic nie zwraca (tylko skutek uboczny: zapis).

    @abstractmethod
    async def get_by_id(self, applicant_id: UUID) -> Optional[Applicant]:
        """Get applicant by ID."""
    #   Optional[Applicant] → może zwrócić Applicant albo None (nie znaleziono).

    @abstractmethod
    async def get_by_email(self, email: str) -> Optional[Applicant]:
        """Get applicant by email."""

    @abstractmethod
    async def exists_by_email(self, email: str) -> bool:
        """Check if applicant with email exists."""
    #   same bool → szybkie sprawdzenie istnienia bez ściągania całego rekordu.


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
    #   update ≠ save: save = nowy rekord, update = podmiana istniejącego.

    @abstractmethod
    async def get_all_valid(self, now: datetime) -> list[RefreshToken]:
        """Get all valid (not expired, not revoked) refresh tokens."""
    #   list[RefreshToken] — lista encji (generyczna adnotacja).
    #   `now` zewnętrzne → testowalne czas.
```

---

### B1.2 `ports/password_hasher.py`

```python
from __future__ import annotations

from abc import ABC, abstractmethod


class PasswordHasher(ABC):
    """Port for password hashing."""

    @abstractmethod
    def hash(self, password: str) -> str:
        """Hash a password."""
    #   Zwraca string (hash gotowy do zapisu w DB).

    @abstractmethod
    def verify(self, password: str, password_hash: str) -> bool:
        """Verify a password against its hash."""
    #   Zwraca True/False czy podane hasło pasuje do hasha.
    #   RÓŻNE operacje: hash() tworzy, verify() potwierdza.
```

---

### B1.3 `ports/token_service.py`

```python
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
    #   Zwraca krotkę (tuple): (id, email). tuple z adnotacjami typów.

    @abstractmethod
    def get_access_token_expiry(self) -> datetime:
        """Get expiry datetime for access token."""

    @abstractmethod
    def get_refresh_token_expiry(self) -> datetime:
        """Get expiry datetime for refresh token."""
    #   expiry zwracane, żeby use case mógł wyliczyć expires_in (sekundy).
```

---

### Python Concepts (porty)

| Koncepcja | Wyjaśnienie |
|-----------|-------------|
| `ABC` + `@abstractmethod` | Wymusza implementację; klasa z metodą abstrakcyjną nie da się instancjonować |
| `async def` w interfejsie | Port mówi, że operacja jest asynchroniczna |
| `Optional[X]` jako "nie znaleziono" | Konwencja: zwróć None zamiast rzucać wyjątek przy braku |
| `tuple[UUID, str]` | Zwracanie wielu wartości jako krotka |
| `list[X]` | Generyczna kolekcja z typem elementu |

---

### Pitfalls / Our Solution / Why (porty)

| ❌ Pitfall | ✅ Our Solution | 💡 Why |
|-----------|----------------|--------|
| Use case importuje `SQLAlchemyApplicantRepository` | Use case zależy od `ApplicantRepository` (ABC) | Odwrócenie zależności — wymienialne implemtacje |
| `get_by_email` rzuca, gdy nie ma | Zwraca `Optional` (None) | Brak rekordu jest normalnym przypadkiem, nie błędem |
| Jeden wielki interfejs "Repository" | Dwa małe: Applicant + RefreshToken | Interface Segregation — mniejsze interfejsy, łatwiejsze fake'y |

---

## B2. `src/application/dto.py` — DTO (Data Transfer Objects)

### Cel
Pydantic `BaseModel` = obiekty, które krążą między warstwami i API. Różnią się od
encji domenowych: DTO mają **walidację** (Pydantic), encje mają **reguły biznesowe**
(dataclass). Podział DTO/Encja = Separation of Concerns.

### Kto wywołuje
- `RegisterUseCase.execute(request: RegisterRequest)` — przyjmuje request
- `LoginUseCase`, `RefreshTokenUseCase` — to samo
- Routes używają ich jako `response_model` do serializacji

```python
from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field


class RegisterRequest(BaseModel):
    """Request to register a new applicant."""

    email: EmailStr
    #   EmailStr — Pydantic WALIDUJE format e-maila po wejściu.
    #   (różnica vs str: str przyjmuje wszystko, EmailStr tylko poprawne e-maile)
    password: str = Field(min_length=8, max_length=128)
    #   Field(min_length=8) → błąd walidacji, gdy hasło krótsze niż 8 znaków.
    first_name: str = Field(min_length=1, max_length=100)
    last_name: str = Field(min_length=1, max_length=100)


class LoginRequest(BaseModel):
    email: EmailStr
    password: str          # Bez Field — logowanie nie wymusza długości (stare hasła)


class RefreshTokenRequest(BaseModel):
    refresh_token: str


class TokenResponse(BaseModel):
    """Response with access and refresh tokens."""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"    # domyślna wartość → nie trzeba podawać
    expires_in: int               # sekundy do wygaśnięcia access tokena


class ApplicantResponse(BaseModel):
    """Applicant profile response."""
    id: UUID
    email: EmailStr
    first_name: str
    last_name: str
    created_at: datetime

    class Config:
        from_attributes = True
    #   W Pydantic v2 zamiast Config:from_attributes — ale stare ulepszenie
    #   działa. from_attributes=True pozwala tworzyć DTO z atrybutów obiektu
    #   (np. encji Applicant). (Nowa składnia: model_config = ConfigDict(from_attributes=True))
```

---

### Python Concepts (DTO)

| Koncepcja | Wyjaśnienie |
|-----------|-------------|
| `BaseModel` | Bazowa klasa Pydantic: walidacja, serializacja, `model_dump()` |
| `EmailStr` | Walidacja e-maila (wymaga pakiety `email-validator`) |
| `Field(min_length, max_length)` | Reguły walidacji per pole |
| `default` wartości | `token_type: str = "bearer"` — opcjonalne pole z domyślną |
| `class Config: from_attributes = True` | Pozwala utworzyć z obiektu (ORM/entity) |

### Pitfalls / Our Solution / Why (DTO)

| ❌ Pitfall | ✅ Our Solution | 💡 Why |
|-----------|----------------|--------|
| Logowanie wymusza min_length hasła | `LoginRequest.password: str` bez Field | Użytkownik mógł mieć konto sprzed zmiany reguł — nigdy nie odrzucaj logowania dla starego hasła |
| Używanie `str` zamiast `EmailStr` | `EmailStr` | Darmowa walidacja formatu na granicy |
| Encja domenowa jako schema API | Rozdziel DTO (`dto.py`) i encję (`entities.py`) | Widok API może różnić się od modelu domenowego |

---

---

## B3. Use case'y — `src/application/use_cases/`

### Wzorzec "Use Case"
Każda operacja biznesowa to **osobna klasa z jedną metodą `execute()`**.
Zalety: SRP (jedna odpowiedzialność), łatwe testy, jawny przepływ.
Zależności wstrzykiwane przez **konstruktor** (`__init__`), nie tworzone
wewnątrz — dzięki temu testy podają fake'i (np. `AsyncMock`).

---

### B3.1 `use_cases/register.py` — rejestracja

#### Call flow
```
POST /auth/register
  → RegisterUseCase.__init__(applicant_repo, refresh_token_repo, hasher, jwt)
  → RegisterUseCase.execute(RegisterRequest)
      ├─ applicant_repo.exists_by_email()      → port / repozytorium → SELECT
      ├─ hasher.hash(password)                  → Argon2 → password_hash
      ├─ Applicant(...)                          → tworzy ENCJĘ
      ├─ applicant_repo.save(applicant)         → INSERT
      ├─ jwt.create_access_token(id, email)     → RS256
      ├─ jwt.create_refresh_token(id)           → RS256
      ├─ hasher.hash(refresh_token)             → hash tokena do DB
      ├─ RefreshToken(...)                       → encja tokena
      ├─ refresh_token_repo.save(token)         → INSERT
      └─ zwróć TokenResponse
```

```python
from __future__ import annotations

from datetime import datetime, timezone            # do liczenia expires_in

from src.application.dto import RegisterRequest, TokenResponse   # DTO
from src.application.ports.password_hasher import PasswordHasher  # PORT (interfejs)
from src.application.ports.repository import ApplicantRepository, RefreshTokenRepository
from src.application.ports.token_service import TokenService
from src.domain.entities import Applicant, RefreshToken           # ENCJE
from src.domain.exceptions import EmailAlreadyRegistered          # WYJĄTEK


class RegisterUseCase:
    """Use case for applicant registration."""

    def __init__(
        self,
        applicant_repo: ApplicantRepository,            # port — nie implem!
        refresh_token_repo: RefreshTokenRepository,
        password_hasher: PasswordHasher,
        token_service: TokenService,
    ) -> None:
        self._applicant_repo = applicant_repo
        #   Podkreślnik _ = konwencja "prywatne" (mypy/IDE to respektuje,
        #   Python technicznie nie blokuje).
        self._refresh_token_repo = refresh_token_repo
        self._password_hasher = password_hasher
        self._token_service = token_service

    async def execute(self, request: RegisterRequest) -> TokenResponse:
        """Execute registration."""

        # 1) Sprawdzamy czy e-mail już istnieje (wczesny błąd)
        if await self._applicant_repo.exists_by_email(request.email):
            raise EmailAlreadyRegistered(request.email)
        #   `await` — czekamy na async repozytorium.
        #   Jeśli istnieje → rzucamy wyjątek DZIEDZICZĄCY po ApplicantDomainError.

        # 2) Hashujemy hasło (szyfrowanie jednokierunkowe)
        password_hash = self._password_hasher.hash(request.password)

        # 3) Tworzymy encję Applicant (dataclass z domain)
        applicant = Applicant(
            email=request.email,
            password_hash=password_hash,   # hash, NIGDY gołe hasło
            first_name=request.first_name,
            last_name=request.last_name,
        )

        # 4) Zapisujemy (INSERT) przez port
        await self._applicant_repo.save(applicant)

        # 5) Generujemy tokeny JWT
        access_token = self._token_service.create_access_token(applicant.id, applicant.email)
        refresh_token = self._token_service.create_refresh_token(applicant.id)

        # 6) Hashujemy refresh tokena przed zapisem
        refresh_token_hash = self._password_hasher.hash(refresh_token)

        # 7) Tworzymy encję tokena
        refresh_token_entity = RefreshToken(
            applicant_id=applicant.id,
            token_hash=refresh_token_hash,
            expires_at=self._token_service.get_refresh_token_expiry(),
        )

        # 8) Zapisujemy (INSERT)
        await self._refresh_token_repo.save(refresh_token_entity)

        # 9) Liczymy ile sekund do wygaśnięcia access tokena
        return TokenResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type="bearer",                        # (domyślne, ale jawnie)
            expires_in=int(
                (
                    self._token_service.get_access_token_expiry()
                    - datetime.now(timezone.utc)          # aware datetime!
                ).total_seconds()
            ),
        )
        #   expires_in = (moment wygaśnięcia) - (teraz), w sekundach.
        #   int() ucina ułamki.
        #   ⚠️ Obie strony odejmowania muszą być aware (stąd timezone.utc).
```

---

#### Python Concepts (register)

| Koncepcja | Linia | Wyjaśnienie |
|-----------|-------|-------------|
| `if ...: raise` | 32-33 | Wczesny błąd (fail fast) przed kosztowną operacją |
| `await` | 32, 44, 55 | Czekanie na async operację bez blokowania |
| Konstruktor DI | 18-28 | Zależności wstrzykiwane, nie tworzone — łatwo testować |
| Kodowanie czasu | 62 | `datetime.now(timezone.utc)` → comparison safe |
| `.total_seconds()` | 64 | Różnica timedelta → liczba sekund jako float |

#### Pitfalls / Our Solution / Why (register)

| ❌ Pitfall | ✅ Our Solution | 💡 Why |
|-----------|----------------|--------|
| Expose nazwa użytkownika już istnieje | `EmailAlreadyRegistered` (wł. wyjątek) | API może zwrócić 409 |
| Zapisywanie gołego hasła | `hasher.hash()` przed zapisem | Argon2id — jednokierunkowe |
| Zapisywanie gołego refresh tokena | `hasher.hash(refresh_token)` | Atak na DB nie ujawnia tokenów |
| `datetime.now()` bez tz | `datetime.now(timezone.utc)` | Naive vs aware subtractions crash/VL |

---

### B3.2 `use_cases/login.py` — logowanie

#### Call flow
```
POST /auth/login
  → LoginUseCase.execute(LoginRequest)
      ├─ applicant_repo.get_by_email(email)   → SELECT ... WHERE email
      ├─ jeśli brak → InvalidCredentials
      ├─ hasher.verify(password, hash)        → True/False
      ├─ jeśli False → InvalidCredentials (TA sama wiadomość)
      ├─ jwt.create_access_token / create_refresh_token
      ├─ hasher.hash(refresh_token)
      ├─ RefreshToken(...) + refresh_token_repo.save()
      └─ TokenResponse
```

```python
from __future__ import annotations
from datetime import datetime, timezone

from src.application.dto import LoginRequest, TokenResponse
from src.application.ports.password_hasher import PasswordHasher
from src.application.ports.repository import ApplicantRepository, RefreshTokenRepository
from src.application.ports.token_service import TokenService
from src.domain.entities import RefreshToken
from src.domain.exceptions import InvalidCredentials


class LoginUseCase:
    def __init__(self, applicant_repo, refresh_token_repo, password_hasher, token_service) -> None:
        self._applicant_repo = applicant_repo
        self._refresh_token_repo = refresh_token_repo
        self._password_hasher = password_hasher
        self._token_service = token_service

    async def execute(self, request: LoginRequest) -> TokenResponse:
        applicant = await self._applicant_repo.get_by_email(request.email)
        if applicant is None:
            raise InvalidCredentials()          # NIE mówimy "brak użytkownika"
        if not self._password_hasher.verify(request.password, applicant.password_hash):
            raise InvalidCredentials()          # ta sama wiadomość
        # (reszta → tokeny, jak w register)
```

**Ważne bezpieczeństwo**: oba błędy rzucają `InvalidCredentials` z tą samą
wiadomością. Gdybyśmy zdradzili "No account with this email", atakujący mógłby
sprawdzać, które e-maile istnieją (user enumeration).

---

### B3.3 `use_cases/refresh_token.py` — obrót tokenem

#### Call flow
```
POST /auth/refresh
  → RefreshTokenUseCase.execute(RefreshTokenRequest)
      ├─ get_all_valid(now)              → wszystkie aktywne tokeny (SELECT)
      ├─ for each: hasher.verify(token, hash)
      │     ├─ jeśli pasuje:
      │     │    ├─ is_valid(now)?  jeśli nie → RefreshTokenRevoked
      │     │    ├─ revoke() + repo.update()         ← unieważniamy STARY
      │     │    ├─ get_by_id(applicant)             → applicant
      │     │    ├─ create_access_token + create_refresh_token  ← NOWE
      │     │    ├─ hasher.hash(new_refresh)
      │     │    ├─ RefreshToken(...) + repo.save()  ← INSERT nowego
      │     │    └─ TokenResponse
      └─ (brak pasującego) → InvalidRefreshToken
```

```python
async def execute(self, request: RefreshTokenRequest) -> TokenResponse:
    now = datetime.now(timezone.utc)                     # jeden "teraz" do calus

    # Iterujemy po wszystkich ważnych tokenach i szukamy tego, co pasuje:
    for token_entity in await self._refresh_token_repo.get_all_valid(now):
        if self._password_hasher.verify(request.refresh_token, token_entity.token_hash):
            # znaleźliśmy dopasowanie
            if not token_entity.is_valid(now):           # podwójna kontrola
                raise RefreshTokenRevoked()

            token_entity.revoke()                        # ustaw revoked_at
            await self._refresh_token_repo.update(token_entity)   # UPDATE DB

            applicant = await self._applicant_repo.get_by_id(token_entity.applicant_id)
            if applicant is None:
                raise InvalidRefreshToken()

            # Generowanie nowych tokenów (rotacja)...
            new_access_token = self._token_service.create_access_token(applicant.id, applicant.email)
            new_refresh_token = self._token_service.create_refresh_token(applicant.id)
            new_refresh_token_hash = self._password_hasher.hash(new_refresh_token)
            new_refresh_token_entity = RefreshToken(
                applicant_id=applicant.id,
                token_hash=new_refresh_token_hash,
                expires_at=self._token_service.get_refresh_token_expiry(),
            )
            await self._refresh_token_repo.save(new_refresh_token_entity)

            return TokenResponse(access_token=new_access_token, refresh_token=new_refresh_token,
                                 expires_in=int((self._token_service.get_access_token_expiry()
                                                  - datetime.now(timezone.utc)).total_seconds()))

    raise InvalidRefreshToken()      # nic nie pasowało
```

**Dlaczego rotacja + unieważnienie?** Jeśli ktoś ukradł refresh token i używa
go, my mimo to zwracamy NOWY token — ale STARY jest już unieważniony.
Gdy złodziej spróbuje użyć starego PONOWNIE, dostanie `RefreshTokenRevoked`
(nie zadziała). Plus: wykrywamy replay.

---

### B3.4 `use_cases/get_me.py` — profil

```python
async def execute(self, applicant_id: UUID) -> ApplicantResponse:
    applicant = await self._applicant_repo.get_by_id(applicant_id)
    if applicant is None:
        raise ApplicantNotFound(str(applicant_id))
    # Mapa encja → DTO (jawna, bez magii):
    return ApplicantResponse(
        id=applicant.id,
        email=applicant.email,
        first_name=applicant.first_name,
        last_name=applicant.last_name,
        created_at=applicant.created_at,
    )
```

---

### Pitfalls / Our Solution / Why (use case'y ogółem)

| ❌ Pitfall | ✅ Our Solution | 💡 Why |
|-----------|----------------|--------|
| Logika w API route | Logika w use case (`execute`) | Testowalne bez HTTP; SRP |
| Rzucanie surowych wyjątków | Wyjątki domenowe | Precyzyjne mapowanie na HTTP |
| Zdradzanie, czy e-mail istnieje | Jeden `InvalidCredentials` | Anty-username-enumeration |
| `for ... in await repo...` łapie całość | `for x in await get_all_valid(...)` | Pobiera wszystkie, filtruje w Pythonie (małe dane) |
| Stary refresh token wciąż działa | `revoke()` + `update()` przed wydaniem nowego | Rotacja — replay odpada |

---

---

# CZĘŚĆ C — WARSTWA INFRASTRUCTURE (implementacje)

## C1. `infrastructure/persistence/models.py` — modele SQLAlchemy

### Cel
Mapowanie tabel PostgreSQL na klasy Pythona (ORM). Tu definiujemy, jak wygląda
schema w DB. Używamy **async SQLAlchemy 2.0** (styl `Mapped`/`mapped_column`).

### Co wywołuje
- `Base.metadata.create_all` w `database.create_all()` → tworzy tabele
- `alembic/env.py` → `target_metadata = Base.metadata` (migracje)
- Repozytoria → `select(ApplicantModel)`, `ApplicantModel(...)`

```python
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import DateTime, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PG_UUID   # alias, bo UUID koliduje
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all models."""
    pass
    # 2.0-style deklaratywny Base. Każdy model dziedziczy po nim.


class ApplicantModel(Base):
    __tablename__ = "applicants"                    # nazwa tabeli w DB

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    #   Mapped[UUID] = adnotacja typu kolumny (2.0 styl).
    #   PG_UUID(os_uuid=True) → kolumna typu UUID w Postgres, zwraca UUID (nie string).
    #   primary_key=True → klucz główny.
    #   default=uuid4 → domyślny UUID generowany po stronie Pythona.

    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    #   String(255) → VARCHAR(255); unique=True → UNIQUE (nie da się duplikować)
    #   nullable=False → NOT NULL; index=True → indeks (szybsze wyszukiwanie).

    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    first_name: Mapped[str] = mapped_column(String(100), nullable=False)
    last_name: Mapped[str] = mapped_column(String(100), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),                     # timestamptz (ze strefą!)
        default=lambda: datetime.now(timezone.utc),  # default po stronie Pythona
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc), # auto-aktualizacja przy UPDATE
        nullable=False,
    )


class RefreshTokenModel(Base):
    __tablename__ = "refresh_tokens"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    applicant_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False, index=True)
    #   UWAGA: NIE ma foreign key do applicants — celowo!
    #   W database-per-service nie ma FK między bazami.
    token_hash: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True),
                                                 default=lambda: datetime.now(timezone.utc), nullable=False)
    revoked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    #   nullable=True → kolumna może być NULL (token nieunieważniony).

    __table_args__ = (
        UniqueConstraint("token_hash", name="uq_refresh_tokens_token_hash"),  # dodatkowy UNIQUE z nazwą
    )
    #   W linii 53 mieliśmy już unique=True; to ASCII podwójne zabezpieczenie
    #   z jawną nazwą dla migracji. (W praktyce jedno wystarczy; tu pokazane.)
```

### Python Concepts (models)

| Koncepcja | Wyjaśnienie |
|-----------|-------------|
| `DeclarativeBase` | Baza dla modeli deklaratywnych (SQLAlchemy 2.0) |
| `Mapped[X]` | Zmodernizowana adnotacja kolumny |
| `mapped_column(...)` | Definicja kolumny z parametrami (typ, unique, nullable...) |
| `import UUID as PG_UUID` | Alias rozważa kolizję nazw (UUID z uuid, UUID z Postgres) |
| `onupdate=` | Automatyczna aktualizacja pola przy UPDATE rekordu |
| `lambda:` jako default | SQLAlchemy wykonuje je przy INSERT |

### Pitfalls / Our Solution / Why

| ❌ Pitfall | ✅ Our Solution | 💡 Why |
|-----------|----------------|--------|
| Kolumna `datetime` bez strefy | `DateTime(timezone=True)` | `timestamptz` — poprawne porównania |
| FK między bazami | Brak FK (`applicant_id` zwykły UUID) | database-per-service: bazy nie mogą się znać |
| `UUID` jako `String` | `PG_UUID(as_uuid=True)` | Natywny typ UUID w Postgres, wydajniejszy |
| Stary styl `Column`/`declarative_base()` | `DeclarativeBase` + `Mapped` | Modern SQLAlchemy 2.0 |

---

## C2. `infrastructure/persistence/repository.py` — implementacje portów

### Cel
To **adaptery** realizujące porty `ApplicantRepository` / `RefreshTokenRepository`
za pomocą SQLAlchemy. Use case'y znają tylko interfejsy — tu działamy na DB.

### Kto wywołuje
- Use case'y (przez porty)
- `api/dependencies.py` tworzy je z sesji: `SQLAlchemyApplicantRepository(session)`

### C2.1 ApplicantRepository

```python
class SQLAlchemyApplicantRepository(ApplicantRepository):   # dziedziczy po PORCIE
    def __init__(self, session: AsyncSession) -> None:
        self._session = session     # sesja współdzielona zależnościami

    async def save(self, applicant: Applicant) -> None:
        # Konwersja encja domenowa → model ORM:
        model = ApplicantModel(
            id=applicant.id,
            email=applicant.email,
            password_hash=applicant.password_hash,
            ...
        )
        self._session.add(model)          # dodaj do sesji (pending)
        await self._session.flush()       # wyślij INSERT (bez COMMIT!)
    #   add() = kolejka; flush() = wykonaj SQL; COMMIT zostaje na końcu requestu
    #   (patrz database.get_session()). To pozwala operować w transakcji.

    async def get_by_id(self, applicant_id: UUID) -> Optional[Applicant]:
        stmt = select(ApplicantModel).where(ApplicantModel.id == applicant_id)
        #   select(Model) = konstrukcja zapytania (not SQL string)
        result = await self._session.execute(stmt)     # wykonaj
        model = result.scalar_one_or_none()            # 0 lub 1 wiersz
        if model is None:
            return None
        return self._to_entity(model)                  # model → encja

    async def exists_by_email(self, email: str) -> bool:
        stmt = select(ApplicantModel.id).where(ApplicantModel.email == email)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None
    #   Pobiera TYLKO id (odciąża DB) i sprawdza istnienie.

    def _to_entity(self, model: ApplicantModel) -> Applicant:
        # Mapper ORM → encja domenowa (czysty typ dataclass)
        return Applicant(id=model.id, email=model.email, ...)
```

### C2.2 RefreshTokenRepository

```python
async def save(self, token: RefreshToken) -> None:        # INSERT
    model = RefreshTokenModel(id=token.id, ...)
    self._session.add(model)
    await self._session.flush()

async def update(self, token: RefreshToken) -> None:      # UPDATE istniejącego
    model = await self._session.get(RefreshTokenModel, token.id)  # pobierz po PK
    if model is None:
        raise ValueError(f"Refresh token with id {token.id} not found")
    model.token_hash = token.token_hash      # zmieniamy atrybuty
    model.expires_at = token.expires_at
    model.revoked_at = token.revoked_at
    await self._session.flush()              # UPDATE (dirty-tracking wykrywa zmiany)

async def get_all_valid(self, now: datetime) -> list[RefreshToken]:
    # select gdzie: expires_at > now ORAZ revoked_at IS NULL
    stmt = select(RefreshTokenModel).where(
        RefreshTokenModel.expires_at > now,
        RefreshTokenModel.revoked_at.is_(None),
    )
    result = await self._session.execute(stmt)
    return [self._to_entity(m) for m in result.scalars().all()]
    #   .revoked_at.is_(None) = SQL "IS NULL"; NIE == None (to byłoby "= NULL" = zawsze fałsz)
```

### Python Concepts (repository)

| Koncepcja | Wyjaśnienie |
|-----------|-------------|
| Dziedziczenie portu | Implementacja musi spełnić wszystkie @abstractmethod |
| `select(Model)` | Wbudowany builder zapytań (SQLAlchemy Core) |
| `.where(...)` | Warunek WHERE |
| `scalar_one_or_none()` | Zwraca jeden wiersz albo None (nie rzuci przy braku) |
| `flush()` vs `commit()` | flush = SQL wykonany; commit = trwały zapis |
| `session.get(Model, pk)` | Szybki lookup po kluczu głównym |
| `.is_(None)` | SQL `IS NULL` (poprawne; `== None` daje `IS NULL` błędnie? tu poprawnie, ale jawnie) |
| List comprehension | `[f(x) for x in obiekt]` — nowa lista |

### Pitfalls / Our Solution / Why

| ❌ Pitfall | ✅ Our Solution | 💡 Why |
|-----------|----------------|--------|
| `commit()` w repo | `flush()` w repo, commit w `get_session()` | Granica transakcji to request; repo nie decyduje o commit |
| `revoked_at == None` w SQL | `revoked_at.is_(None)` | W SQL `= NULL` jest zawsze FALSE; trzeba `IS NULL` |
| `add()` bez flush | `flush()` po add | Od razu wykonuje INSERT, łapie błędy wcześnie |
| Użycie `save` do aktualizacji | osobne `update()` | save = INSERT (dublet PK!), update = UPDATE |

---

## C3. `infrastructure/security/argon2_hasher.py` — hashowanie

### Cel
Adapter portu `PasswordHasher`. Używa **Argon2id** (memory-hard, odporny na GPU).

### Kto wywołuje
- Use case'y (przez port `PasswordHasher`)

```python
import argon2
from argon2 import PasswordHasher as Argon2PasswordHasher   # inne (lib)
from src.application.ports.password_hasher import PasswordHasher as PasswordHasherPort

class Argon2Hasher(PasswordHasherPort):     # implementuje NASZ port
    def __init__(self) -> None:
        self._hasher = Argon2PasswordHasher(
            time_cost=3,        # liczba przebiegów (iteracji)
            memory_cost=65536,  # 64 MiB pamięci (oporność na ASIC/GPU)
            parallelism=4,      # liczba wątków
            hash_len=32,        # długość wyjściowego hasha (bajty)
            salt_len=16,        # długość soli (przypadkowość)
        )

    def hash(self, password: str) -> str:
        return self._hasher.hash(password)     # zwraca string w formacie $argon2id$...

    def verify(self, password: str, password_hash: str) -> bool:
        try:
            self._hasher.verify(password_hash, password)  # kolejność: (hash, password)!
            return True
        except argon2.exceptions.VerifyMismatchError:
            return False        # złe hasło → False (nie rzucamy)
        except argon2.exceptions.HashingError:
            return False        # uszkodzony hash → False
```

### Czym różni się funkcja skrótu (hash) od szyfrowania?
- **Hash** = jednokierunkowy (nie da się odtworzyć hasła), zawsze ta sama długość.
- **Argon2** ma "koszt" (memory + time) → wolniejszy → ataki brute-force droższe.
- **Salt** (sól) = losowa wartość dodawana do hasła → identyczne hasła dają różne hashe.

### Pitfalls / Our Solution / Why

| ❌ Pitfall | ✅ Our Solution | 💡 Why |
|-----------|----------------|--------|
| bcrypt (72-bajtowy limit, brak memory-hard) | argon2id | Odporniejszy na GPU/ASIC |
| `verify(password, hash)` (zła kolejność) | `verify(hash, password)` | API argon2 wymaga (hash, guess) |
| Pozwolenie na wyciek wyjątku | łapanie `VerifyMismatchError`/`HashingError` → False | Nie zdradzaj przyczyn przez stack trace |
| Hash bez parametrów kosztu | jawne `time_cost`, `memory_cost` | Kontrola bezpieczeństwa |

---

## C4. `infrastructure/security/jwt_service.py` — JWT (RS256)

### Cel
Adapter portu `TokenService`. Tworzy i weryfikuje JWT podpisane **RS256**
(asymetrycznie: prywatny klucz podpisuje, publiczny weryfikuje).

### Kto wywołuje
- Use case'y (port `TokenService`)
- `api/dependencies.get_current_applicant_id` → `decode_access_token`

```python
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

from jose import jwt
from jose.exceptions import JWTError


class JWTService(TokenService):
    ALGORITHM = "RS256"                     # algorytm podpisu (asymetryczny)
    ACCESS_TOKEN_EXPIRE_MINUTES = 15        # access: 15 minut
    REFRESH_TOKEN_EXPIRE_DAYS = 7           # refresh: 7 dni

    def __init__(self, private_key_path: str | None = None, public_key_path: str | None = None):
        keys_dir = Path(os.getenv("KEYS_DIR", "/app/keys"))
        #   Docker → /app/keys; lokalnie → KEYS_DIR=./keys
        private_key_path = private_key_path or str(keys_dir / "private_key.pem")
        public_key_path = public_key_path or str(keys_dir / "public_key.pem")
        self._private_key = Path(private_key_path).read_bytes()   # odczyt klucza
        self._public_key = Path(public_key_path).read_bytes()

    def create_access_token(self, applicant_id: UUID, email: str) -> str:
        now = datetime.now(timezone.utc)
        expire = now + timedelta(minutes=self.ACCESS_TOKEN_EXPIRE_MINUTES)
        payload = {                       # "claims"
            "sub": str(applicant_id),     # subject — do kogo token
            "email": email,
            "type": "access",             # typ tokena (rozróżniamy access/refresh)
            "iat": int(now.timestamp()),  # issued-at (unix)
            "exp": int(expire.timestamp()), # expires-at (unix)
        }
        return jwt.encode(payload, self._private_key, algorithm=self.ALGORITHM)
    #   jwt.encode (python-jose) → podpisuje kluczem PRYWATNYM (tylko tu!)

    def create_refresh_token(self, applicant_id: UUID) -> str:
        # analogicznie: sub, type=refresh, iat, exp (7 dni)
        ...

    def decode_access_token(self, token: str) -> tuple[UUID, str]:
        try:
            payload = jwt.decode(token, self._public_key, algorithms=[self.ALGORITHM])
            #   Weryfikacja podpisu KLUCZEM PUBLICZNYM (może robić gateway!)
            if payload.get("type") != "access":
                raise JWTError("Invalid token type")     # odrzucamy refresh tu
            applicant_id = UUID(payload["sub"])
            email = payload["email"]
            return applicant_id, email
        except (JWTError, KeyError, ValueError) as e:
            raise JWTError(f"Invalid access token: {e}") from e
            #  `from e` → prawidłowe łańcuchowanie wyjątków (B904)

    def get_access_token_expiry(self) -> datetime:
        return datetime.now(timezone.utc) + timedelta(minutes=self.ACCESS_TOKEN_EXPIRE_MINUTES)
    def get_refresh_token_expiry(self) -> datetime:
        return datetime.now(timezone.utc) + timedelta(days=self.REFRESH_TOKEN_EXPIRE_DAYS)
```

### Czym jest JWT w środku? (przykład)
```
eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.   ← header (base64): alg=RS256, typ=JWT
eyJzdWIiOiI...fQ.                        ← payload (claims)
<podpis>                                  ← podpisane kluczem prywatnym
```

### RS256 vs HS256 (dlaczego RS?)
| | HS256 | RS256 |
|---|-------|-------|
| Symetria | jeden wspólny sekret | para kluczy (private/public) |
| Podpis | tym samym sekretem | private key |
| Weryfikacja | tym samym sekretem | public key |
| Ryzyko | sekret w wielu serwisach | private key TYLKO w Applicant |

W tym projekcie: private key trzyma tylko Applicant Service; Gateway i inni
otrzymują **public key** do weryfikacji (spec §4.2).

### Pitfalls / Our Solution / Why

| ❌ Pitfall | ✅ Our Solution | 💡 Why |
|-----------|----------------|--------|
| HS256 wspólny sekret | RS256 | Kompromitacja sekretu = wszystkie serwisy |
| Odrzucanie refresh tokena w `decode_access_token` | sprawdza `type == "access"` | Refresh i access to różne tokeny |
| `raise ... from` pomijane | `raise ... from e` | Zachowuje oryginalny wyjątek (łańcuch) |
| Klucze ciężko zakodowane | `os.getenv("KEYS_DIR", "/app/keys")` | Działa i w Dockerze, i lokalnie |

---

## C5. `infrastructure/security/keygen.py` — generowanie kluczy RSA

### Cel
Jednorazowy skrypt tworzący parę kluczy do RS256:
- `private_key.pem` (600 — tylko Applicant)
- `public_key.pem` (644 — dla Gateway)

### Wywołanie
```bash
python3 -m src.infrastructure.security.keygen
```

```python
import os
from pathlib import Path
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

KEYS_DIR = Path(os.getenv("KEYS_DIR", "./keys"))
PRIVATE_KEY_PATH = KEYS_DIR / "private_key.pem"
PUBLIC_KEY_PATH = KEYS_DIR / "public_key.pem"

def generate_keys() -> None:
    KEYS_DIR.mkdir(parents=True, exist_ok=True)   # utwórz katalog

    private_key = rsa.generate_private_key(
        public_exponent=65537,   # standardowa wartość (bezpieczna, szybka)
        key_size=2048,           # 2048 bitów (minimum produkcyjne)
    )

    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,       # format PEM (tekst)
        format=serialization.PrivateFormat.PKCS8,  # standardowy format klucza
        encryption_algorithm=serialization.NoEncryption(),  # bez hasła
    )
    public_key = private_key.public_key()          # wyprowadź publiczny
    public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,  # SPKI
    )

    PRIVATE_KEY_PATH.write_bytes(private_pem)      # zapisz binarnie
    PUBLIC_KEY_PATH.write_bytes(public_pem)
    PRIVATE_KEY_PATH.chmod(0o600)                  # rw------- (tylko właściciel)
    PUBLIC_KEY_PATH.chmod(0o644)                   # rw-r--r--

if __name__ == "__main__":       # wykonuje się TYLKO przy bezpośrednim uruchomieniu
    generate_keys()              # (nie przy import! ważne dla czytelności)
```

### `if __name__ == "__main__":`
Ten idiom sprawia, że kod wykonuje się tylko gdy uruchamiamy plik jako skrypt,
a nie gdy ktoś go importuje. Dlatego `python3 -m ...keygen` działa, a import nie
odpala generacji kluczy.

---

## C6. `infrastructure/database.py` — sesje bazodanowe

### Cel
Tworzy silnik, fabrykę sesji i dostarcza sesję przez FastAPI DI.
Zawiera też commit/rollback/close — **jedno miejsce zarządzania transakcją**.

### Kto wywołuje
- `main.py` (lifespan) → `init_database()` / `close_database()`
- `dependencies.py` → `Depends(get_session)` (każdy request dostaje sesję)

```python
from collections.abc import AsyncGenerator
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool


class Database:
    def __init__(self, url: str) -> None:
        self._engine = create_async_engine(url, poolclass=NullPool)
        #   NullPool: nie trzymaj puli połączeń (prosto w dev; każda sesja 1 połączenie)
        self._session_factory = async_sessionmaker(
            self._engine, class_=AsyncSession, expire_on_commit=False
        )
        #   expire_on_commit=False → po commit obiekty NIE tracą atrybutów
        #   (wygodne, gdy odczytujemy dane po zapisie)

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        return self._session_factory

    async def create_all(self) -> None:
        from src.infrastructure.persistence.models import Base   # import wewnątrz (opóźniony)
        async with self._engine.begin() as conn:        # transakcja
            await conn.run_sync(Base.metadata.create_all)  # utwórz tabele
        #   run_sync: uruchom FUNKCJĘ SYNCHRONICZNĄ na wątku (create_all jest sync)

    async def close(self) -> None:
        await self._engine.dispose()    # zamknij wszystkie połączenia


_db: Optional[Database] = None       # global (singleton)

def get_database() -> Database:
    assert _db is not None, "Database not initialized"
    return _db
    #   assert dla "programming error" — jeśli używamy przed init, to błąd kodu.

async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for database session."""
    db = get_database()
    async with db.session_factory() as session:   # tworzy nową sesję
        try:
            yield session            # <-- przekazuje sesję do use case
            await session.commit()   # po sukcesie: trwały zapis
        except Exception:
            await session.rollback() # po błędzie: wycofaj
            raise
        finally:
            await session.close()    # zawsze zamknij

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

### Dlaczego `yield` + commit tu, a nie w `save()/flush()`?
- Repozytorium robi `flush()` (wysyła SQL, jest w transakcji).
- `get_session()` po `yield` robi `commit()` (trwale zatwierdza) lub `rollback()`.
- Dzięki temu **cały request to jedna transakcja**: jeśli use case rzuci wyjątek,
  wszystko się wycofuje. Odpowiedzialność za transakcję jest w JEDNYM miejscu.

### Pitfalls / Our Solution / Why

| ❌ Pitfall | ✅ Our Solution | 💡 Why |
|-----------|----------------|--------|
| commit w każdej metodzie repo | commit raz w `get_session()` | Spójna transakcja na request; rollback przy błędzie |
| `foo ` przy braku init | `assert _db is not None` | Wykrywamy błąd programisty wcześnie |
| Trzymanie puli w dev | `NullPool` | Prostsze, bez odwiecznych połączeń |
| `expire_on_commit=True` | `False` | Obiekty czytelne po commicie |
| Jedna wielka funkcja utrzymująca wszystko | `init/close/get_session` rozdzielone | SRP, testowalność |

---

---

# CZĘŚĆ D — WARSTWA API (FastAPI) + main

## D1. `api/dependencies.py` — Kontener DI (wstrzykiwanie zależności)

### Cel
Centralne miejsce, które tworzy/spina wszystkie zależności i udostępnia je
FastAPI przez `Depends(...)`. Dzięki temu use case'y nie tworzą nic samodzielnie —
dostają gotowe instancje.

### Call flow
```
FastAPI → Depends(get_register_use_case)
        → get_register_use_case(...)
            → get_applicant_repo → Depends(get_session) → SQLAlchemyApplicantRepository
            → get_refresh_token_repo → Depends(get_session)
            → get_password_hasher → Argon2Hasher()  [singleton]
            → get_token_service → JWTService()      [singleton]
        → RegisterUseCase(applicant_repo, refresh_token_repo, hasher, jwt)
```

```python
from __future__ import annotations
from typing import Optional
from uuid import UUID

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

# sterowanie: application / infrastructure / api
from src.application.ports.token_service import TokenService
from src.application.use_cases.get_me import GetMeUseCase
from src.application.use_cases.login import LoginUseCase
from src.application.use_cases.refresh_token import RefreshTokenUseCase
from src.application.use_cases.register import RegisterUseCase
from src.infrastructure.database import get_session
from src.infrastructure.persistence.repository import (
    SQLAlchemyApplicantRepository, SQLAlchemyRefreshTokenRepository)
from src.infrastructure.security.argon2_hasher import Argon2Hasher
from src.infrastructure.security.jwt_service import JWTService

# Singletony (tworzone przy pierwszym użyciu)
_token_service: Optional[TokenService] = None
_argon2_hasher: Optional[Argon2Hasher] = None


def get_token_service() -> TokenService:
    global _token_service            # odnosimy się do zmiennej modułowej
    if _token_service is None:
        _token_service = JWTService()   # utwórz raz, potem zwracaj to samo
    return _token_service
    #   Wzorzec "lazy singleton": tworzy przy pierwszym dostępie, potem cache.


def get_password_hasher() -> Argon2Hasher:
    global _argon2_hasher
    if _argon2_hasher is None:
        _argon2_hasher = Argon2Hasher()
    return _argon2_hasher


def get_applicant_repo(session: AsyncSession = Depends(get_session)) -> SQLAlchemyApplicantRepository:
    #   Depends(get_session) → FastAPI tworzy sesję i wstrzykuje ją tutaj
    return SQLAlchemyApplicantRepository(session)

def get_refresh_token_repo(session: AsyncSession = Depends(get_session)) -> SQLAlchemyRefreshTokenRepository:
    return SQLAlchemyRefreshTokenRepository(session)


def get_register_use_case(
    applicant_repo: SQLAlchemyApplicantRepository = Depends(get_applicant_repo),
    refresh_token_repo: SQLAlchemyRefreshTokenRepository = Depends(get_refresh_token_repo),
    password_hasher: Argon2Hasher = Depends(get_password_hasher),
    token_service: TokenService = Depends(get_token_service),
) -> RegisterUseCase:
    #   Składanie use case'a z gotowych zależności.
    return RegisterUseCase(applicant_repo, refresh_token_repo, password_hasher, token_service)
    #   (analogicznie get_login_use_case, get_refresh_token_use_case, get_get_me_use_case)


async def get_current_applicant_id(
    authorization: Optional[str] = Header(None, alias="Authorization"),
    token_service: TokenService = Depends(get_token_service),
) -> UUID:
    """Extract and validate applicant ID from Bearer token."""
    #   Header(None, alias="Authorization") → czyta nagłówek HTTP
    if authorization is None or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Missing or invalid Authorization header")
    token = authorization[7:]              # usuń prefiks "Bearer " (7 znaków)
    try:
        applicant_id, _ = token_service.decode_access_token(token)
        return applicant_id
    except Exception as e:                 # każdy błąd JWT → 401
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail=f"Invalid token: {e}")
    #   Ta zależność jest używana przez endpoint /me.
```

### Python Concepts (dep-s)

| Koncepcja | Wyjaśnienie |
|-----------|-------------|
| `Depends(...)` | FastAPI słownie: "najpierw rozwiąż tę zależność" |
| `global <var>` | Modyfikacja zmiennej modułowej z wnętrza funkcji |
| Lazy singleton | Tworzy przy pierwszym użyciu, potem zwraca cache |
| `Header(None, alias=...)` | Czyta konkretny nagłówek HTTP |
| `HTTPException` | Przerwanie requestu z kodem HTTP |
| `startswith("Bearer ")` | Sprawdzanie prefiksu |
| `authorization[7:]` | Slicing — wycięcie od indeksu 7 |

### Pitfalls / Our Solution / Why

| ❌ Pitfall | ✅ Our Solution | 💡 Why |
|-----------|----------------|--------|
| Tworzenie zależności wewnątrz use case'a | Przez DI (`Depends`) | Testowalność; zamiana adresów |
| Nowy JWTService per request | Singleton (lazy) | Ładowanie klucza to koszt; raz wystarczy |
| Wstrzykiwanie `get_session` bezpośrednio | Przez `get_applicant_repo` | Izolacja: routes nie znają sesji |
| `X \| None` parametry | `Optional[str]` | Zgodność z 3.9 |

---

## D2. `api/routes/auth.py` — endpointy auth

### Cel
Definiuje HTTP-owy interfejs: rejestracja, login, refresh.

### Kto wywołuje
- FastAPI (dopasowanie ścieżki /api/v1/auth/*)
- Wywołuje use case'y przez `Depends(...)`

```python
from fastapi import APIRouter, Depends, status
from fastapi.routing import APIRouter as APIRouterType
from src.api.dependencies import (get_login_use_case, get_refresh_token_use_case, get_register_use_case)
from src.application.dto import LoginRequest, RefreshTokenRequest, RegisterRequest, TokenResponse
from src.application.use_cases.login import LoginUseCase
from src.application.use_cases.refresh_token import RefreshTokenUseCase
from src.application.use_cases.register import RegisterUseCase

# Tworzymy router z prefiksem /auth i tagiem "auth" (do docs OpenAPI)
router: APIRouterType = APIRouter(prefix="/auth", tags=["auth"])


@router.post(
    "/register",
    response_model=TokenResponse,        # co zwrócimy (serializacja + schema)
    status_code=status.HTTP_201_CREATED, # kod 201 (utworzono)
    summary="Register a new applicant",
)
async def register(
    request: RegisterRequest,                                   # FastAPI waliduje body
    use_case: RegisterUseCase = Depends(get_register_use_case), # wstrzykuj use case
) -> TokenResponse:
    """Register a new applicant and return access + refresh tokens."""
    return await use_case.execute(request)   # wywołaj logikę i zwróć
    #   Uwaga: route jest CIENKIE — cała logika w use case.

@router.post("/login", response_model=TokenResponse, summary="...")
async def login(request: LoginRequest, use_case: LoginUseCase = Depends(get_login_use_case)) -> TokenResponse:
    return await use_case.execute(request)

@router.post("/refresh", response_model=TokenResponse, summary="...")
async def refresh_token(request: RefreshTokenRequest,
                        use_case: RefreshTokenUseCase = Depends(get_refresh_token_use_case)) -> TokenResponse:
    return await use_case.execute(request)
```

### Python Concepts (routes)

| Koncepcja | Wyjaśnienie |
|-----------|-------------|
| `@router.post(...)` | Dekorator rejestrujący funkcję jako endpoint POST |
| `APIRouter(prefix=, tags=)` | Grupowanie endpointów; tag do Swagger |
| `response_model=` | Schemat Pydantic do serializacji odpowiedzi |
| `status_code=` / `status.HTTP_201_CREATED` | Kod HTTP odpowiedzi |
| Cieńki handler | Route tylko przekazuje; logika w use case |
| `async def` | Handler asynchroniczny (nie blokuje event loop) |

### Pitfalls / Our Solution / Why

| ❌ Pitfall | ✅ Our Solution | 💡 Why |
|-----------|----------------|--------|
| Logika w route | Route = 3 linie, delegacja do use case | Testowalność, SRP |
| Zwracanie encji domenowej | `response_model=TokenResponse` (DTO) | Kontrola, co widzi klient |
| Brak `status_code` | Jawne `201` dla create | Poprawne semantyka HTTP |

---

## D3. `api/routes/me.py` — profil użytkownika

```python
from fastapi import APIRouter, Depends
from fastapi.routing import APIRouter as APIRouterType
from src.api.dependencies import get_current_applicant_id, get_get_me_use_case
from src.application.dto import ApplicantResponse
from src.application.use_cases.get_me import GetMeUseCase

router: APIRouterType = APIRouter(prefix="/me", tags=["me"])

@router.get("", response_model=ApplicantResponse, summary="Get current applicant profile")
async def get_me(
    applicant_id: UUID = Depends(get_current_applicant_id),  # uwierzytelnia + bierze id
    use_case: GetMeUseCase = Depends(get_get_me_use_case),
) -> ApplicantResponse:
    return await use_case.execute(applicant_id)
```
- `Depends(get_current_applicant_id)` → **uwierzytelnienie** (waliduje JWT, zwraca UUID).
- Połączenie: auth + biznes w jednym endpointcie.

---

## D4. `main.py` — tworzenie aplikacji

### Cel
Wejście aplikacji. Tworzy obiekt `FastAPI`, konfiguruje lifespan (start/stop),
rejestruje routery, dodaje health checki.

### Wywołanie
`uvicorn src.main:app` (lub `from src.main import app` w testach)

```python
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from crediguard_observability import configure_logging, get_logger   # z libs/
from fastapi import FastAPI
from src.api.routes.auth import router as auth_router
from src.api.routes.me import router as me_router
from src.infrastructure.database import close_database, init_database

configure_logging("applicant-service")   # skonfiguruj JSON logging
logger = get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Setup przy starcie, cleanup przy stop."""
    import os
    database_url = os.getenv("DATABASE_URL", "postgresql+asyncpg://applicant_svc:applicant_dev_pw@localhost:5433/applicant_db")
    await init_database(database_url)    # <-- polecenia przed yield = START
    logger.info("Applicant service started", database_url=database_url)
    yield                                # <-- aplikacja działa
    await close_database()               # <-- po yield = STOP
    logger.info("Applicant service stopped")


app = FastAPI(title="CrediGuard Applicant Service", version="0.1.0", lifespan=lifespan)

# Rejestruj routery z prefiksem /api/v1
app.include_router(auth_router, prefix="/api/v1")
app.include_router(me_router, prefix="/api/v1")

# Healthchecki
@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}
@app.get("/ready", tags=["health"])
async def ready() -> dict[str, str]:
    return {"status": "ready"}
```

### Python Concepts (main)

| Koncepcja | Wyjaśnienie |
|-----------|-------------|
| `@asynccontextmanager` | Menedżer kontekstu; `yield` = granica start/stop |
| `lifespan` | Nowoczesny sposób na start/stop w FastAPI (zamiast deprecated `@app.on_event`) |
| `os.getenv(key, default)` | Zmienna środowiskowa z domyślnym |
| `app.include_router(prefix=...)` | Podpinanie routera pod prefiks |
| `yield` w async gen | Zawiesza generator, wznawia później |

### Pitfalls / Our Solution / Why

| ❌ Pitfall | ✅ Our Solution | 💡 Why |
|-----------|----------------|--------|
| `@app.on_event("startup")` (deprecated) | `lifespan=` (async context manager) | Nowoczesny, poprawny cleanup przy awarii |
| Twardy URL w kodzie | `os.getenv("DATABASE_URL", default)` | Konfigurowalne przez env (bez sekretów w kodzie) |

---

## D5. Alembic — migracje bazy

### `alembic/env.py`
Alembic (narzędzie migracji) czyta ten plik przy każdym poleceniu.
Ponieważ używamy async, `env.py` musi uruchomić async engine.

```python
from sqlalchemy.ext.asyncio import async_engine_from_config   # async engine
from sqlalchemy.engine import Connection
from src.infrastructure.persistence.models import Base         # metadane modeli

target_metadata = Base.metadata        # Alembic wie, jakie tabele istnieją

async def run_async_migrations() -> None:
    connectable = async_engine_from_config(   # zbuduj async silnik z alembic.ini
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)   # migracje w wątku
    await connectable.dispose()

if context.is_offline_mode():
    run_migrations_offline()
else:
    import asyncio
    asyncio.run(run_async_migrations())   # bootstrap event loop (3.9!)
```

### `alembic/versions/0001_initial.py`
```python
def upgrade() -> None:
    op.create_table("applicants", ...)      # ALTER/CREATE przez Alembic op
    op.create_table("refresh_tokens", ...)

def downgrade() -> None:                    # cofnięcie migracji
    op.drop_table("refresh_tokens")
    op.drop_table("applicants")
```
- `upgrade()` = postęp do przodu, `downgrade()` = cofnięcie.
- `revision = "0001"`, `down_revision = None` (pierwsza migracja).

### Polecenia
```bash
python3 -m alembic upgrade head    # zastosuj wszystkie migracje
python3 -m alembic revision --autogenerate -m "opis"   # wygeneruj z modeli
```

---

## D6. Biblioteki współdzielone — `libs/`

### `libs/observability` — logging (structlog)
Używany w `main.py`: `configure_logging(...)`, `get_logger()`. Produkuje
**JSONowe** logi z `service`, `level`, `timestamp`, i context (np. correlation_id).

### `libs/events` — koperta zdarzeń (na później)
Definiuje `EventEnvelope` (Pydantic + `Generic`) — wspólny kontrakt zdarzeń Kafka.
W Etapie 1 jeszcze nie publikujemy zdarzeń, ale koperta jest już przygotowana.

```python
PayloadT = TypeVar("PayloadT", bound=BaseModel)   # generyczny typ payloadu
class EventEnvelope(BaseModel, Generic[PayloadT]):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: str
    occurred_at: datetime = ...   # timezone-aware (datetime.UTC w 3.11+)
    correlation_id: UUID
    producer: str
    payload: PayloadT
```

---

---

# CZĘŚĆ E — BEZPIECZEŃSTWO (deep-dive)

## E1. Hasła: Argon2id vs bcrypt

| | bcrypt | Argon2id |
|---|--------|----------|
| Limit hasła | 72 bajty (obcina!) | brak limitu |
| Memory-hard | NIE | TAK (wymaga dużo RAM) |
| Odporność na GPU/ASIC | słabsza | znacznie lepsza |
| Konkurencja 2020+ | 1. miejsce | zwycięzca Password Hashing Competition |

Parametry użyte:
```python
time_cost=3          # iteracje — więcej = wolniej = bezpieczniej
memory_cost=65536    # 64 MiB na operację
parallelism=4        # wątki
hash_len=32          # 32-bajtowy hasz
salt_len=16          # 16-bajtowa sól (przypadkowa per hasło)
```

Sól ⟹ dwa identyczne hasła dają różne hashe. Wynik wygląda:
```
$argon2id$v=19$m=65536,t=3,p=4$ <salt> $ <hash>
```
To **samopisujący** format — parametry zapisane w stringu, więc weryfikacja wie,
jakich użyć.

## E2. JWT: RS256 (asymetria)

- **Private key** (podpis): TYLKO u Applicant Service.
- **Public key** (weryfikacja): dystrybuowany do Gateway i innych.
- Zaleta: nawet jeśli ktoś zobaczy public key, nie podpisze tokenów.

Security claims:
- `sub` = subject (id użytkownika)
- `exp` = expiration (unix timestamp) — JWT jest nieważny po tym
- `iat` = issued-at — kiedy wydano
- `type` = access/refresh — rozdzielamy rodzaje tokenów

## E3. Access (15 min) vs Refresh (7 dni)

| | Access token | Refresh token |
|---|--------------|---------------|
| Żywotność | 15 min | 7 dni |
| Wysyłany | przy każdym request | tylko przy /refresh |
| W bazie | NIE (stateless, podpisany) | TAK (hash) |
| Rotacja | — | tak (stary unieważniany) |
| Ryzyko utraty | małe (krótki) | duże (długi) ⟹ chroniony hash + rotacja |

## E4. Rotacja i unieważnienie refresh tokena

1. User wysyła refresh token do `/auth/refresh`
2. Serwis znajduje pasujący, sprawdza `is_valid()`
3. **Unieważnia STARY** (`revoke()` + `update()`)
4. Tworzy NOWY (`save()`)
5. Zwraca nowy access + nowy refresh

Jeśli token zostanie ukradziony i użyty ponownie → `RefreshTokenRevoked`
(albo nie znajdziemy go w `get_all_valid`). To detekcja **replay**.

## E5. Klucze RSA

- `python3 -m src.infrastructure.security.keygen`
- Private: PKCS#8 PEM, `chmod 600`
- Public: SPKI PEM, `chmod 644`
- W Dockerze montowane w `/app/keys` (wolumen); lokalnie `KEYS_DIR=./keys`

---

# CZĘŚĆ F — TESTOWANIE

## F1. Struktura testów
```
tests/unit/
├── domain/test_entities.py          # czyste testowanie encji
└── application/
    ├── test_register.py             # RegisterUseCase z mockami
    ├── test_login.py
    ├── test_refresh_token.py
    └── test_get_me.py
```

## F2. Wzorce testowe

### 1. `pytest.mark.asyncio` — test funkcji async
```python
@pytest.mark.asyncio
async def test_login_success(self, ...): 
    # możemy używać await wewnątrz
```

### 2. `AsyncMock` — mock metod asynchronicznych
```python
repo = AsyncMock()
repo.exists_by_email = AsyncMock(return_value=False)   # async metoda → False
repo.save = AsyncMock()
# Ponieważ repo to fake, test NIE dotyka realnej DB!
```

### 3. `MagicMock` — mock metod synchronicznych
```python
hasher = MagicMock()
hasher.hash = MagicMock(return_value="hashed_password")   # sync → sync
hasher.verify = MagicMock(return_value=True)
```

### 4. Fixtures — współdzielony setup
```python
@pytest.fixture
def mock_applicant_repo(self) -> AsyncMock:
    repo = AsyncMock()
    repo.exists_by_email = AsyncMock(return_value=False)
    return repo

@pytest.fixture
def use_case(self, mock_applicant_repo, ...) -> RegisterUseCase:
    return RegisterUseCase(mock_applicant_repo, ...)
```

### 5. Testowanie wyjątków
```python
with pytest.raises(EmailAlreadyRegistered) as exc_info:
    await use_case.execute(request)
assert exc_info.value.email == "existing@example.com"   # kontekst wyjątku
```

### 6. Weryfikacja wywołań
```python
mock_applicant_repo.exists_by_email.assert_called_once_with("test@example.com")
mock_applicant_repo.save.assert_called_once()     # był dokładnie 1 raz
mock_applicant_repo.save.assert_not_called()      # nie został wywołany
```

## F3. Dlaczego testy nie dotykają bazy?
Ponieważ `RegisterUseCase` zależy od **portu** `ApplicantRepository`, w testach
podajemy fake (AsyncMock). To dowód, że Clean Architecture + DI się opłaca:
**logika testowana w izolacji, błyskawicznie, bez Docker/postgres.**

(Testy integracyjne z prawdziwym Postgresem przez *testcontainers* to dalszy etap.)

---

# CZĘŚĆ G — GLOBALNA TABELA PITFALLS

| ❌ Pitfall | ✅ Our Solution | 💡 Why |
|-----------|----------------|--------|
| `default=uuid4()` | `default_factory=uuid4` | Odpala się raz; wszystkie obiekty dostałyby ten sam UUID |
| `default=[]` (mutable) | `default_factory=list` | Współdzielona lista między instancjami |
| `datetime.now()` (naive) | `datetime.now(timezone.utc)` | Naive vs aware — błąd odejmowania/porównania |
| `datetime.UTC` | `timezone.utc` | `UTC` w 3.11+; w 3.9 nie istnieje |
| `X \| None` | `Optional[X]` | Operator `\|` od 3.10 |
| `slots=True` dataclass | brak | `slots` od 3.10 |
| bcrypt | argon2id | odporność na GPU, bez limitu 72B |
| HS256 (wspólny sekret) | RS256 | private key tylko w jednym serwisie |
| commit w repozytorium | flush; commit w get_session | transakcja na cały request; rollback przy błędzie |
| `revoked_at == None` (SQL) | `.is_(None)` | SQL `= NULL` zawsze fałsz; trzeba `IS NULL` |
| `save()` do aktualizacji | osobne `update()` | save wpycha INSERT → dublet PK |
| Logika w route | cienki route + use case | testowalność, SRP |
| Zdradzanie "brak użytkownika" | jeden `InvalidCredentials` | anty-enumeration |
| Zły refresh tken użyty ponownie działa | rotacja + revoke | replay detection |
| Bucket: `@app.on_event` deprecated | `lifespan=` | nowoczesny cleanup |
| Bez `from __future__ import annotations` | wszędzie | "leniwe" typy, kompatybilność 3.9 |
| `PasswordHasher` kolizja nazw | aliasy importów (`as Argon2PasswordHasher`) | rozróżnienie dwóch klas o tej samej nazwie |

---

# CZĘŚĆ H — GLOSARIUSZ

| Termin | Znaczenie |
|--------|-----------|
| **Entity** (`Applicant`) | Obiekt domenowy z tożsamością (id) i regułami |
| **Aggregate Root** | Główna encja, przez którą operujemy graf |
| **Value Object** | Obiekt bez tożsamości, opisywany tylko wartością |
| **DTO** | Obiekt transferu danych (Pydantic) — walidacja/serializacja |
| **Port** | Interfejs (ABC/Protocol) — *co* umiemy |
| **Adapter** | Implementacja portu — *jak* (np. SQLAlchemy) |
| **Use Case** | Pojedyncza operacja biznesowa (`execute`) |
| **Dependency Inversion** | Zależność od abstrakcji, nie od konkretu |
| **Repository** | Wzorzec dostępu do danych (CRUD przez port) |
| **DI** | Wstrzykiwanie zależności (FastAPI `Depends`) |
| **Lifespan** | Start/stop cykl życia aplikacji |
| **Singleton** | Jedna instancja współdzielona (lazy) |
| **Aware datetime** | `datetime` ze strefą czasową (ma `tzinfo`) |
| **Salt** | Losowa wartość przy hashowaniu hasła |
| **Rotacja tokena** | Unieważnianie starego i wydanie nowego |
| **Replay attack** | Ponowne użycie starego tokena przez złodzieja |

---

# CZĘŚĆ I — ZRÓB TO SAM (podsumowanie kroków)

```bash
# 1. infra
make infra-up

# 2. klucze
cd services/applicant
python3 -m src.infrastructure.security.keygen

# 3. zależności
python3 -m pip install -e ".[dev]"

# 4. migracje
python3 -m alembic upgrade head

# 5. testy
python3 -m pytest tests/unit -v

# 6. typy + lint
python3 -m mypy --strict src
python3 -m ruff check src

# 7. uruchom
KEYS_DIR=./keys python3 -m uvicorn src.main:app --port 8001

# 8. curl (patrz sekcja 0)
```

---

# CZĘŚĆ J — CZEGO SIĘ NAUCZYSZ W TYM ETAPIE

1. **Clean Architecture** — warstwy i reguła zależności (domain nie zna frameworków)
2. **Dependency Inversion / Ports & Adapters** — wymienialne bazy i usługi
3. **Python async** — `async def`, `await`, `async with`, `AsyncGenerator`,
   `asynccontextmanager`
4. **Dataclass** — `@dataclass`, `field(default_factory)`, `@property`
5. **Pydantic v2** — `BaseModel`, `EmailStr`, `Field`, walidacja, serializacja
6. **Kryptografia** — Argon2id, RS256 JWT, solenie, rotacja tokenów
7. **SQLAlchemy 2.0 async** — `Mapped`, `mapped_column`, `AsyncSession`,
   `select`, `flush`/`commit`
8. **FastAPI** — `Depends`, `APIRouter`, `lifespan`, `response_model`
9. **Testowanie** — `pytest.mark.asyncio`, `AsyncMock`, `MagicMock`, fixtures
10. **Typowanie** — `mypy --strict`, `Optional`, `list[X]`, `tuple[X, Y]`

---

**Koniec dokumentu.**
