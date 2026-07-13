CrediGuard — Specyfikacja Techniczna MVP
Platforma pożyczkowa (fintech) oparta o mikroserwisy, architekturę sterowaną zdarzeniami i ML-owy scoring kredytowy. Projekt portfolio na poziomie Senior Full-Stack Developer.

Wersja: 1.0 | Status: Draft do implementacji | Metoda pracy: Claude Code + ten dokument jako źródło prawdy


Spis treści
Cel projektu i zakres MVP
Architektura wysokopoziomowa
Tech stack i uzasadnienie decyzji
Specyfikacja serwisów
Komunikacja między serwisami
Modele danych
Model ML — scoring kredytowy
Bezpieczeństwo
Obserwowalność
Struktura repozytorium
Plan implementacji krok po kroku
Praca z Claude Code


1. Cel projektu i zakres MVP
1.1 Cele
Nauka: Python (async), architektura mikroserwisowa, Kafka, ML w produkcji, obserwowalność.
Portfolio: działające MVP o produkcyjnej jakości architektury, które opowiada historię inżynierską (ADR, tracing, resilience), a nie tylko "działa na moim komputerze".
1.2 Scenariusz biznesowy (happy path)
Klient rejestruje się i loguje (JWT).
Klient składa wniosek o pożyczkę: kwota, okres (miesiące), miesięczny dochód, wiek + upload jednego dokumentu (skan dowodu, PDF/JPG).
API zwraca natychmiast 202 Accepted ze statusem PROCESSING — cała ciężka logika dzieje się asynchronicznie przez Kafkę.
Frontend otwiera połączenie SSE i na żywo pokazuje kolejne etapy: weryfikacja dokumentu → analiza anti-fraud → scoring ML → decyzja.
Przy akceptacji: symulacja wypłaty środków przez Stripe (test mode), status DISBURSED.
Przy odrzuceniu: status REJECTED + uzasadnienie (najważniejsze czynniki z modelu ML).
1.3 Co świadomie POZA zakresem MVP
Świadome cięcie zakresu to też kompetencja seniora — wpisz to do README:

Brak KYC/AML z prawdziwymi dostawcami (mock weryfikacji dokumentu).
Brak harmonogramu spłat, rat, odsetek — pożyczka kończy żywot na wypłacie.
Brak panelu administracyjnego / operatora.
Jedna waluta (PLN), jeden produkt pożyczkowy.
Kubernetes poza zakresem — deployment przez docker compose (K8s jako "future work" w README).


2. Architektura wysokopoziomowa
flowchart TB

    subgraph Client["Przeglądarka"]

        FE["Next.js App Router<br/>(SSR + Client Islands)"]

    end

    subgraph Edge["Warstwa brzegowa"]

        GW["API Gateway (FastAPI)<br/>JWT · Rate Limit · Correlation ID · SSE"]

    end

    subgraph Services["Mikroserwisy (Docker Network — bez dostępu z zewnątrz)"]

        APP["Applicant Service<br/>rejestracja / login / JWT"]

        LOAN["Loan Application Service<br/>wnioski + Transactional Outbox"]

        DOC["Document Service<br/>upload + weryfikacja (MinIO)"]

        UW["AI Underwriting Engine<br/>reguły + model ML (ProcessPool)"]

        DISB["Disbursement Service<br/>wypłata (Stripe test mode)"]

        NOTIF["Notification Service<br/>Kafka → Redis Pub/Sub"]

    end

    subgraph Data["Warstwa danych"]

        PG[("PostgreSQL<br/>database-per-service")]

        REDIS[("Redis<br/>idempotencja · velocity checks · pub/sub")]

        MINIO[("MinIO<br/>S3-compatible storage")]

        KAFKA[["Apache Kafka (KRaft)<br/>topics + DLQ"]]

    end

    subgraph Obs["Obserwowalność"]

        OTEL["OpenTelemetry → Jaeger"]

        LOGS["Structured JSON logs (structlog)"]

    end

    FE -->|HTTPS + JWT| GW

    GW -->|SSE: status wniosku| FE

    GW --> APP & LOAN & DOC

    LOAN -->|outbox worker| KAFKA

    DOC --> MINIO

    DOC --> KAFKA

    KAFKA --> UW

    UW --> KAFKA

    KAFKA --> DISB

    KAFKA --> NOTIF

    NOTIF --> REDIS

    REDIS -->|pub/sub| GW

    APP & LOAN & DOC & UW & DISB --> PG

    GW & UW --> REDIS
Kluczowe przepływy
Przepływ synchroniczny (REST): Przeglądarka → Gateway → serwis. Tylko operacje szybkie: rejestracja, login, złożenie wniosku (sam zapis), upload pliku, odczyt statusu.

Przepływ asynchroniczny (Kafka): wszystko, co ciężkie lub wymagające koordynacji wielu serwisów: weryfikacja dokumentu, scoring, decyzja, wypłata.

Przepływ real-time do klienta (SSE): serwisy publikują zdarzenia statusowe → Notification Service tłumaczy je na Redis Pub/Sub → Gateway strumieniuje do przeglądarki przez Server-Sent Events.


3. Tech stack i uzasadnienie decyzji
Warstwa
Technologia
Dlaczego ta, a nie inna
Frontend
Next.js 15 (App Router), TypeScript
Server Components = szybki FCP i brak wycieku sekretów do przeglądarki. Standard rynkowy — rekruterzy go znają. Alternatywa (Vite + React SPA) nie pokazuje SSR/RSC.
Stan globalny
Zustand
Minimalny boilerplate vs Redux; wystarcza na sesję i toasty. Redux Toolkit byłby przerostem — w MVP stan serwerowy i tak żyje w SSE/React Query.
Formularze
React Hook Form + Zod
Walidacja schematem 1:1 z Pydantic na backendzie (te same reguły po obu stronach).
UI
TailwindCSS + shadcn/ui
Profesjonalny wygląd fintechu bez pisania design systemu od zera.
Backend
Python 3.12 + FastAPI
Natywny async/await (kluczowe przy Kafka + SSE + I/O), Pydantic v2 do walidacji, automatyczny OpenAPI. Django odpada: synchroniczny rdzeń ORM, cięższy narzut; Flask odpada: brak natywnego async i typowania kontraktów.
ORM
SQLAlchemy 2.0 (async) + Alembic
Standard w Pythonie, wsparcie async przez asyncpg, migracje wersjonowane w repo.
Broker
Apache Kafka (tryb KRaft, 1 broker)
Trwały log zdarzeń (replay!), partycjonowanie gwarantujące kolejność per klient, DLQ. RabbitMQ odpada: brak natywnego replay i słabsze gwarancje kolejności per klucz; Redis Streams odpada: mniej "portfolio value" i słabsze narzędzia. KRaft = brak ZooKeepera, prostszy compose.
Klient Kafki
aiokafka
Asynchroniczny producer/consumer — spójny z event loopem FastAPI. confluent-kafka jest szybszy, ale synchroniczny (wymagałby wrapperów).
Bazy
PostgreSQL 16, database-per-service
Jedna instancja kontenera, ale osobne bazy logiczne + osobni użytkownicy bez wzajemnych grantów — izolacja jak w produkcji, bez 5 kontenerów.
Cache / RT
Redis 7
Trzy role: klucze idempotencji (SETNX + TTL), velocity checks (Sorted Sets), Pub/Sub jako most Kafka→SSE.
Storage plików
MinIO
S3-compatible API — kod pisany pod boto3 działa 1:1 z AWS S3. Pokazuje znajomość object storage bez kosztów chmury.
ML
scikit-learn (Gradient Boosting) + pandas
Wystarczający do scoringu tabularycznego; łatwy do wyjaśnienia (feature importance → uzasadnienie decyzji). XGBoost/LightGBM to opcjonalny upgrade.
Płatności
Stripe SDK (test mode)
Realna integracja z prawdziwym SDK i webhookami, zero kosztów w test mode.
Tracing
OpenTelemetry + Jaeger
Standard branżowy; auto-instrumentacja FastAPI/SQLAlchemy/aiokafka.
Logi
structlog (JSON)
Logi strukturalne z correlation_id w każdym wpisie — gotowe pod Loki/ELK.
Konteneryzacja
Docker + docker compose
Jedno make up stawia cały system. K8s świadomie poza MVP.
Testy
pytest + pytest-asyncio + testcontainers
Testy integracyjne na prawdziwym Postgresie/Kafce w kontenerach — wyżej niż mockowanie wszystkiego.
Jakość kodu
ruff + mypy (strict) + pre-commit
Typowanie strict to sygnał seniorski; ruff zastępuje flake8+isort+black jednym narzędziem.



4. Specyfikacja serwisów
Każdy serwis w Pythonie ma identyczną strukturę Clean/Hexagonal Architecture:

services/<nazwa>/

├── src/

│   ├── domain/          # encje, value objects, wyjątki domenowe — ZERO importów frameworków

│   ├── application/     # use case'y, porty (interfejsy repozytoriów), handlery zdarzeń

│   ├── infrastructure/  # SQLAlchemy, Kafka, Redis, Stripe, MinIO — implementacje portów

│   └── api/             # routery FastAPI, schematy Pydantic (DTO), zależności (DI)

├── tests/

│   ├── unit/            # domain + application (bez I/O)

│   └── integration/     # testcontainers: Postgres, Kafka, Redis

├── alembic/

├── Dockerfile

└── pyproject.toml

Reguła zależności: api → application → domain, infrastructure → application (porty). Domain nie importuje niczego spoza stdlib.
4.1 API Gateway (port 8000 — jedyny wystawiony na świat)
Aspekt
Opis
Odpowiedzialność
Walidacja JWT, reverse proxy (httpx) do serwisów wewnętrznych, rate limiting (Redis), generowanie/propagacja X-Correlation-ID, endpoint SSE
Baza
brak (stateless)
Zależności
Redis (rate limit + pub/sub), serwisy wewnętrzne po HTTP


Endpointy publiczne:

Metoda
Ścieżka
Proxy do
Auth
POST
/api/v1/auth/register
Applicant
❌
POST
/api/v1/auth/login
Applicant
❌
GET
/api/v1/me
Applicant
✅
POST
/api/v1/loans
Loan Application
✅
GET
/api/v1/loans/{loan_id}
Loan Application
✅
GET
/api/v1/loans
Loan Application
✅
POST
/api/v1/loans/{loan_id}/documents
Document
✅
GET
/api/v1/loans/{loan_id}/events
SSE — obsługiwane lokalnie
✅ (token w query lub cookie — EventSource nie wysyła nagłówków)
POST
/api/v1/webhooks/stripe
Disbursement
podpis Stripe


Po walidacji JWT gateway usuwa nagłówek Authorization i dokleja X-User-ID, X-User-Roles. Serwisy wewnętrzne ufają tym nagłówkom (sieć zamknięta) i odrzucają ruch bez nich.
4.2 Applicant Service (port 8001)
Aspekt
Opis
Odpowiedzialność
Rejestracja, login, wydawanie JWT (access 15 min + refresh 7 dni), profil klienta
Baza
applicant_db: tabela applicants
Publikuje
applicant.registered.v1 (na przyszłość — np. e-mail powitalny)
Hasła
argon2id (biblioteka argon2-cffi) — nie bcrypt (limit 72 bajtów, słabszy memory-hardness)
JWT
RS256 (klucz prywatny tylko w Applicant, publiczny w Gateway) — pokazuje zrozumienie asymetrii; HS256 wymagałby współdzielenia sekretu

4.3 Loan Application Service (port 8002)
Aspekt
Opis
Odpowiedzialność
Przyjęcie wniosku, walidacja biznesowa, Transactional Outbox, maszyna stanów wniosku, projekcja statusu dla frontendu
Baza
loan_db: loan_applications, outbox_events, processed_events (idempotencja konsumenta)
Publikuje
loan.application.submitted.v1, loan.status.changed.v1
Konsumuje
document.verified.v1, underwriting.completed.v1, disbursement.completed.v1


Maszyna stanów wniosku (jedyne legalne przejścia):

DRAFT → SUBMITTED → DOC_VERIFICATION → UNDERWRITING → APPROVED → DISBURSING → DISBURSED

                          │                  │                        │

                          ▼                  ▼                        ▼

                     DOC_REJECTED        REJECTED              DISBURSEMENT_FAILED

Przejścia egzekwowane w warstwie domain (metody na encji, np. application.approve() rzuca InvalidStateTransition, jeśli stan ≠ UNDERWRITING). Serwis jest właścicielem statusu — inne serwisy tylko raportują wyniki zdarzeniami.
4.4 Document Service (port 8003)
Aspekt
Opis
Odpowiedzialność
Upload pliku (multipart), zapis do MinIO, walidacja (typ MIME po magic bytes, max 5 MB), symulowana weryfikacja OCR (2–5 s sleep + reguły), presigned URL do podglądu
Baza
document_db: documents
Publikuje
document.verified.v1 / document.rejected.v1
Konsumuje
loan.application.submitted.v1 (uruchamia weryfikację wgranego wcześniej dokumentu)


Weryfikacja jest mockiem, ale architektonicznie prawdziwym: osobny consumer, retry, DLQ — podmiana mocka na prawdziwe OCR (np. AWS Textract) to zmiana jednej klasy w infrastructure/.
4.5 AI Underwriting Engine (port 8004)
Aspekt
Opis
Odpowiedzialność
Anti-fraud (velocity checks w Redis), reguły twarde (hard rules), scoring ML, decyzja + uzasadnienie
Baza
underwriting_db: underwriting_results (pełny audyt: wejście, score, wersja modelu, czynniki)
Publikuje
underwriting.completed.v1
Konsumuje
document.verified.v1


Pipeline decyzji (w tej kolejności — fail fast):

Velocity check (Redis Sorted Sets): > 3 wnioski z tego samego applicant_id lub IP w 10 minut → REJECTED (fraud_suspected) bez uruchamiania modelu.
Hard rules: wiek < 18 lub > 75, kwota > 24× dochód miesięczny, okres poza 3–60 mies. → REJECTED z kodem reguły.
Model ML: predykcja probability_of_default; próg: PD < 0.30 → APPROVED, inaczej REJECTED.
Uzasadnienie: top 3 czynniki (feature contributions) tłumaczone na komunikaty PL, np. "Wysoki stosunek raty do dochodu (DTI 62%)".

Izolacja CPU-bound (kluczowy punkt seniorski): consumer działa w event loopie asyncio; sama predykcja (model.predict_proba) wykonywana przez loop.run_in_executor(process_pool, ...) w ProcessPoolExecutor (2 procesy, model ładowany raz per proces przez initializer). Dzięki temu GIL i CPU-bound predykcja nie blokują heartbeatów consumera Kafki ani endpointu /health. W README opisz, dlaczego asyncio.to_thread nie wystarcza dla czystego CPU-bound (wątki dzielą GIL).
4.6 Disbursement Service (port 8005)
Aspekt
Opis
Odpowiedzialność
Symulacja wypłaty przez Stripe (test mode): utworzenie PaymentIntent + confirm testową kartą; obsługa webhooka payment_intent.succeeded
Baza
disbursement_db: disbursements
Publikuje
disbursement.completed.v1 / disbursement.failed.v1
Konsumuje
underwriting.completed.v1 (tylko APPROVED)
Idempotencja
Podwójna: klucz idempotencji Stripe (Idempotency-Key = loan_id) + tabela processed_events — nawet redelivery z Kafki nie wypłaci dwa razy

4.7 Notification Service (bez portu HTTP)
Lekki worker: konsumuje loan.status.changed.v1 i publikuje JSON na kanał Redis Pub/Sub loan-status:{applicant_id}. Gateway w handlerze SSE subskrybuje ten kanał i strumieniuje do przeglądarki. Dzięki temu Gateway nie dotyka Kafki, a SSE przeżywa restart konsumerów.


5. Komunikacja między serwisami
5.1 Topiki Kafki
Topic
Klucz partycji
Producent
Konsumenci
Partycje
loan.application.submitted.v1
applicant_id
Loan App (outbox)
Document
3
document.verified.v1 / document.rejected.v1
applicant_id
Document
Underwriting, Loan App
3
underwriting.completed.v1
applicant_id
Underwriting
Disbursement, Loan App
3
disbursement.completed.v1 / failed.v1
applicant_id
Disbursement
Loan App
3
loan.status.changed.v1
applicant_id
Loan App
Notification
3
*.dlq (per topic)
jak oryginał
retry handler
ręczna inspekcja
1


Klucz = applicant_id ⇒ wszystkie zdarzenia jednego klienta lądują na tej samej partycji ⇒ gwarancja kolejności per klient przy zachowaniu równoległości między klientami. Sufiks .v1 = jawne wersjonowanie kontraktów.
5.2 Koperta zdarzenia (wspólny kontrakt — pakiet libs/events)
{

  "event_id": "uuid4",

  "event_type": "underwriting.completed.v1",

  "occurred_at": "2026-07-13T12:00:00Z",

  "correlation_id": "uuid4 — od pierwszego żądania HTTP",

  "producer": "underwriting-service",

  "payload": { "...": "schemat Pydantic zależny od event_type" }

}

Schematy zdarzeń żyją w pakiecie współdzielonym libs/events (modele Pydantic) — jedno źródło prawdy kontraktu, wersjonowane w monorepo.
5.3 Transactional Outbox (Loan Application Service)
POST /loans → jedna transakcja SQL: INSERT do loan_applications + INSERT do outbox_events (status PENDING).
Background worker (asyncio task w tym samym procesie) co 500 ms: SELECT ... FOR UPDATE SKIP LOCKED z outbox_events, publikacja do Kafki z acks=all i enable_idempotence=True, UPDATE na SENT.
Awaria między zapisem a publikacją? Zdarzenie czeka w tabeli — at-least-once zamiast utraconych zdarzeń. Duplikaty łapie konsument (patrz 5.4).
5.4 Idempotencja (dwa poziomy)
HTTP: frontend wysyła nagłówek Idempotency-Key (UUID generowany przy otwarciu formularza). Gateway/serwis robi SET key value NX EX 86400 w Redis; duplikat → zwrot zapisanej odpowiedzi z nagłówkiem Idempotent-Replay: true.
Kafka: każdy konsument przed przetworzeniem sprawdza event_id w tabeli processed_events (INSERT w tej samej transakcji co skutki biznesowe). Redelivery = no-op.
5.5 Retry + Dead Letter Queue
Konsument łapie wyjątek → retry in-process 3× z exponential backoff (1 s, 4 s, 16 s) + jitter → po wyczerpaniu publikuje oryginalną wiadomość na {topic}.dlq z nagłówkami x-error, x-original-topic, x-retry-count → commituje offset (nie blokuje partycji dla innych klientów). Skrypt scripts/dlq_inspect.py do podglądu i re-publikacji.
5.6 SSE — kontrakt strumienia
GET /api/v1/loans/{loan_id}/events   (Accept: text/event-stream)

event: status

data: {"loan_id":"...","status":"UNDERWRITING","step":3,"total_steps":5,

       "message":"Analiza ryzyka kredytowego...","occurred_at":"..."}

event: decision

data: {"loan_id":"...","status":"REJECTED",

       "reasons":["Wysoki stosunek raty do dochodu (DTI 62%)", "..."]}

Wymogi: heartbeat (: ping) co 15 s, wznawianie po Last-Event-ID, a przy pierwszym połączeniu wysłanie bieżącego statusu z bazy (żeby klient, który otworzył SSE po fakcie, nie czekał w nieskończoność).


6. Modele danych
6.1 applicant_db.applicants
Kolumna
Typ
Uwagi
id
UUID PK


email
citext UNIQUE


password_hash
text
argon2id
first_name / last_name
text


created_at
timestamptz



6.2 loan_db.loan_applications
Kolumna
Typ
Uwagi
id
UUID PK


applicant_id
UUID
brak FK między bazami — celowo (database-per-service)
amount
numeric(12,2)
1 000–200 000 PLN
term_months
int
3–60
monthly_income
numeric(12,2)
deklarowany
applicant_age
int


status
text
enum maszyny stanów
decision_reasons
jsonb
uzasadnienie z underwritingu
idempotency_key
text UNIQUE


created_at / updated_at
timestamptz



6.3 loan_db.outbox_events
Kolumna
Typ
id
UUID PK
aggregate_id
UUID (loan_id)
event_type / payload
text / jsonb
status
PENDING → SENT
created_at / sent_at
timestamptz

6.4 underwriting_db.underwriting_results
Kolumna
Typ
Uwagi
id
UUID PK


loan_id / applicant_id
UUID


features
jsonb
pełny wektor wejściowy — audytowalność
probability_of_default
numeric(5,4)


decision
APPROVED / REJECTED


reasons
jsonb
top czynniki
model_version
text
np. gb-2026-07-01
latency_ms
int
metryka do README


Pozostałe tabele (documents, disbursements, processed_events) analogicznie — kolumny wynikają wprost z sekcji 4.


7. Model ML — scoring kredytowy
7.1 Dane treningowe
Publiczne datasety kredytowe (np. "Give Me Some Credit" z Kaggle) lub generator syntetyczny (ml/generate_dataset.py, ~50 tys. wierszy) z realistycznymi zależnościami: PD rośnie z DTI, maleje z dochodem, U-kształtna zależność od wieku + szum. Generator jest lepszy do portfolio: pokazujesz, że rozumiesz dlaczego model działa, i nie masz problemów licencyjnych.
7.2 Cechy (features)
amount, term_months, monthly_income, age + inżynieria cech: dti (rata annuitetowa / dochód), amount_to_annual_income, income_per_term. Rata liczona wzorem annuitetowym przy stałym oprocentowaniu 12% (stała biznesowa w configu).
7.3 Trening i artefakt
Pipeline sklearn: ColumnTransformer (skalowanie) + GradientBoostingClassifier.
Split 80/20, metryki do README: ROC-AUC, precision/recall przy progu 0.30, macierz pomyłek.
Artefakt: model.joblib + model_card.md (wersja, data, metryki, cechy) — wersjonowane w repo (mały plik). Trening: make train-model.
Serwis ładuje model raz per proces workera (initializer ProcessPoolExecutor), nigdy per request.
7.4 Wyjaśnialność
Do MVP wystarczy ranking wkładów cech (feature importance × odchylenie wartości od średniej) mapowany na komunikaty PL. Upgrade opcjonalny: SHAP — wpisz do "future work".


8. Bezpieczeństwo
JWT RS256: Applicant podpisuje kluczem prywatnym; Gateway waliduje publicznym (dystrybucja przez wolumen w compose; w README wzmianka o JWKS jako produkcyjnym odpowiedniku).
Zaufane nagłówki wewnętrzne: serwisy przyjmują X-User-ID wyłącznie z sieci wewnętrznej; Gateway zdejmuje te nagłówki z ruchu przychodzącego z zewnątrz (ochrona przed spoofingiem).
Network isolation: compose definiuje sieci edge (Gateway + frontend) i internal (wszystko inne); na hosta wystawione tylko porty 3000 (frontend), 8000 (gateway) + porty narzędzi dev (Jaeger UI, MinIO Console) opisane jako dev-only.
Rate limiting: Redis sliding window na /auth/* (5/min/IP) i POST /loans (3/10 min/user).
Walidacja plików: typ po magic bytes (python-magic), nie po rozszerzeniu; limit 5 MB; losowa nazwa obiektu w MinIO.
Sekrety: .env + .env.example; zero sekretów w kodzie i historii gita.
CORS: tylko origin frontendu; cookies HttpOnly + SameSite=Lax dla refresh tokena.


9. Obserwowalność
9.1 Correlation ID i tracing
Gateway generuje X-Correlation-ID (lub przejmuje z frontendu) → propagacja w nagłówkach HTTP i w kopercie każdego zdarzenia Kafki.
OpenTelemetry z auto-instrumentacją (opentelemetry-instrumentation-fastapi, -sqlalchemy, -aiokafka, -httpx) → eksport OTLP do Jaeger. Efekt demo: jeden trace od POST /loans przez Kafkę aż po Stripe — zrzut ekranu z Jaegera obowiązkowo w README.
9.2 Logi
structlog w JSON; każdy wpis zawiera: timestamp, level, service, correlation_id, event, kontekst. Wspólna konfiguracja w libs/observability. docker compose logs -f + grep po correlation_id wystarcza w MVP (Loki/Grafana jako opcjonalny etap).
9.3 Metryki i health checks
/health (liveness) i /ready (readiness: ping DB/Kafka/Redis) w każdym serwisie — używane przez depends_on: condition: service_healthy w compose.
prometheus-fastapi-instrumentator → /metrics (latencja, RPS, błędy) + własne liczniki: underwriting_decisions_total{decision=...}, outbox_lag, dlq_messages_total. Prometheus+Grafana jako etap opcjonalny.


10. Struktura repozytorium (monorepo)
crediguard/

├── README.md                  # diagram, demo GIF, quickstart, ADR

├── docs/

│   ├── adr/                   # 0001-kafka-over-rabbitmq.md, 0002-fastapi-over-django.md, ...

│   └── SPECYFIKACJA.md        # ten dokument

├── CLAUDE.md                  # kontekst dla Claude Code (patrz sekcja 12)

├── docker-compose.yml

├── docker-compose.infra.yml   # sama infrastruktura (dev bez kontenerów appek)

├── Makefile                   # up / down / test / train-model / seed / logs

├── .env.example

├── frontend/                  # Next.js

├── services/

│   ├── gateway/

│   ├── applicant/

│   ├── loan-application/

│   ├── document/

│   ├── underwriting/

│   ├── disbursement/

│   └── notification/

├── libs/                      # pakiety współdzielone (instalowane jako lokalne zależności)

│   ├── events/                # koperta + schematy zdarzeń (Pydantic)

│   ├── kafka/                 # BaseConsumer z retry/DLQ, producer z outboxem

│   └── observability/         # structlog config, middleware correlation ID, OTel setup

├── ml/                        # generate_dataset.py, train.py, model_card.md

└── scripts/                   # create_topics.py, dlq_inspect.py, seed_demo.py, demo_flow.py

Uzasadnienie monorepo: jeden PR = spójna zmiana kontraktu zdarzeń we wszystkich serwisach; libs/ bez publikowania paczek; jedno docker compose up. W realnej firmie serwisy miałyby osobne repo/CI — wspomnij o tym trade-offie w ADR.


11. Plan implementacji krok po kroku
Zasada nadrzędna: po każdym etapie system działa end-to-end w takim zakresie, w jakim istnieje (pionowe przyrosty, nie "najpierw wszystkie backendy"). Każdy etap = osobna gałąź + PR do samego siebie z opisem (świetnie wygląda w historii repo). Szacunki zakładają pracę wieczorami z Claude Code.
Etap 0 — Fundament (1–2 dni)
Zadania:

Repo, README.md (szkic z diagramem z sekcji 2), CLAUDE.md, .editorconfig, pre-commit (ruff, mypy).
docker-compose.infra.yml: Postgres 16 (init-script tworzący 5 baz + userów), Redis 7, Kafka KRaft, MinIO, Jaeger.
Makefile: make infra-up, make infra-down, make logs.
scripts/create_topics.py (topiki + DLQ z sekcji 5.1).
Szkielet libs/observability (structlog JSON + middleware correlation ID) i libs/events (koperta zdarzenia).

Definition of Done: make infra-up stawia infrastrukturę; kafka-topics --list pokazuje topiki; Jaeger UI działa na :16686.
Etap 1 — Applicant Service + auth (2–3 dni)
Zadania: struktura hexagonalna, rejestracja (argon2id), login, JWT RS256 (skrypt generujący parę kluczy), refresh token, Alembic, testy unit (domain) + integracyjne (testcontainers).

DoD: curl rejestruje i loguje użytkownika; pytest zielone; mypy strict przechodzi.

Czego się uczysz: Clean Architecture w praktyce, async SQLAlchemy, kryptografia haseł/JWT.
Etap 2 — API Gateway (1–2 dni)
Zadania: walidacja JWT kluczem publicznym, reverse proxy przez httpx.AsyncClient (streaming), zdejmowanie/doklejanie nagłówków (X-User-ID), rate limiting w Redis, propagacja correlation ID, /health.

DoD: rejestracja/login działa wyłącznie przez :8000; żądanie z fałszywym X-User-ID z zewnątrz jest czyszczone; limit 5 loginów/min zwraca 429.
Etap 3 — Loan Application Service + Outbox (3–4 dni, serce projektu)
Zadania: encja LoanApplication z maszyną stanów (domain), POST /loans z idempotencją HTTP, tabela outbox_events, outbox worker (FOR UPDATE SKIP LOCKED), producer aiokafka (acks=all, idempotent), GET /loans/{id}.

DoD: złożenie wniosku → rekord w bazie → zdarzenie widoczne w topiku (kcat/skrypt); ubicie Kafki na chwilę nie gubi zdarzeń (dowód: test integracyjny); duplikat Idempotency-Key zwraca tę samą odpowiedź.

Czego się uczysz: transactional outbox, semantyka dostarczania, idempotencja.
Etap 4 — Notification Service + SSE (2 dni)
Zadania: consumer loan.status.changed.v1 → Redis Pub/Sub; endpoint SSE w Gateway (heartbeat, initial state z REST, Last-Event-ID); zdarzenie statusowe publikowane przy każdej zmianie stanu wniosku.

DoD: curl -N .../events pokazuje na żywo zmianę statusu wywołaną ręczną publikacją zdarzenia testowego.
Etap 5 — Frontend: auth + wniosek + live status (4–5 dni)
Zadania: Next.js App Router; strony /login, /register, /dashboard, /loans/new, /loans/[id]; Zustand (sesja, toasty); formularz RHF+Zod zmapowany 1:1 z Pydantic; komponent LoanStatusTimeline na EventSource z automatycznym reconnect; shadcn/ui + Tailwind; refresh token w HttpOnly cookie, wywołania API z Server Actions/Route Handlers (sekrety nie trafiają do przeglądarki).

DoD: pełny flow w przeglądarce: rejestracja → wniosek → status zmienia się bez odświeżania (na razie do etapu, który istnieje).
Etap 6 — Document Service + MinIO (2 dni)
Zadania: upload multipart → MinIO (boto3), walidacja magic bytes/rozmiaru, consumer loan.application.submitted.v1 uruchamiający mock-weryfikację (sleep + reguły), publikacja document.verified/rejected.v1; Loan App konsumuje i przechodzi SUBMITTED → DOC_VERIFICATION → UNDERWRITING (albo DOC_REJECTED); dropzone na froncie.

DoD: wgranie PDF → po ~3 s status na froncie sam przechodzi na "Analiza ryzyka".
Etap 7 — ML + Underwriting Engine (4–5 dni)
Zadania: ml/generate_dataset.py, ml/train.py (pipeline, metryki, model.joblib, model_card.md); serwis: velocity checks (Redis Sorted Sets), hard rules, predykcja w ProcessPoolExecutor, uzasadnienie decyzji, zapis audytowy, publikacja underwriting.completed.v1; Loan App aktualizuje status na APPROVED/REJECTED z powodami.

DoD: wniosek z DTI > 60% dostaje odrzucenie z czytelnym uzasadnieniem na froncie; 4. wniosek w 10 minut → fraud_suspected; test dowodzący, że /health odpowiada < 100 ms podczas predykcji (izolacja CPU-bound działa).

Czego się uczysz: ML end-to-end, GIL i ProcessPool, feature engineering.
Etap 8 — Disbursement + Stripe (2–3 dni)
Zadania: consumer underwriting.completed.v1 (APPROVED) → PaymentIntent w Stripe test mode z Idempotency-Key=loan_id → webhook payment_intent.succeeded (Stripe CLI forwarduje na localhost) → disbursement.completed.v1 → Loan App: DISBURSED; tabela processed_events.

DoD: akceptacja kończy się statusem DISBURSED i widoczną płatnością w dashboardzie Stripe; ręczna re-publikacja zdarzenia NIE tworzy drugiej płatności.
Etap 9 — Resilience: retry + DLQ (2 dni)
Zadania: BaseConsumer w libs/kafka (backoff + jitter, DLQ z nagłówkami diagnostycznymi), wdrożenie we wszystkich konsumentach, scripts/dlq_inspect.py, test chaos: flaga env SIMULATE_FAILURE=underwriting wymusza wyjątek → wiadomość ląduje w DLQ, inne wnioski płyną dalej.

DoD: scenariusz chaosu udokumentowany w README z logami.
Etap 10 — Observability + polish + prezentacja (3–4 dni)
Zadania: OTel we wszystkich serwisach + Jaeger (trace przez Kafkę!), /metrics, docker-compose.yml pełny (service_healthy), make up = cały system, scripts/seed_demo.py, finalne README: diagram, GIF/wideo demo (split-screen: frontend + docker compose logs -f), sekcja ADR (min. 4 decyzje: Kafka vs RabbitMQ, FastAPI vs Django, outbox vs 2PC, ProcessPool vs to_thread), sekcja "Known limitations / future work" (K8s, saga compensation, SHAP, JWKS, schema registry).

DoD: świeży klon repo → make up → pełny flow działa; README broni się bez Ciebie.

Suma: ~5–7 tygodni przy pracy wieczorami. Nie zmieniaj kolejności etapów 3→4→5 — dzięki niej frontend od początku pracuje na żywym strumieniu zdarzeń.


12. Praca z Claude Code
12.1 CLAUDE.md w root repo (Claude Code czyta go automatycznie)
Umieść w nim (skrótowo, to plik roboczy, nie esej):

# CrediGuard — kontekst projektu

## Czym jest projekt

Fintech MVP: mikroserwisy (FastAPI), Kafka, ML scoring, Next.js. Pełna specyfikacja: docs/SPECYFIKACJA.md — ZAWSZE sprawdzaj tam kontrakty zdarzeń, maszynę stanów i strukturę warstw przed implementacją.

## Twarde reguły architektury

- Clean Architecture: domain nie importuje NICZEGO spoza stdlib; api → application → domain.

- Serwisy NIE czytają cudzych baz. Komunikacja: Kafka (async) lub HTTP przez gateway (sync).

- Każde zdarzenie: koperta z libs/events, klucz partycji = applicant_id, sufiks .v1.

- Każdy konsument: idempotencja przez processed_events + retry/DLQ z libs/kafka.

- Wszystkie statusy wniosku zmienia WYŁĄCZNIE loan-application-service (maszyna stanów w domain).

## Konwencje

- Python 3.12, typy wszędzie, mypy --strict musi przechodzić, ruff format.

- Testy: pytest; unit dla domain/application, testcontainers dla infrastruktury.

- Commity: conventional commits (feat/fix/test/docs/refactor + scope serwisu).

- Sekrety tylko przez env; nowa zmienna => aktualizacja .env.example.

## Komendy

make infra-up / make up / make test / make train-model

Testy pojedynczego serwisu: cd services/<x> && pytest
12.2 Jak prowadzić Claude Code (żeby faktycznie się uczyć)
Jeden etap = jedna sesja z planem. Zacznij od: "Przeczytaj docs/SPECYFIKACJA.md sekcję X i CLAUDE.md. Zaplanuj implementację etapu N, ale NIE pisz jeszcze kodu — pokaż plan do akceptacji." Przejrzyj plan, skoryguj, dopiero potem zatwierdź. (W Claude Code służy do tego tryb planowania.)
Pionowe plastry, częste commity. Po każdym działającym kawałku: testy + commit. Małe kroki = łatwe review i łatwy rollback.
Ty jesteś reviewerem. Po każdym etapie poproś: "Wytłumacz mi, jak działa outbox worker, który napisałeś, i dlaczego użyłeś SKIP LOCKED" — i nie przechodź dalej, dopóki nie umiesz tego opowiedzieć bez patrzenia w kod. To jest Twoja nauka; na rozmowie rekrutacyjnej obronisz tylko to, co rozumiesz.
TDD tam, gdzie logika: dla maszyny stanów i pipeline'u underwritingu każ najpierw napisać testy domain, potem implementację.
Nie pozwalaj na skróty łamiące specyfikację. Jeśli Claude Code zaproponuje "na razie zapiszmy bezpośrednio do Kafki bez outboxa" — odmów; spec jest źródłem prawdy. Aktualizuj SPECYFIKACJA.md, gdy świadomie zmieniasz decyzję (i dopisz ADR).
Higiena kontekstu: nowa funkcjonalność = nowa sesja/wyczyszczony kontekst + odesłanie do specyfikacji, zamiast jednej niekończącej się rozmowy.

Dokumentacja Claude Code (tryby, konfiguracja, CLAUDE.md): https://docs.claude.com/en/docs/claude-code/overview


Załącznik A — Checklist "czy to wygląda seniorsko?" (przed publikacją)
README z diagramem, GIF-em demo i quickstartem make up (max 3 komendy do uruchomienia)
Min. 4 ADR-y z realnymi trade-offami (nie tylko zaletami wybranej opcji)
Zrzut z Jaegera: jeden trace przez ≥ 4 serwisy i Kafkę
Test integracyjny dowodzący idempotencji wypłaty
Scenariusz chaosu (DLQ) opisany z logami
model_card.md z metrykami (ROC-AUC, macierz pomyłek)
mypy --strict i ruff w CI (GitHub Actions: lint + testy na PR)
Sekcja "Known limitations" — pokazuje, że znasz różnicę między MVP a produkcją

