# CrediGuard

Fintech MVP pożyczkowe: mikroserwisy (FastAPI, async), architektura sterowana zdarzeniami (Kafka), scoring kredytowy ML, frontend Next.js. Projekt portfolio na poziomie Senior Full-Stack Developer — pełny kontekst, decyzje i uzasadnienia w [`docs/SPECYFIKACJA.md`](docs/SPECYFIKACJA.md).

## Status

🚧 **Etap 0 — Fundament.** Infrastruktura lokalna (Postgres, Redis, Kafka, MinIO, Jaeger) + szkielety pakietów współdzielonych. Żaden serwis aplikacyjny nie istnieje jeszcze — patrz [plan implementacji](docs/SPECYFIKACJA.md#11-plan-implementacji-krok-po-kroku) po kolejne etapy.

## Architektura

```mermaid
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
```

Pełny opis przepływów, kontraktów zdarzeń, maszyny stanów i modelu ML: [`docs/SPECYFIKACJA.md`](docs/SPECYFIKACJA.md).

## Quickstart (Etap 0)

```bash
cp .env.example .env
make infra-up      # Postgres, Redis, Kafka (KRaft), MinIO, Jaeger
make topics         # tworzy topiki Kafki (§5.1 specyfikacji)
```

- Jaeger UI: http://localhost:16686
- MinIO Console: http://localhost:9001
- Kafka (host): `localhost:9094`

`make infra-down` zatrzymuje infrastrukturę; `make infra-down-v` dodatkowo usuwa woluminy (czysty reset danych).

Pełny `make up` (całość systemu wraz z serwisami) pojawi się po Etapie 10.

## Struktura repozytorium

Zobacz [§10 specyfikacji](docs/SPECYFIKACJA.md#10-struktura-repozytorium-monorepo).

## Dokumentacja

- [`docs/SPECYFIKACJA.md`](docs/SPECYFIKACJA.md) — pełna specyfikacja techniczna (źródło prawdy)
- [`docs/adr/`](docs/adr/) — Architecture Decision Records (od Etapu 1+)
- [`CLAUDE.md`](CLAUDE.md) — kontekst i twarde reguły dla pracy z Claude Code
