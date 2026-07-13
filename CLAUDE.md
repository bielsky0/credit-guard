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