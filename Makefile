SHELL := /bin/bash
COMPOSE_INFRA := docker compose -f docker-compose.infra.yml

.PHONY: infra-up infra-down infra-down-v logs topics up test train-model seed

infra-up: ## Start local infra: Postgres, Redis, Kafka (KRaft), MinIO, Jaeger
	$(COMPOSE_INFRA) up -d
	@echo "Postgres  -> localhost:5433"
	@echo "Redis     -> localhost:6380"
	@echo "Kafka     -> localhost:9094 (host), kafka:9092 (in-network)"
	@echo "MinIO     -> http://localhost:9001 (console), http://localhost:9000 (API)"
	@echo "Jaeger UI -> http://localhost:16686"

infra-down: ## Stop local infra (keeps volumes)
	$(COMPOSE_INFRA) down

infra-down-v: ## Stop local infra and wipe volumes (clean data reset)
	$(COMPOSE_INFRA) down -v

logs: ## Tail logs for all infra containers
	$(COMPOSE_INFRA) logs -f

topics: ## Create Kafka topics + DLQs (spec §5.1)
	python3 scripts/create_topics.py

# --- Stubs for later etaps (see docs/SPECYFIKACJA.md §11) ---

up: ## TODO(Etap 10): bring up full stack (infra + all services + frontend)
	@echo "Not implemented yet — lands in Etap 10. Use 'make infra-up' for now."
	@exit 1

test: ## Run test suites across services
	@echo "--- Applicant Service ---"
	cd services/applicant && PYTHONPATH=. .venv/bin/pytest tests/unit -q
	@echo "--- Gateway ---"
	cd services/gateway && .venv/bin/pytest tests/unit -q

train-model: ## TODO(Etap 7): train the credit scoring model
	@echo "Not implemented yet — lands in Etap 7."
	@exit 1

seed: ## TODO(Etap 10): seed demo data
	@echo "Not implemented yet — lands in Etap 10."
	@exit 1
