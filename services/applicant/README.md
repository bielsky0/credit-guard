# Applicant Service

Registration, login, and JWT (RS256) token management for CrediGuard.

## Architecture

Clean Architecture (Hexagonal):
- **domain/** - Entities (`Applicant`, `RefreshToken`), exceptions
- **application/** - Use cases (`Register`, `Login`, `RefreshToken`, `GetMe`), ports (interfaces)
- **infrastructure/** - SQLAlchemy repositories, Argon2id hasher, RS256 JWT service, keygen
- **api/** - FastAPI routes, schemas, dependencies

## Quick Start

```bash
# From repo root
make infra-up          # Start Postgres, Redis, Kafka, MinIO, Jaeger
cd services/applicant
cp .env.example .env   # Adjust if needed
pip install -e ".[dev]"

# Generate RSA keys (once)
python -m src.infrastructure.security.keygen

# Run migrations
alembic upgrade head

# Run service
uvicorn src.main:app --reload --port 8001
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/v1/auth/register | Register new applicant |
| POST | /api/v1/auth/login | Login, get access + refresh tokens |
| POST | /api/v1/auth/refresh | Rotate refresh token, get new access token |
| GET | /api/v1/me | Get current applicant profile (requires Bearer token) |
| GET | /health | Liveness probe |
| GET | /ready | Readiness probe |

## Example Usage

```bash
# Register
curl -X POST http://localhost:8001/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"password123","first_name":"John","last_name":"Doe"}'

# Login
curl -X POST http://localhost:8001/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"password123"}'

# Get profile (use access_token from login)
curl http://localhost:8001/api/v1/me \
  -H "Authorization: Bearer <access_token>"

# Refresh token
curl -X POST http://localhost:8001/api/v1/auth/refresh \
  -H "Content-Type: application/json" \
  -d '{"refresh_token":"<refresh_token>"}'
```

## Testing

```bash
# Unit tests (no database)
pytest tests/unit -v

# Integration tests (requires testcontainers, uses real Postgres)
pytest tests/integration -v

# All tests
pytest -v

# Type checking
mypy --strict src

# Linting
ruff check src
ruff format src
```

## Security

- **Passwords**: Argon2id (memory-hard, not bcrypt)
- **JWT**: RS256 asymmetric signing
  - Private key: only in Applicant Service (`/app/keys/private_key.pem`)
  - Public key: shared with Gateway via Docker volume
- **Refresh tokens**: Stored hashed (Argon2id), rotated on use, revoked after rotation
- **Access tokens**: 15 min expiry
- **Refresh tokens**: 7 days expiry

## Database Schema

- `applicants` - id, email (unique), password_hash, first_name, last_name, created_at, updated_at
- `refresh_tokens` - id, applicant_id, token_hash (unique), expires_at, created_at, revoked_at

## Events Published

- `applicant.registered.v1` (future: welcome email, etc.)

## Key Generation

```bash
python -m src.infrastructure.security.keygen
```

Generates:
- `/app/keys/private_key.pem` (600) - Applicant Service only
- `/app/keys/public_key.pem` (644) - Mounted to Gateway