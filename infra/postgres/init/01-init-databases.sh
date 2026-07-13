#!/usr/bin/env bash
# Creates one database + one dedicated role per service ("database-per-service", spec §3/§6).
# Runs once, on first container start, as the Postgres superuser (POSTGRES_USER).
set -euo pipefail

create_service_db() {
  local db_name="$1"
  local db_user="$2"
  local db_password="$3"

  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    DO \$\$
    BEGIN
      IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${db_user}') THEN
        CREATE ROLE "${db_user}" WITH LOGIN PASSWORD '${db_password}';
      END IF;
    END
    \$\$;

    SELECT 'CREATE DATABASE "${db_name}" OWNER "${db_user}"'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${db_name}')\gexec

    REVOKE ALL ON DATABASE "${db_name}" FROM PUBLIC;
    GRANT ALL PRIVILEGES ON DATABASE "${db_name}" TO "${db_user}";
EOSQL
}

create_service_db "${APPLICANT_DB_NAME}" "${APPLICANT_DB_USER}" "${APPLICANT_DB_PASSWORD}"
create_service_db "${LOAN_DB_NAME}" "${LOAN_DB_USER}" "${LOAN_DB_PASSWORD}"
create_service_db "${DOCUMENT_DB_NAME}" "${DOCUMENT_DB_USER}" "${DOCUMENT_DB_PASSWORD}"
create_service_db "${UNDERWRITING_DB_NAME}" "${UNDERWRITING_DB_USER}" "${UNDERWRITING_DB_PASSWORD}"
create_service_db "${DISBURSEMENT_DB_NAME}" "${DISBURSEMENT_DB_USER}" "${DISBURSEMENT_DB_PASSWORD}"

echo "5 service databases + roles created (or already present)."
