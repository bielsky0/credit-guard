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
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )

    op.create_table(
        "outbox_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("aggregate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(255), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="PENDING"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
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
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )


def downgrade() -> None:
    op.drop_table("processed_events")
    op.drop_index("ix_outbox_pending", table_name="outbox_events")
    op.drop_table("outbox_events")
    op.drop_table("loan_applications")