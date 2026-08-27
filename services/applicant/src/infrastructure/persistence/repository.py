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


class SQLAlchemyApplicantRepository(ApplicantRepository):
    """SQLAlchemy implementation of ApplicantRepository."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

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

    async def get_by_id(self, applicant_id: UUID) -> Optional[Applicant]:
        stmt = select(ApplicantModel).where(ApplicantModel.id == applicant_id)
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        if model is None:
            return None
        return self._to_entity(model)

    async def get_by_email(self, email: str) -> Optional[Applicant]:
        stmt = select(ApplicantModel).where(ApplicantModel.email == email)
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        if model is None:
            return None
        return self._to_entity(model)

    async def exists_by_email(self, email: str) -> bool:
        stmt = select(ApplicantModel.id).where(ApplicantModel.email == email)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None

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

    async def update(self, token: RefreshToken) -> None:
        model = await self._session.get(RefreshTokenModel, token.id)
        if model is None:
            raise ValueError(f"Refresh token with id {token.id} not found")
        model.token_hash = token.token_hash
        model.expires_at = token.expires_at
        model.revoked_at = token.revoked_at
        await self._session.flush()

    async def get_by_hash(self, token_hash: str) -> Optional[RefreshToken]:
        stmt = select(RefreshTokenModel).where(RefreshTokenModel.token_hash == token_hash)
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        if model is None:
            return None
        return self._to_entity(model)

    async def revoke_all_for_applicant(self, applicant_id: UUID) -> None:
        stmt = select(RefreshTokenModel).where(
            RefreshTokenModel.applicant_id == applicant_id,
            RefreshTokenModel.revoked_at.is_(None),
        )
        result = await self._session.execute(stmt)
        for model in result.scalars().all():
            model.revoked_at = datetime.now()

    async def get_all_valid(self, now: datetime) -> list[RefreshToken]:
        stmt = select(RefreshTokenModel).where(
            RefreshTokenModel.expires_at > now,
            RefreshTokenModel.revoked_at.is_(None),
        )
        result = await self._session.execute(stmt)
        return [self._to_entity(model) for model in result.scalars().all()]

    def _to_entity(self, model: RefreshTokenModel) -> RefreshToken:
        return RefreshToken(
            id=model.id,
            applicant_id=model.applicant_id,
            token_hash=model.token_hash,
            expires_at=model.expires_at,
            created_at=model.created_at,
            revoked_at=model.revoked_at,
        )