"""Register use case for Applicant Service."""

from __future__ import annotations

from datetime import datetime, timezone

from src.application.dto import RegisterRequest, TokenResponse
from src.application.ports.password_hasher import PasswordHasher
from src.application.ports.repository import ApplicantRepository, RefreshTokenRepository
from src.application.ports.token_service import TokenService
from src.domain.entities import Applicant, RefreshToken
from src.domain.exceptions import EmailAlreadyRegistered


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

    async def execute(self, request: RegisterRequest) -> TokenResponse:
        """Execute registration."""
        if await self._applicant_repo.exists_by_email(request.email):
            raise EmailAlreadyRegistered(request.email)

        password_hash = self._password_hasher.hash(request.password)

        applicant = Applicant(
            email=request.email,
            password_hash=password_hash,
            first_name=request.first_name,
            last_name=request.last_name,
        )

        await self._applicant_repo.save(applicant)

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