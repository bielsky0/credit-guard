"""Login use case for Applicant Service."""

from __future__ import annotations

from datetime import datetime, timezone

from src.application.dto import LoginRequest, TokenResponse
from src.application.ports.password_hasher import PasswordHasher
from src.application.ports.repository import ApplicantRepository, RefreshTokenRepository
from src.application.ports.token_service import TokenService
from src.domain.entities import RefreshToken
from src.domain.exceptions import InvalidCredentials


class LoginUseCase:
    """Use case for applicant login."""

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

    async def execute(self, request: LoginRequest) -> TokenResponse:
        """Execute login."""
        applicant = await self._applicant_repo.get_by_email(request.email)
        if applicant is None:
            raise InvalidCredentials()

        if not self._password_hasher.verify(request.password, applicant.password_hash):
            raise InvalidCredentials()

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