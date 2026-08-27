"""Refresh token use case for Applicant Service."""

from __future__ import annotations

from datetime import datetime, timezone

from src.application.dto import RefreshTokenRequest, TokenResponse
from src.application.ports.password_hasher import PasswordHasher
from src.application.ports.repository import ApplicantRepository, RefreshTokenRepository
from src.application.ports.token_service import TokenService
from src.domain.entities import RefreshToken
from src.domain.exceptions import InvalidRefreshToken, RefreshTokenRevoked


class RefreshTokenUseCase:
    """Use case for refreshing access token."""

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

    async def execute(self, request: RefreshTokenRequest) -> TokenResponse:
        """Execute token refresh."""
        now = datetime.now(timezone.utc)

        for token_entity in await self._refresh_token_repo.get_all_valid(now):
            if self._password_hasher.verify(request.refresh_token, token_entity.token_hash):
                if not token_entity.is_valid(now):
                    raise RefreshTokenRevoked()

                token_entity.revoke()
                await self._refresh_token_repo.update(token_entity)

                applicant = await self._applicant_repo.get_by_id(token_entity.applicant_id)
                if applicant is None:
                    raise InvalidRefreshToken()

                new_access_token = self._token_service.create_access_token(
                    applicant.id, applicant.email
                )
                new_refresh_token = self._token_service.create_refresh_token(applicant.id)

                new_refresh_token_hash = self._password_hasher.hash(new_refresh_token)
                new_refresh_token_entity = RefreshToken(
                    applicant_id=applicant.id,
                    token_hash=new_refresh_token_hash,
                    expires_at=self._token_service.get_refresh_token_expiry(),
                )
                await self._refresh_token_repo.save(new_refresh_token_entity)

                return TokenResponse(
                    access_token=new_access_token,
                    refresh_token=new_refresh_token,
                    expires_in=int(
                        (
                            self._token_service.get_access_token_expiry()
                            - datetime.now(timezone.utc)
                        ).total_seconds()
                    ),
                )

        raise InvalidRefreshToken()