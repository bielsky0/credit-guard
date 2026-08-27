"""Me routes for Applicant Service."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends
from fastapi.routing import APIRouter as APIRouterType
from src.api.dependencies import get_current_applicant_id, get_get_me_use_case
from src.application.dto import ApplicantResponse
from src.application.use_cases.get_me import GetMeUseCase

router: APIRouterType = APIRouter(prefix="/me", tags=["me"])


@router.get(
    "",
    response_model=ApplicantResponse,
    summary="Get current applicant profile",
)
async def get_me(
    applicant_id: UUID = Depends(get_current_applicant_id),
    use_case: GetMeUseCase = Depends(get_get_me_use_case),
) -> ApplicantResponse:
    """Get current authenticated applicant's profile."""
    return await use_case.execute(applicant_id)