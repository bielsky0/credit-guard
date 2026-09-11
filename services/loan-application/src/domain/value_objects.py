from __future__ import annotations

from enum import StrEnum


class LoanStatus(StrEnum):
    DRAFT = "DRAFT"
    SUBMITTED = "SUBMITTED"
    DOC_VERIFICATION = "DOC_VERIFICATION"
    UNDERWRITING = "UNDERWRITING"
    APPROVED = "APPROVED"
    DISBURSING = "DISBURSING"
    DISBURSED = "DISBURSED"
    DOC_REJECTED = "DOC_REJECTED"
    REJECTED = "REJECTED"
    DISBURSEMENT_FAILED = "DISBURSEMENT_FAILED"


# Legal transitions: from_status -> set of allowed to_statuses
VALID_TRANSITIONS: dict[LoanStatus, set[LoanStatus]] = {
    LoanStatus.DRAFT: {LoanStatus.SUBMITTED},
    LoanStatus.SUBMITTED: {LoanStatus.DOC_VERIFICATION, LoanStatus.REJECTED},
    LoanStatus.DOC_VERIFICATION: {
        LoanStatus.UNDERWRITING,
        LoanStatus.DOC_REJECTED,
    },
    LoanStatus.UNDERWRITING: {LoanStatus.APPROVED, LoanStatus.REJECTED},
    LoanStatus.APPROVED: {LoanStatus.DISBURSING},
    LoanStatus.DISBURSING: {LoanStatus.DISBURSED, LoanStatus.DISBURSEMENT_FAILED},
    LoanStatus.DISBURSED: set(),
    LoanStatus.DOC_REJECTED: set(),
    LoanStatus.REJECTED: set(),
    LoanStatus.DISBURSEMENT_FAILED: set(),
}

# Mapping status -> SSE step number and message for the frontend
STATUS_PROGRESS: dict[LoanStatus, tuple[int, str]] = {
    LoanStatus.SUBMITTED: (1, "Wniosek przyjęty..."),
    LoanStatus.DOC_VERIFICATION: (2, "Weryfikacja dokumentu..."),
    LoanStatus.UNDERWRITING: (3, "Analiza ryzyka kredytowego..."),
    LoanStatus.APPROVED: (4, "Wniosek zaakceptowany"),
    LoanStatus.DISBURSING: (5, "Wypłata środków..."),
    LoanStatus.DISBURSED: (5, "Wypłacono"),
    LoanStatus.DOC_REJECTED: (2, "Dokument zweryfikowany negatywnie"),
    LoanStatus.REJECTED: (3, "Wniosek odrzucony"),
    LoanStatus.DISBURSEMENT_FAILED: (5, "Błąd wypłaty"),
}

TOTAL_STEPS = 5