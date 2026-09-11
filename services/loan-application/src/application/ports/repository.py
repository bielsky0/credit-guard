from __future__ import annotations

from abc import ABC, abstractmethod
from uuid import UUID

from src.domain.entities import LoanApplication, OutboxEvent, ProcessedEvent


class LoanApplicationRepository(ABC):
    @abstractmethod
    async def save(self, loan: LoanApplication) -> None: ...

    @abstractmethod
    async def get_by_id(self, loan_id: UUID) -> LoanApplication | None: ...

    @abstractmethod
    async def get_by_applicant_id(self, applicant_id: UUID) -> list[LoanApplication]: ...

    @abstractmethod
    async def exists_active_for_applicant(self, applicant_id: UUID) -> bool: ...

    @abstractmethod
    async def get_by_idempotency_key(self, key: str) -> LoanApplication | None: ...


class OutboxRepository(ABC):
    @abstractmethod
    async def save(self, event: OutboxEvent) -> None: ...

    @abstractmethod
    async def get_pending(self, limit: int = 10) -> list[OutboxEvent]: ...

    @abstractmethod
    async def mark_sent(self, event_id: UUID) -> None: ...


class ProcessedEventRepository(ABC):
    @abstractmethod
    async def exists(self, event_id: UUID) -> bool: ...

    @abstractmethod
    async def save(self, event: ProcessedEvent) -> None: ...