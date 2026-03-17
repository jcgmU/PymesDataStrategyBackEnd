"""Job queue service port - interface for job queue (BullMQ)."""

from abc import ABC, abstractmethod
from typing import Any
from uuid import UUID

from src.domain.entities.transformation_job import TransformationJob


class JobQueueService(ABC):
    """Port for job queue operations."""

    @abstractmethod
    async def enqueue_job(
        self,
        job: TransformationJob,
        priority: int = 0,
        delay: int | None = None,
    ) -> str:
        """Enqueue a transformation job.

        Returns the queue job ID.
        """
        ...

    @abstractmethod
    async def get_job_status(self, job_id: str) -> dict[str, Any] | None:
        """Get the status of a queued job."""
        ...

    @abstractmethod
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a queued or active job."""
        ...

    @abstractmethod
    async def retry_job(self, job_id: str) -> bool:
        """Retry a failed job."""
        ...

    @abstractmethod
    async def get_queue_stats(self) -> dict[str, int]:
        """Get queue statistics (waiting, active, completed, failed)."""
        ...


class JobProcessor(ABC):
    """Port for processing jobs from the queue."""

    @abstractmethod
    async def process(self, job_data: dict[str, Any]) -> dict[str, Any]:
        """Process a job and return the result."""
        ...

    @abstractmethod
    async def on_progress(self, job_id: UUID, progress: int) -> None:
        """Report job progress (0-100)."""
        ...

    @abstractmethod
    async def on_error(self, job_id: UUID, error: str) -> None:
        """Report job error."""
        ...
