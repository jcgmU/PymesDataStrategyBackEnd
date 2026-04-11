"""Job repository port — interface for transformation job persistence."""

from abc import ABC, abstractmethod

from src.domain.entities.anomaly import AnomalyEntity
from src.domain.entities.decision import DecisionEntity
from src.domain.entities.transformation_job import TransformationJob
from src.domain.value_objects.job_status import JobStatus


class JobRepository(ABC):
    """Port for transformation job persistence operations.

    The worker uses this interface to read/write job state, anomalies, and
    human decisions without knowing about the underlying storage technology.
    """

    @abstractmethod
    async def get_job(self, job_id: str) -> TransformationJob | None:
        """Fetch a transformation job by its primary-key ID.

        Args:
            job_id: The CUID/string identifier of the job.

        Returns:
            The job entity, or ``None`` if not found.
        """
        ...

    @abstractmethod
    async def update_job_status(
        self,
        job_id: str,
        status: JobStatus,
        result: dict[str, object] | None = None,
        error: str | None = None,
    ) -> None:
        """Persist a new status (and optional result/error) for a job.

        Args:
            job_id: The job's primary-key ID.
            status: Target ``JobStatus`` value.
            result: Optional result metadata to persist in ``result_metadata``.
            error: Optional error message to persist in ``error_message``.
        """
        ...

    @abstractmethod
    async def save_anomalies(
        self,
        dataset_id: str,
        anomalies: list[AnomalyEntity],
    ) -> None:
        """Bulk-insert anomaly records for a dataset.

        Args:
            dataset_id: The dataset these anomalies belong to.
            anomalies: List of anomaly entities to persist.
        """
        ...

    @abstractmethod
    async def get_anomalies(self, dataset_id: str) -> list[AnomalyEntity]:
        """Return all anomalies associated with *dataset_id*.

        Args:
            dataset_id: The dataset whose anomalies are requested.

        Returns:
            List of anomaly entities.
        """
        ...

    @abstractmethod
    async def get_decisions(self, dataset_id: str) -> list[DecisionEntity]:
        """Return all decisions for anomalies associated with *dataset_id*.

        The query joins ``anomalies`` → ``decisions`` filtering by
        ``anomalies.dataset_id``.

        Args:
            dataset_id: The dataset whose anomaly decisions are requested.

        Returns:
            List of decision entities (may be empty if none exist yet).
        """
        ...

    @abstractmethod
    async def count_pending_anomalies(self, dataset_id: str) -> int:
        """Count anomalies still in PENDING status for *dataset_id*.

        Used during HITL polling to decide whether to proceed.

        Args:
            dataset_id: The dataset to check.

        Returns:
            Number of PENDING anomalies (0 means all have been reviewed).
        """
        ...

    @abstractmethod
    async def save_ai_suggestion(
        self,
        anomaly_id: str,
        action_type: str,
        value: str | None,
        reason: str,
    ) -> None:
        """Persist an AI-generated suggestion for an anomaly.

        Stores a JSON-encoded suggestion in the ``ai_suggestion`` column of the
        ``anomalies`` table.  The format matches the contract established in
        Wave 2:  ``{"actionType": "...", "value": "...", "reason": "..."}``.

        Args:
            anomaly_id: Primary key of the anomaly to update.
            action_type: Suggested action — ``"FILL"``, ``"DELETE"``, or ``"KEEP"``.
            value: Correction value string, or ``None`` when not applicable.
            reason: Human-readable explanation in Spanish.
        """
        ...
