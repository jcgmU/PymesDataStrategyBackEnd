"""SQLAlchemy async implementation of the JobRepository port.

Reads and writes ``transformation_jobs``, ``anomalies``, and ``decisions``
tables using the same PostgreSQL instance managed by the Prisma-based
API Gateway.

JobStatus mapping (worker domain → Prisma DB enum):
    QUEUED          → "QUEUED"
    PROCESSING      → "PROCESSING"
    AWAITING_REVIEW → "PROCESSING"  (+ ai_suggestions.hitl_waiting = true)
    COMPLETED       → "COMPLETED"
    FAILED          → "FAILED"
    CANCELLED       → "CANCELLED"
"""

import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import structlog
from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.domain.entities.anomaly import AnomalyEntity
from src.domain.entities.decision import DecisionEntity
from src.domain.entities.transformation_job import TransformationJob
from src.domain.ports.repositories.job_repository import JobRepository
from src.domain.value_objects.job_status import JobStatus, TransformationType
from src.infrastructure.persistence.models import (
    AnomalyModel,
    DecisionModel,
    TransformationJobModel,
)


logger = structlog.get_logger("pymes.worker.persistence")


# ---------------------------------------------------------------------------
# Status mapping helpers
# ---------------------------------------------------------------------------

_WORKER_TO_DB: dict[JobStatus, str] = {
    JobStatus.QUEUED: "QUEUED",
    JobStatus.PROCESSING: "PROCESSING",
    JobStatus.AWAITING_REVIEW: "PROCESSING",  # No AWAITING_REVIEW in Prisma
    JobStatus.COMPLETED: "COMPLETED",
    JobStatus.FAILED: "FAILED",
    JobStatus.CANCELLED: "CANCELLED",
    JobStatus.PENDING: "QUEUED",  # PENDING is legacy; map to QUEUED
}

_DB_TO_WORKER: dict[str, JobStatus] = {
    "QUEUED": JobStatus.QUEUED,
    "PROCESSING": JobStatus.PROCESSING,
    "COMPLETED": JobStatus.COMPLETED,
    "FAILED": JobStatus.FAILED,
    "CANCELLED": JobStatus.CANCELLED,
}


def _map_status_to_db(status: JobStatus) -> str:
    return _WORKER_TO_DB.get(status, "PROCESSING")


def _map_status_from_db(db_status: str, ai_suggestions: dict[str, Any] | None) -> JobStatus:
    """Derive worker JobStatus from DB value + ai_suggestions marker."""
    if db_status == "PROCESSING" and ai_suggestions and ai_suggestions.get("hitl_waiting"):
        return JobStatus.AWAITING_REVIEW
    return _DB_TO_WORKER.get(db_status, JobStatus.PROCESSING)


def _map_transformation_type(db_type: str) -> TransformationType:
    """Map Prisma TransformationType string to worker enum."""
    mapping = {
        "CLEAN_NULLS": TransformationType.CLEAN,
        "NORMALIZE": TransformationType.NORMALIZE,
        "AGGREGATE": TransformationType.NORMALIZE,
        "FILTER": TransformationType.TRANSFORM,
        "MERGE": TransformationType.MERGE,
        "CUSTOM": TransformationType.TRANSFORM,
    }
    return mapping.get(db_type, TransformationType.TRANSFORM)


# ---------------------------------------------------------------------------
# Repository implementation
# ---------------------------------------------------------------------------


class SQLAlchemyJobRepository(JobRepository):
    """Async SQLAlchemy implementation of ``JobRepository``.

    Args:
        session_factory: Callable that produces an ``AsyncSession``.  This is
            typically the ``async_sessionmaker`` created by ``create_session_factory``
            in ``database.py``.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession] | Callable[[], AsyncSession],
    ) -> None:
        self._session_factory = session_factory

    # ------------------------------------------------------------------
    # JobRepository interface
    # ------------------------------------------------------------------

    async def get_job(self, job_id: str) -> TransformationJob | None:
        """Fetch a job by ID and map to domain entity."""
        async with self._session_factory() as session:
            stmt = select(TransformationJobModel).where(
                TransformationJobModel.id == job_id
            )
            result = await session.execute(stmt)
            row = result.scalar_one_or_none()

            if row is None:
                return None

            return self._model_to_entity(row)

    async def update_job_status(
        self,
        job_id: str,
        status: JobStatus,
        result: dict[str, object] | None = None,
        error: str | None = None,
    ) -> None:
        """Persist a new status and optional result/error."""
        log = logger.bind(job_id=job_id, target_status=status.value)

        db_status = _map_status_to_db(status)

        # Build the values dict for the UPDATE
        values: dict[str, Any] = {"status": db_status}

        if status == JobStatus.PROCESSING:
            values["started_at"] = datetime.now(UTC)

        if status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
            values["completed_at"] = datetime.now(UTC)

        if status == JobStatus.AWAITING_REVIEW:
            # Keep status = PROCESSING in DB; mark hitl_waiting in ai_suggestions
            values["ai_suggestions"] = {"hitl_waiting": True}

        if result is not None:
            values["result_metadata"] = result
            if result.get("output_key"):
                values["result_storage_key"] = result["output_key"]

        if error is not None:
            values["error_message"] = error

        async with self._session_factory() as session:
            stmt = (
                update(TransformationJobModel)
                .where(TransformationJobModel.id == job_id)
                .values(**values)
            )
            await session.execute(stmt)
            await session.commit()

        log.info("Job status updated", db_status=db_status)

    async def save_anomalies(
        self,
        dataset_id: str,
        anomalies: list[AnomalyEntity],
    ) -> None:
        """Bulk-insert anomaly records."""
        if not anomalies:
            return

        async with self._session_factory() as session:
            for anomaly in anomalies:
                model = AnomalyModel(
                    id=anomaly.id,
                    dataset_id=dataset_id,
                    column=anomaly.column,
                    row=anomaly.row,
                    type=anomaly.type,
                    description=anomaly.description,
                    original_value=anomaly.original_value,
                    suggested_value=anomaly.suggested_value,
                    status=anomaly.status,
                    created_at=anomaly.created_at,
                    updated_at=anomaly.created_at,
                )
                session.add(model)
            await session.commit()

        logger.info(
            "Anomalies saved",
            dataset_id=dataset_id,
            count=len(anomalies),
        )

    async def get_anomalies(self, dataset_id: str) -> list[AnomalyEntity]:
        """Fetch all anomalies for the dataset."""
        async with self._session_factory() as session:
            stmt = select(AnomalyModel).where(AnomalyModel.dataset_id == dataset_id)
            result = await session.execute(stmt)
            rows = result.scalars().all()
            
            anomalies = []
            for row in rows:
                anomalies.append(
                    AnomalyEntity(
                        id=row.id,
                        dataset_id=row.dataset_id,
                        column=row.column,
                        row=row.row,
                        type=row.type,
                        description=row.description,
                        original_value=row.original_value,
                        suggested_value=row.suggested_value,
                        status=row.status,
                        created_at=row.created_at,
                    )
                )
            return anomalies

    async def get_decisions(self, dataset_id: str) -> list[DecisionEntity]:
        """Return all decisions for anomalies belonging to dataset_id."""
        async with self._session_factory() as session:
            # Join decisions → anomalies on anomaly.dataset_id
            stmt = (
                select(DecisionModel)
                .join(AnomalyModel, DecisionModel.anomaly_id == AnomalyModel.id)
                .where(AnomalyModel.dataset_id == dataset_id)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()

            return [self._decision_model_to_entity(row) for row in rows]

    async def count_pending_anomalies(self, dataset_id: str) -> int:
        """Count PENDING anomalies for the dataset."""
        async with self._session_factory() as session:
            stmt = (
                select(func.count())
                .select_from(AnomalyModel)
                .where(
                    AnomalyModel.dataset_id == dataset_id,
                    AnomalyModel.status == "PENDING",
                )
            )
            result = await session.execute(stmt)
            count = result.scalar_one()
            return int(count)

    # ------------------------------------------------------------------
    # Private mapping helpers
    # ------------------------------------------------------------------

    def _model_to_entity(self, model: TransformationJobModel) -> TransformationJob:
        """Convert an ORM model to a domain entity."""
        from uuid import UUID

        status = _map_status_from_db(model.status, model.ai_suggestions)
        transformation_type = _map_transformation_type(model.transformation_type)

        return TransformationJob(
            id=UUID(model.id) if len(model.id) == 36 else UUID(int=int(model.id, 16)),
            dataset_id=UUID(model.dataset_id) if len(model.dataset_id) == 36 else UUID(int=0),
            transformation_type=transformation_type,
            status=status,
            config=model.parameters or {},
            result=model.result_metadata,
            error_message=model.error_message,
            progress=100 if status == JobStatus.COMPLETED else 0,
            created_by=UUID(model.user_id) if len(model.user_id) == 36 else UUID(int=0),
            created_at=model.created_at or datetime.now(UTC),
            started_at=model.started_at,
            completed_at=model.completed_at,
            metadata=model.ai_suggestions or {},
        )

    def _decision_model_to_entity(self, model: DecisionModel) -> DecisionEntity:
        """Convert a DecisionModel ORM row to a domain entity."""
        # correction_ir and ir_source may not exist if the DB migration hasn't run yet.
        try:
            correction_ir = model.correction_ir
        except AttributeError:
            correction_ir = None

        try:
            ir_source = model.ir_source
        except AttributeError:
            ir_source = None

        return DecisionEntity(
            id=model.id,
            anomaly_id=model.anomaly_id,
            action=model.action,
            correction=model.correction,
            user_id=model.user_id,
            created_at=model.created_at or datetime.now(UTC),
            correction_ir=correction_ir,
            ir_source=ir_source,
        )
