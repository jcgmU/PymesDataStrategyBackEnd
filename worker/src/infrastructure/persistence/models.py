"""SQLAlchemy ORM models that mirror the Prisma schema tables.

Only the tables the worker needs to read/write are declared here:
- ``transformation_jobs``
- ``anomalies``
- ``decisions``

Column names and types match exactly what Prisma generates in PostgreSQL
(snake_case column names, native enum types represented as strings).
"""

from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, ENUM
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all ORM models."""

    pass


class TransformationJobModel(Base):
    """ORM mapping for the ``transformation_jobs`` table."""

    __tablename__ = "transformation_jobs"

    id: Mapped[str] = mapped_column(String, primary_key=True)

    # Job definition
    transformation_type: Mapped[str] = mapped_column(
        ENUM("CLEAN_NULLS", "NORMALIZE", "AGGREGATE", "FILTER", "MERGE", "CUSTOM", name="TransformationType", create_type=False), nullable=False
    )
    status: Mapped[str] = mapped_column(
        ENUM("QUEUED", "PROCESSING", "COMPLETED", "FAILED", "CANCELLED", name="JobStatus", create_type=False), nullable=False, default="QUEUED"
    )
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Input parameters (JSONB)
    parameters: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

    # AI suggestions (JSONB) — used for HITL signalling
    ai_suggestions: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

    # Output
    result_storage_key: Mapped[str | None] = mapped_column(String, nullable=True)
    result_metadata: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

    # Error handling
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_details: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3)

    # BullMQ reference
    bullmq_job_id: Mapped[str | None] = mapped_column(String, nullable=True)

    # Foreign keys
    dataset_id: Mapped[str] = mapped_column(
        String, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )

    # Timestamps (managed by Prisma / DB default)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    def __repr__(self) -> str:
        return f"<TransformationJobModel id={self.id!r} status={self.status!r}>"


class DatasetModel(Base):
    """Minimal ORM mapping for the ``datasets`` table.

    The worker only needs to reference this table via foreign keys; it does
    not need to read/write most dataset columns, but the model must exist so
    SQLAlchemy can resolve the FK from ``transformation_jobs`` and ``anomalies``.
    """

    __tablename__ = "datasets"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(
        ENUM("PENDING", "PROCESSING", "READY", "FAILED", name="DatasetStatus", create_type=False), nullable=False, default="PENDING"
    )
    original_file_name: Mapped[str] = mapped_column(String, nullable=False)
    storage_key: Mapped[str] = mapped_column(String, nullable=False)
    file_size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    mime_type: Mapped[str] = mapped_column(String, nullable=False)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # Relationships (needed for FK resolution)
    anomalies: Mapped[list["AnomalyModel"]] = relationship(
        "AnomalyModel", back_populates="dataset", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<DatasetModel id={self.id!r} name={self.name!r}>"


class AnomalyModel(Base):
    """ORM mapping for the ``anomalies`` table."""

    __tablename__ = "anomalies"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    dataset_id: Mapped[str] = mapped_column(
        String, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False
    )
    column: Mapped[str] = mapped_column(String, nullable=False)
    row: Mapped[int | None] = mapped_column(Integer, nullable=True)
    type: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    original_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    suggested_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    ai_suggestion: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(
        ENUM("PENDING", "RESOLVED", "IGNORED", name="AnomalyStatus", create_type=False), nullable=False, default="PENDING"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # Relationships
    dataset: Mapped["DatasetModel"] = relationship("DatasetModel", back_populates="anomalies")
    decision: Mapped["DecisionModel | None"] = relationship(
        "DecisionModel", back_populates="anomaly", uselist=False, cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<AnomalyModel id={self.id!r} type={self.type!r} status={self.status!r}>"


class DecisionModel(Base):
    """ORM mapping for the ``decisions`` table."""

    __tablename__ = "decisions"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    anomaly_id: Mapped[str] = mapped_column(
        String, ForeignKey("anomalies.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    action: Mapped[str] = mapped_column(String, nullable=False)  # APPROVED | CORRECTED | DISCARDED
    correction: Mapped[str | None] = mapped_column(Text, nullable=True)
    # IR fields — added by Plan A migration; may be NULL until migration runs
    correction_ir: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    ir_source: Mapped[str | None] = mapped_column(String, nullable=True)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # Relationships
    anomaly: Mapped["AnomalyModel"] = relationship("AnomalyModel", back_populates="decision")

    def __repr__(self) -> str:
        return f"<DecisionModel id={self.id!r} action={self.action!r}>"
