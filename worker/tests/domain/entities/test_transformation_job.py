"""Tests for TransformationJob domain entity."""

from datetime import datetime
from typing import Any
from uuid import UUID

import pytest

from src.domain.entities.transformation_job import TransformationJob
from src.domain.value_objects.job_status import JobStatus, TransformationType


class TestTransformationJobCreate:
    """Test suite for TransformationJob.create factory method."""

    def test_create_with_required_fields(
        self,
        sample_uuid: UUID,
        dataset_uuid: UUID,
        owner_uuid: UUID,
        sample_job_config: dict[str, Any],
    ) -> None:
        """Test creating a job with required fields."""
        job = TransformationJob.create(
            id=sample_uuid,
            dataset_id=dataset_uuid,
            transformation_type=TransformationType.CLEAN,
            config=sample_job_config,
            created_by=owner_uuid,
        )

        assert job.id == sample_uuid
        assert job.dataset_id == dataset_uuid
        assert job.transformation_type == TransformationType.CLEAN
        assert job.config == sample_job_config
        assert job.created_by == owner_uuid

    def test_create_sets_default_values(
        self,
        sample_uuid: UUID,
        dataset_uuid: UUID,
        owner_uuid: UUID,
        sample_job_config: dict[str, Any],
    ) -> None:
        """Test that create sets appropriate default values."""
        job = TransformationJob.create(
            id=sample_uuid,
            dataset_id=dataset_uuid,
            transformation_type=TransformationType.NORMALIZE,
            config=sample_job_config,
            created_by=owner_uuid,
        )

        assert job.status == JobStatus.PENDING
        assert job.result is None
        assert job.error_message is None
        assert job.progress == 0
        assert job.started_at is None
        assert job.completed_at is None
        assert job.metadata == {}

    def test_create_sets_created_at(
        self,
        sample_uuid: UUID,
        dataset_uuid: UUID,
        owner_uuid: UUID,
        sample_job_config: dict[str, Any],
    ) -> None:
        """Test that create sets created_at timestamp."""
        before = datetime.utcnow()
        job = TransformationJob.create(
            id=sample_uuid,
            dataset_id=dataset_uuid,
            transformation_type=TransformationType.VALIDATE,
            config=sample_job_config,
            created_by=owner_uuid,
        )
        after = datetime.utcnow()

        assert before <= job.created_at <= after


class TestTransformationJobLifecycle:
    """Test suite for TransformationJob lifecycle methods."""

    @pytest.fixture
    def pending_job(
        self,
        sample_uuid: UUID,
        dataset_uuid: UUID,
        owner_uuid: UUID,
        sample_job_config: dict[str, Any],
    ) -> TransformationJob:
        """Create a pending job for testing."""
        return TransformationJob.create(
            id=sample_uuid,
            dataset_id=dataset_uuid,
            transformation_type=TransformationType.CLEAN,
            config=sample_job_config,
            created_by=owner_uuid,
        )

    def test_start_changes_status(self, pending_job: TransformationJob) -> None:
        """Test that start changes status to PROCESSING."""
        assert pending_job.status == JobStatus.PENDING

        pending_job.start()

        assert pending_job.status == JobStatus.PROCESSING

    def test_start_sets_started_at(self, pending_job: TransformationJob) -> None:
        """Test that start sets the started_at timestamp."""
        assert pending_job.started_at is None

        before = datetime.utcnow()
        pending_job.start()
        after = datetime.utcnow()

        assert pending_job.started_at is not None
        assert before <= pending_job.started_at <= after

    def test_complete_with_result(
        self, pending_job: TransformationJob, sample_job_result: dict[str, Any]
    ) -> None:
        """Test completing a job with result."""
        pending_job.start()
        pending_job.complete(sample_job_result)

        assert pending_job.status == JobStatus.COMPLETED
        assert pending_job.result == sample_job_result
        assert pending_job.progress == 100
        assert pending_job.completed_at is not None

    def test_fail_with_error(self, pending_job: TransformationJob) -> None:
        """Test failing a job with error message."""
        pending_job.start()
        pending_job.fail("Connection timeout to database")

        assert pending_job.status == JobStatus.FAILED
        assert pending_job.error_message == "Connection timeout to database"
        assert pending_job.completed_at is not None

    def test_request_review(self, pending_job: TransformationJob) -> None:
        """Test requesting human review."""
        pending_job.start()
        pending_job.request_review()

        assert pending_job.status == JobStatus.AWAITING_REVIEW

    def test_cancel(self, pending_job: TransformationJob) -> None:
        """Test cancelling a job."""
        pending_job.cancel()

        assert pending_job.status == JobStatus.CANCELLED
        assert pending_job.completed_at is not None


class TestTransformationJobProgress:
    """Test suite for TransformationJob progress tracking."""

    @pytest.fixture
    def processing_job(
        self,
        sample_uuid: UUID,
        dataset_uuid: UUID,
        owner_uuid: UUID,
        sample_job_config: dict[str, Any],
    ) -> TransformationJob:
        """Create a processing job for testing."""
        job = TransformationJob.create(
            id=sample_uuid,
            dataset_id=dataset_uuid,
            transformation_type=TransformationType.DEDUPLICATE,
            config=sample_job_config,
            created_by=owner_uuid,
        )
        job.start()
        return job

    def test_update_progress_normal(self, processing_job: TransformationJob) -> None:
        """Test updating progress with valid value."""
        processing_job.update_progress(50)
        assert processing_job.progress == 50

    def test_update_progress_clamps_maximum(
        self, processing_job: TransformationJob
    ) -> None:
        """Test that progress is clamped to 100."""
        processing_job.update_progress(150)
        assert processing_job.progress == 100

    def test_update_progress_clamps_minimum(
        self, processing_job: TransformationJob
    ) -> None:
        """Test that progress is clamped to 0."""
        processing_job.update_progress(-10)
        assert processing_job.progress == 0

    @pytest.mark.parametrize(
        "progress,expected",
        [
            (0, 0),
            (50, 50),
            (100, 100),
            (-5, 0),
            (105, 100),
        ],
    )
    def test_update_progress_parametrized(
        self, processing_job: TransformationJob, progress: int, expected: int
    ) -> None:
        """Test progress clamping with various values."""
        processing_job.update_progress(progress)
        assert processing_job.progress == expected


class TestTransformationJobTerminalState:
    """Test suite for TransformationJob.is_terminal property."""

    @pytest.fixture
    def job(
        self,
        sample_uuid: UUID,
        dataset_uuid: UUID,
        owner_uuid: UUID,
        sample_job_config: dict[str, Any],
    ) -> TransformationJob:
        """Create a job for testing."""
        return TransformationJob.create(
            id=sample_uuid,
            dataset_id=dataset_uuid,
            transformation_type=TransformationType.MERGE,
            config=sample_job_config,
            created_by=owner_uuid,
        )

    def test_pending_is_not_terminal(self, job: TransformationJob) -> None:
        """Test that PENDING is not a terminal state."""
        assert job.status == JobStatus.PENDING
        assert job.is_terminal is False

    def test_processing_is_not_terminal(self, job: TransformationJob) -> None:
        """Test that PROCESSING is not a terminal state."""
        job.start()
        assert job.is_terminal is False

    def test_awaiting_review_is_not_terminal(self, job: TransformationJob) -> None:
        """Test that AWAITING_REVIEW is not a terminal state."""
        job.request_review()
        assert job.is_terminal is False

    def test_completed_is_terminal(
        self, job: TransformationJob, sample_job_result: dict[str, Any]
    ) -> None:
        """Test that COMPLETED is a terminal state."""
        job.start()
        job.complete(sample_job_result)
        assert job.is_terminal is True

    def test_failed_is_terminal(self, job: TransformationJob) -> None:
        """Test that FAILED is a terminal state."""
        job.start()
        job.fail("Error occurred")
        assert job.is_terminal is True

    def test_cancelled_is_terminal(self, job: TransformationJob) -> None:
        """Test that CANCELLED is a terminal state."""
        job.cancel()
        assert job.is_terminal is True


class TestTransformationTypes:
    """Test transformation job with different transformation types."""

    @pytest.mark.parametrize(
        "transformation_type",
        [
            TransformationType.CLEAN,
            TransformationType.NORMALIZE,
            TransformationType.DEDUPLICATE,
            TransformationType.MERGE,
            TransformationType.TRANSFORM,
            TransformationType.VALIDATE,
        ],
    )
    def test_create_with_all_transformation_types(
        self,
        sample_uuid: UUID,
        dataset_uuid: UUID,
        owner_uuid: UUID,
        sample_job_config: dict[str, Any],
        transformation_type: TransformationType,
    ) -> None:
        """Test creating jobs with all transformation types."""
        job = TransformationJob.create(
            id=sample_uuid,
            dataset_id=dataset_uuid,
            transformation_type=transformation_type,
            config=sample_job_config,
            created_by=owner_uuid,
        )

        assert job.transformation_type == transformation_type
