"""Tests for job status enumerations."""

import pytest

from src.domain.value_objects.job_status import JobStatus, TransformationType


class TestJobStatus:
    """Test suite for JobStatus enumeration."""

    def test_all_statuses_exist(self) -> None:
        """Verify all expected job statuses are defined."""
        expected_statuses = [
            "PENDING",
            "QUEUED",
            "PROCESSING",
            "AWAITING_REVIEW",
            "COMPLETED",
            "FAILED",
            "CANCELLED",
        ]
        for status_name in expected_statuses:
            assert hasattr(JobStatus, status_name)

    def test_status_values(self) -> None:
        """Verify job status string values."""
        assert JobStatus.PENDING.value == "pending"
        assert JobStatus.QUEUED.value == "queued"
        assert JobStatus.PROCESSING.value == "processing"
        assert JobStatus.AWAITING_REVIEW.value == "awaiting_review"
        assert JobStatus.COMPLETED.value == "completed"
        assert JobStatus.FAILED.value == "failed"
        assert JobStatus.CANCELLED.value == "cancelled"

    def test_status_is_string_enum(self) -> None:
        """Verify JobStatus inherits from str."""
        assert isinstance(JobStatus.PENDING, str)
        assert JobStatus.PENDING == "pending"

    def test_status_comparison(self) -> None:
        """Test status equality comparisons."""
        assert JobStatus.COMPLETED == JobStatus.COMPLETED
        assert JobStatus.PENDING != JobStatus.PROCESSING
        assert JobStatus.FAILED == "failed"

    def test_status_in_collection(self) -> None:
        """Test status membership in collections."""
        terminal_statuses = {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED}
        
        assert JobStatus.COMPLETED in terminal_statuses
        assert JobStatus.FAILED in terminal_statuses
        assert JobStatus.PROCESSING not in terminal_statuses


class TestTransformationType:
    """Test suite for TransformationType enumeration."""

    def test_all_types_exist(self) -> None:
        """Verify all expected transformation types are defined."""
        expected_types = [
            "CLEAN",
            "NORMALIZE",
            "DEDUPLICATE",
            "MERGE",
            "TRANSFORM",
            "VALIDATE",
        ]
        for type_name in expected_types:
            assert hasattr(TransformationType, type_name)

    def test_type_values(self) -> None:
        """Verify transformation type string values."""
        assert TransformationType.CLEAN.value == "clean"
        assert TransformationType.NORMALIZE.value == "normalize"
        assert TransformationType.DEDUPLICATE.value == "deduplicate"
        assert TransformationType.MERGE.value == "merge"
        assert TransformationType.TRANSFORM.value == "transform"
        assert TransformationType.VALIDATE.value == "validate"

    def test_type_is_string_enum(self) -> None:
        """Verify TransformationType inherits from str."""
        assert isinstance(TransformationType.CLEAN, str)
        assert TransformationType.CLEAN == "clean"

    def test_type_comparison(self) -> None:
        """Test type equality comparisons."""
        assert TransformationType.CLEAN == TransformationType.CLEAN
        assert TransformationType.CLEAN != TransformationType.MERGE
        assert TransformationType.VALIDATE == "validate"

    @pytest.mark.parametrize(
        "transformation_type,expected_value",
        [
            (TransformationType.CLEAN, "clean"),
            (TransformationType.NORMALIZE, "normalize"),
            (TransformationType.DEDUPLICATE, "deduplicate"),
            (TransformationType.MERGE, "merge"),
            (TransformationType.TRANSFORM, "transform"),
            (TransformationType.VALIDATE, "validate"),
        ],
    )
    def test_type_values_parametrized(
        self, transformation_type: TransformationType, expected_value: str
    ) -> None:
        """Test all transformation type values with parametrization."""
        assert transformation_type.value == expected_value
        assert transformation_type == expected_value
