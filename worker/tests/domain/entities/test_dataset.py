"""Tests for Dataset domain entity."""

from datetime import datetime
from typing import Any
from uuid import UUID

import pytest

from src.domain.entities.dataset import Dataset


class TestDatasetCreate:
    """Test suite for Dataset.create factory method."""

    def test_create_with_required_fields(
        self, sample_uuid: UUID, owner_uuid: UUID
    ) -> None:
        """Test creating a dataset with required fields only."""
        dataset = Dataset.create(
            id=sample_uuid,
            name="sales_data.csv",
            file_path="/uploads/sales_data.csv",
            file_size=1024000,
            owner_id=owner_uuid,
        )

        assert dataset.id == sample_uuid
        assert dataset.name == "sales_data.csv"
        assert dataset.file_path == "/uploads/sales_data.csv"
        assert dataset.file_size == 1024000
        assert dataset.owner_id == owner_uuid

    def test_create_sets_default_values(
        self, sample_uuid: UUID, owner_uuid: UUID
    ) -> None:
        """Test that create sets appropriate default values."""
        dataset = Dataset.create(
            id=sample_uuid,
            name="test.csv",
            file_path="/test.csv",
            file_size=100,
            owner_id=owner_uuid,
        )

        assert dataset.row_count is None
        assert dataset.column_count is None
        assert dataset.schema_info is None
        assert dataset.status == "pending"

    def test_create_sets_timestamps(
        self, sample_uuid: UUID, owner_uuid: UUID
    ) -> None:
        """Test that create sets created_at and updated_at."""
        before = datetime.utcnow()
        dataset = Dataset.create(
            id=sample_uuid,
            name="test.csv",
            file_path="/test.csv",
            file_size=100,
            owner_id=owner_uuid,
        )
        after = datetime.utcnow()

        assert before <= dataset.created_at <= after
        assert before <= dataset.updated_at <= after
        assert dataset.created_at == dataset.updated_at


class TestDatasetMarkAnalyzed:
    """Test suite for Dataset.mark_analyzed method."""

    @pytest.fixture
    def pending_dataset(self, sample_uuid: UUID, owner_uuid: UUID) -> Dataset:
        """Create a pending dataset for testing."""
        return Dataset.create(
            id=sample_uuid,
            name="test.csv",
            file_path="/test.csv",
            file_size=1000,
            owner_id=owner_uuid,
        )

    def test_mark_analyzed_updates_metadata(
        self, pending_dataset: Dataset, sample_schema_info: dict[str, Any]
    ) -> None:
        """Test that mark_analyzed updates row/column counts and schema."""
        pending_dataset.mark_analyzed(
            row_count=1000,
            column_count=4,
            schema_info=sample_schema_info,
        )

        assert pending_dataset.row_count == 1000
        assert pending_dataset.column_count == 4
        assert pending_dataset.schema_info == sample_schema_info

    def test_mark_analyzed_updates_status(
        self, pending_dataset: Dataset, sample_schema_info: dict[str, Any]
    ) -> None:
        """Test that mark_analyzed changes status to ready."""
        assert pending_dataset.status == "pending"

        pending_dataset.mark_analyzed(
            row_count=500,
            column_count=10,
            schema_info=sample_schema_info,
        )

        assert pending_dataset.status == "ready"

    def test_mark_analyzed_updates_timestamp(
        self, pending_dataset: Dataset, sample_schema_info: dict[str, Any]
    ) -> None:
        """Test that mark_analyzed updates the updated_at timestamp."""
        original_updated = pending_dataset.updated_at

        pending_dataset.mark_analyzed(
            row_count=100,
            column_count=5,
            schema_info=sample_schema_info,
        )

        assert pending_dataset.updated_at >= original_updated


class TestDatasetDataclass:
    """Test suite for Dataset dataclass behavior."""

    def test_dataset_is_mutable(
        self, sample_uuid: UUID, owner_uuid: UUID
    ) -> None:
        """Test that dataset fields can be modified directly."""
        dataset = Dataset.create(
            id=sample_uuid,
            name="original.csv",
            file_path="/original.csv",
            file_size=100,
            owner_id=owner_uuid,
        )

        dataset.name = "modified.csv"
        assert dataset.name == "modified.csv"

    def test_dataset_equality(
        self, sample_uuid: UUID, owner_uuid: UUID
    ) -> None:
        """Test dataset equality based on all fields."""
        now = datetime.utcnow()
        
        dataset1 = Dataset(
            id=sample_uuid,
            name="test.csv",
            file_path="/test.csv",
            file_size=100,
            row_count=None,
            column_count=None,
            schema_info=None,
            status="pending",
            owner_id=owner_uuid,
            created_at=now,
            updated_at=now,
        )
        
        dataset2 = Dataset(
            id=sample_uuid,
            name="test.csv",
            file_path="/test.csv",
            file_size=100,
            row_count=None,
            column_count=None,
            schema_info=None,
            status="pending",
            owner_id=owner_uuid,
            created_at=now,
            updated_at=now,
        )

        assert dataset1 == dataset2

    def test_dataset_with_large_file(
        self, sample_uuid: UUID, owner_uuid: UUID
    ) -> None:
        """Test dataset with a large file size."""
        large_size = 10 * 1024 * 1024 * 1024  # 10 GB
        
        dataset = Dataset.create(
            id=sample_uuid,
            name="big_data.parquet",
            file_path="/uploads/big_data.parquet",
            file_size=large_size,
            owner_id=owner_uuid,
        )

        assert dataset.file_size == large_size
