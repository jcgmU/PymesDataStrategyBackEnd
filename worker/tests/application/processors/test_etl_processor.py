"""Tests for ETLJobProcessor."""

import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.application.processors.etl_processor import ETLJobProcessor
from src.application.use_cases.process_dataset import (
    ProcessDatasetOutput,
    ProcessDatasetUseCase,
)


@pytest.fixture
def mock_process_dataset() -> AsyncMock:
    """Create a mock ProcessDatasetUseCase."""
    mock = AsyncMock(spec=ProcessDatasetUseCase)
    return mock


@pytest.fixture
def processor(mock_process_dataset: AsyncMock) -> ETLJobProcessor:
    """Create an ETLJobProcessor with mocked dependencies."""
    return ETLJobProcessor(process_dataset=mock_process_dataset)


class TestETLJobProcessor:
    """Test suite for ETLJobProcessor."""

    @pytest.mark.asyncio
    async def test_process_success(
        self,
        processor: ETLJobProcessor,
        mock_process_dataset: AsyncMock,
    ) -> None:
        """Test successful job processing."""
        # Arrange
        dataset_id = uuid.uuid4()
        job_id = uuid.uuid4()

        job_data: dict[str, Any] = {
            "datasetId": str(dataset_id),
            "jobId": str(job_id),
            "sourceKey": "uploads/test.csv",
            "filename": "test.csv",
            "transformations": [
                {"type": "lowercase", "column": "name"},
            ],
            "outputFormat": "parquet",
        }

        mock_process_dataset.execute.return_value = ProcessDatasetOutput(
            success=True,
            dataset_id=dataset_id,
            job_id=job_id,
            output_key=f"{dataset_id}/{job_id}.parquet",
            rows_processed=100,
            columns_count=5,
            transformation_results=[
                {"type": "lowercase", "column": "name", "success": True}
            ],
            preview=[{"name": "test", "value": 1}],
            schema=[{"name": "name", "dtype": "String"}],
        )

        # Act
        result = await processor.process(job_data)

        # Assert
        assert result["success"] is True
        assert result["datasetId"] == str(dataset_id)
        assert result["jobId"] == str(job_id)
        assert result["rowsProcessed"] == 100
        assert result["columnsCount"] == 5
        assert len(result["transformationResults"]) == 1

        # Verify use case was called with correct input
        mock_process_dataset.execute.assert_called_once()
        call_input = mock_process_dataset.execute.call_args[0][0]
        assert call_input.dataset_id == dataset_id
        assert call_input.job_id == job_id
        assert call_input.source_key == "uploads/test.csv"

    @pytest.mark.asyncio
    async def test_process_failure(
        self,
        processor: ETLJobProcessor,
        mock_process_dataset: AsyncMock,
    ) -> None:
        """Test job processing with failure."""
        # Arrange
        dataset_id = uuid.uuid4()
        job_id = uuid.uuid4()

        job_data: dict[str, Any] = {
            "datasetId": str(dataset_id),
            "jobId": str(job_id),
            "sourceKey": "uploads/bad.csv",
            "filename": "bad.csv",
            "transformations": [],
            "outputFormat": "parquet",
        }

        mock_process_dataset.execute.return_value = ProcessDatasetOutput(
            success=False,
            dataset_id=dataset_id,
            job_id=job_id,
            error="Failed to parse CSV file",
        )

        # Act
        result = await processor.process(job_data)

        # Assert
        assert result["success"] is False
        assert result["error"] == "Failed to parse CSV file"

    @pytest.mark.asyncio
    async def test_process_with_default_output_format(
        self,
        processor: ETLJobProcessor,
        mock_process_dataset: AsyncMock,
    ) -> None:
        """Test that output format defaults to parquet."""
        # Arrange
        dataset_id = uuid.uuid4()
        job_id = uuid.uuid4()

        job_data: dict[str, Any] = {
            "datasetId": str(dataset_id),
            "jobId": str(job_id),
            "sourceKey": "uploads/test.csv",
            "filename": "test.csv",
            "transformations": [],
            # No outputFormat specified
        }

        mock_process_dataset.execute.return_value = ProcessDatasetOutput(
            success=True,
            dataset_id=dataset_id,
            job_id=job_id,
            output_key=f"{dataset_id}/{job_id}.parquet",
            rows_processed=10,
            columns_count=2,
        )

        # Act
        await processor.process(job_data)

        # Assert: Default format should be parquet
        call_input = mock_process_dataset.execute.call_args[0][0]
        assert call_input.output_format == "parquet"

    @pytest.mark.asyncio
    async def test_process_raises_on_exception(
        self,
        processor: ETLJobProcessor,
        mock_process_dataset: AsyncMock,
    ) -> None:
        """Test that exceptions are propagated."""
        # Arrange
        dataset_id = uuid.uuid4()
        job_id = uuid.uuid4()

        job_data: dict[str, Any] = {
            "datasetId": str(dataset_id),
            "jobId": str(job_id),
            "sourceKey": "uploads/test.csv",
            "filename": "test.csv",
            "transformations": [],
        }

        mock_process_dataset.execute.side_effect = RuntimeError("Storage error")

        # Act & Assert
        with pytest.raises(RuntimeError, match="Storage error"):
            await processor.process(job_data)

    @pytest.mark.asyncio
    async def test_on_progress_with_callback(self) -> None:
        """Test progress callback is invoked."""
        # Arrange
        progress_callback = AsyncMock()
        processor = ETLJobProcessor(
            process_dataset=AsyncMock(),
            on_progress_callback=progress_callback,
        )

        job_id = uuid.uuid4()

        # Act
        await processor.on_progress(job_id, 50)

        # Assert
        progress_callback.assert_called_once_with(job_id, 50)

    @pytest.mark.asyncio
    async def test_on_progress_without_callback(
        self,
        processor: ETLJobProcessor,
    ) -> None:
        """Test progress without callback doesn't raise."""
        # Act & Assert: Should not raise
        await processor.on_progress(uuid.uuid4(), 50)

    @pytest.mark.asyncio
    async def test_on_error_with_callback(self) -> None:
        """Test error callback is invoked."""
        # Arrange
        error_callback = AsyncMock()
        processor = ETLJobProcessor(
            process_dataset=AsyncMock(),
            on_error_callback=error_callback,
        )

        job_id = uuid.uuid4()

        # Act
        await processor.on_error(job_id, "Something went wrong")

        # Assert
        error_callback.assert_called_once_with(job_id, "Something went wrong")

    @pytest.mark.asyncio
    async def test_on_error_without_callback(
        self,
        processor: ETLJobProcessor,
    ) -> None:
        """Test error without callback doesn't raise."""
        # Act & Assert: Should not raise
        await processor.on_error(uuid.uuid4(), "Error message")
