"""Unit tests for ProcessDataset use case."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from src.application.use_cases.process_dataset import (
    ProcessDatasetInput,
    ProcessDatasetOutput,
    ProcessDatasetUseCase,
)
from src.application.transformations import TransformationType
from src.infrastructure.parsers.dataset_parser import DatasetParser


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_storage():
    """Create a mock storage service."""
    storage = AsyncMock()
    storage.download_file = AsyncMock()
    storage.upload_file = AsyncMock()
    return storage


@pytest.fixture
def parser():
    """Create a real parser for testing."""
    return DatasetParser()


@pytest.fixture
def use_case(mock_storage, parser):
    """Create ProcessDatasetUseCase with mocked storage."""
    return ProcessDatasetUseCase(
        storage=mock_storage,
        parser=parser,
        output_bucket="test-processed",
    )


@pytest.fixture
def sample_csv_bytes():
    """Create sample CSV data."""
    return b"name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago"


@pytest.fixture
def csv_with_nulls():
    """Create CSV data with null values."""
    return b"name,age,city\nAlice,30,NYC\nBob,,LA\nCharlie,35,"


@pytest.fixture
def csv_with_whitespace():
    """Create CSV data with whitespace."""
    return b"name,age,city\n  Alice  ,30,  NYC  \n  Bob  ,25,LA\nCharlie,35,Chicago"


@pytest.fixture
def sample_input():
    """Create sample input for the use case."""
    return ProcessDatasetInput(
        dataset_id=uuid4(),
        job_id=uuid4(),
        source_key="raw/test-file.csv",
        filename="test-file.csv",
        transformations=[],
        output_format="parquet",
    )


# =============================================================================
# Basic Execution Tests
# =============================================================================


class TestBasicExecution:
    """Tests for basic use case execution."""

    @pytest.mark.asyncio
    async def test_process_csv_no_transformations(
        self,
        use_case,
        mock_storage,
        sample_csv_bytes,
        sample_input,
    ):
        """Should process CSV without transformations."""
        mock_storage.download_file.return_value = sample_csv_bytes
        
        result = await use_case.execute(sample_input)
        
        assert result.success is True
        assert result.dataset_id == sample_input.dataset_id
        assert result.job_id == sample_input.job_id
        assert result.rows_processed == 3
        assert result.columns_count == 3
        assert result.output_key is not None
        assert result.preview is not None
        assert result.schema is not None
        assert len(result.preview) == 3

    @pytest.mark.asyncio
    async def test_downloads_correct_source(
        self,
        use_case,
        mock_storage,
        sample_csv_bytes,
        sample_input,
    ):
        """Should download from correct source key."""
        mock_storage.download_file.return_value = sample_csv_bytes
        
        await use_case.execute(sample_input)
        
        # Should call download_file with bucket and key
        mock_storage.download_file.assert_called_once()

    @pytest.mark.asyncio
    async def test_uploads_to_correct_bucket(
        self,
        use_case,
        mock_storage,
        sample_csv_bytes,
        sample_input,
    ):
        """Should upload to configured bucket."""
        mock_storage.download_file.return_value = sample_csv_bytes
        
        await use_case.execute(sample_input)
        
        mock_storage.upload_file.assert_called_once()
        call_kwargs = mock_storage.upload_file.call_args[1]
        assert call_kwargs["bucket"] == "test-processed"

    @pytest.mark.asyncio
    async def test_output_key_format(
        self,
        use_case,
        mock_storage,
        sample_csv_bytes,
        sample_input,
    ):
        """Should generate correct output key."""
        mock_storage.download_file.return_value = sample_csv_bytes
        
        result = await use_case.execute(sample_input)
        
        expected_prefix = f"processed/{sample_input.dataset_id}/{sample_input.job_id}/"
        assert result.output_key.startswith(expected_prefix)
        assert result.output_key.endswith(".parquet")


# =============================================================================
# Transformation Tests
# =============================================================================


class TestTransformations:
    """Tests for transformation processing."""

    @pytest.mark.asyncio
    async def test_apply_single_transformation(
        self,
        use_case,
        mock_storage,
        csv_with_nulls,
    ):
        """Should apply single transformation."""
        mock_storage.download_file.return_value = csv_with_nulls
        
        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/data.csv",
            filename="data.csv",
            transformations=[
                {"type": "CLEAN_NULLS"},
            ],
        )
        
        result = await use_case.execute(input_data)
        
        assert result.success is True
        assert result.rows_processed == 1  # Only row without nulls
        assert result.transformation_results is not None
        assert len(result.transformation_results) == 1
        assert result.transformation_results[0]["type"] == "CLEAN_NULLS"
        assert result.transformation_results[0]["success"] is True

    @pytest.mark.asyncio
    async def test_apply_multiple_transformations(
        self,
        use_case,
        mock_storage,
        csv_with_whitespace,
    ):
        """Should apply multiple transformations in order."""
        mock_storage.download_file.return_value = csv_with_whitespace
        
        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/data.csv",
            filename="data.csv",
            transformations=[
                {"type": "TRIM_WHITESPACE"},
                {"type": "UPPERCASE", "columns": ["name"]},
            ],
        )
        
        result = await use_case.execute(input_data)
        
        assert result.success is True
        assert len(result.transformation_results) == 2
        # Check preview shows transformed data
        assert result.preview[0]["name"] == "ALICE"

    @pytest.mark.asyncio
    async def test_transformation_with_params(
        self,
        use_case,
        mock_storage,
        csv_with_nulls,
    ):
        """Should pass params to transformation."""
        mock_storage.download_file.return_value = csv_with_nulls
        
        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/data.csv",
            filename="data.csv",
            transformations=[
                {
                    "type": "FILL_NULLS",
                    "columns": ["age"],
                    "params": {"value": 0, "strategy": "literal"},
                },
            ],
        )
        
        result = await use_case.execute(input_data)
        
        assert result.success is True
        assert result.rows_processed == 3  # All rows kept

    @pytest.mark.asyncio
    async def test_transformation_failure_stops_processing(
        self,
        use_case,
        mock_storage,
        sample_csv_bytes,
    ):
        """Should stop on transformation failure."""
        mock_storage.download_file.return_value = sample_csv_bytes
        
        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/data.csv",
            filename="data.csv",
            transformations=[
                {"type": "CLEAN_NULLS", "columns": ["nonexistent"]},  # Will fail
                {"type": "UPPERCASE"},  # Should not run
            ],
        )
        
        result = await use_case.execute(input_data)
        
        assert result.success is False
        assert result.error is not None
        assert "not found" in result.error


# =============================================================================
# Output Format Tests
# =============================================================================


class TestOutputFormats:
    """Tests for different output formats."""

    @pytest.mark.asyncio
    async def test_output_csv_format(
        self,
        use_case,
        mock_storage,
        sample_csv_bytes,
    ):
        """Should output CSV format."""
        mock_storage.download_file.return_value = sample_csv_bytes
        
        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/data.csv",
            filename="data.csv",
            transformations=[],
            output_format="csv",
        )
        
        result = await use_case.execute(input_data)
        
        assert result.success is True
        assert result.output_key.endswith(".csv")
        
        # Check content type
        call_kwargs = mock_storage.upload_file.call_args[1]
        assert call_kwargs["content_type"] == "text/csv"

    @pytest.mark.asyncio
    async def test_output_json_format(
        self,
        use_case,
        mock_storage,
        sample_csv_bytes,
    ):
        """Should output JSON format."""
        mock_storage.download_file.return_value = sample_csv_bytes
        
        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/data.csv",
            filename="data.csv",
            transformations=[],
            output_format="json",
        )
        
        result = await use_case.execute(input_data)
        
        assert result.success is True
        assert result.output_key.endswith(".json")


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestErrorHandling:
    """Tests for error handling."""

    @pytest.mark.asyncio
    async def test_download_failure(
        self,
        use_case,
        mock_storage,
        sample_input,
    ):
        """Should handle download failure."""
        mock_storage.download_file.side_effect = Exception("Storage unavailable")
        
        result = await use_case.execute(sample_input)
        
        assert result.success is False
        assert "Storage unavailable" in result.error

    @pytest.mark.asyncio
    async def test_parse_failure(
        self,
        use_case,
        mock_storage,
    ):
        """Should handle parse failure."""
        mock_storage.download_file.return_value = b"invalid parquet data"
        
        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/data.parquet",
            filename="data.parquet",  # Will try to parse as parquet
            transformations=[],
        )
        
        result = await use_case.execute(input_data)
        
        assert result.success is False
        assert result.error is not None

    @pytest.mark.asyncio
    async def test_upload_failure(
        self,
        use_case,
        mock_storage,
        sample_csv_bytes,
        sample_input,
    ):
        """Should handle upload failure."""
        mock_storage.download_file.return_value = sample_csv_bytes
        mock_storage.upload_file.side_effect = Exception("Upload failed")
        
        result = await use_case.execute(sample_input)
        
        assert result.success is False
        assert "Upload failed" in result.error


# =============================================================================
# Schema and Preview Tests
# =============================================================================


class TestSchemaAndPreview:
    """Tests for schema and preview generation."""

    @pytest.mark.asyncio
    async def test_schema_returned(
        self,
        use_case,
        mock_storage,
        sample_csv_bytes,
        sample_input,
    ):
        """Should return schema information."""
        mock_storage.download_file.return_value = sample_csv_bytes
        
        result = await use_case.execute(sample_input)
        
        assert result.schema is not None
        assert "name" in result.schema
        assert "age" in result.schema
        assert "city" in result.schema

    @pytest.mark.asyncio
    async def test_preview_limited_rows(
        self,
        use_case,
        mock_storage,
    ):
        """Should limit preview to 10 rows."""
        # Create CSV with 20 rows
        rows = ["name,value"]
        for i in range(20):
            rows.append(f"Name{i},{i}")
        data = "\n".join(rows).encode()
        
        mock_storage.download_file.return_value = data
        
        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/large.csv",
            filename="large.csv",
            transformations=[],
        )
        
        result = await use_case.execute(input_data)
        
        assert result.success is True
        assert len(result.preview) == 10  # Limited to 10
        assert result.rows_processed == 20  # All rows processed


# =============================================================================
# Integration Tests
# =============================================================================


class TestIntegration:
    """Integration tests for full pipeline."""

    @pytest.mark.asyncio
    async def test_full_etl_pipeline(
        self,
        use_case,
        mock_storage,
    ):
        """Should run full ETL pipeline."""
        # Input CSV with issues
        input_csv = b"name,age,city\n  Alice  ,30,NYC\nBob,,LA\n  Charlie  ,35,Chicago\nDiana,28,"
        mock_storage.download_file.return_value = input_csv
        
        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/messy.csv",
            filename="messy.csv",
            transformations=[
                {"type": "TRIM_WHITESPACE"},
                {"type": "FILL_NULLS", "columns": ["age"], "params": {"value": 0, "strategy": "literal"}},
                {"type": "CLEAN_NULLS", "columns": ["city"]},
                {"type": "UPPERCASE", "columns": ["name"]},
            ],
            output_format="parquet",
        )
        
        result = await use_case.execute(input_data)
        
        assert result.success is True
        assert len(result.transformation_results) == 4
        assert all(t["success"] for t in result.transformation_results)
        
        # Check preview shows cleaned data
        names = [row["name"] for row in result.preview]
        assert "ALICE" in names
        assert "BOB" in names
        # Diana should be removed (null city)
        assert "DIANA" not in names
