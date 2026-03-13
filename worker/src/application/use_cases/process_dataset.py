"""Process Dataset use case - orchestrates ETL pipeline."""

from dataclasses import dataclass
from io import BytesIO
from typing import Any
from uuid import UUID

import structlog

from src.application.transformations import (
    DataTransformer,
    TransformationType,
    TransformationConfig,
    TransformationResult,
)
from src.domain.ports.services.storage_service import StorageService
from src.infrastructure.parsers.dataset_parser import (
    DatasetParser,
    FileFormat,
)


logger = structlog.get_logger("pymes.worker.use_cases.process_dataset")


@dataclass
class ProcessDatasetInput:
    """Input for ProcessDataset use case."""
    
    dataset_id: UUID
    job_id: UUID
    source_key: str  # S3/MinIO key for input file
    filename: str  # Original filename (for format detection)
    transformations: list[dict[str, Any]]  # List of transformation configs
    output_format: str = "parquet"  # Output format


@dataclass
class ProcessDatasetOutput:
    """Output from ProcessDataset use case."""
    
    success: bool
    dataset_id: UUID
    job_id: UUID
    output_key: str | None = None
    rows_processed: int = 0
    columns_count: int = 0
    transformation_results: list[dict[str, Any]] | None = None
    error: str | None = None
    preview: list[dict[str, Any]] | None = None
    schema: dict[str, str] | None = None


class ProcessDatasetUseCase:
    """Orchestrates the ETL pipeline for dataset processing.
    
    This use case:
    1. Downloads the raw file from storage
    2. Parses it into a DataFrame
    3. Applies transformations in sequence
    4. Saves the processed result back to storage
    5. Returns metadata about the processing
    """

    def __init__(
        self,
        storage: StorageService,
        parser: DatasetParser | None = None,
        transformer: DataTransformer | None = None,
        output_bucket: str = "processed-datasets",
    ) -> None:
        """Initialize the use case.
        
        Args:
            storage: Storage service for file operations.
            parser: Dataset parser (creates default if None).
            transformer: Data transformer (creates default if None).
            output_bucket: Bucket for processed output files.
        """
        self._storage = storage
        self._parser = parser or DatasetParser()
        self._transformer = transformer or DataTransformer()
        self._output_bucket = output_bucket

    async def execute(self, input_data: ProcessDatasetInput) -> ProcessDatasetOutput:
        """Execute the dataset processing pipeline.
        
        Args:
            input_data: Processing input parameters.
            
        Returns:
            Processing output with results.
        """
        log = logger.bind(
            dataset_id=str(input_data.dataset_id),
            job_id=str(input_data.job_id),
            source_key=input_data.source_key,
        )
        log.info("Starting dataset processing")
        
        try:
            # Step 1: Download file from storage
            log.info("Downloading source file")
            # Parse bucket and key from source_key (format: bucket/key or just key)
            source_bucket, source_key = self._parse_storage_path(input_data.source_key)
            file_data = await self._storage.download_file(source_bucket, source_key)
            
            # Step 2: Parse file into DataFrame
            log.info("Parsing file", filename=input_data.filename)
            df = self._parser.parse(file_data, input_data.filename)
            
            rows_initial = df.height
            log.info("File parsed", rows=rows_initial, columns=df.width)
            
            # Step 3: Apply transformations
            transformation_results = []
            
            if input_data.transformations:
                log.info("Applying transformations", count=len(input_data.transformations))
                
                configs = self._build_transformation_configs(input_data.transformations)
                df, results = self._transformer.transform_many(df, configs)
                
                # Convert results to dicts for serialization
                transformation_results = [
                    {
                        "type": r.transformation.value,
                        "success": r.success,
                        "rows_before": r.rows_before,
                        "rows_after": r.rows_after,
                        "columns_affected": r.columns_affected,
                        "error": r.error,
                        "details": r.details,
                    }
                    for r in results
                ]
                
                # Check for failures
                failures = [r for r in results if not r.success]
                if failures:
                    error_msg = f"Transformation failed: {failures[0].error}"
                    log.error(error_msg)
                    return ProcessDatasetOutput(
                        success=False,
                        dataset_id=input_data.dataset_id,
                        job_id=input_data.job_id,
                        transformation_results=transformation_results,
                        error=error_msg,
                    )
            
            # Step 4: Serialize and upload result
            output_format = self._get_output_format(input_data.output_format)
            output_data = self._parser.to_bytes(df, output_format)
            
            output_key = self._generate_output_key(
                input_data.dataset_id,
                input_data.job_id,
                output_format,
            )
            
            log.info("Uploading processed file", output_key=output_key)
            await self._storage.upload_file(
                bucket=self._output_bucket,
                key=output_key,
                data=BytesIO(output_data),
                content_type=self._get_content_type(output_format),
            )
            
            # Step 5: Generate preview and schema
            preview = self._parser.preview(df, n=10)
            schema = self._parser.get_schema(df)
            
            log.info(
                "Processing complete",
                rows_processed=df.height,
                columns=df.width,
                output_key=output_key,
            )
            
            return ProcessDatasetOutput(
                success=True,
                dataset_id=input_data.dataset_id,
                job_id=input_data.job_id,
                output_key=output_key,
                rows_processed=df.height,
                columns_count=df.width,
                transformation_results=transformation_results,
                preview=preview,
                schema=schema,
            )
            
        except Exception as e:
            log.error("Processing failed", error=str(e), error_type=type(e).__name__)
            return ProcessDatasetOutput(
                success=False,
                dataset_id=input_data.dataset_id,
                job_id=input_data.job_id,
                error=str(e),
            )

    def _build_transformation_configs(
        self,
        raw_configs: list[dict[str, Any]],
    ) -> list[TransformationConfig]:
        """Convert raw config dicts to TransformationConfig objects."""
        configs = []
        
        for raw in raw_configs:
            transformation_type = TransformationType(raw["type"])
            config = TransformationConfig(
                type=transformation_type,
                columns=raw.get("columns"),
                params=raw.get("params", {}),
            )
            configs.append(config)
        
        return configs

    def _get_output_format(self, format_str: str) -> FileFormat:
        """Convert string to FileFormat enum."""
        format_map = {
            "parquet": FileFormat.PARQUET,
            "csv": FileFormat.CSV,
            "json": FileFormat.JSON,
            "xlsx": FileFormat.EXCEL,
        }
        return format_map.get(format_str.lower(), FileFormat.PARQUET)

    def _generate_output_key(
        self,
        dataset_id: UUID,
        job_id: UUID,
        file_format: FileFormat,
    ) -> str:
        """Generate storage key for output file."""
        extension = file_format.value
        return f"processed/{dataset_id}/{job_id}/output.{extension}"

    def _get_content_type(self, file_format: FileFormat) -> str:
        """Get content type for file format."""
        content_types = {
            FileFormat.PARQUET: "application/octet-stream",
            FileFormat.CSV: "text/csv",
            FileFormat.JSON: "application/json",
            FileFormat.EXCEL: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }
        return content_types.get(file_format, "application/octet-stream")

    def _parse_storage_path(self, source_key: str) -> tuple[str, str]:
        """Parse storage path into bucket and key.
        
        Args:
            source_key: Path in format "bucket/key" or just "key".
            
        Returns:
            Tuple of (bucket, key).
        """
        # Check if format is "bucket/path/to/file"
        if "/" in source_key:
            parts = source_key.split("/", 1)
            # If first part looks like a bucket name (no extension), use it
            if "." not in parts[0]:
                return parts[0], parts[1]
        
        # Default: use datasets bucket
        return "datasets", source_key
