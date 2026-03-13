"""ETL Job Processor - bridges BullMQ worker with ProcessDataset use case."""

from typing import Any
from uuid import UUID

import structlog

from src.application.use_cases.process_dataset import (
    ProcessDatasetInput,
    ProcessDatasetUseCase,
)
from src.domain.ports.services.job_queue_service import JobProcessor


logger = structlog.get_logger("pymes.worker.processor")


class ETLJobProcessor(JobProcessor):
    """Processes ETL transformation jobs from the queue.
    
    This processor receives job data from BullMQ and delegates
    to the ProcessDataset use case for actual processing.
    """

    def __init__(
        self,
        process_dataset: ProcessDatasetUseCase,
        on_progress_callback: Any = None,
        on_error_callback: Any = None,
    ) -> None:
        """Initialize the processor.
        
        Args:
            process_dataset: The ProcessDataset use case.
            on_progress_callback: Optional callback for progress updates.
            on_error_callback: Optional callback for error reporting.
        """
        self._process_dataset = process_dataset
        self._on_progress_callback = on_progress_callback
        self._on_error_callback = on_error_callback

    async def process(self, job_data: dict[str, Any]) -> dict[str, Any]:
        """Process an ETL job.
        
        Args:
            job_data: Job data from queue containing:
                - datasetId: UUID of the dataset
                - jobId: UUID of the transformation job
                - sourceKey: S3/MinIO key of source file
                - filename: Original filename
                - transformations: List of transformation configs
                - outputFormat: Optional output format
                
        Returns:
            Processing result dictionary.
        """
        log = logger.bind(
            dataset_id=job_data.get("datasetId"),
            job_id=job_data.get("jobId"),
        )
        log.info("Processing ETL job")
        
        try:
            # Build input from job data
            input_data = ProcessDatasetInput(
                dataset_id=UUID(job_data["datasetId"]),
                job_id=UUID(job_data["jobId"]),
                source_key=job_data["sourceKey"],
                filename=job_data["filename"],
                transformations=job_data.get("transformations", []),
                output_format=job_data.get("outputFormat", "parquet"),
            )
            
            # Execute use case
            output = await self._process_dataset.execute(input_data)
            
            # Build result
            result = {
                "success": output.success,
                "datasetId": str(output.dataset_id),
                "jobId": str(output.job_id),
                "outputKey": output.output_key,
                "rowsProcessed": output.rows_processed,
                "columnsCount": output.columns_count,
                "transformationResults": output.transformation_results,
                "preview": output.preview,
                "schema": output.schema,
            }
            
            if not output.success:
                result["error"] = output.error
                log.error("Job failed", error=output.error)
            else:
                log.info(
                    "Job completed",
                    rows_processed=output.rows_processed,
                    output_key=output.output_key,
                )
            
            return result
            
        except Exception as e:
            log.error("Job processing error", error=str(e))
            raise

    async def on_progress(self, job_id: UUID, progress: int) -> None:
        """Report job progress.
        
        Args:
            job_id: Job identifier.
            progress: Progress percentage (0-100).
        """
        if self._on_progress_callback:
            await self._on_progress_callback(job_id, progress)
        
        logger.debug("Progress update", job_id=str(job_id), progress=progress)

    async def on_error(self, job_id: UUID, error: str) -> None:
        """Report job error.
        
        Args:
            job_id: Job identifier.
            error: Error message.
        """
        if self._on_error_callback:
            await self._on_error_callback(job_id, error)
        
        logger.error("Job error reported", job_id=str(job_id), error=error)
