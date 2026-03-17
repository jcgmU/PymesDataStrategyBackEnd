"""BullMQ worker service for processing transformation jobs."""

import asyncio
from contextlib import suppress
from typing import Any, Callable, Coroutine
from uuid import UUID

import structlog
from bullmq import Worker, Job

from src.domain.ports.services.job_queue_service import JobProcessor


logger = structlog.get_logger("pymes.worker.bullmq")


class BullMQWorkerService:
    """BullMQ worker that processes transformation jobs from the queue.

    This worker connects to Redis and listens for jobs on the specified queue.
    When a job arrives, it delegates processing to the provided JobProcessor.
    """

    def __init__(
        self,
        redis_host: str,
        redis_port: int,
        queue_name: str = "etl-transformations",
        concurrency: int = 2,
    ) -> None:
        """Initialize the BullMQ worker.

        Args:
            redis_host: Redis server hostname.
            redis_port: Redis server port.
            queue_name: Name of the queue to process.
            concurrency: Number of jobs to process concurrently.
        """
        self._redis_host = redis_host
        self._redis_port = redis_port
        self._queue_name = queue_name
        self._concurrency = concurrency
        self._worker: Worker | None = None
        self._processor: JobProcessor | None = None
        self._running = False

    def set_processor(self, processor: JobProcessor) -> None:
        """Set the job processor to use for handling jobs.

        Args:
            processor: JobProcessor implementation.
        """
        self._processor = processor

    async def _process_job(self, job: Job, token: str | None = None) -> dict[str, Any]:
        """Process a single job from the queue.

        Args:
            job: BullMQ job instance.
            token: Optional lock token.

        Returns:
            Job processing result.

        Raises:
            ValueError: If no processor is configured.
        """
        if self._processor is None:
            raise ValueError("No job processor configured. Call set_processor() first.")

        job_id = job.id or "unknown"
        job_data = job.data or {}

        log = logger.bind(
            job_id=job_id,
            job_name=job.name,
            dataset_id=job_data.get("datasetId"),
        )

        log.info("Processing job started")

        try:
            # Update job progress to indicate start
            await job.updateProgress(5)

            # Process the job
            result = await self._processor.process(job_data)

            # Update progress to complete
            await job.updateProgress(100)

            log.info(
                "Job completed successfully",
                result=result,
            )

            return result

        except Exception as e:
            log.error(
                "Job processing failed",
                error=str(e),
                error_type=type(e).__name__,
            )

            # Report error to processor if it has the capability
            if hasattr(job_data, "get") and job_data.get("jobId"):
                with suppress(Exception):
                    await self._processor.on_error(
                        UUID(job_data["jobId"]),
                        str(e),
                    )

            raise

    async def start(self) -> None:
        """Start the worker and begin processing jobs."""
        if self._running:
            logger.warning("Worker already running")
            return

        if self._processor is None:
            raise ValueError("No job processor configured. Call set_processor() first.")

        logger.info(
            "Starting BullMQ worker",
            queue=self._queue_name,
            concurrency=self._concurrency,
            redis=f"{self._redis_host}:{self._redis_port}",
        )

        self._worker = Worker(
            name=self._queue_name,
            processor=self._process_job,
            opts={
                "connection": {
                    "host": self._redis_host,
                    "port": self._redis_port,
                },
                "concurrency": self._concurrency,
            },
        )

        self._running = True
        logger.info("Worker started successfully")

    async def stop(self) -> None:
        """Stop the worker gracefully."""
        if not self._running or self._worker is None:
            return

        logger.info("Stopping BullMQ worker...")

        try:
            await self._worker.close()
        except Exception as e:
            logger.error("Error closing worker", error=str(e))
        finally:
            self._worker = None
            self._running = False
            logger.info("Worker stopped")

    @property
    def is_running(self) -> bool:
        """Check if the worker is currently running."""
        return self._running

    async def run_forever(self) -> None:
        """Run the worker until interrupted.

        This is a blocking call that keeps the worker running.
        Use Ctrl+C or call stop() from another task to stop.
        """
        await self.start()

        try:
            while self._running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("Worker cancelled")
        finally:
            await self.stop()


class SimpleJobProcessor(JobProcessor):
    """Simple job processor that delegates to a callback function.

    Useful for testing or simple use cases where you want to provide
    a processing function directly.
    """

    def __init__(
        self,
        process_fn: Callable[[dict[str, Any]], Coroutine[Any, Any, dict[str, Any]]],
        on_progress_fn: Callable[[UUID, int], Coroutine[Any, Any, None]] | None = None,
        on_error_fn: Callable[[UUID, str], Coroutine[Any, Any, None]] | None = None,
    ) -> None:
        """Initialize the processor with callback functions.

        Args:
            process_fn: Async function to process job data.
            on_progress_fn: Optional async function to report progress.
            on_error_fn: Optional async function to report errors.
        """
        self._process_fn = process_fn
        self._on_progress_fn = on_progress_fn
        self._on_error_fn = on_error_fn

    async def process(self, job_data: dict[str, Any]) -> dict[str, Any]:
        """Process the job data."""
        return await self._process_fn(job_data)

    async def on_progress(self, job_id: UUID, progress: int) -> None:
        """Report job progress."""
        if self._on_progress_fn:
            await self._on_progress_fn(job_id, progress)

    async def on_error(self, job_id: UUID, error: str) -> None:
        """Report job error."""
        if self._on_error_fn:
            await self._on_error_fn(job_id, error)
