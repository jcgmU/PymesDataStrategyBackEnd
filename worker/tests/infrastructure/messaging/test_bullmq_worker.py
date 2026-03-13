"""Unit tests for BullMQ worker service."""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest

from src.domain.ports.services.job_queue_service import JobProcessor
from src.infrastructure.messaging.bullmq_worker import (
    BullMQWorkerService,
    SimpleJobProcessor,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def redis_config():
    """Provide Redis connection configuration."""
    return {
        "host": "localhost",
        "port": 6379,
    }


@pytest.fixture
def worker_service(redis_config):
    """Create a BullMQWorkerService instance for testing."""
    return BullMQWorkerService(
        redis_host=redis_config["host"],
        redis_port=redis_config["port"],
        queue_name="test-queue",
        concurrency=1,
    )


@pytest.fixture
def mock_processor():
    """Create a mock job processor."""
    processor = AsyncMock(spec=JobProcessor)
    processor.process = AsyncMock(return_value={"status": "completed", "rowsProcessed": 100})
    processor.on_progress = AsyncMock()
    processor.on_error = AsyncMock()
    return processor


@pytest.fixture
def mock_job():
    """Create a mock BullMQ job."""
    job = MagicMock()
    job.id = "job-123"
    job.name = "process-dataset"
    job.data = {
        "datasetId": str(uuid4()),
        "jobId": str(uuid4()),
        "transformations": ["CLEAN_NULLS"],
    }
    job.updateProgress = AsyncMock()
    return job


# =============================================================================
# BullMQWorkerService Initialization Tests
# =============================================================================


class TestBullMQWorkerServiceInit:
    """Tests for BullMQWorkerService initialization."""

    def test_init_with_defaults(self, redis_config):
        """Should initialize with default values."""
        service = BullMQWorkerService(
            redis_host=redis_config["host"],
            redis_port=redis_config["port"],
        )
        
        assert service._redis_host == redis_config["host"]
        assert service._redis_port == redis_config["port"]
        assert service._queue_name == "etl-transformations"
        assert service._concurrency == 2
        assert service._worker is None
        assert service._processor is None
        assert service._running is False

    def test_init_with_custom_values(self, redis_config):
        """Should initialize with custom values."""
        service = BullMQWorkerService(
            redis_host=redis_config["host"],
            redis_port=redis_config["port"],
            queue_name="custom-queue",
            concurrency=5,
        )
        
        assert service._queue_name == "custom-queue"
        assert service._concurrency == 5

    def test_is_running_initially_false(self, worker_service):
        """Should return False for is_running initially."""
        assert worker_service.is_running is False


# =============================================================================
# Processor Management Tests
# =============================================================================


class TestProcessorManagement:
    """Tests for processor management."""

    def test_set_processor(self, worker_service, mock_processor):
        """Should set the job processor."""
        worker_service.set_processor(mock_processor)
        assert worker_service._processor is mock_processor

    def test_set_processor_replaces_existing(self, worker_service, mock_processor):
        """Should replace existing processor."""
        first_processor = AsyncMock(spec=JobProcessor)
        worker_service.set_processor(first_processor)
        worker_service.set_processor(mock_processor)
        assert worker_service._processor is mock_processor


# =============================================================================
# Job Processing Tests
# =============================================================================


class TestJobProcessing:
    """Tests for job processing."""

    @pytest.mark.asyncio
    async def test_process_job_calls_processor(self, worker_service, mock_processor, mock_job):
        """Should call processor.process with job data."""
        worker_service.set_processor(mock_processor)
        
        result = await worker_service._process_job(mock_job)
        
        mock_processor.process.assert_called_once_with(mock_job.data)
        assert result == {"status": "completed", "rowsProcessed": 100}

    @pytest.mark.asyncio
    async def test_process_job_updates_progress(self, worker_service, mock_processor, mock_job):
        """Should update job progress at start and end."""
        worker_service.set_processor(mock_processor)
        
        await worker_service._process_job(mock_job)
        
        # Check progress was updated to 5 (start) and 100 (complete)
        assert mock_job.updateProgress.call_count == 2
        mock_job.updateProgress.assert_any_call(5)
        mock_job.updateProgress.assert_any_call(100)

    @pytest.mark.asyncio
    async def test_process_job_without_processor_raises(self, worker_service, mock_job):
        """Should raise ValueError if no processor is set."""
        with pytest.raises(ValueError, match="No job processor configured"):
            await worker_service._process_job(mock_job)

    @pytest.mark.asyncio
    async def test_process_job_handles_processor_error(self, worker_service, mock_processor, mock_job):
        """Should propagate errors from processor."""
        mock_processor.process.side_effect = ValueError("Processing failed")
        worker_service.set_processor(mock_processor)
        
        with pytest.raises(ValueError, match="Processing failed"):
            await worker_service._process_job(mock_job)

    @pytest.mark.asyncio
    async def test_process_job_reports_error_to_processor(self, worker_service, mock_processor, mock_job):
        """Should call on_error when processing fails."""
        mock_processor.process.side_effect = ValueError("Processing failed")
        worker_service.set_processor(mock_processor)
        
        with pytest.raises(ValueError):
            await worker_service._process_job(mock_job)
        
        # Verify on_error was called with job_id and error message
        mock_processor.on_error.assert_called_once()
        call_args = mock_processor.on_error.call_args
        assert str(call_args[0][1]) == "Processing failed"

    @pytest.mark.asyncio
    async def test_process_job_handles_missing_job_id(self, worker_service, mock_processor):
        """Should handle jobs without an id."""
        job = MagicMock()
        job.id = None
        job.name = "test"
        job.data = {}
        job.updateProgress = AsyncMock()
        
        worker_service.set_processor(mock_processor)
        await worker_service._process_job(job)
        
        mock_processor.process.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_job_handles_missing_job_data(self, worker_service, mock_processor):
        """Should handle jobs without data."""
        job = MagicMock()
        job.id = "job-123"
        job.name = "test"
        job.data = None
        job.updateProgress = AsyncMock()
        
        worker_service.set_processor(mock_processor)
        await worker_service._process_job(job)
        
        mock_processor.process.assert_called_once_with({})


# =============================================================================
# Worker Lifecycle Tests
# =============================================================================


class TestWorkerLifecycle:
    """Tests for worker lifecycle (start/stop)."""

    @pytest.mark.asyncio
    async def test_start_without_processor_raises(self, worker_service):
        """Should raise ValueError if no processor is set when starting."""
        with pytest.raises(ValueError, match="No job processor configured"):
            await worker_service.start()

    @pytest.mark.asyncio
    async def test_start_creates_worker(self, worker_service, mock_processor):
        """Should create BullMQ worker when starting."""
        worker_service.set_processor(mock_processor)
        
        with patch("src.infrastructure.messaging.bullmq_worker.Worker") as MockWorker:
            mock_worker_instance = MagicMock()
            MockWorker.return_value = mock_worker_instance
            
            await worker_service.start()
            
            MockWorker.assert_called_once()
            assert worker_service._running is True
            assert worker_service._worker is mock_worker_instance

    @pytest.mark.asyncio
    async def test_start_when_already_running_does_nothing(self, worker_service, mock_processor):
        """Should not create another worker if already running."""
        worker_service.set_processor(mock_processor)
        
        with patch("src.infrastructure.messaging.bullmq_worker.Worker") as MockWorker:
            mock_worker_instance = MagicMock()
            MockWorker.return_value = mock_worker_instance
            
            await worker_service.start()
            await worker_service.start()  # Second call
            
            # Should only create worker once
            assert MockWorker.call_count == 1

    @pytest.mark.asyncio
    async def test_stop_closes_worker(self, worker_service, mock_processor):
        """Should close worker when stopping."""
        worker_service.set_processor(mock_processor)
        
        with patch("src.infrastructure.messaging.bullmq_worker.Worker") as MockWorker:
            mock_worker_instance = MagicMock()
            mock_worker_instance.close = AsyncMock()
            MockWorker.return_value = mock_worker_instance
            
            await worker_service.start()
            await worker_service.stop()
            
            mock_worker_instance.close.assert_called_once()
            assert worker_service._running is False
            assert worker_service._worker is None

    @pytest.mark.asyncio
    async def test_stop_when_not_running_does_nothing(self, worker_service):
        """Should do nothing when stopping if not running."""
        # Should not raise
        await worker_service.stop()
        assert worker_service._running is False

    @pytest.mark.asyncio
    async def test_stop_handles_close_error(self, worker_service, mock_processor):
        """Should handle errors when closing worker."""
        worker_service.set_processor(mock_processor)
        
        with patch("src.infrastructure.messaging.bullmq_worker.Worker") as MockWorker:
            mock_worker_instance = MagicMock()
            mock_worker_instance.close = AsyncMock(side_effect=Exception("Close failed"))
            MockWorker.return_value = mock_worker_instance
            
            await worker_service.start()
            # Should not raise
            await worker_service.stop()
            
            assert worker_service._running is False
            assert worker_service._worker is None


# =============================================================================
# SimpleJobProcessor Tests
# =============================================================================


class TestSimpleJobProcessor:
    """Tests for SimpleJobProcessor utility class."""

    @pytest.mark.asyncio
    async def test_process_calls_callback(self):
        """Should call the process callback with job data."""
        process_fn = AsyncMock(return_value={"result": "success"})
        processor = SimpleJobProcessor(process_fn=process_fn)
        
        job_data = {"datasetId": str(uuid4())}
        result = await processor.process(job_data)
        
        process_fn.assert_called_once_with(job_data)
        assert result == {"result": "success"}

    @pytest.mark.asyncio
    async def test_on_progress_calls_callback(self):
        """Should call the progress callback when set."""
        process_fn = AsyncMock()
        on_progress_fn = AsyncMock()
        processor = SimpleJobProcessor(
            process_fn=process_fn,
            on_progress_fn=on_progress_fn,
        )
        
        job_id = uuid4()
        await processor.on_progress(job_id, 50)
        
        on_progress_fn.assert_called_once_with(job_id, 50)

    @pytest.mark.asyncio
    async def test_on_progress_does_nothing_without_callback(self):
        """Should not raise when progress callback is not set."""
        process_fn = AsyncMock()
        processor = SimpleJobProcessor(process_fn=process_fn)
        
        # Should not raise
        await processor.on_progress(uuid4(), 50)

    @pytest.mark.asyncio
    async def test_on_error_calls_callback(self):
        """Should call the error callback when set."""
        process_fn = AsyncMock()
        on_error_fn = AsyncMock()
        processor = SimpleJobProcessor(
            process_fn=process_fn,
            on_error_fn=on_error_fn,
        )
        
        job_id = uuid4()
        await processor.on_error(job_id, "Something went wrong")
        
        on_error_fn.assert_called_once_with(job_id, "Something went wrong")

    @pytest.mark.asyncio
    async def test_on_error_does_nothing_without_callback(self):
        """Should not raise when error callback is not set."""
        process_fn = AsyncMock()
        processor = SimpleJobProcessor(process_fn=process_fn)
        
        # Should not raise
        await processor.on_error(uuid4(), "Error message")

    @pytest.mark.asyncio
    async def test_full_workflow(self):
        """Should handle full processing workflow."""
        results = []
        
        async def process(data):
            results.append(("process", data))
            return {"processed": True}
        
        async def on_progress(job_id, progress):
            results.append(("progress", job_id, progress))
        
        async def on_error(job_id, error):
            results.append(("error", job_id, error))
        
        processor = SimpleJobProcessor(
            process_fn=process,
            on_progress_fn=on_progress,
            on_error_fn=on_error,
        )
        
        job_id = uuid4()
        job_data = {"key": "value"}
        
        await processor.on_progress(job_id, 0)
        result = await processor.process(job_data)
        await processor.on_progress(job_id, 100)
        
        assert result == {"processed": True}
        assert len(results) == 3
        assert results[0] == ("progress", job_id, 0)
        assert results[1] == ("process", job_data)
        assert results[2] == ("progress", job_id, 100)


# =============================================================================
# Worker Configuration Tests
# =============================================================================


class TestWorkerConfiguration:
    """Tests for worker configuration options."""

    @pytest.mark.asyncio
    async def test_worker_created_with_correct_options(self, mock_processor):
        """Should pass correct options to BullMQ Worker."""
        service = BullMQWorkerService(
            redis_host="redis.example.com",
            redis_port=6380,
            queue_name="my-queue",
            concurrency=4,
        )
        service.set_processor(mock_processor)
        
        with patch("src.infrastructure.messaging.bullmq_worker.Worker") as MockWorker:
            MockWorker.return_value = MagicMock()
            
            await service.start()
            
            MockWorker.assert_called_once()
            call_kwargs = MockWorker.call_args[1]
            
            assert call_kwargs["opts"]["connection"]["host"] == "redis.example.com"
            assert call_kwargs["opts"]["connection"]["port"] == 6380
            assert call_kwargs["opts"]["concurrency"] == 4

    @pytest.mark.asyncio
    async def test_worker_created_with_queue_name(self, mock_processor):
        """Should create worker with correct queue name."""
        service = BullMQWorkerService(
            redis_host="localhost",
            redis_port=6379,
            queue_name="custom-queue",
        )
        service.set_processor(mock_processor)
        
        with patch("src.infrastructure.messaging.bullmq_worker.Worker") as MockWorker:
            MockWorker.return_value = MagicMock()
            
            await service.start()
            
            # Queue name is first positional arg
            call_args = MockWorker.call_args
            assert call_args[1]["name"] == "custom-queue"
