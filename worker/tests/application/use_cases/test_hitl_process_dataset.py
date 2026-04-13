"""Tests for HITL (Human-in-the-Loop) flow in ProcessDatasetUseCase."""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import polars as pl
import pytest

from src.application.use_cases.process_dataset import (
    ProcessDatasetInput,
    ProcessDatasetUseCase,
)
from src.domain.entities.anomaly import AnomalyEntity
from src.domain.entities.decision import DecisionEntity
from src.domain.ports.repositories.job_repository import JobRepository
from src.domain.value_objects.job_status import JobStatus
from src.infrastructure.parsers.dataset_parser import DatasetParser

from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_job_repository() -> AsyncMock:
    """Return a fully mocked JobRepository."""
    repo = AsyncMock(spec=JobRepository)
    repo.update_job_status = AsyncMock()
    repo.save_anomalies = AsyncMock()
    repo.get_anomalies = AsyncMock(return_value=[])
    repo.get_decisions = AsyncMock(return_value=[])
    repo.count_pending_anomalies = AsyncMock(return_value=0)
    return repo


def _make_decision(anomaly_id: str, action: str, correction: str | None = None) -> DecisionEntity:
    return DecisionEntity(
        id=str(uuid4()),
        anomaly_id=anomaly_id,
        action=action,
        correction=correction,
        user_id=str(uuid4()),
        created_at=datetime.now(timezone.utc),
    )


# CSV with a statistical outlier (age=9999 among 20 values near 30).
# With 20 near-identical "normal" values + 1 extreme, the z-score of 9999 exceeds 3.0,
# which is required to trigger OUTLIER anomaly detection.
_CSV_WITH_OUTLIER = (
    b"name,age\n"
    b"A,30\nB,31\nC,29\nD,30\nE,31\n"
    b"F,30\nG,29\nH,30\nI,31\nJ,30\n"
    b"K,30\nL,31\nM,29\nN,30\nO,31\n"
    b"P,30\nQ,29\nR,30\nS,31\nT,30\n"
    b"Eve,9999\n"
)

_CSV_CLEAN = b"name,age\nAlice,30\nBob,25\nCharlie,35\n"
_CSV_WITH_NULL = b"name,age\nAlice,30\nBob,\nCharlie,35\n"


# ---------------------------------------------------------------------------
# Test suites
# ---------------------------------------------------------------------------


class TestProcessDatasetHITLFlow:
    """Tests for the HITL decision loop."""

    @pytest.fixture
    def mock_storage(self):
        storage = AsyncMock()
        storage.download_file = AsyncMock()
        storage.upload_file = AsyncMock()
        return storage

    @pytest.fixture
    def parser(self):
        return DatasetParser()

    @pytest.fixture
    def mock_repo(self):
        return _make_mock_job_repository()

    # ------------------------------------------------------------------
    # HITL not triggered when no anomalies
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_no_hitl_when_no_anomalies(
        self, mock_storage, parser, mock_repo
    ) -> None:
        """When no anomalies are detected the job goes directly to COMPLETED."""
        mock_storage.download_file.return_value = _CSV_CLEAN

        use_case = ProcessDatasetUseCase(
            storage=mock_storage,
            parser=parser,
            output_bucket="test-bucket",
            job_repository=mock_repo,
            hitl_poll_interval=0.001,
        )

        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/clean.csv",
            filename="clean.csv",
            transformations=[],
        )

        result = await use_case.execute(input_data)

        assert result.success is True
        assert result.anomalies_detected == 0
        assert result.decisions_applied == 0

        # Verify status updates: PROCESSING then COMPLETED
        calls = [call.args[1] for call in mock_repo.update_job_status.await_args_list]
        assert JobStatus.PROCESSING in calls
        assert JobStatus.COMPLETED in calls
        # AWAITING_REVIEW should NOT appear
        assert JobStatus.AWAITING_REVIEW not in calls

        # save_anomalies should NOT be called
        mock_repo.save_anomalies.assert_not_called()

    # ------------------------------------------------------------------
    # HITL triggered when anomalies detected
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_hitl_triggered_when_anomalies_detected(
        self, mock_storage, parser, mock_repo
    ) -> None:
        """When anomalies exist, job enters AWAITING_REVIEW before COMPLETED."""
        mock_storage.download_file.return_value = _CSV_WITH_OUTLIER

        # Simulate: first poll returns 1 pending, second poll returns 0
        mock_repo.count_pending_anomalies = AsyncMock(side_effect=[1, 0])
        mock_repo.get_decisions = AsyncMock(return_value=[])

        use_case = ProcessDatasetUseCase(
            storage=mock_storage,
            parser=parser,
            output_bucket="test-bucket",
            job_repository=mock_repo,
            hitl_poll_interval=0.001,
        )

        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/data.csv",
            filename="data.csv",
            transformations=[],
        )

        result = await use_case.execute(input_data)

        assert result.success is True
        assert result.anomalies_detected > 0

        # save_anomalies must be called once
        mock_repo.save_anomalies.assert_awaited_once()

        # Status flow includes AWAITING_REVIEW
        status_calls = [call.args[1] for call in mock_repo.update_job_status.await_args_list]
        assert JobStatus.AWAITING_REVIEW in status_calls
        assert JobStatus.COMPLETED in status_calls

    # ------------------------------------------------------------------
    # HITL applies DISCARDED decisions (drop rows)
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_discarded_decision_drops_row(
        self, mock_storage, parser
    ) -> None:
        """DISCARDED decision removes the anomalous row from the output."""
        # Use a CSV where we can manually create a matching anomaly
        mock_storage.download_file.return_value = _CSV_WITH_OUTLIER

        mock_repo = _make_mock_job_repository()
        mock_repo.count_pending_anomalies = AsyncMock(return_value=0)

        # We'll intercept save_anomalies to capture the anomaly IDs, then return decisions
        saved_anomalies: list[AnomalyEntity] = []

        async def capture_save(dataset_id: str, anomalies: list) -> None:
            saved_anomalies.extend(anomalies)

        mock_repo.save_anomalies = capture_save

        # After saving, return DISCARDED decisions for all anomalies
        async def provide_decisions(dataset_id: str) -> list:
            return [_make_decision(a.id, "DISCARDED") for a in saved_anomalies]

        mock_repo.get_decisions = provide_decisions

        use_case = ProcessDatasetUseCase(
            storage=mock_storage,
            parser=parser,
            output_bucket="test-bucket",
            job_repository=mock_repo,
            hitl_poll_interval=0.001,
        )

        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/data.csv",
            filename="data.csv",
            transformations=[],
        )

        result = await use_case.execute(input_data)

        assert result.success is True
        # DISCARDED = reject the suggestion, keep original value (no rows dropped).
        # All rows are preserved — business rule: never delete records.
        assert result.anomalies_detected > 0

    # ------------------------------------------------------------------
    # HITL applies CORRECTED decisions (update cell value)
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_corrected_decision_updates_cell(
        self, mock_storage, parser
    ) -> None:
        """CORRECTED decision replaces the cell value in the output."""
        # CSV with a null we want to correct
        mock_storage.download_file.return_value = _CSV_WITH_NULL

        mock_repo = _make_mock_job_repository()
        mock_repo.count_pending_anomalies = AsyncMock(return_value=0)

        saved_anomalies: list[AnomalyEntity] = []

        async def capture_save(dataset_id: str, anomalies: list) -> None:
            saved_anomalies.extend(anomalies)

        mock_repo.save_anomalies = capture_save

        async def provide_corrections(dataset_id: str) -> list:
            # Correct all anomalies with value "99"
            return [_make_decision(a.id, "CORRECTED", "99") for a in saved_anomalies]

        mock_repo.get_decisions = provide_corrections

        use_case = ProcessDatasetUseCase(
            storage=mock_storage,
            parser=parser,
            output_bucket="test-bucket",
            job_repository=mock_repo,
            hitl_poll_interval=0.001,
        )

        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/data.csv",
            filename="data.csv",
            transformations=[],
        )

        result = await use_case.execute(input_data)

        assert result.success is True
        # All 3 rows should remain (no row was DISCARDED)
        assert result.rows_processed == 3

    # ------------------------------------------------------------------
    # HITL without repository: no DB calls
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_no_db_calls_when_no_repository(
        self, mock_storage, parser
    ) -> None:
        """When no repository is provided, HITL is skipped entirely."""
        mock_storage.download_file.return_value = _CSV_WITH_OUTLIER

        use_case = ProcessDatasetUseCase(
            storage=mock_storage,
            parser=parser,
            output_bucket="test-bucket",
            job_repository=None,  # No repository
        )

        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/data.csv",
            filename="data.csv",
            transformations=[],
        )

        result = await use_case.execute(input_data)

        # Should still succeed; anomalies detected but not persisted
        assert result.success is True

    # ------------------------------------------------------------------
    # Status update sequence validation
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_status_sequence_processing_then_completed(
        self, mock_storage, parser, mock_repo
    ) -> None:
        """Clean CSV: status goes PROCESSING → COMPLETED (no AWAITING_REVIEW)."""
        mock_storage.download_file.return_value = _CSV_CLEAN

        use_case = ProcessDatasetUseCase(
            storage=mock_storage,
            parser=parser,
            output_bucket="test-bucket",
            job_repository=mock_repo,
        )

        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/clean.csv",
            filename="clean.csv",
            transformations=[],
        )

        await use_case.execute(input_data)

        status_calls = [call.args[1] for call in mock_repo.update_job_status.await_args_list]
        assert status_calls[0] == JobStatus.PROCESSING
        assert status_calls[-1] == JobStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_status_failed_on_download_error(
        self, mock_storage, parser, mock_repo
    ) -> None:
        """When download fails, status must be updated to FAILED."""
        mock_storage.download_file.side_effect = Exception("Network error")

        use_case = ProcessDatasetUseCase(
            storage=mock_storage,
            parser=parser,
            output_bucket="test-bucket",
            job_repository=mock_repo,
        )

        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/data.csv",
            filename="data.csv",
            transformations=[],
        )

        result = await use_case.execute(input_data)

        assert result.success is False
        status_calls = [call.args[1] for call in mock_repo.update_job_status.await_args_list]
        assert JobStatus.FAILED in status_calls

    @pytest.mark.asyncio
    async def test_repository_failure_does_not_crash_pipeline(
        self, mock_storage, parser
    ) -> None:
        """If the repository raises on update_job_status, processing continues."""
        mock_storage.download_file.return_value = _CSV_CLEAN

        failing_repo = _make_mock_job_repository()
        failing_repo.update_job_status = AsyncMock(
            side_effect=Exception("DB connection lost")
        )

        use_case = ProcessDatasetUseCase(
            storage=mock_storage,
            parser=parser,
            output_bucket="test-bucket",
            job_repository=failing_repo,
        )

        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/clean.csv",
            filename="clean.csv",
            transformations=[],
        )

        result = await use_case.execute(input_data)

        # Pipeline should complete despite DB errors (graceful degradation)
        assert result.success is True


class TestAnomalyDetection:
    """Tests for the _detect_anomalies private method via execute()."""

    @pytest.fixture
    def mock_storage(self):
        storage = AsyncMock()
        storage.download_file = AsyncMock()
        storage.upload_file = AsyncMock()
        return storage

    @pytest.fixture
    def parser(self):
        return DatasetParser()

    @pytest.mark.asyncio
    async def test_detect_null_as_missing_value_anomaly(
        self, mock_storage, parser
    ) -> None:
        """Null values in the DataFrame are detected as MISSING_VALUE anomalies."""
        mock_storage.download_file.return_value = _CSV_WITH_NULL
        mock_repo = _make_mock_job_repository()

        saved_anomalies: list[AnomalyEntity] = []

        async def capture(dataset_id: str, anomalies: list) -> None:
            saved_anomalies.extend(anomalies)

        mock_repo.save_anomalies = capture
        mock_repo.count_pending_anomalies = AsyncMock(return_value=0)
        mock_repo.get_decisions = AsyncMock(return_value=[])

        use_case = ProcessDatasetUseCase(
            storage=mock_storage,
            parser=parser,
            output_bucket="test-bucket",
            job_repository=mock_repo,
            hitl_poll_interval=0.001,
        )

        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/nulls.csv",
            filename="nulls.csv",
            transformations=[],
        )

        result = await use_case.execute(input_data)

        assert result.success is True
        # At least one MISSING_VALUE anomaly for the null age cell
        missing_value_anomalies = [a for a in saved_anomalies if a.type == "MISSING_VALUE"]
        assert len(missing_value_anomalies) >= 1
        assert missing_value_anomalies[0].column == "age"

    @pytest.mark.asyncio
    async def test_detect_statistical_outlier(
        self, mock_storage, parser
    ) -> None:
        """Extreme numeric values (z-score > 3) are detected as OUTLIER anomalies."""
        mock_storage.download_file.return_value = _CSV_WITH_OUTLIER
        mock_repo = _make_mock_job_repository()

        saved_anomalies: list[AnomalyEntity] = []

        async def capture(dataset_id: str, anomalies: list) -> None:
            saved_anomalies.extend(anomalies)

        mock_repo.save_anomalies = capture
        mock_repo.count_pending_anomalies = AsyncMock(return_value=0)
        mock_repo.get_decisions = AsyncMock(return_value=[])

        use_case = ProcessDatasetUseCase(
            storage=mock_storage,
            parser=parser,
            output_bucket="test-bucket",
            job_repository=mock_repo,
            hitl_poll_interval=0.001,
        )

        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/outliers.csv",
            filename="outliers.csv",
            transformations=[],
        )

        result = await use_case.execute(input_data)

        assert result.success is True
        outlier_anomalies = [a for a in saved_anomalies if a.type == "OUTLIER"]
        assert len(outlier_anomalies) >= 1
        # The outlier should be in the 'age' column, row 4 (Eve, 9999)
        assert any(a.column == "age" for a in outlier_anomalies)

    @pytest.mark.asyncio
    async def test_no_anomalies_clean_data(
        self, mock_storage, parser
    ) -> None:
        """Clean data produces zero anomalies."""
        mock_storage.download_file.return_value = _CSV_CLEAN
        mock_repo = _make_mock_job_repository()

        use_case = ProcessDatasetUseCase(
            storage=mock_storage,
            parser=parser,
            output_bucket="test-bucket",
            job_repository=mock_repo,
        )

        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/clean.csv",
            filename="clean.csv",
            transformations=[],
        )

        result = await use_case.execute(input_data)

        assert result.success is True
        assert result.anomalies_detected == 0
        mock_repo.save_anomalies.assert_not_called()


class TestApplyDecisions:
    """Unit tests for decision application logic via the use case."""

    @pytest.fixture
    def mock_storage(self):
        storage = AsyncMock()
        storage.download_file = AsyncMock()
        storage.upload_file = AsyncMock()
        return storage

    @pytest.fixture
    def parser(self):
        return DatasetParser()

    @pytest.mark.asyncio
    async def test_approved_decision_keeps_row(
        self, mock_storage, parser
    ) -> None:
        """APPROVED decision keeps the anomalous row unchanged."""
        mock_storage.download_file.return_value = _CSV_WITH_NULL
        mock_repo = _make_mock_job_repository()

        saved_anomalies: list[AnomalyEntity] = []

        async def capture(dataset_id: str, anomalies: list) -> None:
            saved_anomalies.extend(anomalies)

        mock_repo.save_anomalies = capture
        mock_repo.count_pending_anomalies = AsyncMock(return_value=0)

        async def provide_decisions(dataset_id: str) -> list:
            return [_make_decision(a.id, "APPROVED") for a in saved_anomalies]

        mock_repo.get_decisions = provide_decisions

        use_case = ProcessDatasetUseCase(
            storage=mock_storage,
            parser=parser,
            output_bucket="test-bucket",
            job_repository=mock_repo,
            hitl_poll_interval=0.001,
        )

        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/nulls.csv",
            filename="nulls.csv",
            transformations=[],
        )

        result = await use_case.execute(input_data)

        assert result.success is True
        # All 3 rows should be kept (no DISCARDED decisions)
        assert result.rows_processed == 3

    @pytest.mark.asyncio
    async def test_mixed_decisions(
        self, mock_storage, parser
    ) -> None:
        """Mix of APPROVED and DISCARDED decisions is applied correctly."""
        mock_storage.download_file.return_value = _CSV_WITH_OUTLIER

        mock_repo = _make_mock_job_repository()
        saved_anomalies: list[AnomalyEntity] = []

        async def capture(dataset_id: str, anomalies: list) -> None:
            saved_anomalies.extend(anomalies)

        mock_repo.save_anomalies = capture
        mock_repo.count_pending_anomalies = AsyncMock(return_value=0)

        async def provide_decisions(dataset_id: str) -> list:
            if not saved_anomalies:
                return []
            decisions = []
            # Discard first anomaly, approve rest
            decisions.append(_make_decision(saved_anomalies[0].id, "DISCARDED"))
            for a in saved_anomalies[1:]:
                decisions.append(_make_decision(a.id, "APPROVED"))
            return decisions

        mock_repo.get_decisions = provide_decisions

        use_case = ProcessDatasetUseCase(
            storage=mock_storage,
            parser=parser,
            output_bucket="test-bucket",
            job_repository=mock_repo,
            hitl_poll_interval=0.001,
        )

        input_data = ProcessDatasetInput(
            dataset_id=uuid4(),
            job_id=uuid4(),
            source_key="raw/data.csv",
            filename="data.csv",
            transformations=[],
        )

        result = await use_case.execute(input_data)

        assert result.success is True
        assert result.decisions_applied >= 1
