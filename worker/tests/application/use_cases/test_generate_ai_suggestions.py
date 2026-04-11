"""Tests for ProcessDatasetUseCase._generate_ai_suggestions (Opción C).

Covers:
- With service available → calls generate_suggestion and saves to repo
- Without service (None) → skip silently, repo never called
- Service returns None → no save, no failure
- Partial failure (one anomaly throws) → others still processed
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import polars as pl
import pytest

from src.application.use_cases.process_dataset import ProcessDatasetUseCase
from src.domain.entities.anomaly import AnomalyEntity
from src.domain.ports.repositories.job_repository import JobRepository
from src.domain.ports.services.ai_suggestion_service import AiSuggestion, AiSuggestionService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_anomaly(column: str = "precio", anomaly_type: str = "OUTLIER") -> AnomalyEntity:
    return AnomalyEntity.create(
        id=str(uuid4()),
        dataset_id="ds-001",
        column=column,
        row=3,
        anomaly_type=anomaly_type,
        description=f"Test anomaly in '{column}'",
    )


def _make_df() -> pl.DataFrame:
    return pl.DataFrame({"precio": [10.0, 12.0, 11.0, 13.0, 312.5]})


def _make_use_case(
    ai_service: AiSuggestionService | None,
    job_repo: JobRepository | None = None,
) -> ProcessDatasetUseCase:
    mock_storage = AsyncMock()
    return ProcessDatasetUseCase(
        storage=mock_storage,
        job_repository=job_repo,
        ai_suggestion_service=ai_service,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_ai_suggestions_with_service_saves_suggestion() -> None:
    """When service is available, generate_suggestion is called and result is saved."""
    suggestion = AiSuggestion(
        action_type="FILL",
        value="11.5",
        reason="Rellenar con la media de la columna.",
    )

    mock_service = AsyncMock(spec=AiSuggestionService)
    mock_service.generate_suggestion = AsyncMock(return_value=suggestion)

    mock_repo = AsyncMock(spec=JobRepository)
    mock_repo.save_ai_suggestion = AsyncMock()

    use_case = _make_use_case(ai_service=mock_service, job_repo=mock_repo)

    anomaly = _make_anomaly()
    df = _make_df()

    await use_case._generate_ai_suggestions([anomaly], df, "job-001")

    mock_service.generate_suggestion.assert_awaited_once_with(anomaly, df)
    mock_repo.save_ai_suggestion.assert_awaited_once_with(
        anomaly_id=anomaly.id,
        action_type="FILL",
        value="11.5",
        reason="Rellenar con la media de la columna.",
    )


@pytest.mark.asyncio
async def test_generate_ai_suggestions_without_service_is_silent() -> None:
    """When ai_suggestion_service is None, method returns immediately without error."""
    mock_repo = AsyncMock(spec=JobRepository)
    mock_repo.save_ai_suggestion = AsyncMock()

    use_case = _make_use_case(ai_service=None, job_repo=mock_repo)

    anomaly = _make_anomaly()
    df = _make_df()

    # Must not raise
    await use_case._generate_ai_suggestions([anomaly], df, "job-001")

    # Repo must not be called
    mock_repo.save_ai_suggestion.assert_not_awaited()


@pytest.mark.asyncio
async def test_generate_ai_suggestions_without_repo_is_silent() -> None:
    """When job_repo is None, method returns immediately without error."""
    mock_service = AsyncMock(spec=AiSuggestionService)
    mock_service.generate_suggestion = AsyncMock(
        return_value=AiSuggestion(action_type="FILL", value="1", reason="ok")
    )

    use_case = _make_use_case(ai_service=mock_service, job_repo=None)

    # Must not raise
    await use_case._generate_ai_suggestions([_make_anomaly()], _make_df(), "job-001")

    # Service must not be called (repo is required to save)
    mock_service.generate_suggestion.assert_not_awaited()


@pytest.mark.asyncio
async def test_generate_ai_suggestions_service_returns_none_does_not_save() -> None:
    """When generate_suggestion returns None, save_ai_suggestion is not called."""
    mock_service = AsyncMock(spec=AiSuggestionService)
    mock_service.generate_suggestion = AsyncMock(return_value=None)

    mock_repo = AsyncMock(spec=JobRepository)
    mock_repo.save_ai_suggestion = AsyncMock()

    use_case = _make_use_case(ai_service=mock_service, job_repo=mock_repo)

    await use_case._generate_ai_suggestions([_make_anomaly()], _make_df(), "job-001")

    mock_service.generate_suggestion.assert_awaited_once()
    mock_repo.save_ai_suggestion.assert_not_awaited()


@pytest.mark.asyncio
async def test_generate_ai_suggestions_partial_failure_continues() -> None:
    """If one anomaly raises an exception, remaining anomalies are still processed."""
    good_suggestion = AiSuggestion(
        action_type="DELETE",
        value=None,
        reason="Eliminar outlier extremo.",
    )

    anomaly_bad = _make_anomaly(column="precio")
    anomaly_good = _make_anomaly(column="cantidad")

    async def _side_effect(anomaly: AnomalyEntity, df: pl.DataFrame) -> AiSuggestion | None:
        if anomaly.column == "precio":
            raise RuntimeError("Simulated Gemini error")
        return good_suggestion

    mock_service = AsyncMock(spec=AiSuggestionService)
    mock_service.generate_suggestion = AsyncMock(side_effect=_side_effect)

    mock_repo = AsyncMock(spec=JobRepository)
    mock_repo.save_ai_suggestion = AsyncMock()

    use_case = _make_use_case(ai_service=mock_service, job_repo=mock_repo)

    df = pl.DataFrame({"precio": [10.0, 312.5], "cantidad": [5.0, 6.0]})

    # Must not raise even though first anomaly fails
    await use_case._generate_ai_suggestions([anomaly_bad, anomaly_good], df, "job-001")

    # Only the good anomaly should be saved
    mock_repo.save_ai_suggestion.assert_awaited_once_with(
        anomaly_id=anomaly_good.id,
        action_type="DELETE",
        value=None,
        reason="Eliminar outlier extremo.",
    )


@pytest.mark.asyncio
async def test_generate_ai_suggestions_multiple_anomalies_all_saved() -> None:
    """All anomalies in the list get their suggestions saved."""
    anomaly1 = _make_anomaly(column="precio", anomaly_type="OUTLIER")
    anomaly2 = _make_anomaly(column="nombre", anomaly_type="MISSING_VALUE")

    suggestions = {
        anomaly1.id: AiSuggestion(action_type="FILL", value="11.5", reason="Usar la media."),
        anomaly2.id: AiSuggestion(action_type="FILL", value="Desconocido", reason="Valor por defecto."),
    }

    async def _side_effect(anomaly: AnomalyEntity, df: pl.DataFrame) -> AiSuggestion | None:
        return suggestions.get(anomaly.id)

    mock_service = AsyncMock(spec=AiSuggestionService)
    mock_service.generate_suggestion = AsyncMock(side_effect=_side_effect)

    mock_repo = AsyncMock(spec=JobRepository)
    mock_repo.save_ai_suggestion = AsyncMock()

    use_case = _make_use_case(ai_service=mock_service, job_repo=mock_repo)

    df = pl.DataFrame({"precio": [10.0, 312.5], "nombre": ["Alice", None]})
    await use_case._generate_ai_suggestions([anomaly1, anomaly2], df, "job-001")

    assert mock_repo.save_ai_suggestion.await_count == 2
