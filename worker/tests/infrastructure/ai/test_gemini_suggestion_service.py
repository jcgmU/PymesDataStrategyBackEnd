"""Tests for GeminiSuggestionService.

All tests mock ``google.generativeai`` so no real API calls are made.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from src.domain.entities.anomaly import AnomalyEntity
from src.domain.ports.services.ai_suggestion_service import AiSuggestion
from src.infrastructure.ai.gemini_suggestion_service import GeminiSuggestionService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_anomaly(
    *,
    anomaly_type: str = "OUTLIER",
    column: str = "precio",
    row: int | None = 3,
) -> AnomalyEntity:
    return AnomalyEntity.create(
        id="test-anomaly-id-001",
        dataset_id="test-dataset-id",
        column=column,
        row=row,
        anomaly_type=anomaly_type,
        description=f"Test anomaly in column '{column}'",
    )


def _make_df(column: str = "precio") -> pl.DataFrame:
    """Return a small DataFrame with one numeric column containing outliers."""
    return pl.DataFrame(
        {column: [10.0, 12.0, 11.0, 13.0, 10.5, 312.5, 298.0, 11.2, 12.3, 10.8]}
    )


def _make_service() -> tuple[GeminiSuggestionService, MagicMock]:
    """Instantiate GeminiSuggestionService with a fully mocked genai module.

    Returns the service instance and the mock model for further configuration.
    """
    mock_genai = MagicMock()
    mock_model = MagicMock()
    mock_genai.GenerativeModel.return_value = mock_model
    mock_genai.GenerationConfig.return_value = MagicMock()

    with patch.dict("sys.modules", {"google.generativeai": mock_genai}):
        service = GeminiSuggestionService(
            api_key="fake-api-key",
            model_name="gemini-2.0-flash-exp",
            timeout=5.0,
        )
        # Replace the internal model reference with our mock
        service._model = mock_model

    return service, mock_model


# ---------------------------------------------------------------------------
# Test: successful suggestion with real DataFrame stats
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_suggestion_success_outlier() -> None:
    """Service builds a rich prompt and returns a valid AiSuggestion."""
    service, mock_model = _make_service()

    expected_response = json.dumps(
        {
            "actionType": "FILL",
            "value": "11.57",
            "reason": "Reemplazar con la media ya que el valor está a más de 3 desviaciones estándar.",
        }
    )
    mock_response = MagicMock()
    mock_response.text = expected_response
    mock_model.generate_content.return_value = mock_response

    anomaly = _make_anomaly(anomaly_type="OUTLIER", column="precio")
    df = _make_df("precio")

    suggestion = await service.generate_suggestion(anomaly, df)

    assert suggestion is not None
    assert isinstance(suggestion, AiSuggestion)
    assert suggestion.action_type == "FILL"
    assert suggestion.value == "11.57"
    assert "media" in suggestion.reason.lower() or "desviaciones" in suggestion.reason.lower()

    # Verify generate_content was called once
    mock_model.generate_content.assert_called_once()


@pytest.mark.asyncio
async def test_generate_suggestion_success_missing_value() -> None:
    """Service handles MISSING_VALUE anomaly type correctly."""
    service, mock_model = _make_service()

    expected_response = json.dumps(
        {
            "actionType": "FILL",
            "value": "11.57",
            "reason": "Rellenar con la media de la columna.",
        }
    )
    mock_response = MagicMock()
    mock_response.text = expected_response
    mock_model.generate_content.return_value = mock_response

    df = pl.DataFrame({"precio": [10.0, None, 12.0, 11.0, None]})
    anomaly = _make_anomaly(anomaly_type="MISSING_VALUE", column="precio", row=2)

    suggestion = await service.generate_suggestion(anomaly, df)

    assert suggestion is not None
    assert suggestion.action_type == "FILL"


@pytest.mark.asyncio
async def test_generate_suggestion_delete_action() -> None:
    """Service correctly parses DELETE action type."""
    service, mock_model = _make_service()

    mock_response = MagicMock()
    mock_response.text = json.dumps(
        {"actionType": "DELETE", "value": None, "reason": "Eliminar filas con valores extremos."}
    )
    mock_model.generate_content.return_value = mock_response

    suggestion = await service.generate_suggestion(_make_anomaly(), _make_df())

    assert suggestion is not None
    assert suggestion.action_type == "DELETE"
    assert suggestion.value is None


# ---------------------------------------------------------------------------
# Test: prompt contains DataFrame statistics
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_prompt_contains_df_statistics() -> None:
    """The generated prompt includes column dtype, mean, std, and outlier values."""
    service, mock_model = _make_service()

    mock_response = MagicMock()
    mock_response.text = json.dumps(
        {"actionType": "FILL", "value": "11.0", "reason": "Usar la media."}
    )
    mock_model.generate_content.return_value = mock_response

    anomaly = _make_anomaly(anomaly_type="OUTLIER", column="precio")
    df = _make_df("precio")

    await service.generate_suggestion(anomaly, df)

    # Inspect the prompt passed to generate_content
    call_args = mock_model.generate_content.call_args
    prompt: str = call_args[0][0]

    assert "precio" in prompt
    assert "OUTLIER" in prompt
    assert "Media" in prompt or "media" in prompt
    assert "Desviación estándar" in prompt or "std" in prompt.lower()
    # Outlier values (312.5, 298.0) should appear
    assert "312.5" in prompt or "298.0" in prompt


@pytest.mark.asyncio
async def test_prompt_contains_sample_values() -> None:
    """The generated prompt includes sample values from the column."""
    service, mock_model = _make_service()

    mock_response = MagicMock()
    mock_response.text = json.dumps(
        {"actionType": "KEEP", "value": None, "reason": "Valores dentro del rango normal."}
    )
    mock_model.generate_content.return_value = mock_response

    df = pl.DataFrame({"nombre": ["Alice", "Bob", "Carol", "Dave", "Eve"]})
    anomaly = _make_anomaly(anomaly_type="MISSING_VALUE", column="nombre", row=1)

    await service.generate_suggestion(anomaly, df)

    prompt: str = mock_model.generate_content.call_args[0][0]
    assert "nombre" in prompt
    # At least one of the sample values should appear
    assert any(name in prompt for name in ["Alice", "Bob", "Carol"])


# ---------------------------------------------------------------------------
# Test: timeout handled gracefully
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timeout_returns_none() -> None:
    """When the Gemini call times out, generate_suggestion returns None."""
    service, mock_model = _make_service()
    service._timeout = 0.01  # very short timeout

    # Make generate_content block longer than the timeout
    def _slow_generate(_prompt: str) -> MagicMock:
        import time
        time.sleep(1)  # 1 second >> 0.01 timeout
        return MagicMock(text='{"actionType": "FILL", "value": "1", "reason": "ok"}')

    mock_model.generate_content.side_effect = _slow_generate

    suggestion = await service.generate_suggestion(_make_anomaly(), _make_df())

    assert suggestion is None


# ---------------------------------------------------------------------------
# Test: malformed JSON returns None
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_malformed_json_returns_none() -> None:
    """When Gemini returns invalid JSON, generate_suggestion returns None."""
    service, mock_model = _make_service()

    mock_response = MagicMock()
    mock_response.text = "this is not json at all {{{"
    mock_model.generate_content.return_value = mock_response

    suggestion = await service.generate_suggestion(_make_anomaly(), _make_df())

    assert suggestion is None


@pytest.mark.asyncio
async def test_invalid_action_type_returns_none() -> None:
    """When actionType is not in {FILL, DELETE, KEEP}, returns None."""
    service, mock_model = _make_service()

    mock_response = MagicMock()
    mock_response.text = json.dumps(
        {"actionType": "REPLACE", "value": "42", "reason": "Usar valor alternativo."}
    )
    mock_model.generate_content.return_value = mock_response

    suggestion = await service.generate_suggestion(_make_anomaly(), _make_df())

    assert suggestion is None


@pytest.mark.asyncio
async def test_missing_reason_returns_none() -> None:
    """When the 'reason' field is absent or empty, returns None."""
    service, mock_model = _make_service()

    mock_response = MagicMock()
    mock_response.text = json.dumps({"actionType": "DELETE", "value": None, "reason": ""})
    mock_model.generate_content.return_value = mock_response

    suggestion = await service.generate_suggestion(_make_anomaly(), _make_df())

    assert suggestion is None


@pytest.mark.asyncio
async def test_api_exception_returns_none() -> None:
    """When the Gemini SDK raises an exception, generate_suggestion returns None."""
    service, mock_model = _make_service()

    mock_model.generate_content.side_effect = RuntimeError("API quota exceeded")

    suggestion = await service.generate_suggestion(_make_anomaly(), _make_df())

    assert suggestion is None


@pytest.mark.asyncio
async def test_empty_response_returns_none() -> None:
    """When Gemini returns an empty string, generate_suggestion returns None."""
    service, mock_model = _make_service()

    mock_response = MagicMock()
    mock_response.text = ""
    mock_model.generate_content.return_value = mock_response

    suggestion = await service.generate_suggestion(_make_anomaly(), _make_df())

    assert suggestion is None
