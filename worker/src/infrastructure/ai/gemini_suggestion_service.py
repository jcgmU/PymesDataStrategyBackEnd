"""Gemini-based implementation of the AiSuggestionService port.

Uses the ``google-generativeai`` SDK to call the Gemini API directly from the
worker process.  This gives the prompt access to real DataFrame statistics
(dtype, mean, std, quantiles, sample values, outlier values) that n8n could
never compute.

Configuration (from Settings / env vars):
    GEMINI_API_KEY   — Google AI API key (required; service disabled if empty)
    GEMINI_MODEL     — Model name (default: "gemini-2.0-flash-exp")
"""

from __future__ import annotations

import json
import asyncio
import logging
from typing import Any

import polars as pl
import structlog

from src.domain.entities.anomaly import AnomalyEntity
from src.domain.ports.services.ai_suggestion_service import AiSuggestion, AiSuggestionService

logger = structlog.get_logger("pymes.worker.ai.gemini")

# Numeric Polars dtypes used to determine if we can compute stats
_NUMERIC_DTYPES = (
    pl.Int8, pl.Int16, pl.Int32, pl.Int64,
    pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
    pl.Float32, pl.Float64,
)

_VALID_ACTION_TYPES = {"FILL", "DELETE", "KEEP"}


class GeminiSuggestionService(AiSuggestionService):
    """AI suggestion service backed by Google Gemini.

    Args:
        api_key: Google AI API key.
        model_name: Gemini model to use (e.g. ``"gemini-2.0-flash-exp"``).
        timeout: HTTP timeout in seconds for each API call.
    """

    def __init__(
        self,
        api_key: str,
        model_name: str = "gemini-2.0-flash-exp",
        timeout: float = 10.0,
    ) -> None:
        import google.generativeai as genai  # type: ignore[import-untyped]

        genai.configure(api_key=api_key)
        self._model = genai.GenerativeModel(
            model_name=model_name,
            generation_config=genai.GenerationConfig(
                temperature=0,
                response_mime_type="application/json",
            ),
        )
        self._timeout = timeout
        self._model_name = model_name

    async def generate_suggestion(
        self,
        anomaly: AnomalyEntity,
        df: pl.DataFrame,
    ) -> AiSuggestion | None:
        """Call Gemini with a rich, statistics-laden prompt about the anomaly.

        Returns ``None`` on any failure (timeout, API error, bad JSON, missing
        fields) so callers can skip gracefully without aborting the HITL flow.
        """
        try:
            prompt = self._build_prompt(anomaly, df)
        except Exception as exc:
            logger.warning(
                "gemini_prompt_build_failed",
                anomaly_id=anomaly.id,
                error=str(exc),
            )
            return None

        try:
            suggestion = await asyncio.wait_for(
                self._call_gemini(prompt),
                timeout=self._timeout,
            )
            return suggestion
        except asyncio.TimeoutError:
            logger.warning(
                "gemini_timeout",
                anomaly_id=anomaly.id,
                timeout_s=self._timeout,
            )
            return None
        except Exception as exc:
            logger.warning(
                "gemini_call_failed",
                anomaly_id=anomaly.id,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return None

    # -------------------------------------------------------------------------
    # Private helpers
    # -------------------------------------------------------------------------

    async def _call_gemini(self, prompt: str) -> AiSuggestion | None:
        """Run the blocking Gemini SDK call in a thread pool executor."""
        loop = asyncio.get_event_loop()
        raw_text: str = await loop.run_in_executor(None, self._sync_generate, prompt)
        return self._parse_response(raw_text)

    def _sync_generate(self, prompt: str) -> str:
        """Blocking call to the Gemini SDK (runs in executor thread)."""
        response = self._model.generate_content(prompt)
        return response.text

    def _build_prompt(self, anomaly: AnomalyEntity, df: pl.DataFrame) -> str:
        """Build a rich, data-aware prompt for the anomaly.

        Includes Polars dtype, statistical summaries, and sample/outlier
        values so Gemini can give a contextually accurate suggestion.
        """
        col = anomaly.column
        anomaly_type = anomaly.type
        affected_rows = anomaly.row  # stored as count of affected rows

        lines: list[str] = [
            f"Columna: '{col}'",
        ]

        # Column dtype and statistics
        if col in df.columns:
            dtype_str = str(df[col].dtype)
            lines.append(f"Tipo Polars: {dtype_str}")

            series = df[col]
            non_null = series.drop_nulls()
            non_null_count = non_null.len()

            if series.dtype in _NUMERIC_DTYPES and non_null_count >= 2:
                numeric = non_null.cast(pl.Float64)
                mean_val = numeric.mean()
                std_val = numeric.std()
                q05 = numeric.quantile(0.05)
                q95 = numeric.quantile(0.95)
                lines.append(f"Media: {mean_val:.4f}" if mean_val is not None else "Media: N/A")
                lines.append(f"Desviación estándar: {std_val:.4f}" if std_val is not None else "Desv. estándar: N/A")
                lines.append(
                    f"Percentil 5-95: [{q05:.4f}, {q95:.4f}]"
                    if q05 is not None and q95 is not None
                    else "Percentil 5-95: N/A"
                )

                # For outliers: list the actual outlier values
                if anomaly_type == "OUTLIER" and mean_val is not None and std_val is not None and std_val > 0:
                    z_scores = ((series.cast(pl.Float64) - mean_val).abs() / std_val)
                    outlier_vals = (
                        series.cast(pl.Float64)
                        .filter(z_scores > 3.0)
                        .drop_nulls()
                        .head(10)
                        .to_list()
                    )
                    if outlier_vals:
                        lines.append(f"Valores outlier detectados: {outlier_vals}")

            # Sample values (up to 5 non-null)
            try:
                samples = non_null.head(5).to_list()
                sample_strs = [str(v) for v in samples if v is not None]
                if sample_strs:
                    lines.append(f"Muestra de valores: {sample_strs}")
            except Exception:
                pass

        lines.append(f"Tipo de anomalía: {anomaly_type}")
        if affected_rows is not None:
            lines.append(f"Filas afectadas: {affected_rows}")
        if anomaly.description:
            lines.append(f"Descripción: {anomaly.description}")

        data_context = "\n".join(lines)

        prompt = (
            f"{data_context}\n\n"
            "Eres un experto en limpieza de datos para pequeñas y medianas empresas.\n"
            "Analiza la anomalía descrita y responde ÚNICAMENTE con un objeto JSON válido "
            "con exactamente estas tres claves:\n"
            '  "actionType": "FILL" | "DELETE" | "KEEP"\n'
            '  "value": string con el valor de corrección (null si actionType no es FILL)\n'
            '  "reason": explicación breve en español de por qué recomiendas esta acción\n\n'
            "Ejemplo de respuesta válida:\n"
            '{"actionType": "FILL", "value": "45.23", "reason": "Reemplazar con la media de la columna '
            'ya que el valor está a más de 3 desviaciones estándar."}'
        )
        return prompt

    def _parse_response(self, raw_text: str) -> AiSuggestion | None:
        """Parse and validate the Gemini JSON response.

        Returns ``None`` if the response is malformed or missing required fields.
        """
        if not raw_text:
            logger.warning("gemini_empty_response")
            return None

        try:
            data: dict[str, Any] = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            logger.warning("gemini_json_parse_error", error=str(exc), raw=raw_text[:200])
            return None

        action_type = data.get("actionType")
        if action_type not in _VALID_ACTION_TYPES:
            logger.warning(
                "gemini_invalid_action_type",
                action_type=action_type,
                valid=list(_VALID_ACTION_TYPES),
            )
            return None

        reason = data.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            logger.warning("gemini_missing_reason", data=data)
            return None

        value = data.get("value")
        if value is not None and not isinstance(value, str):
            value = str(value)

        return AiSuggestion(
            action_type=action_type,
            value=value,
            reason=reason.strip(),
        )
