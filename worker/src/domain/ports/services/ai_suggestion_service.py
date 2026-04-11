"""AI suggestion service port — interface for generating AI suggestions on anomalies."""

from abc import ABC, abstractmethod
from dataclasses import dataclass

import polars as pl

from src.domain.entities.anomaly import AnomalyEntity


@dataclass
class AiSuggestion:
    """AI-generated suggestion for resolving an anomaly."""

    action_type: str        # "FILL" | "DELETE" | "KEEP"
    value: str | None       # correction value (only meaningful for FILL)
    reason: str             # human-readable explanation in Spanish


class AiSuggestionService(ABC):
    """Port for AI-powered anomaly suggestion generation.

    Implementations may delegate to Gemini, OpenAI, or any other LLM.
    The worker uses this port to avoid coupling to a specific provider.
    """

    @abstractmethod
    async def generate_suggestion(
        self,
        anomaly: AnomalyEntity,
        df: pl.DataFrame,
    ) -> AiSuggestion | None:
        """Generate an AI suggestion for a single anomaly.

        The implementation has access to the full DataFrame so it can
        compute rich statistics (mean, std, quantiles, sample values)
        and include them in the prompt — context that n8n would never have.

        Args:
            anomaly: The detected anomaly entity.
            df: The full DataFrame being processed (used for column stats).

        Returns:
            An ``AiSuggestion`` on success, or ``None`` if generation fails
            (timeout, API error, malformed response).  Returning ``None``
            must never abort the HITL flow.
        """
        ...
