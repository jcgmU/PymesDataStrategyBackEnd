"""Decision domain entity."""

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class DecisionEntity:
    """Represents a human decision on a detected anomaly.

    Maps to the ``decisions`` table in Prisma schema.
    """

    id: str
    anomaly_id: str
    action: str  # "APPROVED" | "CORRECTED" | "DISCARDED"
    correction: str | None
    user_id: str
    created_at: datetime
    correction_ir: dict | None = field(default=None)  # IR tree (new field)
    ir_source: str | None = field(default=None)       # "rule" | "gemini" (new field)

    @property
    def is_approved(self) -> bool:
        """True if the anomaly was approved (keep as-is)."""
        return self.action == "APPROVED"

    @property
    def is_corrected(self) -> bool:
        """True if the anomaly value was corrected."""
        return self.action == "CORRECTED"

    @property
    def is_discarded(self) -> bool:
        """True if the anomalous row should be discarded."""
        return self.action == "DISCARDED"
