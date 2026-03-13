"""Worker state module to avoid circular imports."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.infrastructure.messaging.bullmq_worker import BullMQWorkerService


# Global worker reference
_worker: "BullMQWorkerService | None" = None


def set_worker(worker: "BullMQWorkerService | None") -> None:
    """Set the global worker instance."""
    global _worker
    _worker = worker


def get_worker() -> "BullMQWorkerService | None":
    """Get the global worker instance."""
    return _worker


def is_worker_running() -> bool:
    """Check if the worker is currently running."""
    return _worker is not None and _worker.is_running
