"""Health check endpoint."""

from datetime import UTC, datetime
from typing import Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from src.infrastructure.config.container import Container, get_container

router = APIRouter(tags=["health"])


class HealthCheck(BaseModel):
    """Health check response model."""

    status: Literal["ok", "degraded", "unhealthy"]
    timestamp: datetime
    checks: dict[str, bool]


def get_deps() -> Container:
    """Dependency injection for container."""
    return get_container()


def _get_worker_status() -> bool:
    """Get worker running status without circular import."""
    # Import here to avoid circular dependency
    from src.infrastructure.http import worker_state
    return worker_state.is_worker_running()


@router.get("/health", response_model=HealthCheck)
async def health_check(container: Container = Depends(get_deps)) -> HealthCheck:
    """Check the health of the worker service.

    Returns:
        HealthCheck: Status of all service dependencies.
    """
    checks = await container.health_check()

    # Add worker status check
    checks["worker"] = _get_worker_status()

    # Determine overall status
    all_healthy = all(checks.values())
    any_healthy = any(checks.values())

    if all_healthy:
        status: Literal["ok", "degraded", "unhealthy"] = "ok"
    elif any_healthy:
        status = "degraded"
    else:
        status = "unhealthy"

    return HealthCheck(
        status=status,
        timestamp=datetime.now(UTC),
        checks=checks,
    )


@router.get("/health/live")
async def liveness() -> dict[str, str]:
    """Kubernetes liveness probe - just checks if the service is running."""
    return {"status": "alive"}


@router.get("/health/ready")
async def readiness(container: Container = Depends(get_deps)) -> dict[str, str]:
    """Kubernetes readiness probe - checks if service can handle traffic."""
    checks = await container.health_check()

    # Also check worker is running
    if not _get_worker_status():
        return {"status": "not_ready"}

    if all(checks.values()):
        return {"status": "ready"}
    return {"status": "not_ready"}
