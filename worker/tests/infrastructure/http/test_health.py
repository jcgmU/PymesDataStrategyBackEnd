"""Tests for health check endpoints."""

from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.infrastructure.http.routes.health import router, HealthCheck


@pytest.fixture
def app() -> FastAPI:
    """Create a test FastAPI application with health routes."""
    app = FastAPI()
    app.include_router(router)
    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    """Create a test client."""
    return TestClient(app)


class TestLivenessEndpoint:
    """Tests for /health/live endpoint."""

    def test_liveness_returns_alive(self, client: TestClient) -> None:
        """Test that liveness endpoint returns alive status."""
        response = client.get("/health/live")

        assert response.status_code == 200
        assert response.json() == {"status": "alive"}

    def test_liveness_is_always_available(self, client: TestClient) -> None:
        """Test that liveness doesn't depend on external services."""
        # Even without mocking container, this should work
        response = client.get("/health/live")

        assert response.status_code == 200


class TestReadinessEndpoint:
    """Tests for /health/ready endpoint."""

    def test_readiness_returns_ready_when_healthy(self, client: TestClient) -> None:
        """Test readiness returns ready when all checks pass."""
        mock_container = AsyncMock()
        mock_container.health_check.return_value = {
            "database": True,
            "redis": True,
            "storage": True,
        }

        with patch(
            "src.infrastructure.http.routes.health.get_container",
            return_value=mock_container,
        ), patch(
            "src.infrastructure.http.routes.health._get_worker_status",
            return_value=True,
        ):
            response = client.get("/health/ready")

        assert response.status_code == 200
        assert response.json() == {"status": "ready"}

    def test_readiness_returns_not_ready_when_unhealthy(
        self, client: TestClient
    ) -> None:
        """Test readiness returns not_ready when checks fail."""
        mock_container = AsyncMock()
        mock_container.health_check.return_value = {
            "database": False,
            "redis": True,
            "storage": True,
        }

        with patch(
            "src.infrastructure.http.routes.health.get_container",
            return_value=mock_container,
        ), patch(
            "src.infrastructure.http.routes.health._get_worker_status",
            return_value=True,
        ):
            response = client.get("/health/ready")

        assert response.status_code == 200
        assert response.json() == {"status": "not_ready"}

    def test_readiness_returns_not_ready_when_worker_not_running(
        self, client: TestClient
    ) -> None:
        """Test readiness returns not_ready when worker is not running."""
        mock_container = AsyncMock()
        mock_container.health_check.return_value = {
            "database": True,
            "redis": True,
            "storage": True,
        }

        with patch(
            "src.infrastructure.http.routes.health.get_container",
            return_value=mock_container,
        ), patch(
            "src.infrastructure.http.routes.health._get_worker_status",
            return_value=False,
        ):
            response = client.get("/health/ready")

        assert response.status_code == 200
        assert response.json() == {"status": "not_ready"}


class TestHealthCheckEndpoint:
    """Tests for /health endpoint."""

    def test_health_returns_ok_when_all_healthy(self, client: TestClient) -> None:
        """Test health returns ok status when all checks pass."""
        mock_container = AsyncMock()
        mock_container.health_check.return_value = {
            "database": True,
            "redis": True,
            "storage": True,
        }

        with patch(
            "src.infrastructure.http.routes.health.get_container",
            return_value=mock_container,
        ), patch(
            "src.infrastructure.http.routes.health._get_worker_status",
            return_value=True,
        ):
            response = client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["checks"]["database"] is True
        assert data["checks"]["redis"] is True
        assert data["checks"]["storage"] is True
        assert data["checks"]["worker"] is True

    def test_health_returns_degraded_when_partial(self, client: TestClient) -> None:
        """Test health returns degraded when some checks fail."""
        mock_container = AsyncMock()
        mock_container.health_check.return_value = {
            "database": True,
            "redis": False,
            "storage": True,
        }

        with patch(
            "src.infrastructure.http.routes.health.get_container",
            return_value=mock_container,
        ), patch(
            "src.infrastructure.http.routes.health._get_worker_status",
            return_value=True,
        ):
            response = client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "degraded"

    def test_health_returns_degraded_when_worker_not_running(
        self, client: TestClient
    ) -> None:
        """Test health returns degraded when worker is not running."""
        mock_container = AsyncMock()
        mock_container.health_check.return_value = {
            "database": True,
            "redis": True,
            "storage": True,
        }

        with patch(
            "src.infrastructure.http.routes.health.get_container",
            return_value=mock_container,
        ), patch(
            "src.infrastructure.http.routes.health._get_worker_status",
            return_value=False,
        ):
            response = client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "degraded"
        assert data["checks"]["worker"] is False

    def test_health_returns_unhealthy_when_all_fail(self, client: TestClient) -> None:
        """Test health returns unhealthy when all checks fail."""
        mock_container = AsyncMock()
        mock_container.health_check.return_value = {
            "database": False,
            "redis": False,
            "storage": False,
        }

        with patch(
            "src.infrastructure.http.routes.health.get_container",
            return_value=mock_container,
        ), patch(
            "src.infrastructure.http.routes.health._get_worker_status",
            return_value=False,
        ):
            response = client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "unhealthy"

    def test_health_includes_timestamp(self, client: TestClient) -> None:
        """Test health response includes timestamp."""
        mock_container = AsyncMock()
        mock_container.health_check.return_value = {"database": True}

        with patch(
            "src.infrastructure.http.routes.health.get_container",
            return_value=mock_container,
        ), patch(
            "src.infrastructure.http.routes.health._get_worker_status",
            return_value=True,
        ):
            response = client.get("/health")

        data = response.json()
        assert "timestamp" in data
        # Verify it's a valid ISO timestamp
        timestamp = datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
        assert isinstance(timestamp, datetime)


class TestHealthCheckModel:
    """Tests for HealthCheck Pydantic model."""

    def test_model_with_ok_status(self) -> None:
        """Test creating HealthCheck with ok status."""
        health = HealthCheck(
            status="ok",
            timestamp=datetime.now(timezone.utc),
            checks={"database": True, "redis": True},
        )

        assert health.status == "ok"
        assert health.checks["database"] is True

    def test_model_with_degraded_status(self) -> None:
        """Test creating HealthCheck with degraded status."""
        health = HealthCheck(
            status="degraded",
            timestamp=datetime.now(timezone.utc),
            checks={"database": True, "redis": False},
        )

        assert health.status == "degraded"

    def test_model_with_unhealthy_status(self) -> None:
        """Test creating HealthCheck with unhealthy status."""
        health = HealthCheck(
            status="unhealthy",
            timestamp=datetime.now(timezone.utc),
            checks={"database": False, "redis": False},
        )

        assert health.status == "unhealthy"

    def test_model_serialization(self) -> None:
        """Test HealthCheck JSON serialization."""
        now = datetime.now(timezone.utc)
        health = HealthCheck(
            status="ok",
            timestamp=now,
            checks={"database": True},
        )

        data = health.model_dump()
        assert data["status"] == "ok"
        assert data["timestamp"] == now
        assert data["checks"] == {"database": True}
