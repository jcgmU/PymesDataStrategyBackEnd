"""Pytest configuration and fixtures for Worker tests."""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import pytest


# ============================================================================
# UUID Fixtures
# ============================================================================


@pytest.fixture
def sample_uuid() -> UUID:
    """Provide a consistent UUID for tests."""
    return UUID("12345678-1234-5678-1234-567812345678")


@pytest.fixture
def owner_uuid() -> UUID:
    """Provide a consistent owner UUID for tests."""
    return UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


@pytest.fixture
def dataset_uuid() -> UUID:
    """Provide a consistent dataset UUID for tests."""
    return UUID("dddddddd-eeee-ffff-0000-111111111111")


@pytest.fixture
def random_uuid() -> UUID:
    """Provide a random UUID for tests."""
    return uuid4()


# ============================================================================
# Timestamp Fixtures
# ============================================================================


@pytest.fixture
def fixed_datetime() -> datetime:
    """Provide a fixed datetime for deterministic tests."""
    return datetime(2024, 1, 15, 10, 30, 0)


# ============================================================================
# Sample Data Fixtures
# ============================================================================


@pytest.fixture
def sample_schema_info() -> dict[str, Any]:
    """Provide sample schema info for dataset tests."""
    return {
        "columns": [
            {"name": "id", "type": "int64"},
            {"name": "name", "type": "string"},
            {"name": "email", "type": "string"},
            {"name": "created_at", "type": "datetime64"},
        ],
        "primary_key": "id",
        "nullable_columns": ["email"],
    }


@pytest.fixture
def sample_job_config() -> dict[str, Any]:
    """Provide sample job configuration for transformation tests."""
    return {
        "columns": ["name", "email"],
        "operations": [
            {"type": "trim_whitespace"},
            {"type": "lowercase", "columns": ["email"]},
        ],
    }


@pytest.fixture
def sample_job_result() -> dict[str, Any]:
    """Provide sample job result for transformation tests."""
    return {
        "rows_processed": 1000,
        "rows_modified": 150,
        "errors": [],
        "duration_ms": 2500,
    }
