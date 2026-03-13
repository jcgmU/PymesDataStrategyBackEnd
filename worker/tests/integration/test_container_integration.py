"""Integration tests for Container health_check with real Redis."""

import redis.asyncio as aioredis
import structlog

from src.infrastructure.config.container import Container
from src.infrastructure.config.settings import Settings
from tests.fixtures.containers import RedisConnectionParams


async def test_health_check_returns_redis_true_with_real_container(
    redis_container: RedisConnectionParams,
) -> None:
    """Test that health_check returns redis: True with real Redis container."""
    # Arrange: Create a real Redis client connected to the test container
    redis_client = aioredis.Redis(
        host=redis_container.host,
        port=redis_container.port,
        decode_responses=True,
    )

    # Create settings (values don't matter since we're using direct redis_client)
    settings = Settings(
        environment="test",
        redis_host=redis_container.host,
        redis_port=redis_container.port,
    )

    # Create container with real Redis client
    container = Container(
        settings=settings,
        redis_client=redis_client,
        logger=structlog.get_logger("test"),
    )

    try:
        # Act: Call health_check
        result = await container.health_check()

        # Assert: Redis should be healthy (storage may fail without MinIO)
        assert result["redis"] is True
        assert "storage" in result  # storage check is also included

    finally:
        # Cleanup: Close the Redis connection
        await redis_client.aclose()


async def test_health_check_returns_redis_false_when_connection_fails() -> None:
    """Test that health_check returns redis: False when Redis is unreachable."""
    # Arrange: Create a Redis client with invalid connection (non-existent port)
    redis_client = aioredis.Redis(
        host="localhost",
        port=59999,  # Invalid port - nothing should be running here
        decode_responses=True,
        socket_connect_timeout=0.1,  # Short timeout to fail fast
    )

    settings = Settings(
        environment="test",
        redis_host="localhost",
        redis_port=59999,
    )

    container = Container(
        settings=settings,
        redis_client=redis_client,
        logger=structlog.get_logger("test"),
    )

    try:
        # Act: Call health_check
        result = await container.health_check()

        # Assert: Redis should be unhealthy
        assert result["redis"] is False
        assert "storage" in result  # storage check is also included

    finally:
        await redis_client.aclose()


async def test_redis_container_basic_operations(
    redis_container: RedisConnectionParams,
) -> None:
    """Test basic Redis operations with the test container."""
    # Arrange
    redis_client = aioredis.Redis(
        host=redis_container.host,
        port=redis_container.port,
        decode_responses=True,
    )

    try:
        # Act & Assert: Basic operations
        await redis_client.set("test_key", "test_value")
        value = await redis_client.get("test_key")
        assert value == "test_value"

        # Cleanup key
        await redis_client.delete("test_key")

    finally:
        await redis_client.aclose()
