"""Dependency injection container."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import redis.asyncio as aioredis
import structlog
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, AsyncSession

from src.application.transformations import DataTransformer
from src.application.use_cases.process_dataset import ProcessDatasetUseCase
from src.infrastructure.config.settings import Settings, get_settings
from src.infrastructure.parsers.dataset_parser import DatasetParser
from src.infrastructure.persistence.database import create_engine, create_session_factory
from src.infrastructure.persistence.sqlalchemy_job_repository import SQLAlchemyJobRepository
from src.infrastructure.storage.minio_storage_service import MinioStorageService

if TYPE_CHECKING:
    from redis.asyncio.client import Redis


@dataclass
class Container:
    """Simple dependency injection container."""

    settings: Settings
    redis_client: "Redis[Any]"
    logger: structlog.stdlib.BoundLogger

    # Services (lazy initialized)
    _storage: MinioStorageService | None = field(default=None, repr=False)
    _parser: DatasetParser | None = field(default=None, repr=False)
    _transformer: DataTransformer | None = field(default=None, repr=False)
    _process_dataset_use_case: ProcessDatasetUseCase | None = field(default=None, repr=False)
    _db_engine: AsyncEngine | None = field(default=None, repr=False)
    _session_factory: async_sessionmaker[AsyncSession] | None = field(default=None, repr=False)
    _job_repository: SQLAlchemyJobRepository | None = field(default=None, repr=False)

    @property
    def storage(self) -> MinioStorageService:
        """Get storage service (lazy initialization)."""
        if self._storage is None:
            self._storage = MinioStorageService(settings=self.settings)
        return self._storage

    @property
    def parser(self) -> DatasetParser:
        """Get dataset parser (lazy initialization)."""
        if self._parser is None:
            self._parser = DatasetParser()
        return self._parser

    @property
    def transformer(self) -> DataTransformer:
        """Get data transformer (lazy initialization)."""
        if self._transformer is None:
            self._transformer = DataTransformer()
        return self._transformer

    @property
    def db_engine(self) -> AsyncEngine:
        """Get the async database engine (lazy initialization)."""
        if self._db_engine is None:
            self._db_engine = create_engine(self.settings.database_url)
        return self._db_engine

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        """Get the async session factory (lazy initialization)."""
        if self._session_factory is None:
            self._session_factory = create_session_factory(self.db_engine)
        return self._session_factory

    @property
    def job_repository(self) -> SQLAlchemyJobRepository:
        """Get the job repository (lazy initialization)."""
        if self._job_repository is None:
            self._job_repository = SQLAlchemyJobRepository(self.session_factory)
        return self._job_repository

    @property
    def process_dataset_use_case(self) -> ProcessDatasetUseCase:
        """Get ProcessDataset use case (lazy initialization)."""
        if self._process_dataset_use_case is None:
            self._process_dataset_use_case = ProcessDatasetUseCase(
                storage=self.storage,
                parser=self.parser,
                transformer=self.transformer,
                output_bucket=self.settings.minio_processed_bucket,
                job_repository=self.job_repository,
            )
        return self._process_dataset_use_case

    async def health_check(self) -> dict[str, bool]:
        """Check health of all dependencies."""
        checks: dict[str, bool] = {}

        # Redis health
        try:
            await self.redis_client.ping()
            checks["redis"] = True
        except Exception:
            checks["redis"] = False

        # MinIO/S3 health
        try:
            await self.storage.health_check()
            checks["storage"] = True
        except Exception:
            checks["storage"] = False

        return checks

    async def close(self) -> None:
        """Close all connections."""
        try:
            await self.redis_client.aclose()
        except AttributeError:
            # Fallback for older redis versions
            await self.redis_client.close()
        # Close the DB engine if it was initialized
        if self._db_engine is not None:
            await self._db_engine.dispose()
            self._db_engine = None


_container: Container | None = None


async def init_container() -> Container:
    """Initialize the dependency container."""
    global _container

    if _container is not None:
        return _container

    settings = get_settings()

    # Configure structured logging
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logger: structlog.stdlib.BoundLogger = structlog.get_logger("pymes.worker")

    # Initialize Redis client
    redis_client = aioredis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        decode_responses=True,
    )

    _container = Container(
        settings=settings,
        redis_client=redis_client,
        logger=logger,
    )

    return _container


def get_container() -> Container:
    """Get the current container instance."""
    if _container is None:
        raise RuntimeError("Container not initialized. Call init_container() first.")
    return _container


async def close_container() -> None:
    """Close the container and all connections."""
    global _container
    if _container is not None:
        await _container.close()
        _container = None
