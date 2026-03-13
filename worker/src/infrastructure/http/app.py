"""FastAPI application setup."""

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.application.processors.etl_processor import ETLJobProcessor
from src.infrastructure.config import close_container, get_settings, init_container
from src.infrastructure.http.routes import health_router
from src.infrastructure.http import worker_state
from src.infrastructure.messaging.bullmq_worker import BullMQWorkerService


# Global task reference
_worker_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan - startup and shutdown events."""
    global _worker_task
    
    # Startup
    container = await init_container()
    container.logger.info(
        "Worker ETL starting",
        environment=container.settings.environment,
        port=container.settings.port,
    )
    
    # Initialize and start BullMQ worker
    worker = BullMQWorkerService(
        redis_host=container.settings.redis_host,
        redis_port=container.settings.redis_port,
        queue_name="etl-transformations",
        concurrency=container.settings.worker_concurrency,
    )
    
    # Create ETL processor with the use case from container
    etl_processor = ETLJobProcessor(
        process_dataset=container.process_dataset_use_case
    )
    worker.set_processor(etl_processor)
    
    # Store worker in state module
    worker_state.set_worker(worker)
    
    # Start worker in background task
    _worker_task = asyncio.create_task(worker.run_forever())
    container.logger.info("BullMQ worker started")
    
    yield
    
    # Shutdown
    container.logger.info("Worker ETL shutting down")
    
    # Stop BullMQ worker
    current_worker = worker_state.get_worker()
    if current_worker:
        await current_worker.stop()
        worker_state.set_worker(None)
    if _worker_task:
        _worker_task.cancel()
        try:
            await _worker_task
        except asyncio.CancelledError:
            pass
    
    await close_container()


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()

    app = FastAPI(
        title="PYMES Worker ETL",
        description="ETL Worker Service for PYMES Data Strategy",
        version="0.1.0",
        lifespan=lifespan,
        docs_url="/docs" if settings.is_development else None,
        redoc_url="/redoc" if settings.is_development else None,
    )

    # CORS middleware
    if settings.is_development:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    # Include routers
    app.include_router(health_router)

    return app
