"""PYMES Worker ETL - Entry Point."""

import uvicorn

from src.infrastructure.config import get_settings
from src.infrastructure.http.app import create_app

# Create the FastAPI application
app = create_app()

if __name__ == "__main__":
    settings = get_settings()

    print(f"PYMES Worker ETL starting...")
    print(f"Environment: {settings.environment}")
    print(f"Log level: {settings.log_level}")
    print(f"Health check: http://localhost:{settings.port}/health")
    print(f"API docs: http://localhost:{settings.port}/docs")

    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=settings.port,
        reload=settings.is_development,
        log_level=settings.log_level,
    )
