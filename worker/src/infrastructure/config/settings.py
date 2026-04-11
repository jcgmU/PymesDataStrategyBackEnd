"""Application settings using pydantic-settings."""

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application
    environment: Literal["development", "production", "test"] = "development"
    port: int = Field(default=8000, ge=1, le=65535)
    log_level: Literal["debug", "info", "warning", "error", "critical"] = "info"

    # Database
    database_url: str = Field(
        default="postgresql://pymes:pymes_dev_password@localhost:5432/pymes_dev"
    )

    # Redis
    redis_host: str = "localhost"
    redis_port: int = Field(default=6379, ge=1, le=65535)

    # MinIO / S3
    minio_endpoint: str = "localhost"
    minio_port: int = Field(default=9000, ge=1, le=65535)
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_use_ssl: bool = False
    minio_bucket: str = "datasets"
    minio_processed_bucket: str = "processed-datasets"
    minio_region: str = "us-east-1"
    minio_bucket_datasets: str = "datasets"
    minio_bucket_results: str = "results"
    minio_bucket_temp: str = "temp"

    # Worker
    worker_concurrency: int = Field(default=2, ge=1, le=10)

    # n8n / AI suggestions (legacy — kept as fallback)
    n8n_suggestions_webhook_url: str = ""
    n8n_webhook_secret: str = "pymes-n8n-secret-2024"

    # Gemini direct integration
    gemini_api_key: str = ""
    gemini_model: str = "gemini-2.0-flash-exp"

    @property
    def redis_url(self) -> str:
        """Build Redis URL from components."""
        return f"redis://{self.redis_host}:{self.redis_port}"

    @property
    def minio_url(self) -> str:
        """Build MinIO URL from components."""
        protocol = "https" if self.minio_use_ssl else "http"
        return f"{protocol}://{self.minio_endpoint}:{self.minio_port}"

    @property
    def is_development(self) -> bool:
        """Check if running in development mode."""
        return self.environment == "development"


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
