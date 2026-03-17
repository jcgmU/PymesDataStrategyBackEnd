"""Storage service port - interface for object storage (S3/MinIO)."""

from abc import ABC, abstractmethod
from typing import BinaryIO


class StorageService(ABC):
    """Port for object storage operations."""

    @abstractmethod
    async def upload_file(
        self,
        bucket: str,
        key: str,
        data: BinaryIO,
        content_type: str | None = None,
    ) -> str:
        """Upload a file to storage.

        Returns the storage URL/path.
        """
        ...

    @abstractmethod
    async def download_file(self, bucket: str, key: str) -> bytes:
        """Download a file from storage."""
        ...

    @abstractmethod
    async def delete_file(self, bucket: str, key: str) -> None:
        """Delete a file from storage."""
        ...

    @abstractmethod
    async def file_exists(self, bucket: str, key: str) -> bool:
        """Check if a file exists in storage."""
        ...

    @abstractmethod
    async def get_presigned_url(
        self,
        bucket: str,
        key: str,
        expires_in: int = 3600,
    ) -> str:
        """Generate a presigned URL for file access."""
        ...
