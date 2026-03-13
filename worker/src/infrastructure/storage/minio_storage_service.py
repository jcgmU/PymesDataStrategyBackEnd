"""MinIO/S3 storage service implementation using boto3."""

from io import BytesIO
from typing import BinaryIO

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from src.domain.ports.services.storage_service import StorageService
from src.infrastructure.config.settings import Settings


class MinioStorageService(StorageService):
    """MinIO/S3 storage implementation using boto3."""

    def __init__(self, settings: Settings) -> None:
        """Initialize MinIO client.
        
        Args:
            settings: Application settings with MinIO configuration.
        """
        self._settings = settings
        
        # Build endpoint URL
        protocol = "https" if settings.minio_use_ssl else "http"
        endpoint_url = f"{protocol}://{settings.minio_endpoint}:{settings.minio_port}"
        
        # Create boto3 S3 client with MinIO config
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=settings.minio_access_key or "minioadmin",
            aws_secret_access_key=settings.minio_secret_key or "minioadmin123",
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},  # Required for MinIO
            ),
        )
        
        # Bucket names
        self._bucket_datasets = settings.minio_bucket_datasets
        self._bucket_results = settings.minio_bucket_results
        self._bucket_temp = settings.minio_bucket_temp

    @property
    def datasets_bucket(self) -> str:
        """Get datasets bucket name."""
        return self._bucket_datasets

    @property
    def results_bucket(self) -> str:
        """Get results bucket name."""
        return self._bucket_results

    @property
    def temp_bucket(self) -> str:
        """Get temp bucket name."""
        return self._bucket_temp

    async def upload_file(
        self,
        bucket: str,
        key: str,
        data: BinaryIO,
        content_type: str | None = None,
    ) -> str:
        """Upload a file to storage.
        
        Args:
            bucket: Target bucket name.
            key: Object key (path) in the bucket.
            data: File data as binary stream.
            content_type: Optional MIME type.
            
        Returns:
            The storage path (s3://bucket/key).
        """
        extra_args = {}
        if content_type:
            extra_args["ContentType"] = content_type
        
        # Read data into bytes if needed
        if hasattr(data, "read"):
            content = data.read()
            if isinstance(content, str):
                content = content.encode("utf-8")
        else:
            content = data  # type: ignore
        
        self._client.put_object(
            Bucket=bucket,
            Key=key,
            Body=content,
            **extra_args,
        )
        
        return f"s3://{bucket}/{key}"

    async def download_file(self, bucket: str, key: str) -> bytes:
        """Download a file from storage.
        
        Args:
            bucket: Source bucket name.
            key: Object key to download.
            
        Returns:
            File contents as bytes.
            
        Raises:
            FileNotFoundError: If the object doesn't exist.
        """
        try:
            response = self._client.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(f"Object not found: {bucket}/{key}") from e
            raise

    async def download_file_stream(self, bucket: str, key: str) -> BytesIO:
        """Download a file as a stream (useful for large files).
        
        Args:
            bucket: Source bucket name.
            key: Object key to download.
            
        Returns:
            File contents as BytesIO stream.
        """
        content = await self.download_file(bucket, key)
        return BytesIO(content)

    async def delete_file(self, bucket: str, key: str) -> None:
        """Delete a file from storage.
        
        Args:
            bucket: Bucket containing the object.
            key: Object key to delete.
        """
        self._client.delete_object(Bucket=bucket, Key=key)

    async def file_exists(self, bucket: str, key: str) -> bool:
        """Check if a file exists in storage.
        
        Args:
            bucket: Bucket to check.
            key: Object key to check.
            
        Returns:
            True if the object exists, False otherwise.
        """
        try:
            self._client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    async def get_presigned_url(
        self,
        bucket: str,
        key: str,
        expires_in: int = 3600,
    ) -> str:
        """Generate a presigned URL for file access.
        
        Args:
            bucket: Bucket containing the object.
            key: Object key.
            expires_in: URL expiration time in seconds.
            
        Returns:
            Presigned URL string.
        """
        return self._client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires_in,
        )

    async def get_object_metadata(
        self,
        bucket: str,
        key: str,
    ) -> dict[str, str]:
        """Get object metadata.
        
        Args:
            bucket: Bucket containing the object.
            key: Object key.
            
        Returns:
            Dictionary of metadata.
            
        Raises:
            FileNotFoundError: If the object doesn't exist.
        """
        try:
            response = self._client.head_object(Bucket=bucket, Key=key)
            return {
                "content_type": response.get("ContentType", ""),
                "content_length": str(response.get("ContentLength", 0)),
                "last_modified": str(response.get("LastModified", "")),
                "etag": response.get("ETag", "").strip('"'),
            }
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                raise FileNotFoundError(f"Object not found: {bucket}/{key}") from e
            raise

    async def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        max_keys: int = 1000,
    ) -> list[dict[str, str]]:
        """List objects in a bucket with optional prefix filter.
        
        Args:
            bucket: Bucket to list.
            prefix: Filter objects by key prefix.
            max_keys: Maximum number of keys to return.
            
        Returns:
            List of object info dictionaries.
        """
        response = self._client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=max_keys,
        )
        
        objects = []
        for obj in response.get("Contents", []):
            objects.append({
                "key": obj["Key"],
                "size": str(obj["Size"]),
                "last_modified": str(obj["LastModified"]),
                "etag": obj["ETag"].strip('"'),
            })
        
        return objects

    async def ensure_bucket_exists(self, bucket: str) -> None:
        """Ensure a bucket exists, creating it if necessary.
        
        Args:
            bucket: Bucket name to ensure exists.
        """
        try:
            self._client.head_bucket(Bucket=bucket)
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                self._client.create_bucket(Bucket=bucket)
            else:
                raise

    async def health_check(self) -> bool:
        """Check if MinIO is accessible.
        
        Returns:
            True if MinIO is reachable, False otherwise.
        """
        try:
            self._client.list_buckets()
            return True
        except Exception:
            return False
