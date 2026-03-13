"""Unit tests for MinioStorageService."""

from io import BytesIO
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from src.infrastructure.config.settings import Settings
from src.infrastructure.storage.minio_storage_service import MinioStorageService


@pytest.fixture
def mock_settings() -> Settings:
    """Create mock settings for tests."""
    return Settings(
        minio_endpoint="localhost",
        minio_port=9000,
        minio_access_key="testkey",
        minio_secret_key="testsecret",
        minio_use_ssl=False,
        minio_bucket_datasets="test-datasets",
        minio_bucket_results="test-results",
        minio_bucket_temp="test-temp",
    )


@pytest.fixture
def mock_s3_client() -> MagicMock:
    """Create a mock S3 client."""
    return MagicMock()


@pytest.fixture
def storage_service(
    mock_settings: Settings,
    mock_s3_client: MagicMock,
) -> MinioStorageService:
    """Create MinioStorageService with mocked client."""
    with patch("src.infrastructure.storage.minio_storage_service.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_s3_client
        service = MinioStorageService(mock_settings)
    # Inject the mock client
    service._client = mock_s3_client
    return service


class TestMinioStorageServiceProperties:
    """Tests for storage service properties."""

    def test_datasets_bucket(self, storage_service: MinioStorageService) -> None:
        """Should return correct datasets bucket name."""
        assert storage_service.datasets_bucket == "test-datasets"

    def test_results_bucket(self, storage_service: MinioStorageService) -> None:
        """Should return correct results bucket name."""
        assert storage_service.results_bucket == "test-results"

    def test_temp_bucket(self, storage_service: MinioStorageService) -> None:
        """Should return correct temp bucket name."""
        assert storage_service.temp_bucket == "test-temp"


class TestUploadFile:
    """Tests for upload_file method."""

    async def test_upload_file_success(
        self,
        storage_service: MinioStorageService,
        mock_s3_client: MagicMock,
    ) -> None:
        """Should upload file and return storage path."""
        data = BytesIO(b"test content")
        
        result = await storage_service.upload_file(
            bucket="test-bucket",
            key="path/to/file.csv",
            data=data,
            content_type="text/csv",
        )
        
        assert result == "s3://test-bucket/path/to/file.csv"
        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args
        assert call_args.kwargs["Bucket"] == "test-bucket"
        assert call_args.kwargs["Key"] == "path/to/file.csv"
        assert call_args.kwargs["ContentType"] == "text/csv"

    async def test_upload_file_without_content_type(
        self,
        storage_service: MinioStorageService,
        mock_s3_client: MagicMock,
    ) -> None:
        """Should upload file without content type."""
        data = BytesIO(b"test content")
        
        await storage_service.upload_file(
            bucket="test-bucket",
            key="file.txt",
            data=data,
        )
        
        call_args = mock_s3_client.put_object.call_args
        assert "ContentType" not in call_args.kwargs


class TestDownloadFile:
    """Tests for download_file method."""

    async def test_download_file_success(
        self,
        storage_service: MinioStorageService,
        mock_s3_client: MagicMock,
    ) -> None:
        """Should download file and return bytes."""
        expected_content = b"file content"
        mock_body = MagicMock()
        mock_body.read.return_value = expected_content
        mock_s3_client.get_object.return_value = {"Body": mock_body}
        
        result = await storage_service.download_file(
            bucket="test-bucket",
            key="path/to/file.csv",
        )
        
        assert result == expected_content
        mock_s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="path/to/file.csv",
        )

    async def test_download_file_not_found(
        self,
        storage_service: MinioStorageService,
        mock_s3_client: MagicMock,
    ) -> None:
        """Should raise FileNotFoundError when object doesn't exist."""
        error_response = {"Error": {"Code": "NoSuchKey"}}
        mock_s3_client.get_object.side_effect = ClientError(
            error_response,
            "GetObject",
        )
        
        with pytest.raises(FileNotFoundError) as exc_info:
            await storage_service.download_file(
                bucket="test-bucket",
                key="nonexistent.csv",
            )
        
        assert "Object not found" in str(exc_info.value)


class TestDownloadFileStream:
    """Tests for download_file_stream method."""

    async def test_download_file_stream_success(
        self,
        storage_service: MinioStorageService,
        mock_s3_client: MagicMock,
    ) -> None:
        """Should download file as BytesIO stream."""
        expected_content = b"stream content"
        mock_body = MagicMock()
        mock_body.read.return_value = expected_content
        mock_s3_client.get_object.return_value = {"Body": mock_body}
        
        result = await storage_service.download_file_stream(
            bucket="test-bucket",
            key="file.csv",
        )
        
        assert isinstance(result, BytesIO)
        assert result.read() == expected_content


class TestDeleteFile:
    """Tests for delete_file method."""

    async def test_delete_file_success(
        self,
        storage_service: MinioStorageService,
        mock_s3_client: MagicMock,
    ) -> None:
        """Should delete file from storage."""
        await storage_service.delete_file(
            bucket="test-bucket",
            key="path/to/file.csv",
        )
        
        mock_s3_client.delete_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="path/to/file.csv",
        )


class TestFileExists:
    """Tests for file_exists method."""

    async def test_file_exists_true(
        self,
        storage_service: MinioStorageService,
        mock_s3_client: MagicMock,
    ) -> None:
        """Should return True when file exists."""
        mock_s3_client.head_object.return_value = {}
        
        result = await storage_service.file_exists(
            bucket="test-bucket",
            key="existing.csv",
        )
        
        assert result is True

    async def test_file_exists_false(
        self,
        storage_service: MinioStorageService,
        mock_s3_client: MagicMock,
    ) -> None:
        """Should return False when file doesn't exist."""
        error_response = {"Error": {"Code": "404"}}
        mock_s3_client.head_object.side_effect = ClientError(
            error_response,
            "HeadObject",
        )
        
        result = await storage_service.file_exists(
            bucket="test-bucket",
            key="nonexistent.csv",
        )
        
        assert result is False


class TestGetPresignedUrl:
    """Tests for get_presigned_url method."""

    async def test_get_presigned_url_success(
        self,
        storage_service: MinioStorageService,
        mock_s3_client: MagicMock,
    ) -> None:
        """Should generate presigned URL."""
        expected_url = "https://minio.example.com/signed-url"
        mock_s3_client.generate_presigned_url.return_value = expected_url
        
        result = await storage_service.get_presigned_url(
            bucket="test-bucket",
            key="file.csv",
            expires_in=7200,
        )
        
        assert result == expected_url
        mock_s3_client.generate_presigned_url.assert_called_once_with(
            "get_object",
            Params={"Bucket": "test-bucket", "Key": "file.csv"},
            ExpiresIn=7200,
        )


class TestGetObjectMetadata:
    """Tests for get_object_metadata method."""

    async def test_get_object_metadata_success(
        self,
        storage_service: MinioStorageService,
        mock_s3_client: MagicMock,
    ) -> None:
        """Should return object metadata."""
        mock_s3_client.head_object.return_value = {
            "ContentType": "text/csv",
            "ContentLength": 1024,
            "LastModified": "2024-01-15T10:00:00Z",
            "ETag": '"abc123"',
        }
        
        result = await storage_service.get_object_metadata(
            bucket="test-bucket",
            key="file.csv",
        )
        
        assert result["content_type"] == "text/csv"
        assert result["content_length"] == "1024"
        assert result["etag"] == "abc123"

    async def test_get_object_metadata_not_found(
        self,
        storage_service: MinioStorageService,
        mock_s3_client: MagicMock,
    ) -> None:
        """Should raise FileNotFoundError when object doesn't exist."""
        error_response = {"Error": {"Code": "404"}}
        mock_s3_client.head_object.side_effect = ClientError(
            error_response,
            "HeadObject",
        )
        
        with pytest.raises(FileNotFoundError):
            await storage_service.get_object_metadata(
                bucket="test-bucket",
                key="nonexistent.csv",
            )


class TestListObjects:
    """Tests for list_objects method."""

    async def test_list_objects_success(
        self,
        storage_service: MinioStorageService,
        mock_s3_client: MagicMock,
    ) -> None:
        """Should list objects in bucket."""
        mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "file1.csv", "Size": 100, "LastModified": "2024-01-15", "ETag": '"a"'},
                {"Key": "file2.csv", "Size": 200, "LastModified": "2024-01-16", "ETag": '"b"'},
            ]
        }
        
        result = await storage_service.list_objects(
            bucket="test-bucket",
            prefix="data/",
        )
        
        assert len(result) == 2
        assert result[0]["key"] == "file1.csv"
        assert result[1]["key"] == "file2.csv"

    async def test_list_objects_empty(
        self,
        storage_service: MinioStorageService,
        mock_s3_client: MagicMock,
    ) -> None:
        """Should return empty list when no objects."""
        mock_s3_client.list_objects_v2.return_value = {}
        
        result = await storage_service.list_objects(
            bucket="test-bucket",
        )
        
        assert result == []


class TestEnsureBucketExists:
    """Tests for ensure_bucket_exists method."""

    async def test_ensure_bucket_exists_already_exists(
        self,
        storage_service: MinioStorageService,
        mock_s3_client: MagicMock,
    ) -> None:
        """Should not create bucket if it exists."""
        mock_s3_client.head_bucket.return_value = {}
        
        await storage_service.ensure_bucket_exists("existing-bucket")
        
        mock_s3_client.create_bucket.assert_not_called()

    async def test_ensure_bucket_exists_creates_bucket(
        self,
        storage_service: MinioStorageService,
        mock_s3_client: MagicMock,
    ) -> None:
        """Should create bucket if it doesn't exist."""
        error_response = {"Error": {"Code": "404"}}
        mock_s3_client.head_bucket.side_effect = ClientError(
            error_response,
            "HeadBucket",
        )
        
        await storage_service.ensure_bucket_exists("new-bucket")
        
        mock_s3_client.create_bucket.assert_called_once_with(Bucket="new-bucket")


class TestHealthCheck:
    """Tests for health_check method."""

    async def test_health_check_success(
        self,
        storage_service: MinioStorageService,
        mock_s3_client: MagicMock,
    ) -> None:
        """Should return True when MinIO is reachable."""
        mock_s3_client.list_buckets.return_value = {"Buckets": []}
        
        result = await storage_service.health_check()
        
        assert result is True

    async def test_health_check_failure(
        self,
        storage_service: MinioStorageService,
        mock_s3_client: MagicMock,
    ) -> None:
        """Should return False when MinIO is not reachable."""
        mock_s3_client.list_buckets.side_effect = Exception("Connection failed")
        
        result = await storage_service.health_check()
        
        assert result is False
