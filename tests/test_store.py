"""Tests for inmoba_s3.store.S3Store."""

from __future__ import annotations

from unittest.mock import MagicMock

from botocore.exceptions import ClientError

from inmoba_s3.store import S3Store


class TestS3StoreUpload:
    """Tests for S3Store.upload."""

    def test_upload_returns_s3_uri(self, mock_boto3_client: MagicMock):
        """upload() must return s3://{bucket}/{key}."""
        store = S3Store("my-bucket")
        uri = store.upload(b"hello", "some/key.txt", content_type="text/plain")
        assert uri == "s3://my-bucket/some/key.txt"

    def test_upload_calls_put_object(self, mock_boto3_client: MagicMock):
        """upload() must call put_object with correct params."""
        store = S3Store("my-bucket")
        store.upload(b"data", "the-key", content_type="application/pdf")
        mock_boto3_client.put_object.assert_called_once_with(
            Bucket="my-bucket",
            Key="the-key",
            Body=b"data",
            ContentType="application/pdf",
        )


class TestS3StoreDownload:
    """Tests for S3Store.download."""

    def test_download_returns_bytes(self, mock_boto3_client: MagicMock):
        """download() must return body bytes on success."""
        body = MagicMock()
        body.read.return_value = b"file-contents"
        mock_boto3_client.get_object.return_value = {"Body": body}

        store = S3Store("my-bucket")
        result = store.download("some/key.txt")
        assert result == b"file-contents"

    def test_download_returns_none_on_404(self, mock_boto3_client: MagicMock):
        """download() must return None when key does not exist."""
        error = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}},
            "GetObject",
        )
        mock_boto3_client.get_object.side_effect = error

        store = S3Store("my-bucket")
        result = store.download("missing/key.txt")
        assert result is None


class TestS3StoreExists:
    """Tests for S3Store.exists."""

    def test_exists_returns_true(self, mock_boto3_client: MagicMock):
        """exists() must return True when head_object succeeds."""
        mock_boto3_client.head_object.return_value = {}
        store = S3Store("my-bucket")
        assert store.exists("some/key.txt") is True

    def test_exists_returns_false_on_404(self, mock_boto3_client: MagicMock):
        """exists() must return False when key is missing."""
        error = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}},
            "HeadObject",
        )
        mock_boto3_client.head_object.side_effect = error

        store = S3Store("my-bucket")
        assert store.exists("missing.txt") is False


class TestS3StorePresignedUrl:
    """Tests for S3Store.presigned_url."""

    def test_presigned_url_calls_generate_presigned_url(
        self, mock_boto3_client: MagicMock
    ):
        """presigned_url() must call generate_presigned_url with correct params."""
        mock_boto3_client.generate_presigned_url.return_value = (
            "https://s3.example.com/signed"
        )

        store = S3Store("my-bucket")
        url = store.presigned_url(
            "some/key.pdf", expiry_seconds=3600, response_content_type="application/pdf"
        )

        mock_boto3_client.generate_presigned_url.assert_called_once_with(
            "get_object",
            Params={
                "Bucket": "my-bucket",
                "Key": "some/key.pdf",
                "ResponseContentType": "application/pdf",
            },
            ExpiresIn=3600,
        )
        assert url == "https://s3.example.com/signed"


class TestS3StoreListKeys:
    """Tests for S3Store.list_keys."""

    def test_list_keys_paginates(self, mock_boto3_client: MagicMock):
        """list_keys() must handle paginated results and return all keys."""
        mock_paginator = MagicMock()
        mock_boto3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"Contents": [{"Key": "a/1.txt"}, {"Key": "a/2.txt"}]},
            {"Contents": [{"Key": "a/3.txt"}]},
        ]

        store = S3Store("my-bucket")
        keys = store.list_keys(prefix="a/")

        assert keys == ["a/1.txt", "a/2.txt", "a/3.txt"]
        mock_boto3_client.get_paginator.assert_called_once_with("list_objects_v2")
        mock_paginator.paginate.assert_called_once_with(Bucket="my-bucket", Prefix="a/")

    def test_list_keys_empty_bucket(self, mock_boto3_client: MagicMock):
        """list_keys() must return empty list when no keys exist."""
        mock_paginator = MagicMock()
        mock_boto3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{}]  # No "Contents"

        store = S3Store("my-bucket")
        keys = store.list_keys()
        assert keys == []
