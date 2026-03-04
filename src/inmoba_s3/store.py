from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
import structlog
from botocore.exceptions import ClientError

logger = structlog.get_logger(__name__)


class S3Store:
    """Generic, domain-agnostic S3 storage client."""

    def __init__(
        self,
        bucket: str,
        region: Optional[str] = None,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        session_token: Optional[str] = None,
        profile_name: Optional[str] = None,
    ) -> None:
        self._bucket = bucket
        self._region = region
        kwargs: dict = {}
        if region:
            kwargs["region_name"] = region
        if access_key_id and secret_access_key:
            kwargs["aws_access_key_id"] = access_key_id
            kwargs["aws_secret_access_key"] = secret_access_key
            if session_token:
                kwargs["aws_session_token"] = session_token
        if profile_name:
            session = boto3.Session(profile_name=profile_name)
            self._client = session.client("s3", **kwargs)
        else:
            self._client = boto3.client("s3", **kwargs)

    def upload(
        self, data: bytes, key: str, content_type: str = "application/octet-stream"
    ) -> str:
        """Upload bytes to S3. Returns s3://{bucket}/{key}."""
        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )
        uri = f"s3://{self._bucket}/{key}"
        logger.info("s3.upload", key=key, size=len(data))
        return uri

    def download(self, key: str) -> Optional[bytes]:
        """Download object bytes. Returns None if key does not exist (404)."""
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
            return response["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise

    def exists(self, key: str) -> bool:
        """Return True if key exists in bucket."""
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404", "403"):
                return False
            raise

    def object_last_modified(self, key: str) -> datetime | None:
        try:
            response = self._client.head_object(Bucket=self._bucket, Key=key)
            value = response.get("LastModified")
            if not isinstance(value, datetime):
                return None
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404", "403"):
                return None
            raise

    def exists_within_ttl(self, key: str, ttl_seconds: int) -> bool:
        last_modified = self.object_last_modified(key)
        if last_modified is None:
            return False
        ttl = timedelta(seconds=ttl_seconds)
        return datetime.now(timezone.utc) - last_modified <= ttl

    def presigned_url(
        self,
        key: str,
        expiry_seconds: int = 604800,
        response_content_type: Optional[str] = None,
    ) -> str:
        """Generate a presigned GET URL for the given key."""
        params: dict[str, str | int] = {"Bucket": self._bucket, "Key": key}
        if response_content_type:
            params["ResponseContentType"] = response_content_type
        return self._client.generate_presigned_url(
            "get_object",
            Params=params,
            ExpiresIn=expiry_seconds,
        )

    def presigned_put_url(
        self,
        key: str,
        expiry_seconds: int = 86400,
        content_type: str | None = None,
    ) -> str:
        """Generate a presigned PUT URL for uploading an object."""
        params: dict = {"Bucket": self._bucket, "Key": key}
        if content_type is not None:
            params["ContentType"] = content_type
        return self._client.generate_presigned_url(
            "put_object",
            Params=params,
            ExpiresIn=expiry_seconds,
        )

    def list_keys(self, prefix: Optional[str] = None) -> list[str]:
        """List all keys under optional prefix. Handles pagination."""
        keys: list[str] = []
        kwargs: dict[str, str] = {"Bucket": self._bucket}
        if prefix:
            kwargs["Prefix"] = prefix
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(**kwargs):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys
