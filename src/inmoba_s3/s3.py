"""Standalone S3 client for file operations against AWS S3."""

from __future__ import annotations

import io
import json
import os
from typing import Any

import boto3
import structlog
from botocore.config import Config
from botocore.exceptions import ClientError

logger = structlog.get_logger()


class S3Client:
    """Client for reading and writing files to AWS S3."""

    def __init__(
        self,
        bucket: str | None = None,
        prefix: str = "",
        region: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
    ) -> None:
        """Initialize S3 client.

        Args:
            bucket: S3 bucket name (required, or set AWS_S3_BUCKET env var)
            prefix: Key prefix for uploaded objects (default: empty string)
            region: AWS region (or set AWS_REGION / AWS_DEFAULT_REGION env var)
            access_key_id: AWS access key (or set AWS_ACCESS_KEY_ID env var)
            secret_access_key: AWS secret key (or set AWS_SECRET_ACCESS_KEY env var)
        """
        self.bucket = bucket or os.getenv("AWS_S3_BUCKET")
        if not self.bucket:
            raise ValueError(
                "S3 bucket is required. Provide bucket parameter or set AWS_S3_BUCKET env var."
            )

        self.prefix = prefix
        self.region = (
            region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
        )

        client_kwargs: dict[str, Any] = {
            "config": Config(signature_version="s3v4"),
        }
        if self.region:
            client_kwargs["region_name"] = self.region

        ak = access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
        sk = secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        if ak and sk:
            client_kwargs["aws_access_key_id"] = ak
            client_kwargs["aws_secret_access_key"] = sk

        self._client = boto3.client("s3", **client_kwargs)
        self._key_cache: set[str] | None = None
        logger.info(
            "s3_client_initialized",
            bucket=self.bucket,
            prefix=self.prefix,
            region=self.region,
        )

    def _get_key(self, partida: str, filename: str) -> str:
        """Build S3 object key."""
        return f"{partida}/{filename}"

    def preload_key_cache(self, prefix: str | None = None) -> int:
        """Load keys into an in-memory cache for faster existence checks."""
        list_prefix = None
        if prefix is not None:
            list_prefix = prefix.strip("/")
            if list_prefix:
                list_prefix = f"{list_prefix}/"
            else:
                list_prefix = None

        paginator = self._client.get_paginator("list_objects_v2")
        params: dict[str, Any] = {"Bucket": self.bucket}
        if list_prefix:
            params["Prefix"] = list_prefix

        key_cache: set[str] = set()
        for page in paginator.paginate(**params):
            for item in page.get("Contents", []):
                key = item.get("Key")
                if key:
                    key_cache.add(key)

        self._key_cache = key_cache
        logger.info("s3_key_cache_loaded", keys=len(key_cache), prefix=list_prefix)
        return len(key_cache)

    def object_exists(self, partida: str, filename: str) -> bool:
        """Check if an object already exists in S3."""
        key = self._get_key(partida, filename)
        if self._key_cache is not None:
            return key in self._key_cache
        try:
            self._client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def upload_bytes(
        self,
        data: bytes,
        partida: str,
        filename: str,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Upload bytes to S3 and return the S3 URI."""
        key = self._get_key(partida, filename)
        self._client.upload_fileobj(
            io.BytesIO(data),
            self.bucket,
            key,
            ExtraArgs={"ContentType": content_type},
        )
        if self._key_cache is not None:
            self._key_cache.add(key)
        s3_uri = f"s3://{self.bucket}/{key}"
        logger.info("s3_upload_complete", key=key, size=len(data))
        return s3_uri

    def upload_json(
        self,
        data: dict[str, Any],
        partida: str,
        filename: str = "metadata.json",
    ) -> str:
        """Upload JSON data to S3 and return the S3 URI."""
        json_bytes = json.dumps(data, indent=2, ensure_ascii=False, default=str).encode(
            "utf-8"
        )
        return self.upload_bytes(
            json_bytes, partida, filename, content_type="application/json"
        )

    def upload_pdf(self, pdf_bytes: bytes, partida: str) -> str:
        """Upload PDF to S3 and return the S3 URI."""
        filename = f"partida_{partida}.pdf"
        return self.upload_bytes(
            pdf_bytes, partida, filename, content_type="application/pdf"
        )

    def generate_presigned_url(
        self,
        partida: str,
        filename: str,
        expiry_seconds: int = 604800,
        response_content_type: str | None = None,
    ) -> str:
        """Generate a presigned URL for viewing a file in browser.

        Args:
            partida: Partida registral number
            filename: Filename within partida folder
            expiry_seconds: URL validity duration (default: 7 days)
            response_content_type: Optional content type override for response

        Returns:
            Presigned URL string
        """
        key = self._get_key(partida, filename)
        params: dict[str, Any] = {"Bucket": self.bucket, "Key": key}
        if response_content_type:
            params["ResponseContentType"] = response_content_type
        url = self._client.generate_presigned_url(
            ClientMethod="get_object",
            Params=params,
            ExpiresIn=expiry_seconds,
        )
        logger.info("s3_presigned_url_generated", key=key, expiry=expiry_seconds)
        return url

    def download_bytes(self, partida: str, filename: str) -> bytes | None:
        """Download file bytes from S3. Returns None if object doesn't exist.

        Args:
            partida: Partida registral number
            filename: Filename within partida folder

        Returns:
            File bytes or None if not found
        """
        key = self._get_key(partida, filename)
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=key)
            data = response["Body"].read()
            logger.info("s3_download_complete", key=key, size=len(data))
            return data
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ["404", "NoSuchKey"]:
                logger.warning("s3_object_not_found", key=key)
                return None
            raise

    def download_pdf(self, partida: str) -> bytes | None:
        """Download partida PDF from S3. Returns None if not found.

        Args:
            partida: Partida registral number

        Returns:
            PDF bytes or None if not found
        """
        filename = f"partida_{partida}.pdf"
        return self.download_bytes(partida, filename)
