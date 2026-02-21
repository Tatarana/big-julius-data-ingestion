"""S3 service for listing and downloading CSV files."""

import logging
from typing import List, Protocol, Tuple

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)


class S3ClientProtocol(Protocol):
    """Protocol for an injectable S3 client (for testability)."""

    def list_objects_v2(self, **kwargs) -> dict: ...

    def get_object(self, **kwargs) -> dict: ...


class S3ServiceError(Exception):
    """Raised when an unrecoverable S3 error occurs."""


class S3Service:
    """Provides methods to interact with AWS S3 for CSV file retrieval.

    Attributes:
        _client: The underlying boto3 S3 client.
        _bucket: The target S3 bucket name.
        _prefix: The folder prefix to list files under.
    """

    def __init__(self, client: S3ClientProtocol, bucket: str, prefix: str) -> None:
        """Initialize the S3Service.

        Args:
            client: An injectable boto3 S3 client (or mock).
            bucket: Name of the S3 bucket.
            prefix: Folder path within the bucket.
        """
        self._client = client
        self._bucket = bucket
        self._prefix = prefix

    def list_csv_files(self) -> List[str]:
        """List all .csv file keys under the configured bucket and prefix.

        Returns:
            A list of S3 object keys ending with '.csv'.

        Raises:
            S3ServiceError: If the bucket or prefix cannot be accessed.
        """
        try:
            paginator = self._client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self._bucket, Prefix=self._prefix)
            keys = []
            for page in pages:
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith(".csv"):
                        keys.append(key)
            logger.info("Found %d CSV file(s) in s3://%s/%s.", len(keys), self._bucket, self._prefix)
            return keys
        except ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            logger.error("S3 ClientError when listing objects: %s — %s", error_code, exc)
            raise S3ServiceError(f"Cannot list S3 objects: {error_code}") from exc
        except BotoCoreError as exc:
            logger.error("BotoCoreError when listing objects: %s", exc)
            raise S3ServiceError(f"S3 connection error: {exc}") from exc

    def download_file(self, key: str) -> Tuple[str, bytes]:
        """Download a single S3 object and return its content as bytes.

        Args:
            key: The S3 object key to download.

        Returns:
            A tuple of (filename, raw_bytes).

        Raises:
            S3ServiceError: If the object cannot be downloaded.
        """
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
            content: bytes = response["Body"].read()
            filename = key.split("/")[-1]
            logger.debug("Downloaded '%s' (%d bytes).", key, len(content))
            return filename, content
        except ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            logger.error("S3 ClientError downloading '%s': %s", key, error_code)
            raise S3ServiceError(f"Cannot download '{key}': {error_code}") from exc
        except BotoCoreError as exc:
            logger.error("BotoCoreError downloading '%s': %s", key, exc)
            raise S3ServiceError(f"S3 error downloading '{key}': {exc}") from exc


def build_s3_client(
    aws_access_key_id: str,
    aws_secret_access_key: str,
    aws_region: str,
) -> S3ClientProtocol:
    """Create a production boto3 S3 client.

    Args:
        aws_access_key_id: AWS access key ID.
        aws_secret_access_key: AWS secret access key.
        aws_region: AWS region name.

    Returns:
        A configured boto3 S3 client.
    """
    return boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region,
    )
