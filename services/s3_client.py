# services/s3_client.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import pandas as pd

from core.config import settings

try:
    import boto3  # will only be used when ENV == "cloud"
except ImportError:
    boto3 = None


class BaseS3Client:
    """
    Abstract base for S3-like storage.
    """

    def upload_file(self, local_path: str | Path, s3_key: str) -> None:
        raise NotImplementedError

    def download_file(self, s3_key: str, local_path: str | Path) -> None:
        raise NotImplementedError

    def read_csv(self, s3_key: str, **read_kwargs) -> pd.DataFrame:
        """
        Convenience: read a CSV file stored under s3_key into a DataFrame.
        """
        raise NotImplementedError

    def write_csv(self, df: pd.DataFrame, s3_key: str, **to_csv_kwargs) -> None:
        """
        Convenience: write a DataFrame to s3_key as CSV.
        """
        raise NotImplementedError


# ---------------------------------------------------------
# Local implementation (mock S3 using a folder)
# ---------------------------------------------------------
class LocalS3Client(BaseS3Client):
    """
    Mock S3 client that uses a local folder as the "bucket".

    This is used when ENV != "cloud".
    """

    def __init__(self, root_dir: str | Path) -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def _resolve_key(self, s3_key: str) -> Path:
        """
        Map a logical s3_key like "raw/results.csv"
        to a real file path under root_dir.
        """
        full_path = self.root_dir / s3_key
        full_path.parent.mkdir(parents=True, exist_ok=True)
        return full_path

    def upload_file(self, local_path: str | Path, s3_key: str) -> None:
        local_path = Path(local_path)
        dest_path = self._resolve_key(s3_key)

        if not local_path.exists():
            raise FileNotFoundError(f"Local file to upload not found: {local_path}")

        dest_path.write_bytes(local_path.read_bytes())

    def download_file(self, s3_key: str, local_path: str | Path) -> None:
        src_path = self._resolve_key(s3_key)
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        if not src_path.exists():
            raise FileNotFoundError(f"Mock S3 key not found: {s3_key} ({src_path})")

        local_path.write_bytes(src_path.read_bytes())

    def read_csv(self, s3_key: str, **read_kwargs) -> pd.DataFrame:
        file_path = self._resolve_key(s3_key)
        if not file_path.exists():
            raise FileNotFoundError(f"Mock S3 CSV not found: {s3_key} ({file_path})")

        return pd.read_csv(file_path, **read_kwargs)

    def write_csv(self, df: pd.DataFrame, s3_key: str, **to_csv_kwargs) -> None:
        file_path = self._resolve_key(s3_key)
        # default: no index when saving
        if "index" not in to_csv_kwargs:
            to_csv_kwargs["index"] = False
        df.to_csv(file_path, **to_csv_kwargs)


# ---------------------------------------------------------
# Real S3 implementation (for later, when ENV == "cloud")
# ---------------------------------------------------------
class RealS3Client(BaseS3Client):
    """
    Thin wrapper around boto3 S3 for when you deploy to AWS.

    For now you don't *need* to use this locally.
    """

    def __init__(self, bucket_name: str, boto3_client: Optional[object] = None) -> None:
        if boto3 is None:
            raise ImportError(
                "boto3 is required for RealS3Client but is not installed."
            )

        self.bucket_name = bucket_name
        self.s3 = boto3_client or boto3.client("s3")

    def upload_file(self, local_path: str | Path, s3_key: str) -> None:
        local_path = Path(local_path)
        if not local_path.exists():
            raise FileNotFoundError(f"Local file to upload not found: {local_path}")

        self.s3.upload_file(str(local_path), self.bucket_name, s3_key)

    def download_file(self, s3_key: str, local_path: str | Path) -> None:
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        self.s3.download_file(self.bucket_name, s3_key, str(local_path))

    def read_csv(self, s3_key: str, **read_kwargs) -> pd.DataFrame:
        """
        Read CSV directly from S3 into DataFrame using S3 object body.
        """
        obj = self.s3.get_object(Bucket=self.bucket_name, Key=s3_key)
        return pd.read_csv(obj["Body"], **read_kwargs)

    def write_csv(self, df: pd.DataFrame, s3_key: str, **to_csv_kwargs) -> None:
        """
        Write CSV to S3 using put_object.
        """
        from io import StringIO

        if "index" not in to_csv_kwargs:
            to_csv_kwargs["index"] = False

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, **to_csv_kwargs)
        self.s3.put_object(
            Bucket=self.bucket_name,
            Key=s3_key,
            Body=csv_buffer.getvalue().encode("utf-8"),
        )


# ---------------------------------------------------------
# Factory: get_s3_client() based on ENV
# ---------------------------------------------------------
def get_s3_client() -> BaseS3Client:
    """
    Returns the appropriate S3 client depending on environment.

    - ENV == "cloud" -> RealS3Client (boto3 + real S3)
    - otherwise      -> LocalS3Client (mock folder)
    """
    if settings.ENV == "cloud":
        return RealS3Client(bucket_name=settings.S3_BUCKET)
    else:
        # local dev by default
        return LocalS3Client(root_dir=settings.MOCK_S3_ROOT)


# Create a module-level singleton for convenience
s3_client: BaseS3Client = get_s3_client()