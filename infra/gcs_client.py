from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from google.cloud import storage
from google.oauth2 import service_account
from datetime import timedelta


@dataclass(frozen=True)
class GcsConfig:
    bucket: str
    prefix: str = ""
    credentials_path: Optional[str] = None


def _normalize_prefix(prefix: str) -> str:
    raw = (prefix or "").strip().strip("/")
    return raw


class GcsClient:
    def __init__(self, cfg: GcsConfig):
        if not cfg.bucket:
            raise ValueError("gcs.bucket is required")
        self.bucket_name = cfg.bucket
        self.prefix = _normalize_prefix(cfg.prefix)
        if cfg.credentials_path:
            credentials = service_account.Credentials.from_service_account_file(cfg.credentials_path)
            self.client = storage.Client(credentials=credentials)
        else:
            self.client = storage.Client()

    def _with_prefix(self, path: str) -> str:
        clean = (path or "").lstrip("/")
        if not self.prefix:
            return clean
        if not clean:
            return self.prefix
        return f"{self.prefix}/{clean}"

    def _bucket(self) -> storage.Bucket:
        return self.client.bucket(self.bucket_name)

    def upload_bytes(self, path: str, data: bytes, *, content_type: Optional[str] = None) -> None:
        blob = self._bucket().blob(self._with_prefix(path))
        blob.upload_from_string(data, content_type=content_type)

    def upload_file(self, path: str, file_path: str, *, content_type: Optional[str] = None) -> None:
        blob = self._bucket().blob(self._with_prefix(path))
        blob.upload_from_filename(file_path, content_type=content_type)

    def download_bytes(self, path: str) -> bytes:
        blob = self._bucket().blob(self._with_prefix(path))
        return blob.download_as_bytes()

    def download_to_file(self, path: str, file_path: str) -> None:
        blob = self._bucket().blob(self._with_prefix(path))
        blob.download_to_filename(file_path)

    def download_bytes_raw(self, path: str) -> bytes:
        blob = self._bucket().blob(path)
        return blob.download_as_bytes()

    def download_to_file_raw(self, path: str, file_path: str) -> None:
        blob = self._bucket().blob(path)
        blob.download_to_filename(file_path)

    def exists(self, path: str) -> bool:
        blob = self._bucket().blob(self._with_prefix(path))
        return blob.exists()

    def generate_signed_url(
        self,
        path: str,
        *,
        expires_seconds: int = 600,
        method: str = "GET",
    ) -> str:
        blob = self._bucket().blob(self._with_prefix(path))
        return blob.generate_signed_url(
            version="v4",
            expiration=timedelta(seconds=int(expires_seconds)),
            method=method,
        )

    def generate_signed_url_raw(
        self,
        path: str,
        *,
        expires_seconds: int = 600,
        method: str = "GET",
    ) -> str:
        blob = self._bucket().blob(path)
        return blob.generate_signed_url(
            version="v4",
            expiration=timedelta(seconds=int(expires_seconds)),
            method=method,
        )
