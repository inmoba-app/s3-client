"""inmoba-s3: S3 storage clients for inmoba-sunarp-vispartida."""
from __future__ import annotations

from inmoba_s3.partida_store import PartidaStore
from inmoba_s3.schema import PARTIDA_SCHEMA, normalize_record
from inmoba_s3.store import S3Store

# Deprecated alias — use PartidaStore instead
S3Client = PartidaStore


__all__ = [
    "PartidaStore",
    "S3Client",
    "S3Store",
    "PARTIDA_SCHEMA",
    "normalize_record",
]
