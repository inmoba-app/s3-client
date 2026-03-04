from __future__ import annotations

import io
import json

import pyarrow as pa  # pyright: ignore[reportMissingImports]
import pyarrow.compute as pc  # pyright: ignore[reportMissingImports]
import pyarrow.parquet as pq  # pyright: ignore[reportMissingImports]

from inmoba_s3.schema import PARTIDA_SCHEMA, normalize_record
from inmoba_s3.store import S3Store

CURATED_KEY = "curated/partidas.parquet"
DOCUMENTS_PREFIX = "documents/"


class PartidaStore(S3Store):
    def __init__(
        self,
        bucket: str,
        region: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        session_token: str | None = None,
        profile_name: str | None = None,
        athena_output_location: str = "s3://inmoba-sunarp-vispartida/athena-results/",
        athena_database: str = "inmoba_sunarp",
    ) -> None:
        super().__init__(bucket, region, access_key_id, secret_access_key, session_token, profile_name)
        self._index: pa.Table | None = None
        self.athena_output_location = athena_output_location
        self.athena_database = athena_database
        self._athena_client = None
        self._session_token = session_token
        self._profile_name = profile_name

    def load_index(self) -> int:
        raw = self.download(CURATED_KEY)
        if raw is None:
            self._index = pa.table(
                {field.name: pa.array([], type=field.type) for field in PARTIDA_SCHEMA},
                schema=PARTIDA_SCHEMA,
            )
        else:
            self._index = pq.read_table(pa.BufferReader(raw))
        assert self._index is not None
        return self._index.num_rows

    def metadata_exists(self, partida: str) -> bool:
        if self._index is None:
            _ = self.load_index()
        assert self._index is not None
        mask = pc.equal(self._index.column("partida_registral"), partida)
        return pc.sum(mask).as_py() > 0

    def get_metadata(self, partida: str) -> dict[str, object] | None:
        if self._index is None:
            _ = self.load_index()
        assert self._index is not None
        mask = pc.equal(self._index.column("partida_registral"), partida)
        filtered = self._index.filter(mask)
        if filtered.num_rows == 0:
            return None
        row = filtered.to_pylist()[0]
        raw_response = row.get("raw_response")
        if isinstance(raw_response, str):
            try:
                row["raw_response"] = json.loads(raw_response)
            except (json.JSONDecodeError, TypeError):
                pass
        return row

    def save_metadata(self, partida: str, data: dict[str, object]) -> None:
        if self._index is None:
            _ = self.load_index()
        assert self._index is not None

        record = normalize_record(data)
        record["partida_registral"] = partida

        mask = pc.not_equal(self._index.column("partida_registral"), partida)
        existing = self._index.filter(mask)
        new_row = pa.Table.from_pylist([record], schema=PARTIDA_SCHEMA)
        combined = pa.concat_tables([existing, new_row])

        buf = io.BytesIO()
        pq.write_table(combined, buf, compression="zstd")
        _ = self.upload(
            buf.getvalue(),
            CURATED_KEY,
            content_type="application/octet-stream",
        )
        self._index = combined

    # ── Documents ─────────────────────────────────────────────────────────────

    def _document_key(self, partida: str) -> str:
        """Return S3 key for partida's PDF document."""
        return f"{DOCUMENTS_PREFIX}partida_{partida}.pdf"

    def save_document(self, partida: str, pdf_bytes: bytes) -> str:
        """Upload PDF for partida. Returns s3://{bucket}/{key}."""
        key = self._document_key(partida)
        return self.upload(pdf_bytes, key, content_type="application/pdf")

    def get_document_url(self, partida: str, expiry_seconds: int = 604800) -> str:
        """Return presigned GET URL for partida's PDF."""
        key = self._document_key(partida)
        return self.presigned_url(key, expiry_seconds=expiry_seconds, response_content_type="application/pdf")

    def document_exists(self, partida: str) -> bool:
        """Return True if PDF document for partida exists in S3."""
        return self.exists(self._document_key(partida))

    def partida_exists(self, partida: str) -> bool:
        """Return True if BOTH metadata and document exist for partida."""
        return self.metadata_exists(partida) and self.document_exists(partida)

    def get_output_put_url(self, key: str, expiry_seconds: int = 86400) -> str:
        """Generate a presigned PUT URL for JSON output at the given key."""
        return self.presigned_put_url(key, expiry_seconds=expiry_seconds, content_type="application/json")

    def query_athena(self, sql: str, timeout: float = 300.0) -> list[dict]:
        """Execute SQL via Athena. Lazily initializes AthenaClient on first call."""
        if self._athena_client is None:
            from inmoba_s3.athena import AthenaClient
            self._athena_client = AthenaClient(
                region=self._region,
                output_location=self.athena_output_location,
                database=self.athena_database,
                session_token=self._session_token,
                profile_name=self._profile_name,
            )
        return self._athena_client.query(sql, timeout=timeout)
