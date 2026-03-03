"""Tests for inmoba_s3.partida_store.PartidaStore."""

from __future__ import annotations

import io
import json
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from inmoba_s3.partida_store import CURATED_KEY, PartidaStore
from inmoba_s3.schema import PARTIDA_SCHEMA, normalize_record


def _make_parquet_bytes(
    records: list[dict], schema: pa.Schema = PARTIDA_SCHEMA
) -> bytes:
    """Helper: create zstd-compressed parquet bytes from list of dicts."""
    table = pa.Table.from_pylist(records, schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    return buf.getvalue()


class TestLoadIndex:
    """Tests for PartidaStore.load_index."""

    def test_load_index_empty_when_parquet_missing(self, mock_boto3_client: MagicMock):
        """load_index returns 0 rows when curated parquet does not exist in S3."""
        # download returns None → NoSuchKey
        error = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}},
            "GetObject",
        )
        mock_boto3_client.get_object.side_effect = error

        store = PartidaStore("my-bucket")
        count = store.load_index()

        assert count == 0
        assert store._index is not None
        assert store._index.num_rows == 0
        assert store._index.schema == PARTIDA_SCHEMA

    def test_load_index_reads_existing_parquet(
        self, mock_boto3_client: MagicMock, sample_metadata_dict: dict
    ):
        """load_index reads existing parquet and returns correct row count."""
        record = normalize_record(sample_metadata_dict)
        parquet_bytes = _make_parquet_bytes([record])

        body = MagicMock()
        body.read.return_value = parquet_bytes
        mock_boto3_client.get_object.return_value = {"Body": body}

        store = PartidaStore("my-bucket")
        count = store.load_index()

        assert count == 1
        assert store._index is not None
        assert store._index.num_rows == 1


class TestMetadataExists:
    """Tests for PartidaStore.metadata_exists."""

    def test_metadata_exists_true(
        self, mock_boto3_client: MagicMock, sample_metadata_dict: dict
    ):
        """metadata_exists returns True when partida is in the index."""
        record = normalize_record(sample_metadata_dict)
        parquet_bytes = _make_parquet_bytes([record])

        body = MagicMock()
        body.read.return_value = parquet_bytes
        mock_boto3_client.get_object.return_value = {"Body": body}

        store = PartidaStore("my-bucket")
        assert store.metadata_exists("00708079") is True

    def test_metadata_exists_false(
        self, mock_boto3_client: MagicMock, sample_metadata_dict: dict
    ):
        """metadata_exists returns False when partida is NOT in the index."""
        record = normalize_record(sample_metadata_dict)
        parquet_bytes = _make_parquet_bytes([record])

        body = MagicMock()
        body.read.return_value = parquet_bytes
        mock_boto3_client.get_object.return_value = {"Body": body}

        store = PartidaStore("my-bucket")
        assert store.metadata_exists("99999999") is False


class TestGetMetadata:
    """Tests for PartidaStore.get_metadata."""

    def test_get_metadata_returns_dict_with_deserialized_raw_response(
        self, mock_boto3_client: MagicMock, sample_metadata_dict: dict
    ):
        """get_metadata returns dict with raw_response deserialized from JSON string."""
        record = normalize_record(sample_metadata_dict)
        parquet_bytes = _make_parquet_bytes([record])

        body = MagicMock()
        body.read.return_value = parquet_bytes
        mock_boto3_client.get_object.return_value = {"Body": body}

        store = PartidaStore("my-bucket")
        meta = store.get_metadata("00708079")

        assert meta is not None
        assert isinstance(meta["raw_response"], dict)
        assert meta["raw_response"] == {"some": "raw", "data": "here"}
        assert meta["partida_registral"] == "00708079"

    def test_get_metadata_returns_none_for_missing_partida(
        self, mock_boto3_client: MagicMock
    ):
        """get_metadata returns None when partida not found."""
        # Empty index
        error = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}},
            "GetObject",
        )
        mock_boto3_client.get_object.side_effect = error

        store = PartidaStore("my-bucket")
        assert store.get_metadata("99999999") is None


class TestSaveMetadata:
    """Tests for PartidaStore.save_metadata."""

    def test_save_metadata_uploads_parquet(
        self, mock_boto3_client: MagicMock, sample_metadata_dict: dict
    ):
        """save_metadata serializes to parquet and uploads to curated key."""
        # Start with empty index
        error = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}},
            "GetObject",
        )
        mock_boto3_client.get_object.side_effect = error

        store = PartidaStore("my-bucket")
        store.save_metadata("00708079", sample_metadata_dict)

        # Verify put_object was called with the curated key
        mock_boto3_client.put_object.assert_called_once()
        call_kwargs = mock_boto3_client.put_object.call_args
        assert (
            call_kwargs.kwargs["Key"] == CURATED_KEY
            or call_kwargs[1]["Key"] == CURATED_KEY
        )

        # Verify internal index updated
        assert store._index is not None
        assert store._index.num_rows == 1


class TestSaveDocument:
    """Tests for PartidaStore.save_document."""

    def test_save_document_uploads_pdf(self, mock_boto3_client: MagicMock):
        """save_document uploads PDF bytes with correct key and content type."""
        store = PartidaStore("my-bucket")
        uri = store.save_document("00708079", b"%PDF-1.4 fake pdf")

        assert uri == "s3://my-bucket/documents/partida_00708079.pdf"
        mock_boto3_client.put_object.assert_called_once_with(
            Bucket="my-bucket",
            Key="documents/partida_00708079.pdf",
            Body=b"%PDF-1.4 fake pdf",
            ContentType="application/pdf",
        )


class TestDocumentExists:
    """Tests for PartidaStore.document_exists."""

    def test_document_exists_true(self, mock_boto3_client: MagicMock):
        """document_exists returns True when PDF exists in S3."""
        mock_boto3_client.head_object.return_value = {}

        store = PartidaStore("my-bucket")
        assert store.document_exists("00708079") is True
        mock_boto3_client.head_object.assert_called_once_with(
            Bucket="my-bucket",
            Key="documents/partida_00708079.pdf",
        )

    def test_document_exists_false(self, mock_boto3_client: MagicMock):
        """document_exists returns False when PDF does not exist."""
        error = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}},
            "HeadObject",
        )
        mock_boto3_client.head_object.side_effect = error

        store = PartidaStore("my-bucket")
        assert store.document_exists("00708079") is False


class TestPartidaExists:
    """Tests for PartidaStore.partida_exists."""

    def test_partida_exists_true_when_both_exist(
        self, mock_boto3_client: MagicMock, sample_metadata_dict: dict
    ):
        """partida_exists returns True when BOTH metadata and document exist."""
        # Set up index with metadata
        record = normalize_record(sample_metadata_dict)
        parquet_bytes = _make_parquet_bytes([record])

        body = MagicMock()
        body.read.return_value = parquet_bytes
        mock_boto3_client.get_object.return_value = {"Body": body}

        # head_object succeeds (document exists)
        mock_boto3_client.head_object.return_value = {}

        store = PartidaStore("my-bucket")
        assert store.partida_exists("00708079") is True

    def test_partida_exists_false_when_metadata_missing(
        self, mock_boto3_client: MagicMock, sample_metadata_dict: dict
    ):
        """partida_exists returns False when metadata does not exist (even if document does)."""
        # Index has data for a different partida, so metadata_exists returns False
        record = normalize_record(sample_metadata_dict)
        parquet_bytes = _make_parquet_bytes([record])

        body = MagicMock()
        body.read.return_value = parquet_bytes
        mock_boto3_client.get_object.return_value = {"Body": body}

        # head_object succeeds (document exists) — but metadata_exists is False
        mock_boto3_client.head_object.return_value = {}

        store = PartidaStore("my-bucket")
        # sample_metadata_dict has partida_registral='00708079', so '99999999' won't match
        assert store.partida_exists("99999999") is False
