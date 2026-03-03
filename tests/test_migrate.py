"""Tests for scripts/migrate.py migration script."""

from __future__ import annotations

import json
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

# scripts/ is not a package; add it to sys.path so we can import migrate
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import migrate  # noqa: E402


class TestListPartidaPrefixes:
    """Tests for list_partida_prefixes."""

    def test_filters_runs_prefix_and_strips_trailing_slash(self):
        """list_partida_prefixes filters out 'runs/' prefixes and strips trailing '/'."""
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "CommonPrefixes": [
                    {"Prefix": "12345P-/"},
                    {"Prefix": "runs/run1/"},
                    {"Prefix": "67890P-/"},
                ]
            }
        ]

        result = migrate.list_partida_prefixes(mock_s3, "test-bucket")

        assert result == ["12345P-", "67890P-"]
        mock_s3.get_paginator.assert_called_once_with("list_objects_v2")
        mock_paginator.paginate.assert_called_once_with(
            Bucket="test-bucket", Delimiter="/"
        )

    def test_returns_empty_for_no_common_prefixes(self):
        """list_partida_prefixes returns [] when pages have no CommonPrefixes."""
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{}]

        result = migrate.list_partida_prefixes(mock_s3, "test-bucket")

        assert result == []


class TestMigrateMetadata:
    """Tests for migrate_metadata."""

    def _make_s3_body(self, data: dict) -> MagicMock:
        """Create a mock S3 response Body for get_object."""
        body = MagicMock()
        body.read.return_value = json.dumps(data).encode("utf-8")
        return body

    def test_processes_valid_metadata(self, sample_metadata_dict):
        """migrate_metadata returns normalized records for valid metadata.json."""
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": self._make_s3_body(sample_metadata_dict)
        }

        args = MagicMock()
        records, failures = migrate.migrate_metadata(
            mock_s3, "test-bucket", ["00708079"], args
        )

        assert len(records) == 1
        assert failures == []
        assert records[0]["partida_registral"] == "00708079"
        # normalize_record should have serialized raw_response to string
        assert isinstance(records[0]["raw_response"], str)

    def test_handles_s3_error_gracefully(self):
        """migrate_metadata appends to failures on S3 error and continues."""
        mock_s3 = MagicMock()
        # First call raises, second succeeds
        good_data = {
            "partida_registral": "BBB",
            "raw_response": "{}",
        }
        mock_s3.get_object.side_effect = [
            Exception("NoSuchKey"),
            {"Body": self._make_s3_body(good_data)},
        ]

        args = MagicMock()
        records, failures = migrate.migrate_metadata(
            mock_s3, "test-bucket", ["AAA", "BBB"], args
        )

        assert failures == ["AAA"]
        assert len(records) == 1
        assert records[0]["partida_registral"] == "BBB"

    def test_sets_partida_registral_on_record(self, sample_metadata_dict):
        """migrate_metadata overrides partida_registral with the prefix key."""
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": self._make_s3_body(sample_metadata_dict)
        }

        args = MagicMock()
        records, _ = migrate.migrate_metadata(
            mock_s3, "test-bucket", ["OVERRIDE-KEY"], args
        )

        # partida_registral should be set to the prefix, not the original value
        assert records[0]["partida_registral"] == "OVERRIDE-KEY"


class TestMigratePdfs:
    """Tests for migrate_pdfs."""

    def test_copies_pdfs_with_correct_keys(self):
        """migrate_pdfs calls copy_object with correct source and dest keys."""
        mock_s3 = MagicMock()
        args = MagicMock()

        copied, failures = migrate.migrate_pdfs(
            mock_s3, "test-bucket", ["12345P-", "67890P-"], args
        )

        assert copied == 2
        assert failures == []
        calls = mock_s3.copy_object.call_args_list
        assert len(calls) == 2

        # First call
        assert calls[0].kwargs["Bucket"] == "test-bucket"
        assert calls[0].kwargs["Key"] == "documents/partida_12345P-.pdf"
        assert calls[0].kwargs["CopySource"] == {
            "Bucket": "test-bucket",
            "Key": "12345P-/partida_12345P-.pdf",
        }

        # Second call
        assert calls[1].kwargs["Key"] == "documents/partida_67890P-.pdf"

    def test_handles_copy_failure(self):
        """migrate_pdfs appends to failures on copy error and continues."""
        mock_s3 = MagicMock()
        mock_s3.copy_object.side_effect = [Exception("Access Denied"), None]
        args = MagicMock()

        copied, failures = migrate.migrate_pdfs(
            mock_s3, "test-bucket", ["FAIL", "OK"], args
        )

        assert copied == 1
        assert failures == ["FAIL"]


class TestBuildParquetTable:
    """Tests for build_parquet_table."""

    def test_produces_table_with_correct_schema(self):
        """build_parquet_table returns a pyarrow Table with 12-column PARTIDA_SCHEMA."""
        records = [
            {
                "partida_registral": "12345P-",
                "busqueda_id": "b-001",
                "oficina_registral": "LIMA",
                "area_registral": "PROPIEDAD INMUEBLE",
                "total_pages": 2,
                "asientos": [],
                "fichas": [],
                "folios": [],
                "raw_response": "{}",
                "scraped_at": "2024-01-01T00:00:00Z",
                "is_sarp": None,
                "sarp_source": None,
            }
        ]

        table = migrate.build_parquet_table(records)

        assert isinstance(table, pa.Table)
        assert table.num_rows == 1
        assert table.num_columns == 12
        assert table.schema == migrate.PARTIDA_SCHEMA


class TestWriteParquet:
    """Tests for write_parquet."""

    def test_creates_file_at_output_path(self):
        """write_parquet creates a valid parquet file at the given path."""
        records = [
            {
                "partida_registral": "12345P-",
                "busqueda_id": None,
                "oficina_registral": None,
                "area_registral": None,
                "total_pages": None,
                "asientos": [],
                "fichas": [],
                "folios": [],
                "raw_response": "{}",
                "scraped_at": None,
                "is_sarp": None,
                "sarp_source": None,
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "subdir", "test.parquet")
            migrate.write_parquet(records, output_path)

            assert os.path.isfile(output_path)
            assert os.path.getsize(output_path) > 0


class TestMainDryRun:
    """Tests for main with --dry-run flag."""

    @patch.object(migrate, "list_partida_prefixes")
    @patch("boto3.client")
    def test_dry_run_does_not_call_put_or_copy(
        self, mock_boto3_client, mock_list_prefixes
    ):
        """main with --dry-run does NOT call put_object or copy_object."""
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_list_prefixes.return_value = ["12345P-"]

        # Mock get_object for migrate_metadata
        body = MagicMock()
        body.read.return_value = json.dumps(
            {
                "partida_registral": "12345P-",
                "raw_response": "{}",
            }
        ).encode("utf-8")
        mock_s3.get_object.return_value = {"Body": body}

        with tempfile.TemporaryDirectory() as tmpdir:
            args = migrate.parse_args(["--dry-run", "--output-dir", tmpdir])
            migrate.main(args)

        mock_s3.put_object.assert_not_called()
        mock_s3.copy_object.assert_not_called()
