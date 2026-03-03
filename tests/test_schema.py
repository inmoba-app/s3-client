"""Tests for inmoba_s3.schema module."""

from __future__ import annotations

import json

from inmoba_s3.schema import PARTIDA_SCHEMA, normalize_record


class TestPartidaSchema:
    """Tests for PARTIDA_SCHEMA definition."""

    def test_schema_has_12_top_level_fields(self):
        """PARTIDA_SCHEMA must have exactly 12 top-level fields."""
        assert len(PARTIDA_SCHEMA) == 12

    def test_schema_field_names(self):
        """All expected field names must be present in the schema."""
        expected = {
            "partida_registral",
            "busqueda_id",
            "oficina_registral",
            "area_registral",
            "total_pages",
            "asientos",
            "fichas",
            "folios",
            "raw_response",
            "scraped_at",
            "is_sarp",
            "sarp_source",
        }
        actual = {field.name for field in PARTIDA_SCHEMA}
        assert actual == expected

    def test_nullable_fields(self):
        """is_sarp and sarp_source must be nullable."""
        is_sarp = PARTIDA_SCHEMA.field("is_sarp")
        sarp_source = PARTIDA_SCHEMA.field("sarp_source")
        assert is_sarp.nullable is True
        assert sarp_source.nullable is True


class TestNormalizeRecord:
    """Tests for normalize_record function."""

    def test_normalize_serializes_raw_response_dict(self):
        """normalize_record serializes raw_response dict to JSON string."""
        data = {"raw_response": {"foo": "bar"}}
        result = normalize_record(data)
        assert result["raw_response"] == json.dumps({"foo": "bar"})

    def test_normalize_keeps_raw_response_string(self):
        """normalize_record keeps raw_response as-is when already a string."""
        data = {"raw_response": '{"already": "string"}'}
        result = normalize_record(data)
        assert result["raw_response"] == '{"already": "string"}'

    def test_normalize_defaults_raw_response_when_missing(self):
        """normalize_record defaults raw_response to '{}' when missing."""
        result = normalize_record({})
        assert result["raw_response"] == "{}"

    def test_normalize_defaults_optional_fields(self):
        """normalize_record sets is_sarp and sarp_source to None when missing."""
        result = normalize_record({})
        assert result["is_sarp"] is None
        assert result["sarp_source"] is None

    def test_normalize_does_not_mutate_input(self):
        """normalize_record must not mutate the original dict."""
        original = {"raw_response": {"key": "val"}}
        _ = normalize_record(original)
        # Original should still have dict, not JSON string
        assert isinstance(original["raw_response"], dict)
