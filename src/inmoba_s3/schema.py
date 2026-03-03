"""PyArrow schema definitions for PARTIDA records and data normalization."""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

# Define nested struct types for asientos, fichas, and folios
asiento_type = pa.struct(
    [
        pa.field("numero_asiento", pa.string()),
        pa.field("acto", pa.string()),
        pa.field("monto", pa.string()),
        pa.field("moneda", pa.string()),
        pa.field("fecha", pa.string()),
        pa.field("descripcion", pa.string()),
        pa.field("tipo_acto", pa.string()),
        pa.field("estado", pa.string()),
        pa.field("tomo", pa.string()),
        pa.field("ficha", pa.string()),
        pa.field("folio", pa.string()),
        pa.field("partida", pa.string()),
        pa.field("pagina", pa.string()),
        pa.field("ciento", pa.string()),
        pa.field("oficina_origen", pa.string()),
        pa.field("domicilio", pa.string()),
        pa.field("titular", pa.string()),
        pa.field("concubino", pa.string()),
        pa.field("gravamen", pa.string()),
        pa.field("importe_letra", pa.string()),
        pa.field("plazo", pa.string()),
        pa.field("tasa", pa.string()),
        pa.field("correlativo", pa.string()),
        pa.field("raw_text", pa.string()),
        pa.field("orden", pa.string()),  # Additional fields for completeness
    ]
)

ficha_type = pa.struct(
    [
        pa.field("numero_ficha", pa.string()),
        pa.field("tipo", pa.string()),
        pa.field("fecha", pa.string()),
        pa.field("descripcion", pa.string()),
        pa.field("estado", pa.string()),
    ]
)

folio_type = pa.struct(
    [
        pa.field("numero_folio", pa.string()),
        pa.field("tipo", pa.string()),
        pa.field("fecha", pa.string()),
        pa.field("descripcion", pa.string()),
        pa.field("estado", pa.string()),
    ]
)

# Main PARTIDA schema with 12 top-level fields
PARTIDA_SCHEMA = pa.schema(
    [
        pa.field("partida_registral", pa.string()),
        pa.field("busqueda_id", pa.string()),
        pa.field("oficina_registral", pa.string()),
        pa.field("area_registral", pa.string()),
        pa.field("total_pages", pa.int32()),
        pa.field("asientos", pa.list_(asiento_type)),
        pa.field("fichas", pa.list_(ficha_type)),
        pa.field("folios", pa.list_(folio_type)),
        pa.field("raw_response", pa.string()),
        pa.field("scraped_at", pa.string()),
        pa.field("is_sarp", pa.bool_(), nullable=True),
        pa.field("sarp_source", pa.string(), nullable=True),
    ]
)


def normalize_record(data: dict[str, Any]) -> dict[str, Any]:
    """Normalize a raw PARTIDA record for storage.

    Serializes raw_response dict to JSON string and provides defaults for optional fields.

    Args:
        data: Raw PARTIDA record dictionary

    Returns:
        Normalized dictionary with serialized raw_response and defaults for optional fields
    """
    result = data.copy()

    # Serialize raw_response if it's a dict, otherwise keep as string or default to "{}"
    if "raw_response" in result:
        raw = result["raw_response"]
        if isinstance(raw, dict):
            result["raw_response"] = json.dumps(raw)
        elif isinstance(raw, str):
            result["raw_response"] = raw
        else:
            result["raw_response"] = "{}"
    else:
        result["raw_response"] = "{}"

    # Set defaults for optional fields if missing
    if "is_sarp" not in result:
        result["is_sarp"] = None

    if "sarp_source" not in result:
        result["sarp_source"] = None

    return result
