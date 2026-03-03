"""PyArrow schema definitions for PARTIDA records and data normalization."""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

# Struct for listPag inside asientos/fichas
list_pag_type = pa.struct([
    pa.field("pagina", pa.string()),
    pa.field("nroPagRef", pa.string())
])

# Define nested struct types for asientos, fichas, and folios based on actual SUNARP API data
asiento_type = pa.struct([
    pa.field("codActo", pa.string()),
    pa.field("codRubro", pa.string()),
    pa.field("aaTitu", pa.string()),
    pa.field("numTitu", pa.string()),
    pa.field("descActo", pa.string()),
    pa.field("nsAsiento", pa.string()),
    pa.field("nsAsiePlaca", pa.string()),
    pa.field("numPlaca", pa.string()),
    pa.field("fgExonPub", pa.string()),
    pa.field("fgExonJud", pa.string()),
    pa.field("numPag", pa.int32()),
    pa.field("idImgAsiento", pa.int32()),
    pa.field("letraRubro", pa.string()),
    pa.field("nombreRubro", pa.string()),
    pa.field("fechaInscripcion", pa.string()),
    pa.field("numRef", pa.string()),
    pa.field("numPagRef", pa.string()),
    pa.field("listPag", pa.list_(list_pag_type)),
    pa.field("nroPagTotalAiento", pa.string()),
    pa.field("refNumPart", pa.string()),
    pa.field("oficRegId", pa.string()),
    pa.field("regPubId", pa.string()),
    pa.field("areaRegId", pa.string()),
    pa.field("numPartida", pa.string()),
    pa.field("esSARP", pa.bool_())
])

ficha_type = pa.struct([
    pa.field("numFicha", pa.string()),
    pa.field("fichaBis", pa.string()),
    pa.field("idImgFicha", pa.int32()),
    pa.field("numPag", pa.int32()),
    pa.field("listPag", pa.list_(list_pag_type))
])

folio_type = pa.struct([
    pa.field("nuFoja", pa.string()),
    pa.field("nuTomo", pa.string()),
    pa.field("idImgFolio", pa.int32()),
    pa.field("nsCade", pa.int32()),
    pa.field("nroPagRef", pa.string())
])

# Main PARTIDA schema with 12 top-level fields
PARTIDA_SCHEMA = pa.schema([
    pa.field("partida_registral", pa.string(), nullable=False),
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
    pa.field("sarp_source", pa.string(), nullable=True)
])


def normalize_record(data: dict[str, Any]) -> dict[str, Any]:
    """
    Normalizes a dictionary to exactly match PARTIDA_SCHEMA.
    Ensures missing fields are set to None and raw_response is a JSON string.
    """
    result = dict(data)  # shallow copy

    # Ensure busqueda_id is string if it exists
    if "busqueda_id" in result and result["busqueda_id"] is not None:
        result["busqueda_id"] = str(result["busqueda_id"])

    # Serialize raw_response if it's a dict, otherwise keep as string or default to "{}"
    if "raw_response" in result:
        if isinstance(result["raw_response"], dict):
            result["raw_response"] = json.dumps(result["raw_response"], ensure_ascii=False)
    else:
        result["raw_response"] = "{}"

    # Ensure all top-level keys exist (set missing to None)
    for field in PARTIDA_SCHEMA:
        if field.name not in result:
            if field.name in ("asientos", "fichas", "folios"):
                result[field.name] = []
            else:
                result[field.name] = None
                
    # Also drop keys that are not in the schema
    schema_keys = set(PARTIDA_SCHEMA.names)
    for k in list(result.keys()):
        if k not in schema_keys:
            del result[k]

    return result
