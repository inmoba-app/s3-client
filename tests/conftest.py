"""Shared pytest fixtures for inmoba_s3 tests."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def sample_metadata_dict() -> dict:
    """Complete sample metadata dict with all 12 schema fields."""
    return {
        "partida_registral": "00708079",
        "busqueda_id": "test-busqueda-001",
        "oficina_registral": "LIMA",
        "area_registral": "PROPIEDAD INMUEBLE",
        "total_pages": 3,
        "asientos": [
            {
                "numero_asiento": "C00001",
                "acto": "COMPRAVENTA",
                "monto": "150000",
                "moneda": "USD",
                "fecha": "2020-01-15",
                "descripcion": "Transferencia de propiedad",
                "tipo_acto": "DOMINIO",
                "estado": "VIGENTE",
                "tomo": "001",
                "ficha": "001",
                "folio": "001",
                "partida": "00708079",
                "pagina": "1",
                "ciento": "1",
                "oficina_origen": "LIMA",
                "domicilio": "AV TEST 123",
                "titular": "JUAN PEREZ",
                "concubino": "",
                "gravamen": "",
                "importe_letra": "CIENTO CINCUENTA MIL",
                "plazo": "30 AÑOS",
                "tasa": "10%",
                "correlativo": "001",
                "raw_text": "raw asiento text here",
            }
        ],
        "fichas": [],
        "folios": [],
        "raw_response": {"some": "raw", "data": "here"},
        "scraped_at": "2024-01-01T00:00:00Z",
        "is_sarp": False,
        "sarp_source": None,
    }


@pytest.fixture
def mock_boto3_client():
    """Patch boto3.client to return a MagicMock."""
    with patch("boto3.client") as mock_client:
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance
        yield mock_instance
