"""Tests for data generation logic."""

import datetime
from harmonization.data_generation import (
    weighted_choice,
    generate_raw_rows,
    PROVINCIAS,
    TIPOS_RIESGO,
    CANALES,
    SEGMENTOS,
    ZONAS,
    COBERTURAS,
)


class TestWeightedChoice:
    def test_returns_valid_value(self):
        for _ in range(100):
            result = weighted_choice(PROVINCIAS)
            valid_values = [v for v, _ in PROVINCIAS]
            assert result in valid_values

    def test_returns_valid_risk_type(self):
        for _ in range(50):
            result = weighted_choice(TIPOS_RIESGO)
            valid_values = [v for v, _ in TIPOS_RIESGO]
            assert result in valid_values

    def test_returns_string(self):
        result = weighted_choice(CANALES)
        assert isinstance(result, str)

    def test_all_lookup_tables(self):
        for table in [PROVINCIAS, TIPOS_RIESGO, CANALES, SEGMENTOS, ZONAS, COBERTURAS]:
            result = weighted_choice(table)
            valid = [v for v, _ in table]
            assert result in valid


class TestGenerateRawRows:
    def test_returns_correct_count(self):
        rows = generate_raw_rows(100)
        assert len(rows) == 100

    def test_each_row_has_24_fields(self):
        rows = generate_raw_rows(10)
        for row in rows:
            assert len(row) == 24

    def test_field_names(self):
        rows = generate_raw_rows(1)
        expected_keys = {
            "id_registro",
            "anio",
            "mes",
            "codigo_poliza",
            "tipo_riesgo",
            "provincia",
            "canal_distribucion",
            "prima_neta",
            "prima_bruta",
            "num_polizas_nuevas",
            "num_polizas_renovadas",
            "num_polizas_canceladas",
            "num_siniestros_declarados",
            "num_siniestros_pagados",
            "importe_siniestros_bruto",
            "importe_reservas",
            "gastos_gestion",
            "comisiones",
            "ratio_siniestralidad",
            "segmento_cliente",
            "zona_riesgo",
            "cobertura_principal",
            "moneda",
            "fecha_carga",
        }
        assert set(rows[0].keys()) == expected_keys

    def test_prima_neta_le_prima_bruta(self):
        rows = generate_raw_rows(500)
        for row in rows:
            assert row["prima_neta"] <= row["prima_bruta"]

    def test_prima_bruta_positive(self):
        rows = generate_raw_rows(500)
        for row in rows:
            assert row["prima_bruta"] > 0

    def test_claims_paid_le_reported(self):
        rows = generate_raw_rows(500)
        for row in rows:
            assert row["num_siniestros_pagados"] <= row["num_siniestros_declarados"]

    def test_moneda_is_eur(self):
        rows = generate_raw_rows(10)
        for row in rows:
            assert row["moneda"] == "EUR"

    def test_fecha_carga_is_datetime(self):
        rows = generate_raw_rows(5)
        for row in rows:
            assert isinstance(row["fecha_carga"], datetime.datetime)

    def test_anio_in_valid_range(self):
        rows = generate_raw_rows(200)
        for row in rows:
            assert row["anio"] in [2022, 2023, 2024, 2025]

    def test_mes_in_valid_range(self):
        rows = generate_raw_rows(200)
        for row in rows:
            assert 1 <= row["mes"] <= 12

    def test_seed_reproducibility(self):
        rows_a = generate_raw_rows(50, seed=42)
        rows_b = generate_raw_rows(50, seed=42)
        assert rows_a == rows_b

    def test_different_seeds_differ(self):
        rows_a = generate_raw_rows(50, seed=42)
        rows_b = generate_raw_rows(50, seed=99)
        assert rows_a != rows_b

    def test_id_registro_sequential(self):
        rows = generate_raw_rows(100)
        ids = [row["id_registro"] for row in rows]
        assert ids == list(range(1, 101))

    def test_policy_code_format(self):
        rows = generate_raw_rows(5)
        for row in rows:
            code = row["codigo_poliza"]
            assert code.startswith("ES-")
            parts = code.split("-")
            assert len(parts) == 3

    def test_ratio_siniestralidad_non_negative(self):
        rows = generate_raw_rows(500)
        for row in rows:
            assert row["ratio_siniestralidad"] >= 0
