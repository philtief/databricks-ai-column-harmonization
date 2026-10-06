"""Tests for the deterministic country file generator."""

from __future__ import annotations

import json

import pytest

from harmonization.config import load_config
from harmonization.generator import COUNTRIES, column_specs, file_name, generate_rows, month_partitions, to_csv


@pytest.mark.parametrize("country", ["ES", "IT"])
def test_specs_and_rows_are_deterministic(country):
    first = generate_rows(country, 31, COUNTRIES[country]["seed"])
    second = generate_rows(country, 31, COUNTRIES[country]["seed"])

    assert len(column_specs(country)) == 24
    assert first == second
    assert len(first) == 31


@pytest.mark.parametrize("country", ["ES", "IT"])
def test_rows_cover_twelve_distinct_months(country):
    rows = generate_rows(country, 250, COUNTRIES[country]["seed"])
    partitions = month_partitions(rows)

    assert len(partitions) == 12
    assert list(partitions) == [f"2025{month:02d}" for month in range(1, 13)]


@pytest.mark.parametrize("country", ["ES", "IT"])
def test_csv_header_matches_specs(country):
    specs = column_specs(country)
    columns = [spec.name for spec in specs]
    csv_value = to_csv(generate_rows(country, 2, COUNTRIES[country]["seed"]), columns)

    assert csv_value.splitlines()[0] == ",".join(columns)
    assert len(csv_value.splitlines()) == 3


@pytest.mark.parametrize("country", ["ES", "IT"])
def test_answer_keys_cover_specs_with_valid_targets(country):
    config = load_config()
    target_names = {column["name"] for column in config["target_model"]["columns"]}
    with open(f"examples/answer_keys/{country.lower()}.json") as answer_key_file:
        answer_key = json.load(answer_key_file)

    local_names = [spec.name for spec in column_specs(country)]
    assert answer_key["source_system"] == config["sources"][country]["source_system"]
    assert set(answer_key["mappings"]) == set(local_names)
    assert len(answer_key["mappings"]) == len(local_names)
    for target in answer_key["mappings"].values():
        assert target is None or target in target_names


def test_italian_schema_contains_required_semantic_traps():
    names = {spec.name for spec in column_specs("IT")}

    assert {
        "premi_netti",
        "premi_lordi_contabilizzati",
        "sinistri_denunciati",
        "sinistri_pagati",
        "rapporto_sinistri_premi",
        "codice_agenzia_interno",
    } <= names


def test_unknown_country_and_month_key_are_handled():
    with pytest.raises(KeyError, match="Valid countries"):
        column_specs("FR")
    with pytest.raises(KeyError, match="Valid countries"):
        generate_rows("FR", 1, 42)

    row = {"anio": 2025, "mes": 7}
    assert month_partitions([row]) == {"202507": [row]}
    assert file_name("202507") == "property_monthly_202507.csv"
