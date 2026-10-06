"""Deterministic country generators for the property-insurance example."""

from __future__ import annotations

import csv
import io
import random
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ColumnSpec:
    """A generated source column and its Delta-compatible type."""

    name: str
    data_type: str


COUNTRIES: dict[str, dict] = {
    "ES": {"seed": 42, "prefix": "ES"},
    "IT": {"seed": 43, "prefix": "IT"},
}

PROVINCES_ES = [
    ("Madrid", 0.20),
    ("Barcelona", 0.18),
    ("Valencia", 0.10),
    ("Sevilla", 0.08),
    ("Bilbao", 0.07),
    ("Málaga", 0.06),
    ("Zaragoza", 0.05),
    ("Murcia", 0.05),
    ("Palma", 0.04),
    ("Las Palmas", 0.04),
    ("Alicante", 0.04),
    ("Valladolid", 0.03),
    ("Córdoba", 0.03),
    ("Vigo", 0.03),
]

REGIONS_IT = [
    ("Lombardia", 0.20),
    ("Lazio", 0.15),
    ("Campania", 0.12),
    ("Veneto", 0.10),
    ("Piemonte", 0.09),
    ("Emilia-Romagna", 0.08),
    ("Sicilia", 0.07),
    ("Toscana", 0.06),
    ("Puglia", 0.05),
    ("Liguria", 0.04),
    ("Sardegna", 0.02),
    ("Trentino-Alto Adige", 0.02),
]

RISK_TYPES_ES = [
    ("Incendio", 0.25),
    ("Inundación", 0.20),
    ("Robo", 0.15),
    ("Daños por Agua", 0.20),
    ("Responsabilidad Civil", 0.10),
    ("Daños Eléctricos", 0.10),
]

RISK_TYPES_IT = [
    ("Incendio", 0.25),
    ("Alluvione", 0.20),
    ("Furto", 0.15),
    ("Danni Idrici", 0.20),
    ("Responsabilità Civile", 0.10),
    ("Danni Elettrici", 0.10),
]

CHANNELS_ES = [
    ("Agente", 0.35),
    ("Corredor", 0.25),
    ("Directo", 0.20),
    ("Bancaseguros", 0.15),
    ("Digital", 0.05),
]

CHANNELS_IT = [
    ("Agente", 0.35),
    ("Broker", 0.25),
    ("Diretto", 0.20),
    ("Bancassicurazione", 0.15),
    ("Digitale", 0.05),
]

SEGMENTS_ES = [
    ("Particular", 0.45),
    ("Pequeña Empresa", 0.30),
    ("Mediana Empresa", 0.15),
    ("Gran Empresa", 0.10),
]

SEGMENTS_IT = [
    ("Privato", 0.45),
    ("Piccola Impresa", 0.30),
    ("Media Impresa", 0.15),
    ("Grande Impresa", 0.10),
]

RISK_ZONES_ES = [("Zona A", 0.30), ("Zona B", 0.40), ("Zona C", 0.20), ("Zona D", 0.10)]
RISK_ZONES_IT = [("Zona A", 0.30), ("Zona B", 0.40), ("Zona C", 0.20), ("Zona D", 0.10)]

COVERAGES_ES = [
    ("Edificio", 0.30),
    ("Contenido", 0.25),
    ("Ambos", 0.30),
    ("Responsabilidad Civil", 0.15),
]

COVERAGES_IT = [
    ("Edificio", 0.30),
    ("Contenuto", 0.25),
    ("Entrambi", 0.30),
    ("Responsabilità Civile", 0.15),
]

REPORTING_YEAR = 2025
REPORTING_MONTHS = tuple(range(1, 13))


def _column_specs_es() -> list[ColumnSpec]:
    return [
        ColumnSpec("id_registro", "BIGINT"),
        ColumnSpec("anio", "INT"),
        ColumnSpec("mes", "INT"),
        ColumnSpec("codigo_poliza", "STRING"),
        ColumnSpec("tipo_riesgo", "STRING"),
        ColumnSpec("provincia", "STRING"),
        ColumnSpec("canal_distribucion", "STRING"),
        ColumnSpec("prima_neta", "DOUBLE"),
        ColumnSpec("prima_bruta", "DOUBLE"),
        ColumnSpec("num_polizas_nuevas", "INT"),
        ColumnSpec("num_polizas_renovadas", "INT"),
        ColumnSpec("num_polizas_canceladas", "INT"),
        ColumnSpec("num_siniestros_declarados", "INT"),
        ColumnSpec("num_siniestros_pagados", "INT"),
        ColumnSpec("importe_siniestros_bruto", "DOUBLE"),
        ColumnSpec("importe_reservas", "DOUBLE"),
        ColumnSpec("gastos_gestion", "DOUBLE"),
        ColumnSpec("comisiones", "DOUBLE"),
        ColumnSpec("ratio_siniestralidad", "DOUBLE"),
        ColumnSpec("segmento_cliente", "STRING"),
        ColumnSpec("zona_riesgo", "STRING"),
        ColumnSpec("cobertura_principal", "STRING"),
        ColumnSpec("moneda", "STRING"),
        ColumnSpec("fecha_carga", "TIMESTAMP"),
    ]


def _column_specs_it() -> list[ColumnSpec]:
    return [
        ColumnSpec("id_riga", "BIGINT"),
        ColumnSpec("anno_riferimento", "INT"),
        ColumnSpec("mese_riferimento", "INT"),
        ColumnSpec("codice_polizza", "STRING"),
        ColumnSpec("tipo_rischio", "STRING"),
        ColumnSpec("regione", "STRING"),
        ColumnSpec("canale_distributivo", "STRING"),
        ColumnSpec("premi_netti", "DOUBLE"),
        ColumnSpec("premi_lordi_contabilizzati", "DOUBLE"),
        ColumnSpec("num_polizze_nuove", "INT"),
        ColumnSpec("num_polizze_rinnovate", "INT"),
        ColumnSpec("num_polizze_cancellate", "INT"),
        ColumnSpec("sinistri_denunciati", "INT"),
        ColumnSpec("sinistri_pagati", "INT"),
        ColumnSpec("costo_sinistri_lordo", "DOUBLE"),
        ColumnSpec("riserva_sinistri", "DOUBLE"),
        ColumnSpec("spese_gestione", "DOUBLE"),
        ColumnSpec("provvigioni", "DOUBLE"),
        ColumnSpec("rapporto_sinistri_premi", "DOUBLE"),
        ColumnSpec("segmento_cliente", "STRING"),
        ColumnSpec("zona_rischio", "STRING"),
        ColumnSpec("copertura_principale", "STRING"),
        ColumnSpec("valuta", "STRING"),
        ColumnSpec("codice_agenzia_interno", "STRING"),
    ]


def _weighted_choice(options: list[tuple[str, float]], rng: random.Random) -> str:
    values, weights = zip(*options, strict=True)
    return rng.choices(values, weights=weights, k=1)[0]


def _load_timestamp(rng: random.Random, month: int) -> datetime:
    day = rng.randint(1, 28)
    hour = rng.randint(0, 23)
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)
    return datetime(REPORTING_YEAR, month, day, hour, minute, second)


def column_specs(country: str) -> list[ColumnSpec]:
    """Return the local schema for ``country``."""
    specs = {
        "ES": _column_specs_es,
        "IT": _column_specs_it,
    }
    if country not in specs:
        raise KeyError(f"Unknown country '{country}'. Valid countries: {sorted(specs)}")
    return specs[country]()


def generate_rows(country: str, n_rows: int, seed: int) -> list[dict]:
    """Generate deterministic local-schema rows for one country.

    Rows are distributed evenly across the 12 reporting months for 2025.
    """
    if country not in COUNTRIES:
        raise KeyError(f"Unknown country '{country}'. Valid countries: {sorted(COUNTRIES)}")
    if n_rows < 0:
        raise ValueError("n_rows must not be negative")

    rng = random.Random(seed)
    options = {
        "ES": (PROVINCES_ES, RISK_TYPES_ES, CHANNELS_ES, SEGMENTS_ES, RISK_ZONES_ES, COVERAGES_ES),
        "IT": (REGIONS_IT, RISK_TYPES_IT, CHANNELS_IT, SEGMENTS_IT, RISK_ZONES_IT, COVERAGES_IT),
    }[country]
    geography, risk_types, channels, segments, risk_zones, coverages = options
    prefix = COUNTRIES[country]["prefix"]
    rows = []

    for index in range(1, n_rows + 1):
        month = REPORTING_MONTHS[(index - 1) % len(REPORTING_MONTHS)]
        gross_premium = round(rng.uniform(300.0, 8000.0), 2)
        net_premium = round(gross_premium * rng.uniform(0.75, 0.97), 2)
        reported_claims = rng.randint(0, 8)
        paid_claims = rng.randint(0, reported_claims)
        gross_claims = round(gross_premium * rng.uniform(0.10, 1.20), 2)
        claims_reserve = round(gross_claims * rng.uniform(0.05, 0.30), 2)
        management_expenses = round(gross_premium * rng.uniform(0.03, 0.12), 2)
        commissions = round(gross_premium * rng.uniform(0.05, 0.18), 2)
        loss_ratio = round(gross_claims / gross_premium, 4) if gross_premium > 0 else 0.0
        agency_code = f"{prefix}-{rng.randint(1000, 9999)}"

        common_values = {
            "record_id": index,
            "policy_number": f"{prefix}-{index:05d}-{REPORTING_YEAR}",
            "reporting_year": REPORTING_YEAR,
            "reporting_month": month,
            "risk_type": _weighted_choice(risk_types, rng),
            "region": _weighted_choice(geography, rng),
            "distribution_channel": _weighted_choice(channels, rng),
            "net_written_premium_eur": net_premium,
            "gross_written_premium_eur": gross_premium,
            "new_policies_count": rng.randint(0, 15),
            "renewed_policies_count": rng.randint(5, 60),
            "cancelled_policies_count": rng.randint(0, 5),
            "claims_reported_count": reported_claims,
            "claims_paid_count": paid_claims,
            "gross_claims_incurred_eur": gross_claims,
            "claims_reserve_eur": claims_reserve,
            "management_expenses_eur": management_expenses,
            "commissions_eur": commissions,
            "loss_ratio": loss_ratio,
            "customer_segment": _weighted_choice(segments, rng),
            "risk_zone": _weighted_choice(risk_zones, rng),
            "primary_coverage": _weighted_choice(coverages, rng),
            "currency": "EUR",
        }

        if country == "ES":
            row = {
                "id_registro": common_values["record_id"],
                "anio": common_values["reporting_year"],
                "mes": common_values["reporting_month"],
                "codigo_poliza": common_values["policy_number"],
                "tipo_riesgo": common_values["risk_type"],
                "provincia": common_values["region"],
                "canal_distribucion": common_values["distribution_channel"],
                "prima_neta": common_values["net_written_premium_eur"],
                "prima_bruta": common_values["gross_written_premium_eur"],
                "num_polizas_nuevas": common_values["new_policies_count"],
                "num_polizas_renovadas": common_values["renewed_policies_count"],
                "num_polizas_canceladas": common_values["cancelled_policies_count"],
                "num_siniestros_declarados": common_values["claims_reported_count"],
                "num_siniestros_pagados": common_values["claims_paid_count"],
                "importe_siniestros_bruto": common_values["gross_claims_incurred_eur"],
                "importe_reservas": common_values["claims_reserve_eur"],
                "gastos_gestion": common_values["management_expenses_eur"],
                "comisiones": common_values["commissions_eur"],
                "ratio_siniestralidad": common_values["loss_ratio"],
                "segmento_cliente": common_values["customer_segment"],
                "zona_riesgo": common_values["risk_zone"],
                "cobertura_principal": common_values["primary_coverage"],
                "moneda": common_values["currency"],
                "fecha_carga": _load_timestamp(rng, month),
            }
        else:
            row = {
                "id_riga": common_values["record_id"],
                "anno_riferimento": common_values["reporting_year"],
                "mese_riferimento": common_values["reporting_month"],
                "codice_polizza": common_values["policy_number"],
                "tipo_rischio": common_values["risk_type"],
                "regione": common_values["region"],
                "canale_distributivo": common_values["distribution_channel"],
                "premi_netti": common_values["net_written_premium_eur"],
                "premi_lordi_contabilizzati": common_values["gross_written_premium_eur"],
                "num_polizze_nuove": common_values["new_policies_count"],
                "num_polizze_rinnovate": common_values["renewed_policies_count"],
                "num_polizze_cancellate": common_values["cancelled_policies_count"],
                "sinistri_denunciati": common_values["claims_reported_count"],
                "sinistri_pagati": common_values["claims_paid_count"],
                "costo_sinistri_lordo": common_values["gross_claims_incurred_eur"],
                "riserva_sinistri": common_values["claims_reserve_eur"],
                "spese_gestione": common_values["management_expenses_eur"],
                "provvigioni": common_values["commissions_eur"],
                "rapporto_sinistri_premi": common_values["loss_ratio"],
                "segmento_cliente": common_values["customer_segment"],
                "zona_rischio": common_values["risk_zone"],
                "copertura_principale": common_values["primary_coverage"],
                "valuta": common_values["currency"],
                "codice_agenzia_interno": agency_code,
            }
        rows.append(row)

    return rows


def month_partitions(rows: list[dict]) -> dict[str, list[dict]]:
    """Group rows by reporting month, using ``YYYYMM`` keys."""
    year_fields = ("reporting_year", "anio", "anno_riferimento")
    month_fields = ("reporting_month", "mes", "mese_riferimento")
    partitions: dict[str, list[dict]] = {}
    for row in rows:
        year = next(row[field] for field in year_fields if field in row)
        month = next(row[field] for field in month_fields if field in row)
        partitions.setdefault(f"{year}{month:02d}", []).append(row)
    return dict(sorted(partitions.items()))


def to_csv(rows: list[dict], columns: list[str]) -> str:
    """Render rows as RFC-compatible CSV with a UTF-8 text header."""
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=columns, extrasaction="raise", lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def file_name(yyyymm: str) -> str:
    """Return the fixed landing-zone file name for a reporting month."""
    return f"property_monthly_{yyyymm}.csv"
