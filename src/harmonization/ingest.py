"""Pure helpers for Lakeflow country-feed ingestion."""

from __future__ import annotations

# (year column, month column) per country, as written by examples/generate_country_files.py
PERIOD_COLUMNS = {
    "es": ("anio", "mes"),
    "it": ("anno_riferimento", "mese_riferimento"),
}

PREMIUM_COLUMNS = {
    "es": "prima_bruta",
    "it": "premi_lordi_contabilizzati",
}


def bronze_table_name(cc: str) -> str:
    """Return the unqualified bronze table name for a country code."""
    return f"bronze_property_monthly_{cc}"


def landing_path(catalog: str, schema: str, cc: str) -> str:
    """Return the Auto Loader landing path for a country code."""
    return f"/Volumes/{catalog}/{schema}/landing/{cc}"


def parse_countries(raw_countries: str) -> list[str]:
    """Parse a comma-separated country-code list in a stable, lowercase order."""
    countries: list[str] = []
    for value in raw_countries.split(","):
        country = value.strip().lower()
        if not country or not country.isalpha():
            raise ValueError(f"Invalid country code: {value.strip()!r}")
        if country not in countries:
            countries.append(country)
    return countries


def expectations_for(cc: str) -> dict[str, dict[str, str]]:
    """Return warn and drop expectation SQL for a supported country."""
    try:
        year_column, month_column = PERIOD_COLUMNS[cc]
        premium_column = PREMIUM_COLUMNS[cc]
    except KeyError as error:
        raise ValueError(f"Unsupported country code: {cc}") from error

    return {
        "warn": {"valid_period": f"{year_column} IS NOT NULL AND {month_column} BETWEEN 1 AND 12"},
        "drop": {"has_premium": f"{premium_column} IS NOT NULL AND {premium_column} >= 0"},
    }
