"""Pure-Python data generation logic extracted from notebook 01.

This module contains the lookup tables, weighted random choice helper,
and row generation function used to create synthetic Spain property
insurance data. It does not depend on PySpark.
"""

import random
import datetime

PROVINCIAS = [
    ("Madrid", 0.20),
    ("Barcelona", 0.18),
    ("Valencia", 0.10),
    ("Sevilla", 0.08),
    ("Bilbao", 0.07),
    ("Malaga", 0.06),
    ("Zaragoza", 0.05),
    ("Murcia", 0.05),
    ("Palma", 0.04),
    ("Las Palmas", 0.04),
    ("Alicante", 0.04),
    ("Valladolid", 0.03),
    ("Cordoba", 0.03),
    ("Vigo", 0.03),
]

TIPOS_RIESGO = [
    ("Incendio", 0.25),
    ("Inundacion", 0.20),
    ("Robo", 0.15),
    ("Danos por Agua", 0.20),
    ("Responsabilidad Civil", 0.10),
    ("Danos Electricos", 0.10),
]

CANALES = [
    ("Agente", 0.35),
    ("Corredor", 0.25),
    ("Directo", 0.20),
    ("Bancaseguros", 0.15),
    ("Digital", 0.05),
]

SEGMENTOS = [
    ("Particular", 0.45),
    ("Pequena Empresa", 0.30),
    ("Mediana Empresa", 0.15),
    ("Gran Empresa", 0.10),
]

ZONAS = [
    ("Zona A", 0.30),
    ("Zona B", 0.40),
    ("Zona C", 0.20),
    ("Zona D", 0.10),
]

COBERTURAS = [
    ("Edificio", 0.30),
    ("Contenido", 0.25),
    ("Ambos", 0.30),
    ("Responsabilidad Civil", 0.15),
]

MONEDA = "EUR"


def weighted_choice(options: list[tuple[str, float]]) -> str:
    """Pick a random value from weighted options.

    Args:
        options: list of (value, weight) tuples.

    Returns:
        A randomly selected value string.
    """
    vals, weights = zip(*options)
    result: str = random.choices(vals, weights=weights, k=1)[0]
    return result


def generate_raw_rows(n: int, seed: int = 42) -> list[dict]:
    """Generate n rows of synthetic Spain property insurance data.

    Returns a list of dicts with 24 fields matching the raw table schema.
    Does not depend on PySpark.
    """
    random.seed(seed)
    rows = []

    start_date = datetime.datetime(2022, 1, 1)
    end_date = datetime.datetime(2025, 12, 31)
    date_range_days = (end_date - start_date).days

    for i in range(1, n + 1):
        anio = random.choice([2022, 2023, 2024, 2025])
        mes = random.randint(1, 12)

        prima_bruta = round(random.uniform(300.0, 8000.0), 2)
        net_multiplier = random.uniform(0.75, 0.97)
        prima_neta = round(min(prima_bruta, prima_bruta * net_multiplier), 2)

        num_polizas_nuevas = random.randint(0, 15)
        num_polizas_renovadas = random.randint(5, 60)
        num_polizas_canceladas = random.randint(0, 5)

        num_siniestros_declarados = random.randint(0, 8)
        num_siniestros_pagados = random.randint(0, num_siniestros_declarados)

        importe_siniestros_bruto = round(prima_bruta * random.uniform(0.10, 1.20), 2)
        importe_reservas = round(importe_siniestros_bruto * random.uniform(0.05, 0.30), 2)
        gastos_gestion = round(prima_bruta * random.uniform(0.03, 0.12), 2)
        comisiones = round(prima_bruta * random.uniform(0.05, 0.18), 2)
        ratio_siniestralidad = round(importe_siniestros_bruto / prima_bruta, 4) if prima_bruta > 0 else 0.0

        load_offset = random.randint(0, date_range_days)
        fecha_carga = start_date + datetime.timedelta(days=load_offset)

        rows.append(
            {
                "id_registro": i,
                "anio": anio,
                "mes": mes,
                "codigo_poliza": f"ES-{i:05d}-{anio}",
                "tipo_riesgo": weighted_choice(TIPOS_RIESGO),
                "provincia": weighted_choice(PROVINCIAS),
                "canal_distribucion": weighted_choice(CANALES),
                "prima_neta": prima_neta,
                "prima_bruta": prima_bruta,
                "num_polizas_nuevas": num_polizas_nuevas,
                "num_polizas_renovadas": num_polizas_renovadas,
                "num_polizas_canceladas": num_polizas_canceladas,
                "num_siniestros_declarados": num_siniestros_declarados,
                "num_siniestros_pagados": num_siniestros_pagados,
                "importe_siniestros_bruto": importe_siniestros_bruto,
                "importe_reservas": importe_reservas,
                "gastos_gestion": gastos_gestion,
                "comisiones": comisiones,
                "ratio_siniestralidad": ratio_siniestralidad,
                "segmento_cliente": weighted_choice(SEGMENTOS),
                "zona_riesgo": weighted_choice(ZONAS),
                "cobertura_principal": weighted_choice(COBERTURAS),
                "moneda": MONEDA,
                "fecha_carga": fecha_carga,
            }
        )

    return rows
