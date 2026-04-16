# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Generate Spain Raw Data
# MAGIC
# MAGIC > **DEMO DATA GENERATOR** — This notebook generates synthetic Spain property
# MAGIC > insurance data for the shipped example. When adapting to your own domain,
# MAGIC > replace this notebook with your own data ingestion step. All downstream
# MAGIC > notebooks (02-10) are domain-agnostic and driven by
# MAGIC > `config/harmonization_config.yaml`.
# MAGIC
# MAGIC Generates synthetic Spain property insurance monthly reporting data
# MAGIC and writes to `{catalog_name}.{schema_name}.property_insurance_monthly_raw`.
# MAGIC
# MAGIC The table has **24 Spanish-language columns** representing the local entity schema.

# COMMAND ----------

# MAGIC %run ./_shared_utils

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name",    "pt_catalog",        "Catalog Name")
dbutils.widgets.text("schema_name",     "harmonizing_agent", "Schema Name")
dbutils.widgets.text("mapping_version", "v1",                "Mapping Version")

catalog_name    = dbutils.widgets.get("catalog_name").strip()
schema_name     = dbutils.widgets.get("schema_name").strip()
mapping_version = dbutils.widgets.get("mapping_version").strip()

DB        = f"`{catalog_name}`.`{schema_name}`"
RAW_TABLE = f"{DB}.`property_insurance_monthly_raw`"
OPS_TABLE = f"{DB}.`workflow_run_metrics`"
N_ROWS    = 10_000

print(f"Config: {DB}, rows={N_ROWS:,}")

# COMMAND ----------

# MAGIC %md ## Imports

# COMMAND ----------

import random
import datetime
from uuid import uuid4

from pyspark.sql.types import (
    StructType, StructField,
    LongType, IntegerType, StringType, DoubleType, TimestampType
)

RUN_ID = str(uuid4())
_start = datetime.datetime.utcnow()

# COMMAND ----------

# MAGIC %md ## Weighted Lookup Tables

# COMMAND ----------

PROVINCIAS = [
    ("Madrid", 0.20), ("Barcelona", 0.18), ("Valencia", 0.10),
    ("Sevilla", 0.08), ("Bilbao", 0.07), ("Málaga", 0.06),
    ("Zaragoza", 0.05), ("Murcia", 0.05), ("Palma", 0.04),
    ("Las Palmas", 0.04), ("Alicante", 0.04), ("Valladolid", 0.03),
    ("Córdoba", 0.03), ("Vigo", 0.03),
]

TIPOS_RIESGO = [
    ("Incendio", 0.25), ("Inundación", 0.20), ("Robo", 0.15),
    ("Daños por Agua", 0.20), ("Responsabilidad Civil", 0.10),
    ("Daños Eléctricos", 0.10),
]

CANALES = [
    ("Agente", 0.35), ("Corredor", 0.25), ("Directo", 0.20),
    ("Bancaseguros", 0.15), ("Digital", 0.05),
]

SEGMENTOS = [
    ("Particular", 0.45), ("Pequeña Empresa", 0.30),
    ("Mediana Empresa", 0.15), ("Gran Empresa", 0.10),
]

ZONAS = [
    ("Zona A", 0.30), ("Zona B", 0.40), ("Zona C", 0.20), ("Zona D", 0.10),
]

COBERTURAS = [
    ("Edificio", 0.30), ("Contenido", 0.25),
    ("Ambos", 0.30), ("Responsabilidad Civil", 0.15),
]

MONEDA = "EUR"

def weighted_choice(options):
    vals, weights = zip(*options)
    return random.choices(vals, weights=weights, k=1)[0]

# COMMAND ----------

# MAGIC %md ## Generate Rows

# COMMAND ----------

random.seed(42)
rows = []

start_date = datetime.datetime(2022, 1, 1)
end_date   = datetime.datetime(2025, 12, 31)
date_range_days = (end_date - start_date).days

for i in range(1, N_ROWS + 1):
    anio = random.choice([2022, 2023, 2024, 2025])
    mes  = random.randint(1, 12)

    prima_bruta         = round(random.uniform(300.0, 8000.0), 2)
    net_multiplier      = random.uniform(0.75, 0.97)
    prima_neta          = round(min(prima_bruta, prima_bruta * net_multiplier), 2)

    num_polizas_nuevas     = random.randint(0, 15)
    num_polizas_renovadas  = random.randint(5, 60)
    num_polizas_canceladas = random.randint(0, 5)

    num_siniestros_declarados = random.randint(0, 8)
    num_siniestros_pagados    = random.randint(0, num_siniestros_declarados)

    importe_siniestros_bruto = round(prima_bruta * random.uniform(0.10, 1.20), 2)
    importe_reservas         = round(importe_siniestros_bruto * random.uniform(0.05, 0.30), 2)
    gastos_gestion           = round(prima_bruta * random.uniform(0.03, 0.12), 2)
    comisiones               = round(prima_bruta * random.uniform(0.05, 0.18), 2)
    ratio_siniestralidad     = round(importe_siniestros_bruto / prima_bruta, 4) if prima_bruta > 0 else 0.0

    load_offset = random.randint(0, date_range_days)
    fecha_carga = start_date + datetime.timedelta(days=load_offset)

    rows.append((
        i, anio, mes,
        f"ES-{i:05d}-{anio}",
        weighted_choice(TIPOS_RIESGO), weighted_choice(PROVINCIAS), weighted_choice(CANALES),
        prima_neta, prima_bruta,
        num_polizas_nuevas, num_polizas_renovadas, num_polizas_canceladas,
        num_siniestros_declarados, num_siniestros_pagados,
        importe_siniestros_bruto, importe_reservas, gastos_gestion, comisiones,
        ratio_siniestralidad,
        weighted_choice(SEGMENTOS), weighted_choice(ZONAS), weighted_choice(COBERTURAS),
        MONEDA, fecha_carga,
    ))

print(f"Generated {len(rows):,} rows in Python.")

# COMMAND ----------

# MAGIC %md ## Create DataFrame and Write to Delta

# COMMAND ----------

schema = StructType([
    StructField("id_registro",               LongType(),      False),
    StructField("anio",                      IntegerType(),   False),
    StructField("mes",                       IntegerType(),   False),
    StructField("codigo_poliza",             StringType(),    False),
    StructField("tipo_riesgo",               StringType(),    False),
    StructField("provincia",                 StringType(),    False),
    StructField("canal_distribucion",        StringType(),    False),
    StructField("prima_neta",                DoubleType(),    False),
    StructField("prima_bruta",               DoubleType(),    False),
    StructField("num_polizas_nuevas",        IntegerType(),   False),
    StructField("num_polizas_renovadas",     IntegerType(),   False),
    StructField("num_polizas_canceladas",    IntegerType(),   False),
    StructField("num_siniestros_declarados", IntegerType(),   False),
    StructField("num_siniestros_pagados",    IntegerType(),   False),
    StructField("importe_siniestros_bruto",  DoubleType(),    False),
    StructField("importe_reservas",          DoubleType(),    False),
    StructField("gastos_gestion",            DoubleType(),    False),
    StructField("comisiones",                DoubleType(),    False),
    StructField("ratio_siniestralidad",      DoubleType(),    False),
    StructField("segmento_cliente",          StringType(),    False),
    StructField("zona_riesgo",               StringType(),    False),
    StructField("cobertura_principal",       StringType(),    False),
    StructField("moneda",                    StringType(),    False),
    StructField("fecha_carga",               TimestampType(), False),
])

raw_df = spark.createDataFrame(rows, schema=schema)

(
    raw_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(RAW_TABLE)
)

final_count = spark.table(RAW_TABLE).count()
print(f"Written {final_count:,} rows to {RAW_TABLE}")

# COMMAND ----------

# MAGIC %md ## Sample Output

# COMMAND ----------

display(spark.table(RAW_TABLE).limit(5))

# COMMAND ----------

# MAGIC %md ## Log to workflow_run_metrics

# COMMAND ----------

log_run_metric(spark, OPS_TABLE, RUN_ID, "generate_spain_raw_data", "SUCCEEDED", _start, final_count,
               f"Generated {final_count:,} synthetic Spain property insurance rows. mapping_version={mapping_version}")
