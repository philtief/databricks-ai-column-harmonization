# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Generate Spain Raw Data
# MAGIC
# MAGIC Generates 10,000 rows of synthetic Spain property insurance monthly reporting data
# MAGIC and writes them to `{catalog_name}.{schema_name}.property_insurance_monthly_raw`.
# MAGIC
# MAGIC The table has **24 Spanish-language columns** representing the local entity schema.

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

RAW_TABLE  = f"`{catalog_name}`.`{schema_name}`.`property_insurance_monthly_raw`"
OPS_TABLE  = f"`{catalog_name}`.`{schema_name}`.`workflow_run_metrics`"
N_ROWS     = 10_000
SOURCE_SYSTEM = "ES_PROPERTY_RAW"

print("STEP 1 — Parameters loaded")
print(f"  catalog_name    : {catalog_name}")
print(f"  schema_name     : {schema_name}")
print(f"  mapping_version : {mapping_version}")
print(f"  raw_table       : {RAW_TABLE}")
print(f"  target_rows     : {N_ROWS:,}")

# COMMAND ----------

# MAGIC %md ## STEP 2 — Imports

# COMMAND ----------

import random
import datetime
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, IntegerType, StringType, DoubleType, TimestampType
)

RUN_ID = str(uuid4())
print(f"STEP 2 — Imports done. RUN_ID = {RUN_ID}")

# COMMAND ----------

# MAGIC %md ## STEP 3 — Define Weighted Lookup Tables

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

print("STEP 3 — Lookup tables defined.")
print(f"  Provincias   : {len(PROVINCIAS)}")
print(f"  Tipos riesgo : {len(TIPOS_RIESGO)}")
print(f"  Canales      : {len(CANALES)}")
print(f"  Segmentos    : {len(SEGMENTOS)}")
print(f"  Zonas        : {len(ZONAS)}")
print(f"  Coberturas   : {len(COBERTURAS)}")

# COMMAND ----------

# MAGIC %md ## STEP 4 — Generate Rows

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
        i,                                         # id_registro
        anio,                                      # anio
        mes,                                       # mes
        f"ES-{i:05d}-{anio}",                      # codigo_poliza
        weighted_choice(TIPOS_RIESGO),             # tipo_riesgo
        weighted_choice(PROVINCIAS),               # provincia
        weighted_choice(CANALES),                  # canal_distribucion
        prima_neta,                                # prima_neta
        prima_bruta,                               # prima_bruta
        num_polizas_nuevas,                        # num_polizas_nuevas
        num_polizas_renovadas,                     # num_polizas_renovadas
        num_polizas_canceladas,                    # num_polizas_canceladas
        num_siniestros_declarados,                 # num_siniestros_declarados
        num_siniestros_pagados,                    # num_siniestros_pagados
        importe_siniestros_bruto,                  # importe_siniestros_bruto
        importe_reservas,                          # importe_reservas
        gastos_gestion,                            # gastos_gestion
        comisiones,                                # comisiones
        ratio_siniestralidad,                      # ratio_siniestralidad
        weighted_choice(SEGMENTOS),                # segmento_cliente
        weighted_choice(ZONAS),                    # zona_riesgo
        weighted_choice(COBERTURAS),               # cobertura_principal
        MONEDA,                                    # moneda
        fecha_carga,                               # fecha_carga
    ))

print(f"STEP 4 — Generated {len(rows):,} rows in Python.")

# COMMAND ----------

# MAGIC %md ## STEP 5 — Create DataFrame and Write

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

print(f"STEP 5 — DataFrame created: {raw_df.count():,} rows, {len(raw_df.columns)} columns.")
print("  Columns:", raw_df.columns)

# COMMAND ----------

# MAGIC %md ## STEP 6 — Write to Delta Table

# COMMAND ----------

print(f"STEP 6 — Writing to {RAW_TABLE} (overwrite + overwriteSchema) ...")

(
    raw_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(RAW_TABLE)
)

final_count = spark.table(RAW_TABLE).count()
print(f"  Written rows : {final_count:,}")
print(f"  Table        : {RAW_TABLE}")

# COMMAND ----------

# MAGIC %md ## STEP 7 — Sample Output

# COMMAND ----------

print("STEP 7 — Sample rows from raw table:")
display(spark.table(RAW_TABLE).limit(5))

# COMMAND ----------

# MAGIC %md ## STEP 8 — Log to workflow_run_metrics

# COMMAND ----------

import datetime as _dt

log_rows = [(
    RUN_ID,
    "PT_ES_Column_Mapping_To_Global_Model",
    "generate_spain_raw_data",
    "SUCCEEDED",
    _dt.datetime.utcnow(),
    _dt.datetime.utcnow(),
    final_count,
    f"Generated {final_count:,} synthetic Spain property insurance rows. mapping_version={mapping_version}",
)]

log_schema = StructType([
    StructField("run_id",        StringType(),    False),
    StructField("workflow_name", StringType(),    True),
    StructField("task_name",     StringType(),    True),
    StructField("task_status",   StringType(),    True),
    StructField("started_at",    TimestampType(), True),
    StructField("finished_at",   TimestampType(), True),
    StructField("row_count",     LongType(),      True),
    StructField("message",       StringType(),    True),
])

log_df = spark.createDataFrame(log_rows, schema=log_schema)
log_df.write.format("delta").mode("append").saveAsTable(OPS_TABLE)

print(f"STEP 8 — Logged run record to {OPS_TABLE}. RUN_ID={RUN_ID}")
print()
print("=" * 60)
print(f"  01_generate_spain_raw_data COMPLETE")
print(f"  Rows written : {final_count:,}")
print(f"  Table        : {RAW_TABLE}")
print("=" * 60)
