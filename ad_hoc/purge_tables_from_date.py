# Databricks notebook source
date_from = '2025-06-01'

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "nyctaxi.`01_bronze`.yellow_trips_raw")

dt.delete(f"tpep_pickup_datetime >= '{date_from}'")

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "nyctaxi.`02_silver`.yellow_trips_cleansed")

dt.delete(f"tpep_pickup_datetime >= '{date_from}'")

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "nyctaxi.`02_silver`.yellow_trips_enriched")

dt.delete(f"tpep_pickup_datetime >= '{date_from}'")

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "nyctaxi.`03_gold`.daily_trip_summary")

dt.delete(f"pickup_date >= '{date_from}'")
