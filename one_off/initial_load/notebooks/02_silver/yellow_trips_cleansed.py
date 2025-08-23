# Databricks notebook source
from pyspark.sql.functions import col, when, timestamp_diff

# COMMAND ----------

# Read raw trip data from the bronze table
df = spark.read.table("nyctaxi.01_bronze.yellow_trips_raw")

# COMMAND ----------

# Filter trips to ensure they align with the date range for the parquet files
# In this example I expect the tpep_pickup_datetime to be within January 2025 and June 2025 as these are the parquet files I initially loaded
df = df.filter("tpep_pickup_datetime >= '2024-12-01' AND tpep_pickup_datetime < '2025-06-01'")

# COMMAND ----------

# Select and transform fields, decoding codes and computing duration
df = df.select(
    # Map numeric VendorID to vendor names
    when(col("VendorID") == 1, "Creative Mobile Technologies, LLC")
      .when(col("VendorID") == 2, "Curb Mobility, LLC")
      .when(col("VendorID") == 6, "Myle Technologies Inc")
      .when(col("VendorID") == 7, "Helix")
      .otherwise("Unknown")
      .alias("vendor"),
    
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    # Calculate trip duration in minutes
    timestamp_diff('MINUTE', df.tpep_pickup_datetime, df.tpep_dropoff_datetime).alias("trip_duration"),
    "passenger_count",
    "trip_distance",

    # Decode rate codes into readable rate types
    when(col("RatecodeID") == 1, "Standard Rate")
      .when(col("RatecodeID") == 2, "JFK")
      .when(col("RatecodeID") == 3, "Newark")
      .when(col("RatecodeID") == 4, "Nassau or Westchester")
      .when(col("RatecodeID") == 5, "Negotiated Fare")
      .when(col("RatecodeID") == 6, "Group Ride")
      .otherwise("Unknown")
      .alias("rate_type"),
    
    "store_and_fwd_flag",
    # alias columns for consistent naming convention
    col("PULocationID").alias("pu_location_id"),
    col("DOLocationID").alias("do_location_id"),
    
    # Decode payment types
    when(col("payment_type") == 0, "Flex Fare trip")
      .when(col("payment_type") == 1, "Credit card")
      .when(col("payment_type") == 2, "Cash")
      .when(col("payment_type") == 3, "No charge")
      .when(col("payment_type") == 4, "Dispute")
      .when(col("payment_type") == 6, "Voided trip")
      .otherwise("Unknown")
      .alias("payment_type"),
    
    "fare_amount",
    "extra",
    "mta_tax",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    # alias columns for consistent naming convention
    col("Airport_fee").alias("airport_fee"),
    "cbd_congestion_fee",
    "processed_timestamp"
)

# COMMAND ----------

# Write cleansed data to a Unity Catalog managed Delta table in the silver schema, overwriting existing data
df.write.mode("overwrite").saveAsTable("nyctaxi.02_silver.yellow_trips_cleansed")