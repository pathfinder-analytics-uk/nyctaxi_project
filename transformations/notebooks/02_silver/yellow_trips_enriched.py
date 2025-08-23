# Databricks notebook source
import sys
import os
# Go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from datetime import date
from dateutil.relativedelta import relativedelta
from modules.utils.date_utils import get_month_start_n_months_ago

# COMMAND ----------

# Get the first day of the month two months ago
two_months_ago_start = get_month_start_n_months_ago(2)

# COMMAND ----------

# Load cleansed yellow taxi trip data from the Silver layer
# and filter to only include trips with a pickup datetime
# later than the start date from two months ago
df_trips = spark.read.table("nyctaxi.02_silver.yellow_trips_cleansed").filter(f"tpep_pickup_datetime > '{two_months_ago_start}'")

# Load taxi zone lookup data from the Silver layer
df_zones = spark.read.table("nyctaxi.02_silver.taxi_zone_lookup")

# COMMAND ----------

# Join trips with pickup zone details (borough and zone name)
df_join_1 = df_trips.join(
                df_zones, 
                df_trips.pu_location_id == df_zones.location_id,
                "left"
                ).select(
                    df_trips.vendor,
                    df_trips.tpep_pickup_datetime,
                    df_trips.tpep_dropoff_datetime,
                    df_trips.trip_duration,
                    df_trips.passenger_count,
                    df_trips.trip_distance,
                    df_trips.rate_type,
                    df_zones.borough.alias("pu_borough"),   # pickup borough
                    df_zones.zone.alias("pu_zone"),         # pickup zone
                    df_trips.do_location_id,                # dropoff location ID for next join
                    df_trips.payment_type,
                    df_trips.fare_amount,
                    df_trips.extra,
                    df_trips.mta_tax,
                    df_trips.tolls_amount,
                    df_trips.improvement_surcharge,
                    df_trips.total_amount,
                    df_trips.congestion_surcharge,
                    df_trips.airport_fee,  
                    df_trips.cbd_congestion_fee,
                    df_trips.processed_timestamp
                )

# COMMAND ----------

# Join the result with dropoff zone details to complete enrichment
df_join_final = df_join_1.join(
                                df_zones, 
                                df_join_1.do_location_id == df_zones.location_id,
                                "left"
                                ).select(
                                            df_join_1.vendor,
                                            df_join_1.tpep_pickup_datetime,
                                            df_join_1.tpep_dropoff_datetime,
                                            df_trips.trip_duration,
                                            df_join_1.passenger_count,
                                            df_join_1.trip_distance,
                                            df_join_1.rate_type,
                                            df_join_1.pu_borough,
                                            df_zones.borough.alias("do_borough"), # dropoff borough
                                            df_join_1.pu_zone,
                                            df_zones.zone.alias("do_zone"),       # dropoff zone
                                            df_join_1.payment_type,
                                            df_join_1.fare_amount,
                                            df_join_1.extra,
                                            df_join_1.mta_tax,
                                            df_join_1.tolls_amount,
                                            df_join_1.improvement_surcharge,
                                            df_join_1.total_amount,
                                            df_join_1.congestion_surcharge,
                                            df_join_1.airport_fee,  
                                            df_join_1.cbd_congestion_fee,
                                            df_join_1.processed_timestamp
                                )

# COMMAND ----------

# Write the enriched dataset to a Unity Catalog managed Delta table in the silver schema
df_join_final.write.mode("append").saveAsTable("nyctaxi.02_silver.yellow_trips_enriched")