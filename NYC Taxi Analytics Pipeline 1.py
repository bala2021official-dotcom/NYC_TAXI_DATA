# Databricks notebook source
# ================================================================
# NYC Taxi Analytics Pipeline (Bronze → Silver → Gold)
# Author: Bala
# Timezone: All timestamps converted from UTC to IST (Asia/Kolkata)
# UC-safe: Use Auto Loader _metadata.file_path instead of input_file_name()
# ================================================================

import dlt
from pyspark.sql.functions import (
    col, to_timestamp, when, current_timestamp, avg, count, sum as _sum,
    struct, from_utc_timestamp, unix_timestamp, hour, lit, concat
)
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Keep session TZ as UTC and explicitly convert to IST everywhere for consistency
spark.conf.set("spark.sql.session.timeZone", "UTC")
IST_TZ = "Asia/Kolkata"

# -----------------------------
# 1) BRONZE TABLE (Streaming)
# -----------------------------
@dlt.table(
    name="nyc_taxi_cat.bronze_sch.bronze_nyc_taxi_tab",
    comment="Bronze: Raw NYC Taxi data with IST ingestion timestamp & UC Auto Loader metadata",
    table_properties={"quality": "bronze"}
)
def bronze_nyc_taxi_tab():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("/Volumes/nyc_taxi_cat/bronze_sch/nyc_taxi_path_val/taxi_raw_dir/")
        # UC-safe source file path from Auto Loader metadata
        .withColumn("source_file", col("_metadata.file_path"))
        # IST ingestion time
        .withColumn("ingestion_timestamp", from_utc_timestamp(current_timestamp(), IST_TZ))
        .drop("_rescued_data")
        # Watermark on ingestion time for any stateful ops (e.g., dedup) downstream
        .withWatermark("ingestion_timestamp", "10 minutes")
    )

# -----------------------------
# 2) TAXI ZONE LOOKUP (Static)
# -----------------------------
@dlt.table(
    name="nyc_taxi_cat.silver_sch.taxi_zone_lookup_tab",
    comment="Taxi Zone Lookup for enrichment"
)
def taxi_zone_lookup_tab():
    data = [
        ("Bronx", "Zone-01"),
        ("Brooklyn", "Zone-02"),
        ("Manhattan", "Zone-03"),
        ("Queens", "Zone-04"),
        ("Staten Island", "Zone-05")
    ]
    return spark.createDataFrame(data, ["zone_name", "zone_id"])

# -----------------------------
# Helper: sanitize numeric columns (defensive casting)
# -----------------------------
def sanitize_numeric(df):
    for c, t in [
        ("trip_distance", DoubleType()),
        ("fare_amount",   DoubleType()),
        ("tip_amount",    DoubleType()),
        ("total_amount",  DoubleType()),
        ("passenger_count", IntegerType()),
        ("driver_id", StringType()),
    ]:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(t))
    return df

# -----------------------------
# 3) SILVER TABLE (Streaming)
#    - Deduplicate by trip_id (keeps first occurrence)
#    - Invalid speed/duration rows dropped via expectations
# -----------------------------
@dlt.table(
    name="nyc_taxi_cat.silver_sch.silver_nyc_taxi_tab",
    comment="Silver: Cleaned, validated, and de-duplicated NYC Taxi data (IST timestamps)",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_speed", "speed <= 120")
@dlt.expect_or_drop("valid_duration", "trip_duration <= 180")
def silver_nyc_taxi_tab():
    bronze_df = dlt.read_stream("nyc_taxi_cat.bronze_sch.bronze_nyc_taxi_tab")

    # Defensive cast & require non-null trip_id
    bronze_df = sanitize_numeric(bronze_df).filter(col("trip_id").isNotNull())

    # Transform with IST datetimes
    transformed = (
        bronze_df
        .withColumn("pickup_datetime_utc", to_timestamp(col("pickup_datetime"), "dd-MM-yyyy HH:mm"))
        .withColumn("dropoff_datetime_utc", to_timestamp(col("dropoff_datetime"), "dd-MM-yyyy HH:mm"))
        .withColumn("pickup_datetime", from_utc_timestamp(col("pickup_datetime_utc"), IST_TZ))
        .withColumn("dropoff_datetime", from_utc_timestamp(col("dropoff_datetime_utc"), IST_TZ))
        .withColumn("trip_duration", (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60.0)
        .withColumn("speed", when(col("trip_duration") > 0, col("trip_distance") / (col("trip_duration") / 60.0)).otherwise(None))
        .withColumn("total_amount", when(col("total_amount").isNull(), col("fare_amount") + col("tip_amount")).otherwise(col("total_amount")))
        .withColumn("revenue_valid", when(col("total_amount") >= col("fare_amount"), lit(True)).otherwise(lit(False)))
    )

    # Deduplicate by trip_id (stateful—bounded by Bronze watermark)
    deduped = transformed.dropDuplicates(["trip_id"])

    # Enrichment with lookup (left join)
    enriched = (
        deduped
        .join(dlt.read("nyc_taxi_cat.silver_sch.taxi_zone_lookup_tab"), col("pickup_zone") == col("zone_name"), "left")
        .withColumn("zone_id", col("zone_id"))
        .withColumn("processing_timestamp", from_utc_timestamp(current_timestamp(), IST_TZ))
        .drop("_rescued_data", "pickup_datetime_utc", "dropoff_datetime_utc")
        .withWatermark("pickup_datetime", "10 minutes")
    )

    return enriched

# -----------------------------
# 4) ERROR LOG (Streaming) — keep the same table type
#    - INVALID: speed > 120 OR trip_duration > 180
#    - ANOMALY: trip_id IS NULL
# (Duplicates will be captured in a separate materialized table.)
# -----------------------------
@dlt.table(
    name="nyc_taxi_cat.error_lg_sch.error_log_tab",
    comment="Error: Invalid data (speed/duration) and anomalies (IST timestamps)",
    table_properties={"quality": "error"}
)
def error_log_tab():
    bronze_df = dlt.read_stream("nyc_taxi_cat.bronze_sch.bronze_nyc_taxi_tab") \
        .withWatermark("ingestion_timestamp", "10 minutes")
    bronze_df = sanitize_numeric(bronze_df)

    # Transform enough to evaluate speed/duration
    transformed = (
        bronze_df
        .withColumn("pickup_datetime_utc", to_timestamp(col("pickup_datetime"), "dd-MM-yyyy HH:mm"))
        .withColumn("dropoff_datetime_utc", to_timestamp(col("dropoff_datetime"), "dd-MM-yyyy HH:mm"))
        .withColumn("pickup_datetime", from_utc_timestamp(col("pickup_datetime_utc"), IST_TZ))
        .withColumn("dropoff_datetime", from_utc_timestamp(col("dropoff_datetime_utc"), IST_TZ))
        .withColumn("trip_duration", (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60.0)
        .withColumn("speed", when(col("trip_duration") > 0, col("trip_distance") / (col("trip_duration") / 60.0)).otherwise(None))
    )

    # INVALID rows
    invalid_rows = (
        transformed.filter((col("speed") > 120) | (col("trip_duration") > 180))
        .withColumn("error_reason", when(col("speed") > 120, lit("Invalid Speed")).otherwise(lit("Invalid Duration")))
        .withColumn("error_category", lit("INVALID"))
        .withColumn("row_data", struct([col(c) for c in bronze_df.columns]))
        .withColumn("error_timestamp", from_utc_timestamp(current_timestamp(), IST_TZ))
        .drop("_rescued_data", "pickup_datetime_utc", "dropoff_datetime_utc")
    )

    # ANOMALY: Missing trip_id
    null_trip_id = (
        bronze_df.filter(col("trip_id").isNull())
        .withColumn("error_reason", lit("Missing trip_id"))
        .withColumn("error_category", lit("ANOMALY"))
        .withColumn("row_data", struct([col(c) for c in bronze_df.columns]))
        .withColumn("error_timestamp", from_utc_timestamp(current_timestamp(), IST_TZ))
        .drop("_rescued_data")
    )

    return invalid_rows.unionByName(null_trip_id, allowMissingColumns=True)

# -----------------------------
# 5) DUPLICATE LOG (Materialized View)
#    - DUPLICATE: row_number > 1 over ingestion_timestamp per trip_id
# -----------------------------
@dlt.table(
    name="nyc_taxi_cat.error_lg_sch.duplicate_log_tab",
    comment="Duplicate rows by trip_id based on ingestion order (IST timestamps)"
)
def duplicate_log_tab():
    bronze_batch = dlt.read("nyc_taxi_cat.bronze_sch.bronze_nyc_taxi_tab")
    bronze_batch = sanitize_numeric(bronze_batch)

    w = Window.partitionBy("trip_id").orderBy(col("ingestion_timestamp").asc())
    with_rn = bronze_batch.withColumn("rn", row_number().over(w))

    duplicates = (
        with_rn.filter(col("trip_id").isNotNull() & (col("rn") > 1))
        .drop("rn")
        .withColumn("error_reason", lit("Duplicate trip_id"))
        .withColumn("error_category", lit("DUPLICATE"))
        .withColumn("row_data", struct([col(c) for c in bronze_batch.columns]))
        .withColumn("error_timestamp", from_utc_timestamp(current_timestamp(), IST_TZ))
    )

    return duplicates

# -----------------------------
# 6) GOLD: Zone-wise Revenue (Streaming)
# -----------------------------
@dlt.table(
    name="nyc_taxi_cat.gold_sch.gold_zone_revenue_tab",
    comment="Gold: Zone-wise revenue & KPIs (IST)",
    table_properties={"quality": "gold"}
)
def gold_zone_revenue_tab():
    df = dlt.read_stream("nyc_taxi_cat.silver_sch.silver_nyc_taxi_tab")
    return (
        df.groupBy("pickup_zone", "dropoff_zone", "zone_id").agg(
            _sum("total_amount").alias("total_revenue"),
            count("trip_id").alias("trip_count"),
            avg("fare_amount").alias("avg_fare"),
            avg("trip_duration").alias("avg_duration"),
            avg("trip_distance").alias("avg_distance")
        )
        .withColumn("processing_timestamp", from_utc_timestamp(current_timestamp(), IST_TZ))
    )

# -----------------------------
# 7) GOLD: Peak Hours (Streaming)
# -----------------------------
@dlt.table(
    name="nyc_taxi_cat.gold_sch.gold_peak_hours_tab",
    comment="Gold: Peak demand hours (IST)",
    table_properties={"quality": "gold"}
)
def gold_peak_hours_tab():
    df = dlt.read_stream("nyc_taxi_cat.silver_sch.silver_nyc_taxi_tab")
    return (
        df.withColumn("pickup_hour", hour(col("pickup_datetime")))
          .groupBy("pickup_hour")
          .agg(count("trip_id").alias("trips"))
          .withColumn("processing_timestamp", from_utc_timestamp(current_timestamp(), IST_TZ))
    )

# -----------------------------
# 8) GOLD: Driver Performance (Streaming)
# -----------------------------
@dlt.table(
    name="nyc_taxi_cat.gold_sch.gold_driver_perf_tab",
    comment="Gold: Driver performance metrics (IST)",
    table_properties={"quality": "gold"}
)
def gold_driver_perf_tab():
    df = dlt.read_stream("nyc_taxi_cat.silver_sch.silver_nyc_taxi_tab")
    return (
        df.groupBy("driver_id")
          .agg(
              avg("total_amount").alias("avg_fare"),
              count("trip_id").alias("total_trips")
          )
          .withColumn("processing_timestamp", from_utc_timestamp(current_timestamp(), IST_TZ))
    )

# -----------------------------
# 9) PIPELINE LOG (Materialized View)
# -----------------------------
@dlt.table(
    name="nyc_taxi_cat.pipeline_log_sch.pipeline_log_tab",
    comment="Pipeline log: counts & IST timestamp"
)
def pipeline_log_tab():
    silver_counts = dlt.read("nyc_taxi_cat.silver_sch.silver_nyc_taxi_tab").select(count("*").alias("ProcessedRecordCount"))
    invalid_anomaly_counts = dlt.read("nyc_taxi_cat.error_lg_sch.error_log_tab").select(count("*").alias("InvalidAnomalyCount"))
    duplicate_counts  = dlt.read("nyc_taxi_cat.error_lg_sch.duplicate_log_tab").select(count("*").alias("DuplicateCount"))

    log_df = (
        silver_counts.crossJoin(invalid_anomaly_counts).crossJoin(duplicate_counts)
        .withColumn("RunId", concat(lit("Run-"), unix_timestamp(current_timestamp())))
        .withColumn("Timestamp", from_utc_timestamp(current_timestamp(), IST_TZ))
        .withColumn("Status", lit("SUCCESS"))
        .withColumn("FileName", lit("N/A"))
    )

    return log_df.select("RunId", "Timestamp", "Status", "ProcessedRecordCount", "InvalidAnomalyCount", "DuplicateCount", "FileName")













# # ================================================================
# # NYC Taxi Analytics Pipeline (Bronze → Silver → Gold)
# # Author: Bala
# # Purpose: End-to-End Data Engineering Pipeline using Delta Live Tables with Streaming
# # ================================================================

# import dlt
# from pyspark.sql.functions import (
#     col, to_timestamp, when, current_timestamp, avg, count, sum as _sum,
#     struct, from_utc_timestamp, unix_timestamp
# )
# from datetime import datetime

# # -----------------------------
# # 1. BRONZE TABLE (Streaming)
# # -----------------------------
# @dlt.table(
#     name="nyc_taxi_cat.bronze_sch.bronze_nyc_taxi_tab",
#     comment="Bronze table: Raw NYC Taxi data with ingestion timestamp",
#     table_properties={"quality": "bronze"}
# )
# def bronze_nyc_taxi_tab():
#     return (
#         spark.readStream.format("cloudFiles")
#         .option("cloudFiles.format", "csv")
#         .option("header", "true")
#         .load("/Volumes/nyc_taxi_cat/bronze_sch/nyc_taxi_path_val/taxi_raw_dir/")
#         .withColumn("ingestion_timestamp", from_ist_timestamp(current_timestamp(), "Asia/Kolkata"))
#         .drop("_rescued_data")
#     )

# # -----------------------------
# # 2. TAXI ZONE LOOKUP (Static)
# # -----------------------------
# @dlt.table(
#     name="nyc_taxi_cat.silver_sch.taxi_zone_lookup_tab",
#     comment="Taxi Zone Lookup table for enrichment"
# )
# def taxi_zone_lookup_tab():
#     data = [
#         ("Bronx", "Zone-01"),
#         ("Brooklyn", "Zone-02"),
#         ("Manhattan", "Zone-03"),
#         ("Queens", "Zone-04"),
#         ("Staten Island", "Zone-05")
#     ]
#     return spark.createDataFrame(data, ["zone_name", "zone_id"])

# # -----------------------------
# # 3. SILVER TABLE (Streaming)
# # -----------------------------
# @dlt.table(
#     name="nyc_taxi_cat.silver_sch.silver_nyc_taxi_tab",
#     comment="Silver table: Cleaned and validated NYC Taxi data",
#     table_properties={"quality": "silver"}
# )
# @dlt.expect("valid_speed", "speed <= 120")
# @dlt.expect("valid_duration", "trip_duration <= 180")
# def silver_nyc_taxi_tab():
#     # Read from Bronze
#     bronze_df = dlt.read_stream("nyc_taxi_cat.bronze_sch.bronze_nyc_taxi_tab")

#     # Deduplicate based on trip_id (or combination of fields)
#     bronze_df = bronze_df.dropDuplicates(["trip_id"])

#     # Transformations
#     df = bronze_df \
#         .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime"), "dd-MM-yyyy HH:mm")) \
#         .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime"), "dd-MM-yyyy HH:mm")) \
#         .withColumn("trip_duration", (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime")))/60) \
#         .withColumn("speed", when(col("trip_duration") > 0, col("trip_distance") / (col("trip_duration")/60)).otherwise(None)) \
#         .withColumn("total_amount", when(col("total_amount").isNull(), col("fare_amount") + col("tip_amount")).otherwise(col("total_amount"))) \
#         .withColumn("revenue_valid", when(col("total_amount") >= col("fare_amount"), True).otherwise(False)) \
#         .join(dlt.read("nyc_taxi_cat.silver_sch.taxi_zone_lookup"), col("pickup_zone") == col("zone_name"), "left") \
#         .withColumn("zone_id", col("zone_id")) \
#         .withColumn("processing_timestamp", from_utc_timestamp(current_timestamp(), "Asia/Kolkata")) \
#         .drop("_rescued_data") \
#         .withWatermark("pickup_datetime", "10 minutes")

#     return df
# # -----------------------------
# # 4. ERROR TABLE (Streaming)
# # -----------------------------
# @dlt.table(
#     name="nyc_taxi_cat.error_lg_sch.error_log_tab",
#     comment="Error table: Rows failing expectations",
#     table_properties={"quality": "error"}
# )
# def error_log_tab():
#     bronze_df = dlt.read_stream("nyc_taxi_cat.bronze_sch.bronze_nyc_taxi_tab")
#     return bronze_df.filter("speed > 120 OR trip_duration > 180") \
#         .withColumn("error_reason", when(col("speed") > 120, "Invalid Speed")
#                     .when(col("trip_duration") > 180, "Invalid Duration")) \
#         .withColumn("row_data", struct([col(c) for c in bronze_df.columns])) \
#         .withColumn("error_timestamp", from_utc_timestamp(current_timestamp(), "Asia/Kolkata")) \
#         .drop("_rescued_data")

# # -----------------------------
# # 5. GOLD TABLE: Zone-wise Revenue (Pickup & Dropoff)
# # -----------------------------
# @dlt.table(
#     name="nyc_taxi_cat.gold_sch.gold_zone_revenue_tab",
#     comment="Gold table: Zone-wise revenue and KPIs",
#     table_properties={"quality": "gold"}
# )
# def gold_zone_revenue_tab():
#     df = dlt.read_stream("nyc_taxi_cat.silver_sch.silver_nyc_taxi_tab")
#     return df.groupBy("pickup_zone", "dropoff_zone", "zone_id").agg(
#         _sum("total_amount").alias("total_revenue"),
#         count("trip_id").alias("trip_count"),
#         avg("fare_amount").alias("avg_fare"),
#         avg("trip_duration").alias("avg_duration"),
#         avg("trip_distance").alias("avg_distance")
#     ).withColumn("processing_timestamp", from_utc_timestamp(current_timestamp(), "Asia/Kolkata"))

# # -----------------------------
# # 6. GOLD TABLE: Peak Hours
# # -----------------------------
# @dlt.table(
#     name="nyc_taxi_cat.gold_sch.gold_peak_hours_tab",
#     comment="Gold table: Peak demand hours",
#     table_properties={"quality": "gold"}
# )
# def gold_peak_hours_tab():
#     df = dlt.read_stream("nyc_taxi_cat.silver_sch.silver_nyc_taxi_tab").withColumn("pickup_hour", col("pickup_datetime").substr(12, 2))
#     return df.groupBy("pickup_hour").agg(count("trip_id").alias("trips")).withColumn("processing_timestamp", from_utc_timestamp(current_timestamp(), "Asia/Kolkata"))

# # -----------------------------
# # 7. GOLD TABLE: Driver Performance
# # -----------------------------
# @dlt.table(
#     name="nyc_taxi_cat.gold_sch.gold_driver_perf_tab",
#     comment="Gold table: Driver performance metrics",
#     table_properties={"quality": "gold"}
# )
# def gold_driver_perf_tab():
#     df = dlt.read_stream("nyc_taxi_cat.silver_sch.silver_nyc_taxi_tab")
#     return df.groupBy("driver_id").agg(
#         avg("total_amount").alias("avg_fare"),
#         count("trip_id").alias("total_trips")
#     ).withColumn("processing_timestamp", from_utc_timestamp(current_timestamp(), "Asia/Kolkata"))

# # # -----------------------------
# # # 8. LOGGING TABLE (Dynamic)
# # # -----------------------------

# @dlt.table(
#     name="nyc_taxi_cat.pipeline_log_sch.pipeline_log_tab",
#     comment="Pipeline log table for monitoring pipeline runs"
# )
# def pipeline_log_tab():
#     # Dynamic metrics
#     processed_count = spark.sql("""
#         SELECT COUNT(*) AS cnt FROM nyc_taxi_cat.silver_sch.silver_nyc_taxi_tab
#     """).collect()[0][0]

#     error_count = spark.sql("""
#         SELECT COUNT(*) AS cnt FROM nyc_taxi_cat.error_lg_sch.error_log_tab
#     """).collect()[0][0]

#     # Dynamic RunId
#     run_id = f"Run-{int(datetime.now().timestamp())}"

#     # Capture file name from Auto Loader metadata (fallback if not available)
#     input_file_name = spark.conf.get("cloudFiles.sourceFile", "Unknown_File")

#     # Create DataFrame with one row
#     log_data = [(run_id, datetime.now(), "SUCCESS", processed_count, error_count, input_file_name)]

#     return spark.createDataFrame(
#         log_data,
#         ["RunId", "Timestamp", "Status", "ProcessedRecordCount", "ErrorMessages", "FileName"]
#     )
