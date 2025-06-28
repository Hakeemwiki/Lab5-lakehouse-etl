
"""
Orders ETL Job for AWS Glue
Processes preprocessed CSV orders data and stores clean Delta Lake tables in lakehouse-dwh.

GOALS:
1. Delta table check & creation      — Prevents job failure if Delta table does not exist
2. Upsert logic with merge           — Allows idempotent reruns (safe for retries)
3. Record count logging              — Helps verify row-level changes & track data movement
4. Partitioning by 'date' column     — Optimizes storage & query performance
"""

import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, row_number, to_timestamp
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ========== Configuration ==========
S3_BUCKET = "ecommerce-lakehouse-001"
RAW_ZONE = f"s3://{S3_BUCKET}/preprocessed/orders"
REJECTED_ZONE = f"s3://{S3_BUCKET}/rejected/orders/{datetime.now().date()}"
WAREHOUSE_ZONE = f"s3://{S3_BUCKET}/warehouse/lakehouse-dwh/orders"
LOG_PATH = f"s3://{S3_BUCKET}/logs/orders/{datetime.now().date()}/glue_log.txt"
ORDER_ITEMS_PATH = f"s3://{S3_BUCKET}/warehouse/lakehouse-dwh/order_items"

EXPECTED_COLUMNS = [
    "order_num", "order_id", "user_id", "order_timestamp",
    "total_amount", "date", "sheet_name", "source_file"
]

# ========== Spark Session Setup ==========
spark = SparkSession.builder \
    .appName("Orders ETL Job") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

log_messages = []

def log(message: str):
    print(message)
    log_messages.append(f"{datetime.now().isoformat()} - {message}")

# ========== Data Read ==========
def read_orders_data(path: str) -> DataFrame:
    try:
        df = spark.read.option("header", True).csv(path)
        log(f"Successfully read orders data from {path}")
        return df
    except Exception as e:
        log(f"Failed to read orders data: {e}")
        raise

# ========== Schema Validation ==========
def validate_schema(df: DataFrame) -> DataFrame:
    actual_columns = set(df.columns)
    missing = set(EXPECTED_COLUMNS) - actual_columns
    if missing:
        log(f"Missing columns in source data: {missing}")
        raise ValueError(f"Missing columns: {missing}")
    return df.select(*EXPECTED_COLUMNS)

# ========== Data Transformation ==========
def apply_transformations(df: DataFrame) -> (DataFrame, DataFrame):
    df = df.withColumn("order_id", col("order_id").cast("long")) \
           .withColumn("user_id", col("user_id").cast("long")) \
           .withColumn("total_amount", col("total_amount").cast("double")) \
           .withColumn("order_timestamp", to_timestamp("order_timestamp")) \
           .withColumn("date", col("date").cast("date"))

    df_clean = df.filter(col("order_id").isNotNull() & col("user_id").isNotNull() & col("order_timestamp").isNotNull())

    # Deduplication using latest order_timestamp per order_id
    window_spec = Window.partitionBy("order_id").orderBy(col("order_timestamp").desc_nulls_last())
    df_clean = df_clean.withColumn("row_num", row_number().over(window_spec))
    df_deduped = df_clean.filter(col("row_num") == 1).drop("row_num")

    # Add ingestion timestamp
    df_deduped = df_deduped.withColumn("ingested_at", current_timestamp())

    # Identify invalid records
    df_rejected = df.filter(
        col("order_id").isNull() |
        col("user_id").isNull() |
        col("order_timestamp").isNull()
    )

    log("Applied type casting, deduplication, and null filtering")
    return df_deduped, df_rejected

# ========== Referential Integrity Check ==========
def enforce_referential_integrity(df: DataFrame) -> DataFrame:
    try:
        items_df = spark.read.format("delta").load(ORDER_ITEMS_PATH).select("order_id").dropDuplicates()
        df_integrity = df.join(items_df, on="order_id", how="inner")
        log("Enforced referential integrity by joining with order_items")
        return df_integrity
    except Exception as e:
        log(f"⚠️ Could not enforce referential integrity: {e}. Proceeding without join.")
        return df

# ========== Delta Lake Write ==========
def write_to_delta(df: DataFrame, path: str):
    try:
        # Record count logging
        log(f"Writing {df.count()} rows to Delta Lake")

        # 1: Delta table check — prevents crash on first load
        if DeltaTable.isDeltaTable(spark, path):
            # Upsert logic using merge — safe for reruns
            delta_table = DeltaTable.forPath(spark, path)
            delta_table.alias("target").merge(
                df.alias("source"),
                "target.order_id = source.order_id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            # Enhancement 4: Partition by 'date' for performance & storage optimization
            df.write.format("delta").mode("overwrite").partitionBy("date").save(path)

        log(f"Orders written to Delta Lake at {path}")
    except Exception as e:
        log(f"Failed to write to Delta Lake: {e}")
        raise

# ========== Write Rejected Records ==========
def write_rejected(df: DataFrame, path: str):
    if df.count() == 0:
        log("No rejected records to write.")
        return
    try:
        df.write.mode("overwrite").json(path)
        log(f"Rejected records written to {path}")
    except Exception as e:
        log(f"Failed to write rejected records: {e}")

# ========== Write Logs ==========
def write_log_file(logs: list, path: str):
    try:
        log_df = spark.createDataFrame([(m,) for m in logs], ["log"])
        log_df.write.mode("overwrite").text(path)
        log(f"Log written to {path}")
    except Exception as e:
        print(f"Failed to write logs to {path}: {e}")

# ========== Main ETL ==========
try:
    raw_df = read_orders_data(RAW_ZONE)
    validated_df = validate_schema(raw_df)
    clean_df, rejected_df = apply_transformations(validated_df)
    final_df = enforce_referential_integrity(clean_df)
    write_to_delta(final_df, WAREHOUSE_ZONE)
    write_rejected(rejected_df, REJECTED_ZONE)
except Exception as e:
    log(f"ETL process failed: {e}")
finally:
    write_log_file(log_messages, LOG_PATH)
    spark.stop()
