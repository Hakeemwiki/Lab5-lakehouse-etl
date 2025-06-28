#!/usr/bin/env python3
"""
Orders ETL Job for AWS Glue
Processes preprocessed CSV orders data and stores clean Delta Lake tables in lakehouse-dwh.
- Enforces schema validation
- Deduplicates records
- Applies referential integrity against order_items table
- Writes clean data to Delta Lake
- Logs rejections and transformation logs
"""

import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, row_number, to_timestamp
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ========== Configuration ==========
# Define S3 paths and expected schema for the ETL job
S3_BUCKET = "ecommerce-lakehouse-001"
RAW_ZONE = f"s3://{S3_BUCKET}/preprocessed/orders"  # Source path for raw CSV orders data
REJECTED_ZONE = f"s3://{S3_BUCKET}/rejected/orders/{datetime.now().date()}"  # Path for rejected records
WAREHOUSE_ZONE = f"s3://{S3_BUCKET}/warehouse/lakehouse-dwh/orders"  # Target path for Delta Lake table
LOG_PATH = f"s3://{S3_BUCKET}/logs/orders/{datetime.now().date()}/glue_log.txt"  # Path for ETL logs
ORDER_ITEMS_PATH = f"s3://{S3_BUCKET}/warehouse/lakehouse-dwh/order_items"  # Path for order_items reference table

# Expected columns in the input CSV
EXPECTED_COLUMNS = [
    "order_num", "order_id", "user_id", "order_timestamp",
    "total_amount", "date", "sheet_name", "source_file"
]

# ========== Spark Session Setup ==========
# Initialize Spark session with Delta Lake support
spark = SparkSession.builder \
    .appName("Orders ETL Job") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# List to store log messages for the ETL process
log_messages = []

def log(message: str):
    """
    Log a message to console and append to log_messages list for later storage.
    
    Args:
        message (str): The message to log
    """
    print(message)
    log_messages.append(f"{datetime.now().isoformat()} - {message}")

# ========== Data Read ==========
def read_orders_data(path: str) -> DataFrame:
    """
    Read CSV orders data from the specified S3 path.
    
    Args:
        path (str): S3 path to raw CSV data
        
    Returns:
        DataFrame: Spark DataFrame containing raw orders data
        
    Raises:
        Exception: If reading the data fails
    """
    try:
        df = spark.read.option("header", True).csv(path)
        log(f"Successfully read orders data from {path}")
        return df
    except Exception as e:
        log(f"Failed to read orders data: {e}")
        raise

# ========== Schema Validation ==========
def validate_schema(df: DataFrame) -> DataFrame:
    """
    Validate that the input DataFrame has all expected columns.
    
    Args:
        df (DataFrame): Input Spark DataFrame
        
    Returns:
        DataFrame: DataFrame with only the expected columns
        
    Raises:
        ValueError: If required columns are missing
    """
    actual_columns = set(df.columns)
    missing = set(EXPECTED_COLUMNS) - actual_columns
    if missing:
        log(f"Missing columns in source data: {missing}")
        raise ValueError(f"Missing columns: {missing}")
    return df.select(*EXPECTED_COLUMNS)

# ========== Data Transformation ==========
def apply_transformations(df: DataFrame) -> (DataFrame, DataFrame):
    """
    Apply data transformations: type casting, deduplication, and null filtering.
    
    Args:
        df (DataFrame): Input DataFrame after schema validation
        
    Returns:
        tuple: (cleanස Clean DataFrame, Rejected DataFrame)
    """
    # Cast columns to appropriate data types
    df = df.withColumn("order_id", col("order_id").cast("long")) \
           .withColumn("user_id", col("user_id").cast("long")) \
           .withColumn("total_amount", col("total_amount").cast("double")) \
           .withColumn("order_timestamp", to_timestamp("order_timestamp")) \
           .withColumn("date", col("date").cast("date"))

    # Filter out records with null values in critical columns
    df_clean = df.filter(col("order_id").isNotNull() & col("user_id").isNotNull() & col("order_timestamp").isNotNull())

    # Deduplicate by keeping the latest record per order_id based on order_timestamp
    window_spec = Window.partitionBy("order_id").orderBy(col("order_timestamp").desc_nulls_last())
    df_clean = df_clean.withColumn("row_num", row_number().over(window_spec))
    df_deduped = df_clean.filter(col("row_num") == 1).drop("row_num")

    # Add ingestion timestamp for tracking
    df_deduped = df_deduped.withColumn("ingested_at", current_timestamp())

    # Identify records with null values for rejection
    df_rejected = df.filter(
        col("order_id").isNull() |
        col("user_id").isNull() |
        col("order_timestamp").isNull()
    )

    log("Applied type casting, deduplication, and null filtering")
    return df_deduped, df_rejected

# ========== Referential Integrity Check ==========
def enforce_referential_integrity(df: DataFrame) -> DataFrame:
    """
    Ensure all orders have corresponding order_items by joining with the order_items table.
    
    Args:
        df (DataFrame): Cleaned orders DataFrame
        
    Returns:
        DataFrame: Filtered DataFrame with valid order_ids
    """
    try:
        # Read unique order_ids from order_items Delta table
        items_df = spark.read.format("delta").load(ORDER_ITEMS_PATH).select("order_id").dropDuplicates()
        # Perform inner join to keep only orders with matching order_items
        df_integrity = df.join(items_df, on="order_id", how="inner")
        log("Enforced referential integrity by joining with order_items")
        return df_integrity
    except Exception as e:
        # If join fails, proceed with original DataFrame to avoid stopping the ETL
        log(f"Could not enforce referential integrity: {e}. Proceeding without join.")
        return df

# ========== Delta Lake Write ==========
def write_to_delta(df: DataFrame, path: str):
    """
    Write the transformed DataFrame to a Delta Lake table, merging if the table exists.
    
    Args:
        df (DataFrame): Final cleaned DataFrame
        path (str): S3 path for the Delta Lake table
        
    Raises:
        Exception: If writing to Delta Lake fails
    """
    try:
        if DeltaTable.isDeltaTable(spark, path):
            # Merge new data into existing Delta table based on order_id
            delta_table = DeltaTable.forPath(spark, path)
            delta_table.alias("target").merge(
                df.alias("source"),
                "target.order_id = source.order_id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            # Create new Delta table partitioned by date
            df.write.format("delta").mode("overwrite").partitionBy("date").save(path)
        log(f"Orders written to Delta Lake at {path}")
    except Exception as e:
        log(f"Failed to write to Delta Lake: {e}")
        raise

# ========== Write Rejected Records ==========
def write_rejected(df: DataFrame, path: str):
    """
    Write invalid records to a JSON file in terciaries in the rejected zone.
    
    Args:
        df (DataFrame): DataFrame containing rejected records
        path (str): S3 path for storing rejected records
    """
    if df.count() == 0:
        log("No rejected records to write.")
        return
    try:
        df.write.mode("overwrite").json(path)
        log(f"⚠️ Rejected records written to {path}")
    except Exception as e:
        log(f"Failed to write rejected records: {e}")

# ========== Write Logs ==========
def write_log_file(logs: list, path: str):
    """
    Write ETL process logs to a text file in S3.
    
    Args:
        logs (list): List of log messages
        path (str): S3 path for the log file
    """
    try:
        log_df = spark.createDataFrame([(m,) for m in logs], ["log"])
        log_df.write.mode("overwrite").text(path)
        log(f"Log written to {path}")
    except Exception as e:
        print(f"Failed to write logs to {path}: {e}")

# ========== Main ETL ==========
try:
    # Execute the ETL pipeline
    raw_df = read_orders_data(RAW_ZONE)  # Read raw data
    validated_df = validate_schema(raw_df)  # Validate schema
    clean_df, rejected_df = apply_transformations(validated_df)  # Apply transformations
    final_df = enforce_referential_integrity(clean_df)  # Enforce referential integrity
    write_to_delta(final_df, WAREHOUSE_ZONE)  # Write clean data to Delta Lake
    write_rejected(rejected_df, REJECTED_ZONE)  # Write rejected records
except Exception as e:
    # Log any errors during ETL process
    log(f"ETL process failed: {e}")
finally:
    # Ensure logs are written and Spark session is stopped
    write_log_file(log_messages, LOG_PATH)
    spark.stop()