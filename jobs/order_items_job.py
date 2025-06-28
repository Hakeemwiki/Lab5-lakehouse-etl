#!/usr/bin/env python3
"""
order_items_job.py - AWS Glue ETL script for processing order items data in Lakehouse architecture.

This job:
- Reads preprocessed order items CSV files from S3.
- Validates schema and enforces data quality rules.
- Joins with Orders Delta table to enforce referential integrity.
- Deduplicates and merges data into Delta Lake.
- Logs rejected records and runtime details.

Expected columns:
id, order_id, user_id, days_since_prior_order, product_id, add_to_cart_order,
reordered, order_timestamp, date, sheet_name, source_file
"""

import sys
import os
import datetime
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp

# -----------------------------------
# 🔧 SparkSession with Delta support
# -----------------------------------
spark = (
    SparkSession.builder
    .appName("order_items_job")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
glueContext = GlueContext(spark.sparkContext)
spark.sparkContext.setLogLevel("WARN")

# -----------------------------------
# Job Configuration
# -----------------------------------
DATE = datetime.date.today().strftime("%Y-%m-%d")
S3_BUCKET = "ecommerce-lakehouse-001"
RAW_PATH = f"s3://{S3_BUCKET}/preprocessed/order_items/"
WAREHOUSE_PATH = f"s3://{S3_BUCKET}/warehouse/lakehouse-dwh/order_items/"
REJECT_PATH = f"s3://{S3_BUCKET}/rejected/order_items/{datetime.date.today()}/"
LOG_PATH = f"s3://ecommerce-lakehouse-001/logs/products_job/{DATE}/glue_log_{datetime}.txt"
ORDERS_TABLE_PATH = f"s3://{S3_BUCKET}/warehouse/lakehouse-dwh/orders/"

# -----------------------------------
# Load raw CSV data from S3
# -----------------------------------
df = spark.read.option("header", True).csv(RAW_PATH)

# -----------------------------------
# 🧪 Enforce expected schema
# -----------------------------------
expected_columns = [
    "id", "order_id", "user_id", "days_since_prior_order", "product_id",
    "add_to_cart_order", "reordered", "order_timestamp", "date",
    "sheet_name", "source_file"
]

# Keep only expected columns (if present)
df = df.select([col(c) for c in expected_columns if c in df.columns])

# Explicitly cast columns to expected types
df = df.withColumn("id", col("id").cast("long")) \
       .withColumn("order_id", col("order_id").cast("long")) \
       .withColumn("user_id", col("user_id").cast("long")) \
       .withColumn("days_since_prior_order", col("days_since_prior_order").cast("int")) \
       .withColumn("product_id", col("product_id").cast("long")) \
       .withColumn("add_to_cart_order", col("add_to_cart_order").cast("int")) \
       .withColumn("reordered", col("reordered").cast("int")) \
       .withColumn("order_timestamp", to_timestamp("order_timestamp")) \
       .withColumn("date", col("date").cast("date")) \
       .withColumn("sheet_name", col("sheet_name").cast("string")) \
       .withColumn("source_file", col("source_file").cast("string"))

# -----------------------------------
# Validate required fields
# Drop rows missing essential fields
# -----------------------------------
valid_df = df.filter(
    col("id").isNotNull() &
    col("order_id").isNotNull() &
    col("user_id").isNotNull() &
    col("product_id").isNotNull() &
    col("order_timestamp").isNotNull()
)

# Identify rejected records
rejected_df = df.subtract(valid_df)
rejected_count = rejected_df.count()
if rejected_count > 0:
    rejected_df.write.mode("overwrite").csv(REJECT_PATH)
    print(f"⚠️ Rejected {rejected_count} rows written to: {REJECT_PATH}")
else:
    print("No rejected records found.")

# -----------------------------------
# Enforce referential integrity
# Keep only order_items linked to known order_id from Orders Delta
# -----------------------------------
orders_df = spark.read.format("delta").load(ORDERS_TABLE_PATH).select("order_id").dropDuplicates()
valid_df = valid_df.join(orders_df, on="order_id", how="inner")

# -----------------------------------
# Deduplication + Add ingestion timestamp
# Drop duplicates on composite key
# -----------------------------------
deduped_df = valid_df.dropDuplicates([
    "id", "order_id", "user_id", "product_id", "order_timestamp"
]).withColumn("ingested_at", current_timestamp())

# -----------------------------------
# Delta Lake Write (Merge or Overwrite)
# 1. Delta check before writing
# 2. Merge/upsert if exists
# 3. Partition by `date`
# -----------------------------------
write_count = deduped_df.count()
if DeltaTable.isDeltaTable(spark, WAREHOUSE_PATH):
    delta_table = DeltaTable.forPath(spark, WAREHOUSE_PATH)
    delta_table.alias("target").merge(
        deduped_df.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print(f"{write_count} rows merged into existing Delta table.")
else:
    deduped_df.write.format("delta").partitionBy("date").mode("overwrite").save(WAREHOUSE_PATH)
    print(f"{write_count} rows written to new Delta table with partitioning by 'date'.")

# -----------------------------------
# Write ETL log to S3
# -----------------------------------
log_data = f"""
ETL Job Completed: order_items_job.py
Input Rows:      {df.count()}
Valid Rows:      {valid_df.count()}
Deduplicated:    {write_count}
Rejected Rows:   {rejected_count}
Output Location: {WAREHOUSE_PATH}
Run Timestamp:   {datetime.datetime.now().isoformat()}
"""
# Save log to S3
spark.sparkContext.parallelize([log_data]).coalesce(1).saveAsTextFile(LOG_PATH)
print("Log written to:", LOG_PATH)
