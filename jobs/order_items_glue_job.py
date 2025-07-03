"""
order_items_job.py - AWS Glue ETL script for processing order items data in Lakehouse architecture.

This job:
- Reads preprocessed order items CSV files from S3.
- Validates schema and enforces data quality rules.
- Joins with Orders Delta table to enforce referential integrity.
- Deduplicates and merges data into Delta Lake.
- Logs rejected records and runtime details.
- Archives original Excel files after successful ingestion.

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
import boto3
import logging

# -----------------------------------
# Logging Setup
# -----------------------------------
logger = logging.getLogger('glue_logger')
logger.setLevel(logging.INFO)
log_stream = []

def log(msg):
    """Log messages to in-memory stream for later upload to S3."""
    log_stream.append(f"{datetime.datetime.now().isoformat()} - {msg}")
    logger.info(msg)

# -----------------------------------
# SparkSession with Delta support
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
LOG_PATH = f"s3://ecommerce-lakehouse-001/logs/order_items_job/{DATE}/glue_log_{datetime.datetime.now().strftime('%H%M%S')}.txt"  # Unique log file per run
ORDERS_TABLE_PATH = f"s3://{S3_BUCKET}/warehouse/lakehouse-dwh/orders/"
ARCHIVE_PREFIX = f"s3://{S3_BUCKET}/archive/order_items/"  # New archive location

# Initialize S3 client for archiving
s3_client = boto3.client("s3")

def archive_original_files():
    """
    Archives original Excel files from raw/order_items/ to archive/order_items/ after successful ingestion.
    
    Note: Moves files with a timestamp suffix to preserve history.
    """
    raw_prefix = "raw/order_items/"
    try:
        # List objects in raw/order_items/
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=raw_prefix)
        if 'Contents' not in response:
            log("No files found in raw/order_items/ to archive.")
            return

        for obj in response['Contents']:
            s3_key = obj['Key']
            if s3_key.endswith('.xlsx'):  # Only archive Excel files
                # Create archive key with timestamp
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                archive_key = f"archive/order_items/{os.path.basename(s3_key).replace('.xlsx', f'_{timestamp}.xlsx')}"
                s3_client.copy_object(Bucket=S3_BUCKET, CopySource={'Bucket': S3_BUCKET, 'Key': s3_key}, Key=archive_key)
                s3_client.delete_object(Bucket=S3_BUCKET, Key=s3_key)
                log(f"Archived {s3_key} to {archive_key}")
    except Exception as e:
        log(f"Failed to archive files: {e}")

# -----------------------------------
# Load raw CSV data from S3
# -----------------------------------
df = spark.read.option("header", True).csv(RAW_PATH)
log(f"Loaded raw CSV data from {RAW_PATH}")

# -----------------------------------
# Enforce expected schema
# -----------------------------------
expected_columns = [
    "id", "order_id", "user_id", "days_since_prior_order", "product_id",
    "add_to_cart_order", "reordered", "order_timestamp", "date",
    "sheet_name", "source_file"
]

# Keep only expected columns (if present)
df = df.select([col(c) for c in expected_columns if c in df.columns])
log(f"Selected columns: {expected_columns}")

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
log("Applied column type casting")

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
log(f"Validated required fields, kept {valid_df.count()} valid rows")

# Identify rejected records
rejected_df = df.subtract(valid_df)
rejected_count = rejected_df.count()
if rejected_count > 0:
    rejected_df.write.mode("overwrite").csv(REJECT_PATH)
    log(f"Rejected {rejected_count} rows written to: {REJECT_PATH}")
else:
    log("No rejected records found.")

# -----------------------------------
# Enforce referential integrity
# Keep only order_items linked to known order_id from Orders Delta
# -----------------------------------
orders_df = spark.read.format("delta").load(ORDERS_TABLE_PATH).select("order_id").dropDuplicates()
valid_df = valid_df.join(orders_df, on="order_id", how="inner")
log(f"Enforced referential integrity, joined with Orders table")

# -----------------------------------
# Deduplication + Add ingestion timestamp
# Drop duplicates on composite key
# -----------------------------------
deduped_df = valid_df.dropDuplicates([
    "id", "order_id", "user_id", "product_id", "order_timestamp"
]).withColumn("ingested_at", current_timestamp())
log(f"Deduplicated data, final row count: {deduped_df.count()}")

# -----------------------------------
# Delta Lake Write (Merge or Overwrite)
# Enhancements:
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
    log(f"{write_count} rows merged into existing Delta table.")
else:
    deduped_df.write.format("delta").partitionBy("date").mode("overwrite").save(WAREHOUSE_PATH)
    log(f"{write_count} rows written to new Delta table with partitioning by 'date'.")

# -----------------------------------
# Archive original files after successful ingestion
# -----------------------------------
archive_original_files()
log("Original Excel files archived.")

# -----------------------------------
# Write ETL log to S3
# -----------------------------------
current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current time: 2025-06-28 22:15:00 GMT
log_data = f"""
ETL Job Completed: order_items_job.py
Input Rows:      {df.count()}
Valid Rows:      {valid_df.count()}
Deduplicated:    {write_count}
Rejected Rows:   {rejected_count}
Output Location: {WAREHOUSE_PATH}
Run Timestamp:   {current_time}
Archived Files:  Archived original Excel files from raw/order_items/ to archive/order_items/
"""
# Save log to S3
spark.sparkContext.parallelize([log_data]).coalesce(1).saveAsTextFile(LOG_PATH)
log(f"Log written to: {LOG_PATH}")

def upload_logs_to_s3():
    """Upload log stream to S3 after job completion."""
    s3 = boto3.client('s3')
    bucket = LOG_PATH.split('/')[2]
    key = '/'.join(LOG_PATH.split('/')[3:])
    s3.put_object(
        Body="\n".join(log_stream),
        Bucket=bucket,
        Key=key
    )
    log("Logs uploaded to S3 successfully.")

# Execute log upload on job completion
upload_logs_to_s3()