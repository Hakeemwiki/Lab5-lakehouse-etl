
"""
AWS Glue ETL Job: products_job.py
----------------------------------------------------
Reads 'products.csv' from S3 (preprocessed zone),
validates schema, enforces referential integrity (product_id must exist in order_items),
deduplicates, logs counts, and performs upserts into Delta Lake.

Logs are saved to:
  s3://<bucket>/logs/products_job/{date}/glue_log.txt
"""
import os
import sys
import logging
from datetime import datetime
import boto3
import uuid

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable

# -------------------------------------------
# Spark & Glue setup
# -------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
# Configure Spark session with Delta Lake support
spark = (
    glueContext.spark_session.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# -------------------------------------------
# Parse job arguments
# -------------------------------------------
# args = getResolvedOptions(sys.argv, [
#     'JOB_NAME', 'bucket_name', 'database_name', 'table_name'
# ])
# JOB_NAME = args['JOB_NAME']
# BUCKET = args['bucket_name']
# DATABASE = args['database_name']
# TABLE_NAME = args['table_name']
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'bucket_name'
])
JOB_NAME = args['JOB_NAME']
BUCKET = args['bucket_name']


# -------------------------------------------
# Paths
# -------------------------------------------
DATE = datetime.now().strftime("%Y-%m-%d")
# Add milliseconds and UUID for uniqueness
TIMESTAMP = datetime.now().strftime("%H%M%S%f")[:-3]  # e.g., 231616123 (HHMMSSmmm)
UNIQUE_ID = uuid.uuid4().hex[:6]  # First 6 chars of UUID for brevity
LOG_PATH = f"s3://{BUCKET}/logs/products_job/{DATE}/glue_log_{TIMESTAMP}_{UNIQUE_ID}.txt"
INPUT_PATH = f"s3://{BUCKET}/preprocessed/products/products.csv"
OUTPUT_PATH = f"s3://{BUCKET}/warehouse/lakehouse-dwh/products/"
ORDER_ITEMS_PATH = f"s3://{BUCKET}/warehouse/lakehouse-dwh/order_items"
ARCHIVE_PREFIX = f"s3://{BUCKET}/archive/products/"  # New archive location

# -------------------------------------------
# Logging
# -------------------------------------------
logger = logging.getLogger('glue_logger')
logger.setLevel(logging.INFO)
log_stream = []

def log(msg):
    """Log messages to in-memory stream for later upload to S3."""
    timestamped = f"{datetime.now().isoformat()} - {msg}"
    log_stream.append(timestamped)
    logger.info(timestamped)
    print(timestamped)

def upload_logs_to_s3():
    s3 = boto3.client('s3')
    bucket = LOG_PATH.split('/')[2]
    key = '/'.join(LOG_PATH.split('/')[3:])
    s3.put_object(
        Body="\n".join(log_stream),
        Bucket=bucket,
        Key=key
    )

def archive_original_files():
    """
    Archives original CSV files from raw/products/ to archive/products/ after successful ingestion.

    Moves files using timestamp suffix to preserve history.
    """
    raw_prefix = "raw/products/"
    try:
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=raw_prefix)
        if 'Contents' not in response:
            log(f"No files found in {raw_prefix} to archive.")
            return

        for obj in response['Contents']:
            s3_key = obj['Key']
            if s3_key.endswith('.csv'):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                base_filename = os.path.basename(s3_key).replace('.csv', f'_{timestamp}.csv')
                archive_key = f"archive/products/{base_filename}"

                # Perform copy and delete (emulates move)
                s3.copy_object(Bucket=BUCKET, CopySource={'Bucket': BUCKET, 'Key': s3_key}, Key=archive_key)
                s3.delete_object(Bucket=BUCKET, Key=s3_key)
                log(f"Archived {s3_key} to {archive_key}")
    except Exception as e:
        log(f"Failed to archive files from {raw_prefix}: {e}")

# -------------------------------------------
# Schema
# -------------------------------------------
expected_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("department_id", StringType(), True),
    StructField("department", StringType(), True),
    StructField("product_name", StringType(), False)
])

# -------------------------------------------
# ETL Pipeline
# -------------------------------------------
try:
    log("Starting products_job ETL")

    # 1. Read products CSV with schema
    df = spark.read.option("header", True).schema(expected_schema).csv(INPUT_PATH)
    if df.rdd.isEmpty():
        raise ValueError("Input file is empty.")
    total_count = df.count()
    log(f"Loaded {total_count} records from {INPUT_PATH}")

    # 2. Deduplicate
    df = df.dropDuplicates(["product_id"])

    # 3. Validate required fields
    df_valid = df.filter(col("product_id").isNotNull() & col("product_name").isNotNull())

    # 4. Enforce referential integrity: product_id must exist in order_items
    try:
        items_df = spark.read.format("delta").load(ORDER_ITEMS_PATH).select("product_id").dropDuplicates()
        df_valid = df_valid.join(items_df, on="product_id", how="inner")
        log("Enforced referential integrity with order_items.product_id")
    except Exception as e:
        log(f"Failed to apply referential integrity: {e}. Proceeding without join.")

    # 5. Count valid/dropped records
    valid_count = df_valid.count()
    dropped = total_count - valid_count
    log(f"Valid records: {valid_count}")
    log(f"Dropped invalid or unmatched records: {dropped}")

    # 6. Add ingestion timestamp
    df_final = df_valid.withColumn("ingestion_timestamp", current_timestamp())

    # 7. Delta Lake write with CDC (MERGE)
    if not DeltaTable.isDeltaTable(spark, OUTPUT_PATH):
        df_final.write.format("delta") \
            .mode("overwrite") \
            .partitionBy("department_id") \
            .save(OUTPUT_PATH)
        log(f"Delta table created at {OUTPUT_PATH}")
    else:
        delta_table = DeltaTable.forPath(spark, OUTPUT_PATH)
        delta_table.alias("target").merge(
            df_final.alias("source"),
            "target.product_id = source.product_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        log(f"MERGE upsert completed into {OUTPUT_PATH}")

    # 8. Archive original files only if all steps succeed
    archive_original_files()
    log("Original Excel files archived.")

except Exception as e:
    log(f"ETL job failed: {e}")
    upload_logs_to_s3()
    raise
finally:
    upload_logs_to_s3()
    log("Logs uploaded successfully.")

    # Finalize Log Data
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 2025-06-28 23:16:00 GMT
    log_data = f"""
ETL Job Completed: products_job.py
Input Rows:      {total_count}
Valid Rows:      {valid_count}
Deduplicated:    {valid_count}
Rejected Rows:   {dropped}
Output Location: {OUTPUT_PATH}
Run Timestamp:   {current_time}
Archived Files:  Archived original Excel files from raw/products/ to archive/products/ (if successful)
"""
    spark.createDataFrame([(log_data,)], ["log"]).write.mode("overwrite").text(LOG_PATH)
    log(f"Log written to: {LOG_PATH}")