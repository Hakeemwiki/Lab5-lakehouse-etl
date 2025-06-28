#!/usr/bin/env python3

"""
AWS Glue ETL Job: products_job.py
----------------------------------------------------
Reads 'products.csv' from S3 (preprocessed zone),
validates schema, handles errors, and writes it
to Delta Lake (warehouse zone).

Logs are saved to S3 in structured path:
  s3://<bucket>/logs/products_job/{date}/glue_log.txt
"""

import sys
import os
import logging
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable

# -------------------------------------------
# Set up Glue and Spark contexts
# -------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.sparkContext.setLogLevel("ERROR")

# -------------------------------------------
# Parse job arguments
# -------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job_name = args['JOB_NAME']

# -------------------------------------------
# Configurable paths
# -------------------------------------------
DATE = datetime.now().strftime("%Y-%m-%d")
INPUT_PATH = "s3://ecommerce-lakehouse-001/preprocessed/products/products.csv"
OUTPUT_PATH = "s3://ecommerce-lakehouse-001/warehouse/lakehouse-dwh/products/"
LOG_PATH = f"s3://ecommerce-lakehouse-001/logs/products_job/{DATE}/glue_log.txt"

# -------------------------------------------
# Initialize logger and file handler
# -------------------------------------------
import boto3

logger = logging.getLogger('glue_logger')
logger.setLevel(logging.INFO)
log_stream = []

def log(msg):
    log_stream.append(f"{datetime.now().isoformat()} - {msg}")
    logger.info(msg)

def upload_logs_to_s3():
    s3 = boto3.client('s3')
    bucket = LOG_PATH.split('/')[2]
    key = '/'.join(LOG_PATH.split('/')[3:])
    s3.put_object(
        Body="\n".join(log_stream),
        Bucket=bucket,
        Key=key
    )

# -------------------------------------------
# Define expected schema
# -------------------------------------------
expected_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("department_id", StringType(), True),
    StructField("department", StringType(), True),
    StructField("product_name", StringType(), False)
])

# -------------------------------------------
# ETL PROCESS
# -------------------------------------------
try:
    log("Starting products_job ETL")

    # 1. Read CSV with schema
    df = spark.read.option("header", True).schema(expected_schema).csv(INPUT_PATH)

    if df.rdd.isEmpty():
        raise ValueError("Input file is empty.")

    log(f"Loaded input CSV from {INPUT_PATH}")

    # 2. Drop duplicates based on product_id
    df = df.dropDuplicates(["product_id"])

    # 3. Validate essential fields
    df_validated = df.filter(col("product_id").isNotNull() & col("product_name").isNotNull())

    invalid_count = df.count() - df_validated.count()
    if invalid_count > 0:
        log(f"{invalid_count} invalid rows dropped due to missing product_id or product_name")

    # 4. Add ingestion timestamp
    df_final = df_validated.withColumn("ingestion_timestamp", current_timestamp())

    # 5. Write to Delta Lake
    df_final.write.format("delta").mode("overwrite").save(OUTPUT_PATH)
    log(f"Successfully wrote to Delta table at {OUTPUT_PATH}")

except Exception as e:
    log(f"ETL job failed: {str(e)}")
    upload_logs_to_s3()
    raise

# -------------------------------------------
# Upload logs to S3
# -------------------------------------------
upload_logs_to_s3()
log("Logs uploaded successfully.")
