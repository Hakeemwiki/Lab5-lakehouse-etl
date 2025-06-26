# import necessary libraries
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit
import sys
import os

# Add the parent folder (lakehouse-etl/) to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common.utils import read_csv_from_s3
from config import job_config
from delta.tables import DeltaTable

# === Step 0: Initialize GlueContext ===
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

try:
    print("Starting Products ETL Job")

    # Load CSV data
    raw_path = f"{job_config.RAW_PATH}/products.csv"
    df = read_csv_from_s3(spark, raw_path)

    # Filter out rows with null product_id
    valid_df = df.filter(col("product_id").isNotNull())

    # Rejected = null product_id rows
    rejected_df = df.subtract(valid_df).withColumn("rejection_reason", lit("Missing product_id"))

    # Save to Delta format
    valid_df.write.format("delta") \
        .mode("overwrite") \
        .save(f"{job_config.PROCESSED_PATH}/products")

    rejected_df.write.format("delta") \
        .mode("overwrite") \
        .save(f"{job_config.REJECTED_PATH}/products")

    print("Products ETL job completed successfully.")

except Exception as e:
    print(f"Job failed: {e}")
    raise
