from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, lit
from common.utils import read_excel_sheet, get_excel_sheet_names
from config import job_config
from delta.tables import DeltaTable

# === Step 1: Initialize Spark ===
spark = SparkSession.builder \
    .appName("OrderItems ETL Job") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# === Step 2: Load Excel sheets from S3 ===
# Retrieve S3 bucket and key for order_items_apr_2025.xlsx from centralized config
bucket = job_config.S3_BUCKET
key = job_config.ORDER_ITEMS_FILE_KEY

# Dynamically get all sheet names from the Excel file (each sheet represents a day in April 2025)
sheet_names = get_excel_sheet_names(bucket, key)

# Initialize a list to store DataFrames from each sheet
order_items_dfs = []
for sheet in sheet_names:
    # Read each sheet using the reusable utility function from common.utils
    df = read_excel_sheet(spark, f"s3://{bucket}/{key}", sheet_name=sheet)
    # Add a column to track the source sheet for debugging and auditability
    df = df.withColumn("source_sheet", lit(sheet))
    order_items_dfs.append(df)

# Merge all sheets into a single DataFrame
# Start with the first sheet's DataFrame
order_items_df = order_items_dfs[0]
# Union with remaining sheets, preserving column names
for df in order_items_dfs[1:]:
    order_items_df = order_items_df.unionByName(df)