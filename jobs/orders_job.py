# import necessary libraries
from pyspark.sql import SparkSession, dataframe
from pyspark.sql.functions import col, to_timestamp, lit
import sys
import os
# Add the parent folder (lakehouse-etl/) to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common.utils import read_excel_sheet, get_excel_sheet_names
from config import job_config
import boto3

# initialize Spark session with Delta Lake Configuration
spark = SparkSession.builder \
    .appName("Orders ETL Job") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# === Step 1: Identify the Excel file and all sheets ===
bucket = job_config.S3_BUCKET
key = job_config.ORDERS_FILE_KEY

sheet_names = get_excel_sheet_names(bucket, key)

# === Step 2: Read and merge all sheets ===
orders_df_list = [] # List to store DataFrames from each sheet
for sheet in sheet_names:
    # Read each sheet from the Excel file
    df = read_excel_sheet(spark, f"s3://{bucket}/{key}", sheet_name=sheet)
    # Add a column to track the source sheet for debugging
    df = df.withColumn("source_sheet", lit(sheet)) 
    orders_df_list.append(df)

# Merge all sheets into a single DataFrame
# Handle the case where there might be only one sheet
orders_df = orders_df_list[0]
for df in orders_df_list[1:]:
    orders_df = orders_df.unionByName(df)


# === Step 3: Apply transformations and validation ===

# Select relevant columns with casting
orders_df = orders_df.select(
    col("order_num").cast("string"),
    col("order_id").cast("string"),
    col("user_id").cast("string"),
    to_timestamp("order_timestamp").alias("order_timestamp"),
    col("total_amount").cast("double"),
    to_timestamp("order_timestamp").cast("date").alias("date")
)

# === Step 4: Validate records ===
valid_df = orders_df.filter(
    col("order_id").isNotNull() &
    col("user_id").isNotNull() &
    col("order_timestamp").isNotNull()
)
rejected_df = orders_df.subtract(valid_df).withColumn("rejection_reason", lit("Missing required fields or bad timestamp"))

# === Step 5: Deduplicate records ===
valid_df = valid_df.dropDuplicates(["order_id"])

# === Step 6: Merge into existing Delta table ===
from delta.tables import DeltaTable

target_path = f"{job_config.PROCESSED_PATH}/orders"
rejected_path = f"{job_config.REJECTED_PATH}/orders"

# Check if Delta table exists
import os
if not DeltaTable.isDeltaTable(spark, target_path):
    # Write new table
    valid_df.write.format("delta").mode("overwrite").partitionBy("date").save(target_path)
else:
    delta_table = DeltaTable.forPath(spark, target_path)
    delta_table.alias("target").merge(
        valid_df.alias("source"),
        "target.order_id = source.order_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

# === Step 7: Write rejected records to S3 ===
rejected_df.write.format("delta").mode("overwrite").save(rejected_path)

print("Orders job completed successfully.")