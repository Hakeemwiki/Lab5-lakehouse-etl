# import necessary libraries
from pyspark.sql import SparkSession, dataframe
from pyspark.sql.functions import col, to_timestamp, lit
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


