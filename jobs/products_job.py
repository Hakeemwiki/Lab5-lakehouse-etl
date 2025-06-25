# import necessary libraries
from pyspark.sql import SparkSession
from common.utils import read_csv_from_s3
from config import job_config
import sys

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Products ETL Job") \
    .getOrCreate()

# Load CSV data
raw_path = f"{job_config.RAW_PATH}/products.csv"
df = read_csv_from_s3(spark, raw_path)

# Filter out rows with null product_id
valid_df = df.filter(df.product_id.isNotNull())