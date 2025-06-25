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