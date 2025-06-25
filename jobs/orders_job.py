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