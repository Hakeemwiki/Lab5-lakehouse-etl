# import necessary libraries
from pyspark.sql import SparkSession
from common.utils import read_csv_from_s3
from config import job_config
import sys

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Products ETL Job") \
    .getOrCreate()