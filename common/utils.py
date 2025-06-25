from pyspark.sql import SparkSession, DataFrame
from typing import List
import boto3


def read_csv_from_s3(spark: SparkSession, s3_path: str) -> DataFrame:
    """
    Reads a CSV file from S3 into a Spark DataFrame.
    Assumes the file has a header row.

    Args:
        spark (SparkSession): The active Spark session.
        s3_path (str): The S3 path to the CSV file (e.g., 's3://bucket-name/path/to/file.csv').

    Returns:
        DataFrame: A Spark DataFrame containing the CSV data."""
    # # Reads CSV file with header option enabled
    return spark.read.option("header", True).csv(s3_path)