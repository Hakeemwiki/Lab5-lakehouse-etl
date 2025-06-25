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


def read_excel_sheet(spark: SparkSession, s3_path: str, sheet_name: str) -> DataFrame:
    """
    Reads a single Excel sheet from S3 into a DataFrame using the spark-excel package.

    Args:
        spark (SparkSession): The active Spark session.
        s3_path (str): The S3 path to the Excel file (e.g., 's3://bucket-name/path/to/file.xlsx').
        sheet_name (str): The name of the Excel sheet to read.

    Returns:
        DataFrame: A Spark DataFrame containing the data from the specified sheet.
    """
    # Configures the spark-excel format to read a specific sheet
    return spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("dataAddress", f"'{sheet_name}'!") \
        .load(s3_path)

def get_excel_sheet_names(bucket: str, key: str) -> List[str]:
    """
    Downloads the Excel file locally and lists all sheet names using openpyxl.

    Args:
        bucket (str): The S3 bucket name.
        key (str): The S3 key (path) to the Excel file (e.g., 'path/to/file.xlsx').

    Returns:
        List[str]: A list of sheet names in the Excel file.
    """
    import tempfile
    import openpyxl

    # Initializes an S3 client using boto3
    s3 = boto3.client('s3')
    # Creates a temporary file to store the downloaded Excel file
    with tempfile.NamedTemporaryFile() as tmp:
        # Downloads the Excel file from S3 to the temporary file
        s3.download_fileobj(bucket, key, tmp)
        # Resets file pointer to the beginning
        tmp.seek(0)
        # Loads the Excel workbook in read-only mode using openpyxl
        wb = openpyxl.load_workbook(tmp.name, read_only=True)
        # Returns the list of sheet names
        return wb.sheetnames