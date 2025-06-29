
# utils.py
import boto3
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType

# Define bucket name as a constant
BUCKET_NAME = "ecommerce-lakehouse-001"

def archive_original_files(prefix, archive_prefix):
    """
    Archives files from raw S3 prefix to archive prefix and deletes originals.
    Args:
        prefix (str): S3 source prefix (e.g., 'raw/order_items/')
        archive_prefix (str): S3 destination prefix (e.g., 'archive/order_items/')
    """
    # Initialize S3 client inside the function to allow mocking
    s3_client = boto3.client('s3')
    try:
        # List objects in the source prefix
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        if 'Contents' not in response:
            print(f"No files found in {prefix}")
            return

        # Archive each file
        for obj in response['Contents']:
            source_key = obj['Key']
            # Skip if not a file (e.g., directory)
            if source_key.endswith('/'):
                continue
            # Create archive key with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = source_key.split('/')[-1]
            archive_key = f"{archive_prefix}{file_name}_{timestamp}"
            # Copy to archive
            s3_client.copy_object(
                Bucket=BUCKET_NAME,
                CopySource={'Bucket': BUCKET_NAME, 'Key': source_key},
                Key=archive_key
            )
            # Delete original
            s3_client.delete_object(Bucket=BUCKET_NAME, Key=source_key)
            print(f"Archived {source_key} to {archive_key}")
    except Exception as e:
        print(f"Error archiving files: {str(e)}")

def validate_schema(df):
    """
    Validates the schema for orders_job, ensuring all required columns are present.
    Args:
        df (DataFrame): PySpark DataFrame to validate
    Returns:
        DataFrame: Validated DataFrame with required columns
    """
    required_columns = [
        "order_num", "order_id", "user_id", "order_timestamp",
        "total_amount", "date", "sheet_name", "source_file"
    ]
    # Check for missing columns
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns: {missing_columns}")
    return df.select(required_columns)

def download_xlsx_from_s3(source_key, local_path):
    """
    Downloads an Excel file from S3 to a local path.
    Args:
        source_key (str): S3 key of the Excel file (e.g., 'raw/orders/orders.xlsx')
        local_path (str): Local path to save the file (e.g., '/tmp/orders.xlsx')
    """
    # Initialize S3 client inside the function
    s3_client = boto3.client('s3')
    try:
        s3_client.download_file(BUCKET_NAME, source_key, local_path)
        print(f"Downloaded {source_key} to {local_path}")
    except Exception as e:
        print(f"Error downloading {source_key}: {str(e)}")

def copy_products_file():
    """
    Copies products.csv from raw to preprocessed S3 location.
    """
    # Initialize S3 client inside the function
    s3_client = boto3.client('s3')
    try:
        source_key = "raw/products/products.csv"
        destination_key = "preprocessed/products/products.csv"
        s3_client.copy_object(
            Bucket=BUCKET_NAME,
            CopySource={'Bucket': BUCKET_NAME, 'Key': source_key},
            Key=destination_key
        )
        print(f"Copied {source_key} to {destination_key}")
    except Exception as e:
        print(f"Error copying products.csv: {str(e)}")
