# tests/test_orders_job.py
import pytest
from unittest.mock import patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType
# Import from utils.py
from utils import validate_schema, archive_original_files

# Fixture to set up and tear down a SparkSession for all tests in this file
@pytest.fixture(scope="module")
def spark_session():
    # Create a SparkSession for testing orders job
    spark = SparkSession.builder.appName("test_orders").getOrCreate()
    # Yield the SparkSession for tests
    yield spark
    # Cleanup: stop SparkSession
    spark.stop()

# Test schema validation for orders_job using utils.validate_schema
def test_validate_schema(spark_session):
    # Define schema with all expected columns for orders
    schema = StructType([
        StructField("order_num", StringType(), True),  # Order number
        StructField("order_id", LongType(), True),  # Order ID
        StructField("user_id", LongType(), True),  # User ID
        StructField("order_timestamp", StringType(), True),  # Order timestamp
        StructField("total_amount", StringType(), True),  # Total amount as string
        StructField("date", StringType(), True),  # Date
        StructField("sheet_name", StringType(), True),  # Excel sheet name
        StructField("source_file", StringType(), True)  # Source file name
    ])
    # Create test data with one valid row
    data = [("ORD001", 1, 201, "2025-06-01 10:00:00", "99.99", "2025-06-01", "Sheet1", "file1.xlsx")]
    df = spark_session.createDataFrame(data, schema)
    
    # Call the validate_schema function from utils.py
    validated_df = validate_schema(df)
    
    # Assert the DataFrame has 1 row and all expected columns
    assert validated_df.count() == 1, "Expected 1 valid row after validation"
    assert validated_df.schema.fieldNames() == [
        "order_num", "order_id", "user_id", "order_timestamp",
        "total_amount", "date", "sheet_name", "source_file"
    ], "Schema fields do not match"

# Test the archive_original_files function with mocked S3 for orders
@patch('utils.boto3.client')
def test_archive_original_files(mock_s3_client):
    # Mock S3 response to simulate one Excel file in raw/orders/
    mock_s3_client.return_value.list_objects_v2.return_value = {
        'Contents': [{'Key': 'raw/orders/file1.xlsx'}]
    }
    
    # Call the archiving function with orders-specific prefixes
    archive_original_files(prefix="raw/orders/", archive_prefix="archive/orders/")
    
    # Verify S3 copy and delete were called exactly once
    mock_s3_client.return_value.copy_object.assert_called_once(), "S3 copy_object not called"
    mock_s3_client.return_value.delete_object.assert_called_once(), "S3 delete_object not called"
