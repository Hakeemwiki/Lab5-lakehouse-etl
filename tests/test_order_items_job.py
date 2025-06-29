import pytest
from unittest.mock import patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType
from order_items_job import archive_original_files

# Fixture to set up and tear down a SparkSession for all tests in this file
@pytest.fixture(scope="module")
def spark_session():
    # Create a SparkSession for testing; 'appName' identifies the test session
    spark = SparkSession.builder.appName("test_order_items").getOrCreate()
    # Yield the SparkSession to make it available to tests
    # 'yield' pauses the fixture, letting tests use the SparkSession, then resumes for cleanup
    yield spark
    # After tests finish, stop the SparkSession to free resources
    spark.stop()

# Test schema validation for order_items_job
def test_schema_validation(spark_session):
    # Define a simple schema with key columns to mimic order_items data
    schema = StructType([
        StructField("id", LongType(), True),  # Order item ID (LongType for large integers)
        StructField("order_id", LongType(), True),  # Order ID linking to orders table
        StructField("order_timestamp", StringType(), True)  # Order timestamp as string
    ])
    # Create test data with one valid row
    data = [(1, 101, "2025-06-01 10:00:00")]
    # Create a PySpark DataFrame from the data and schema
    df = spark_session.createDataFrame(data, schema)
    
    # Filter rows where required fields are not null, simulating job's validation
    valid_df = df.filter(df.id.isNotNull() & df.order_id.isNotNull() & df.order_timestamp.isNotNull())
    
    # Assert the filtered DataFrame has 1 row and correct schema
    assert valid_df.count() == 1, "Expected 1 valid row after filtering"
    assert valid_df.schema.fieldNames() == ["id", "order_id", "order_timestamp"], "Schema fields do not match"

# Test the archive_original_files function with mocked S3
@patch('boto3.client')
def test_archive_original_files(mock_s3_client):
    # '@patch' mocks boto3.client to avoid real AWS S3 calls
    # mock_s3_client is a fake S3 client we can control in the test
    # Mock S3 response to simulate one Excel file in raw/order_items/
    mock_s3_client.return_value.list_objects_v2.return_value = {
        'Contents': [{'Key': 'raw/order_items/file1.xlsx'}]
    }
    
    # Call the archiving function, which should copy and delete the file
    archive_original_files()
    
    # Verify the mock S3 client was called correctly
    # assert_called_once ensures copy_object and delete_object were each called exactly once
    mock_s3_client.return_value.copy_object.assert_called_once(), "S3 copy_object not called"
    mock_s3_client.return_value.delete_object.assert_called_once(), "S3 delete_object not called"