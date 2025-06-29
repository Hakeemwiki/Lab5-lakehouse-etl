# tests/test_products_job.py
import pytest
from unittest.mock import patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
# Import from utils.py
from utils import archive_original_files

# Fixture to set up and tear down a SparkSession for all tests in this file
@pytest.fixture(scope="module")
def spark_session():
    # Create a SparkSession for testing products job
    spark = SparkSession.builder.appName("test_products").getOrCreate()
    # Yield pauses the fixture, letting tests use the SparkSession
    yield spark
    # Cleanup: stop SparkSession
    spark.stop()

# Test schema validation for products data
def test_schema_validation(spark_session):
    # Define a simple schema with key columns for products
    schema = StructType([
        StructField("product_id", StringType(), True),  # Product ID as string
        StructField("product_name", StringType(), True)  # Product name
    ])
    # Create test data with one valid row
    data = [("1", "Laptop")]
    # Create a PySpark DataFrame
    df = spark_session.createDataFrame(data, schema)
    
    # Filter rows where required fields are not null
    valid_df = df.filter(df.product_id.isNotNull() & df.product_name.isNotNull())
    
    # Assert the filtered DataFrame has 1 row and correct schema
    assert valid_df.count() == 1, "Expected 1 valid row after filtering"
    assert valid_df.schema.fieldNames() == ["product_id", "product_name"], "Schema fields do not match"

# Test the archive_original_files function with mocked S3 for products
@patch('utils.boto3.client')
def test_archive_original_files(mock_s3_client):
    # Mock boto3.client to simulate S3 interactions without real AWS calls
    # Simulate one CSV file in raw/products/
    mock_s3_client.return_value.list_objects_v2.return_value = {
        'Contents': [{'Key': 'raw/products/products.csv'}]
    }
    
    # Call the archiving function with products-specific prefixes
    archive_original_files(prefix="raw/products/", archive_prefix="archive/products/")
    
    # Verify S3 copy and delete were called exactly once
    mock_s3_client.return_value.copy_object.assert_called_once(), "S3 copy_object not called"
    mock_s3_client.return_value.delete_object.assert_called_once(), "S3 delete_object not called"
