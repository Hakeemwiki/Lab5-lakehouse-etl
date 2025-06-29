# tests/test_xlsx_to_csv_job.py
import pytest
from unittest.mock import patch
# Import from utils.py
from utils import download_xlsx_from_s3, copy_products_file

# Test the download_xlsx_from_s3 function with mocked S3
@patch('utils.boto3.client')
def test_download_xlsx_from_s3(mock_s3_client):
    # Mock boto3.client to avoid real S3 calls
    # Call the function to download an Excel file
    download_xlsx_from_s3("raw/orders/orders.xlsx", "/tmp/orders.xlsx")
    
    # Verify the S3 download_file method was called with correct bucket, key, and local path
    mock_s3_client.return_value.download_file.assert_called_with(
        "ecommerce-lakehouse-001", "raw/orders/orders.xlsx", "/tmp/orders.xlsx"
    ), "S3 download_file not called correctly"

# Test the copy_products_file function with mocked S3
@patch('utils.boto3.client')
def test_copy_products_file(mock_s3_client):
    # Mock boto3.client for S3 copy operation
    # Call the function to copy products.csv
    copy_products_file()
    
    # Verify the S3 copy_object method was called with correct parameters
    mock_s3_client.return_value.copy_object.assert_called_with(
        Bucket="ecommerce-lakehouse-001",
        CopySource={'Bucket': "ecommerce-lakehouse-001", 'Key': "raw/products/products.csv"},
        Key="preprocessed/products/products.csv"
    ), "S3 copy_object not called correctly"

