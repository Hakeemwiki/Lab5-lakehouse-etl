import boto3
import os
from datetime import datetime
import openpyxl

# S3 Configuration
S3_BUCKET = "ecommerce-lakehouse-001"
RAW_PREFIX = "raw/"
OUTPUT_PREFIX = "preprocessed/"
LOCAL_TMP_PATH = "/tmp"

# Initialize S3 client
s3 = boto3.client("s3")

def download_xlsx_from_s3(s3_key, local_path):
    s3.download_file(S3_BUCKET, s3_key, local_path)
    print(f"Downloaded {s3_key} to {local_path}")