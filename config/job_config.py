# Central config file for ETL jobs

S3_BUCKET = "ecommerce-lakehouse"
RAW_PATH = f"s3://{S3_BUCKET}/raw"
PROCESSED_PATH = f"s3://{S3_BUCKET}/processed"
REJECTED_PATH = f"s3://{S3_BUCKET}/rejected"
ARCHIVE_PATH = f"s3://{S3_BUCKET}/archive"

# Excel file keys in raw S3 bucket
ORDERS_FILE_KEY = "raw/orders_apr_2025.xlsx"
ORDER_ITEMS_FILE_KEY = "raw/order_items_apr_2025.xlsx"