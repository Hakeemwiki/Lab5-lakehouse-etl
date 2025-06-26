from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_timestamp, lit
from delta.tables import DeltaTable
import sys
import os

# Add the parent folder (lakehouse-etl/) to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common.utils import read_excel_sheet, get_excel_sheet_names
from config import job_config


# === Step 1: Initialize Spark ===
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


try:
    print("🚀 Starting Order Items ETL Job")

    # === Step 2: Load Excel sheets from S3 ===
    # Retrieve S3 bucket and key for order_items_apr_2025.xlsx from centralized config
    bucket = job_config.S3_BUCKET
    key = job_config.ORDER_ITEMS_FILE_KEY

    # Dynamically get all sheet names from the Excel file (each sheet represents a day in April 2025)
    sheet_names = get_excel_sheet_names(bucket, key)

    # Initialize a list to store DataFrames from each sheet
    order_items_dfs = []
    for sheet in sheet_names:
        # Read each sheet using the reusable utility function from common.utils
        df = read_excel_sheet(spark, f"s3://{bucket}/{key}", sheet_name=sheet)
        # Add a column to track the source sheet for debugging and auditability
        df = df.withColumn("source_sheet", lit(sheet))
        order_items_dfs.append(df)

    # Merge all sheets into a single DataFrame
    # Start with the first sheet's DataFrame
    order_items_df = order_items_dfs[0]
    # Union with remaining sheets, preserving column names
    for df in order_items_dfs[1:]:
        order_items_df = order_items_df.unionByName(df)


    # === Step 3: Select & cast columns ===
    order_items_df = order_items_df.select(
        col("id").cast("string"),
        col("order_id").cast("string"),
        col("user_id").cast("string"),
        col("product_id").cast("string"),
        col("days_since_prior_order").cast("int"),
        col("add_to_cart_order").cast("int"),
        col("reordered").cast("boolean"),
        to_timestamp("order_timestamp").alias("order_timestamp"),
        to_timestamp("order_timestamp").cast("date").alias("date")
    )

    # === Step 4: Validate required fields ===
    # Ensure no nulls in critical fields (id, order_id, product_id, user_id, order_timestamp)
    required_df = order_items_df.filter(
        col("id").isNotNull() &
        col("order_id").isNotNull() &
        col("product_id").isNotNull() &
        col("user_id").isNotNull() &
        col("order_timestamp").isNotNull()
    )
    # Identify records that fail validation (nulls or invalid timestamps)
    rejected_df = order_items_df.subtract(required_df).withColumn("rejection_reason", lit("Missing fields or bad timestamp"))

    # === Step 5: Load reference Delta tables for FK validation ===
    orders_df = spark.read.format("delta").load(f"{job_config.PROCESSED_PATH}/orders").select("order_id").distinct()
    products_df = spark.read.format("delta").load(f"{job_config.PROCESSED_PATH}/products").select("product_id").distinct()

    # === Step 6: Enforce referential integrity ===
    valid_fk_df = required_df \
        .join(orders_df, on="order_id", how="inner") \
        .join(products_df, on="product_id", how="inner")

    invalid_fk_df = required_df.subtract(valid_fk_df).withColumn("rejection_reason", lit("FK constraint failed"))

    # === Step 7: Final valid data = passes all checks ===
    final_valid_df = valid_fk_df.dropDuplicates(["id"])

    # === Step 8: Merge/Upsert into Delta table ===
    # Define output paths for processed and rejected data
    target_path = f"{job_config.PROCESSED_PATH}/order_items"
    rejected_path = f"{job_config.REJECTED_PATH}/order_items"

    if not DeltaTable.isDeltaTable(spark, target_path):
        # If no table exists, create a new one with partitioning by date and product_id
        final_valid_df.write.format("delta") \
            .mode("overwrite") \
            .partitionBy("date", "product_id") \
            .save(target_path)
    else:
        # Merge new data into existing Delta table for idempotent updates
        delta_table = DeltaTable.forPath(spark, target_path)
        delta_table.alias("target").merge(
            final_valid_df.alias("source"),
            "target.id = source.id" # Match on primary key
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

    # === Step 9: Write rejected records to S3 ===
    # Combine all rejected records (from null checks and FK failures)
    all_rejected_df = rejected_df.unionByName(invalid_fk_df)
    # Save rejected records to Delta table for auditability
    all_rejected_df.write.format("delta").mode("overwrite").save(rejected_path)

    print("Order Items ETL job completed successfully.")

except Exception as e:
    print(f"Job failed: {e}")
    raise
