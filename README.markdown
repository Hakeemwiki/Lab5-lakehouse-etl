
![alt text](docs/lab5.drawio.svg)

# Lakehouse Architecture for E-Commerce Transactions

This repository implements a production-grade Lakehouse architecture for an e-commerce platform on AWS. It leverages Amazon S3 for storage, AWS Glue with Apache Spark and Delta Lake for ETL processing, AWS Step Functions for orchestration, AWS Glue Data Catalog for metadata management, Amazon Athena for querying, and GitHub Actions for CI/CD automation. The system ensures high data reliability, schema enforcement, and consistent data freshness for analytical workloads, addressing the business's core needs.

## Project Overview

### Problem Statement
The e-commerce platform requires a robust data pipeline to ingest, clean, and transform raw transactional data (products, orders, and order items) stored in Amazon S3. The system must handle deduplication, schema validation, referential integrity, and partitioning while ensuring ACID compliance. Challenges include managing multi-sheet Excel files, orchestrating multiple ETL jobs, handling failures gracefully, and automating deployment with CI/CD. The solution must support downstream analytics via Athena and provide observability through detailed logging.

### How We Tackled It
- **Modular Design**: Centralized reusable functions (e.g., `archive_original_files`, `validate_schema`) in `utils.py` to avoid import issues and enhance maintainability.
- **Schema Enforcement**: Defined and validated expected schemas for each dataset, rejecting rows with missing primary keys or invalid timestamps.
- **Referential Integrity**: Ensured `order_id` and `product_id` relationships between tables using Delta Lake joins.
- **Deduplication and Upserts**: Implemented merge/upsert logic with Delta Lake to handle idempotent reruns and avoid duplicates.
- **Partitioning**: Partitioned Delta tables by `date` or `department_id` for performance optimization.
- **Orchestration**: Used AWS Step Functions to sequence Glue jobs, crawlers, and Athena queries, with failure handling via SNS notifications.
- **CI/CD Automation**: Integrated GitHub Actions to run Pytest unit tests and deploy Step Function definitions on the `main` branch.
- **Observability**: Logged detailed metrics (input/output rows, rejected records) to S3 for each job run.

### Core Services
- **Amazon S3**: Stores raw, preprocessed, warehouse, rejected, and log data.
- **AWS Glue + Spark**: Executes distributed ETL jobs with Delta Lake integration.
- **Delta Lake**: Provides ACID transactions and time travel on S3.
- **AWS Step Functions**: Orchestrates the ETL lifecycle.
- **AWS Glue Data Catalog**: Manages metadata for Athena and Glue interoperability.
- **Amazon Athena**: Enables SQL-based analytics.
- **GitHub Actions**: Automates CI/CD pipelines.

## Project Structure
```
Lab5-lakehouse-etl/
├── .github/workflows/
│   └── ci-cd.yml              # GitHub Actions workflow for CI/CD
├── order_items_job.py         # Glue job for order items ETL
├── products_job.py            # Glue job for products ETL
├── orders_job.py              # Glue job for orders ETL
├── xlsx_to_csv_job.py         # Glue job to convert Excel to CSV
├── utils.py                   # Reusable utility functions
├── state_machine.json         # AWS Step Function definition
├── tests/
│   ├── test_order_items_job.py # Unit tests for order_items_job
│   ├── test_orders_job.py     # Unit tests for orders_job
│   └── ...                    # Additional test files (e.g., products_job)
├── requirements.txt           # Python dependencies
└── README.md                  # Project documentation
```

## Data Flow Overview
1. **Raw Data Layer (S3 Raw Zone)**:
   - CSV and Excel files (e.g., `products.csv`, `orders.xlsx`, `order_items.xlsx`) are dropped into `s3://ecommerce-lakehouse-001/raw/`.
   - Excel files contain multiple sheets, requiring preprocessing.
2. **Preprocessing**:
   - `xlsx_to_csv_job.py` converts Excel sheets into individual CSVs under `s3://ecommerce-lakehouse-001/preprocessed/`.
3. **ETL with AWS Glue (Delta Lake)**:
   - Three Glue jobs process the data:
     - `products_job.py`: Ingests and validates product data, partitioned by `department_id`.
     - `orders_job.py`: Handles order data, partitioned by `date`.
     - `order_items_job.py`: Enforces referential integrity with `orders`, partitioned by `date`.
   - Writes clean, deduplicated Delta tables to `s3://ecommerce-lakehouse-001/warehouse/lakehouse-dwh/`.
4. **Metadata Management**:
   - AWS Glue Crawlers update the Data Catalog for Athena querying.
5. **Athena Querying**:
   - SQL queries access `lakehouse_dwh.products`, `orders`, and `order_items`.

![alt text](docs/preview-Athena.png)

6. **Orchestration with Step Functions**:
   - `state_machine.json` triggers the pipeline, including Glue jobs, crawlers, and an Athena preview query.
7. **CI/CD with GitHub Actions**:
   - Automates testing and deployment on the `main` branch.

## 📋 Data Sources
![alt text](docs/ER_diagram.svg)
### Product Data
- **Fields**: `product_id`, `department_id`, `department`, `product_name`
- **Validation**: No null `product_id` or `product_name`; referential integrity with `order_items.product_id`.

### Orders
- **Fields**: `order_num`, `order_id`, `user_id`, `order_timestamp`, `total_amount`, `date`, `sheet_name`, `source_file`
- **Validation**: No null `order_id`, `user_id`, or `order_timestamp`; valid date format.

### Order Items
- **Fields**: `id`, `order_id`, `user_id`, `days_since_prior_order`, `product_id`, `add_to_cart_order`, `reordered`, `order_timestamp`, `date`, `sheet_name`, `source_file`
- **Validation**: No null `id`, `order_id`, `user_id`, `product_id`, or `order_timestamp`; referential integrity with `orders.order_id`.

## 🛠️ Setup and Deployment
### Prerequisites
- AWS account with permissions for Glue, S3, Step Functions, Athena, and SNS.
- Python 3.11 installed locally for testing.
- GitHub repository with write access.

### Installation
1. **Clone the Repository**:
   ```bash
   git clone https://github.com/xAI/Lab5-lakehouse-etl.git
   cd Lab5-lakehouse-etl
   ```
2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
   Required packages: `pytest`, `pyspark==3.4.0`, `boto3`, `openpyxl`.
3. **Configure AWS Credentials**:
   - Set up AWS CLI with `aws configure`.
   - Ensure an IAM role with `s3:PutObject`, `s3:DeleteObject`, `glue:StartJobRun`, `athena:StartQueryExecution`, and `sns:Publish` permissions.
4. **Upload Scripts to S3**:
   - Create a bucket (`ecommerce-lakehouse-001`) and upload scripts to `s3://ecommerce-lakehouse-001/scripts/`.
   - Include `--extra-py-files s3://ecommerce-lakehouse-001/scripts/utils.py` in Glue job parameters.
5. **Create Glue Jobs**:
   - Configure `xlsx_to_csv`, `products_glue_job`, `order_glue_job`, and `order_items_glue_job` with the uploaded scripts.
6. **Set Up Glue Crawlers**:
   - Create crawlers for `products_crawler`, `order_crawler`, and `order_items_crawler` targeting `warehouse/lakehouse-dwh/`.
7. **Deploy Step Function**:
   - Upload `state_machine.json` to AWS Step Functions as `Lakehouse-step-function`.

![alt text](docs/lab5-stepfn.png)

8. **Run Pipeline**:
   - Trigger the Step Function manually or via S3 event notifications.

## 🧪 Testing
### Unit Tests
- Tests are located in `tests/` using Pytest.
- Coverage includes schema validation and S3 mocking for `archive_original_files`.

### Run Tests Locally
```bash
pytest tests/
```
Expected Output: 8 tests passed (2 per file for `test_order_items_job.py`, `test_orders_job.py`, etc.).

### CI/CD with GitHub Actions
- Workflow defined in `.github/workflows/ci-cd.yml`.
- Triggers on push to `main` branch.
- Runs:
  - Dependency installation.
  - Pytest with `DeprecationWarning` suppression.
  - Directory structure debug.
  - Deploys Step Function on success.

![alt text](docs/ci-cd.png)

## 📂 S3 Directory Structure
```
s3://ecommerce-lakehouse-001/
├── raw/
│   ├── products/
│   ├── orders/
│   └── order_items/
├── preprocessed/
│   ├── products/
│   ├── orders/
│   └── order_items/
├── warehouse/
│   └── lakehouse-dwh/
│       ├── products/
│       ├── orders/
│       └── order_items/
├── rejected/
│   ├── orders/
│   └── order_items/
├── archive/
│   ├── products/
│   ├── orders/
│   └── order_items/
├── logs/
│   ├── products_job/
│   ├── orders/
│   └── order_items_job/
├── athena-queries/
└── scripts/                # Glue job scripts
```

## ✅ Best Practices
### Architecture
- **Lakehouse Design**: Combines S3 scalability with Delta Lake reliability.
- **Modularity**: `utils.py` centralizes reusable logic, reducing duplication.
- **Partitioning**: Optimizes query performance with `date` and `department_id` partitioning.

### Data Quality
- **Schema Enforcement**: Validates all expected columns and types.
- **Referential Integrity**: Ensures `order_id` and `product_id` consistency across tables.
- **Deduplication**: Uses Delta merge/upsert to handle duplicates.
- **Rejected Records**: Logs invalid rows to `rejected/` for auditing.

### Observability
- **Logging**: Detailed logs (input/output rows, errors) saved to `logs/` with timestamps.
- **Alerts**: SNS notifications for success/failure via Step Functions.

### CI/CD
- **Automation**: GitHub Actions ensures code quality and deployment consistency.
- **Testing**: Pytest mocks S3 calls to avoid real AWS interactions.

### Security
- **IAM Roles**: Restricts permissions to necessary actions.
- **Encryption**: S3 data encrypted at rest (default AWS setting).

## 🚧 Challenges and Solutions
- **Import Issues**:
  - **Problem**: Glue job scripts in `jobs/` caused `ModuleNotFoundError` due to inconsistent paths.
  - **Solution**: Moved shared functions to `utils.py` in the root, imported by all jobs and tests.
- **S3 Mocking in Tests**:
  - **Problem**: Global `boto3.client` in `utils.py` bypassed mocks, causing `Unable to locate credentials` errors.
  - **Solution**: Initialized `s3_client` inside functions, enabling `@patch('utils.boto3.client')` to work.
- **Multi-Sheet Excel Processing**:
  - **Problem**: Excel files with multiple sheets required individual CSV conversion.
  - **Solution**: `xlsx_to_csv_job.py` uses `openpyxl` to split sheets into CSVs.
- **Orchestration Complexity**:
  - **Problem**: Coordinating multiple jobs and crawlers was error-prone.
  - **Solution**: Step Functions with parallel branches and catch blocks handle failures and sequencing.
- **Test Coverage**:
  - **Problem**: Only 3 of 8 tests ran due to missing files.
  - **Solution**: Ensured all test files (`test_order_items_job.py`, `test_orders_job.py`, etc.) are present and import from `utils.py`.


## Contributing
1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/new-feature`).
3. Commit changes (`git commit -m 'Add new feature'`).
4. Push to the branch (`git push origin feature/new-feature`).
5. Open a Pull Request against `main`.

## 📞 Contact
For questions or support, reach out via the GitHub Issues tab.