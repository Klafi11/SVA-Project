# Highest E10 Price – Dataproc Serverless (2023) Aufgabe 2

## Project Overview

This project computes the highest E10 fuel price in 2023 and returns all records tied to that maximum price using PySpark on Dataproc Serverless. Infrastructure is provisioned with OpenTofu (Terraform-compatible), and local development/testing uses uv.

## Architecture Overview

* Google Cloud Storage (GCS)
    * raw/ – input CSV data
    * processed/ – Spark output
    * jobs/ – Dataproc job entrypoint and Python package
* Dataproc Serverless (Batches)
    * Runs PySpark jobs without managing clusters
* Service Account
    * Used by Dataproc workers to access GCS
* OpenTofu
    * Provisions buckets, IAM roles, and service accounts
* uv
    * Python dependency management and local testing

## Prerequisites

1. Google Cloud project
2. gcloud and gsutil installed
3. OpenTofu (tofu) installed
4. uv installed – https://github.com/astral-sh/uv
5. Python 3.10+
6. Authenticate and set the project

## Authentication & Project Setup

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID #See infra variables
```

### From the infrastructure directory:

```bash
tofu init
tofu plan -out=tfplan
tofu apply tfplan
```

This creates:
* GCS bucket for data, jobs, and staging
* Dataproc service account with required IAM roles

## Local Development & Testing (using uv)

Install the package in editable mode:

```bash
uv pip install -e .[dev]
```

Run tests:

```bash
uv run pytest
```

This allows validating logic locally before running on Dataproc.

## Package the PySpark Code

Dataproc Serverless requires Python dependencies to be shipped as a ZIP file.

From the src/ directory (parent of highest_e10_2023/):

``` bash
zip -r highest_e10_2023.zip highest_e10_2023 \
  -x "*/__pycache__/*" -x "*.pyc"
```
Important always rebuild ZIP after code changes 

## Upload Code and Data to GCS

Upload the Dataproc job entrypoint:

```bash
gsutil cp main.py gs://<BUCKET_NAME>/jobs/main.py
```

Upload the packaged Python code:

```bash
gsutil cp highest_e10_2023.zip gs://<BUCKET_NAME>/jobs/highest_e10_2023.zip
```

Upload raw data:

```bash
gsutil -m cp ./local_data/* gs://<BUCKET_NAME>/raw/
```

Expected input layout:

```yaml
text
raw/
└── prices/
    └── 2023/
        ├── 1/
        │   ├── 2023-01-01.csv
        │   └── ...
        ├── 2/
        └── ...
        └── 12/
```

## Run the Dataproc Serverless Job

Without partitioning (default)

```bash
gcloud dataproc batches submit pyspark \
  gs://<BUCKET_NAME>/jobs/main.py \
  --region=europe-west3 \
  --staging-bucket=<BUCKET_NAME> \
  --deps-bucket=<BUCKET_NAME> \
  --service-account=<SPARK_WORKER_SA_EMAIL> \
  --py-files=gs://<BUCKET_NAME>/jobs/highest_e10_2023.zip \
  -- \
  --input-path "gs://<BUCKET_NAME>/raw/prices/2023/*/*.csv" \
  --output-path gs://<BUCKET_NAME>/processed/
```

With partitioning by year_month

``` bash
gcloud dataproc batches submit pyspark \
  gs://<BUCKET_NAME>/jobs/main.py \
  --region=europe-west3 \
  --staging-bucket=<BUCKET_NAME> \
  --deps-bucket=<BUCKET_NAME> \
  --service-account=<SPARK_WORKER_SA_EMAIL> \
  --py-files=gs://<BUCKET_NAME>/jobs/highest_e10_2023.zip \
  -- \
  --input-path "gs://<BUCKET_NAME>/raw/prices/2023/*/*.csv" \
  --output-path gs://<BUCKET_NAME>/processed/ \
  --partition-by-month
```
### Job Arguments

- --input-path	GCS glob path to input CSV files
- --output-path	GCS output directory
- --partition-by-month	Optional flag to partition output by year_month action="store_true" flags default to False.
- --shuffle-partitions, default=200 -> Number of shuffle partitions
- --disable-aqe Disable Adaptive Query Execution (AQE)
   
## Output Format

Spark writes directories, not single files.

Example (non-partitioned):

```yaml
text
processed/
├── part-00000-....csv
└── _SUCCESS
Example (partitioned):
```
```yaml
text
processed/
├── year_month=2023-03/
│   └── part-00000-....csv
├── year_month=2023-07/
│   └── part-00000-....csv
└── _SUCCESS
```

Preview output:

``` bash
gsutil cat gs://<BUCKET_NAME>/processed/**/part-*.csv | head
```

## Cleanup Infrastructure

When finished:

``` bash
tofu destroy
```

### Notes & Best Practices

- Always rebuild and re-upload the ZIP after code changes
- tofu output lets you see the build up Plan
- Dataproc Serverless logs and metadata are stored under Dataproc-managed prefixes in the staging bucket
- Ensure proper IAM permissions for the service account to access GCS buckets
- Monitor job execution via Google Cloud Console → Dataproc → Batches

