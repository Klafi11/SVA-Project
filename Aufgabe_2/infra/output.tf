output "bucket_name" {
  value       = google_storage_bucket.data.name
  description = "Bucket you created"
}

output "spark_runner_service_account" {
  value       = google_service_account.spark_worker.email
  description = "Service account Spark should run as"
}

output "upload_examples" {
  value = <<EOT
Provision infrastructure (OpenTofu/Terraform):
  tofu init
  tofu plan -out=tfplan
  tofu apply tfplan

Upload the Dataproc job entrypoint:
  gsutil cp main.py gs://${google_storage_bucket.data.name}/jobs/main.py

Upload the PySpark package (zip):
  cd src
  zip -r highest_e10_2023.zip highest_e10_2023 -x "*/__pycache__/*" -x "*.pyc"
  gsutil cp highest_e10_2023.zip gs://${google_storage_bucket.data.name}/jobs/highest_e10_2023.zip

Upload raw data:
  gsutil -m cp ./local_data/* gs://${google_storage_bucket.data.name}/raw/

Run the Dataproc Serverless batch (no partitioning):
  gcloud dataproc batches submit pyspark \
    gs://${google_storage_bucket.data.name}/jobs/main.py \
    --region=${var.region} \
    --staging-bucket=${google_storage_bucket.data.name} \
    --deps-bucket=${google_storage_bucket.data.name} \
    --service-account=${google_service_account.spark_worker.email} \
    --py-files=gs://${google_storage_bucket.data.name}/jobs/highest_e10_2023.zip \
    -- \
    --input-path "gs://${google_storage_bucket.data.name}/raw/prices/2023/*/*.csv" \
    --output-path gs://${google_storage_bucket.data.name}/processed/

Run the Dataproc Serverless batch (partitioned by year_month):
  gcloud dataproc batches submit pyspark \
    gs://${google_storage_bucket.data.name}/jobs/main.py \
    --region=${var.region} \
    --staging-bucket=${google_storage_bucket.data.name} \
    --deps-bucket=${google_storage_bucket.data.name} \
    --service-account=${google_service_account.spark_worker.email} \
    --py-files=gs://${google_storage_bucket.data.name}/jobs/highest_e10_2023.zip \
    -- \
    --input-path "gs://${google_storage_bucket.data.name}/raw/prices/2023/*/*.csv" \
    --output-path gs://${google_storage_bucket.data.name}/processed/ \
    --partition-by-month

Local development & testing (using uv):
  uv pip install "-e .[dev]"
  uv run pytest
EOT
}

