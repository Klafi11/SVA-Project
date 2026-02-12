# API's

resource "google_project_service" "dataproc_service" {
  service = "dataproc.googleapis.com"
}
resource "google_project_service" "storage_service" {
  service = "storage.googleapis.com"
}
resource "google_project_service" "iam" {
  service = "iam.googleapis.com"
}
resource "google_project_service" "logging" {
  service = "logging.googleapis.com"
}

# Bucket

resource "google_storage_bucket" "data" {
  name                        = var.bucket_name
  location                    = var.region
  uniform_bucket_level_access = true
}

# Create Folder Structure in GCS

resource "google_storage_bucket_object" "raw" {
  bucket  = google_storage_bucket.data.name
  name    = "raw/.keep"
  content = " "
}

resource "google_storage_bucket_object" "staging" {
  name    = "staging/.keep"
  bucket  = google_storage_bucket.data.name
  content = " "
}

resource "google_storage_bucket_object" "processed" {
  bucket  = google_storage_bucket.data.name
  name    = "processed/.keep"
  content = " "
}

resource "google_storage_bucket_object" "folder_jobs" {
  bucket  = google_storage_bucket.data.name
  name    = "jobs/.keep"
  content = " "
}

# Create Service ACC

resource "google_service_account" "spark_worker" {
  account_id   = "spark-worker"
  display_name = "Dataproc Serverless Worker Service ACC"
}

# Service ACC permissions

# Read & Write objects in the bucket
resource "google_storage_bucket_iam_member" "bucket_rights" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.spark_worker.email}"
}

# Dataproc Runner Identity
resource "google_project_iam_member" "spark_runner" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.spark_worker.email}"
}

# Jobs Logs
resource "google_project_iam_member" "log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.spark_worker.email}"
}
