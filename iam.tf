# IAM permissions for silver bucket (SA for Dataproc)
resource "google_storage_bucket_iam_member" "silver_editor" {
  bucket = google_storage_bucket.silver.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:966844133549-compute@developer.gserviceaccount.com"
}

# BigQuery permissions for Dataproc service account
resource "google_bigquery_dataset_iam_member" "breweries_foundation_editor" {
  dataset_id = google_bigquery_dataset.breweries_foundation.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:966844133549-compute@developer.gserviceaccount.com"
}

resource "google_bigquery_dataset_iam_member" "breweries_foundation_user" {
  dataset_id = google_bigquery_dataset.breweries_foundation.dataset_id
  role       = "roles/bigquery.user"
  member     = "serviceAccount:966844133549-compute@developer.gserviceaccount.com"
}

# Project-level BigQuery permissions for the service account
resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:966844133549-compute@developer.gserviceaccount.com"
}