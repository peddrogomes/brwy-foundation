# IAM permissions for silver bucket (SA for Dataproc)
resource "google_storage_bucket_iam_member" "silver_editor" {
  bucket = google_storage_bucket.silver.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:966844133549-compute@developer.gserviceaccount.com"
}