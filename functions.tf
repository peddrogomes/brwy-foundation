resource "google_storage_bucket" "function_bucket" {
  name     = "${var.project}-function-bucket"
  location = "${var.region}"
  force_destroy = true
}

resource "google_storage_bucket_object" "function_zip" {
  name   = "function-source.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = "function-source.zip"
}

resource "google_cloudfunctions_function" "hello_function" {
  name        = "hello-function"
  description = "Print Hello Word"
  runtime     = "python310"
  entry_point = "main"
  trigger_topic = google_pubsub_topic.hello_topic.name
  source_archive_bucket = google_storage_bucket.function_bucket.name
  source_archive_object = google_storage_bucket_object.function_zip.name
  available_memory_mb   = 128
  region                = var.region
  environment_variables = local.labels
  
}