resource "google_storage_bucket" "function_bucket" {
  name     = "${var.project}-function-bucket"
  location = "${var.region}"
  force_destroy = true
}

data "archive_file" "api_extract_zip" {
    type = "zip"
    source_dir = "scr/functions/api-extract"
    output_path = "functions/api-extract.zip"
}

resource "google_storage_bucket_object" "api_extract_code" {
  name   = "${data.archive_file.api_extract_zip.output_path}_${data.archive_file.api_extract_zip.output_sha}.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.api_extract_zip.output_path
}

resource "google_cloudfunctions_function" "api_extract" {
  name        = "api-extract"
  description = "Extracts brewery data from Open Brewery DB API and saves as JSON files to Cloud Storage bucket"
  runtime     = "python310"
  entry_point = "main"
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource = google_pubsub_topic.api_extract_topic.id
  }
  source_archive_bucket = google_storage_bucket.function_bucket.name
  source_archive_object = google_storage_bucket_object.api_extract_code.name
  available_memory_mb   = 256
  region                = var.region
  environment_variables = {
    # is_prd = "True"
    PUBSUB_TOPIC = google_pubsub_topic.api_extract_topic.id
    GCS_BUCKET_LANDING = google_storage_bucket.landing.name
    
  }
  labels = local.labels
  
}