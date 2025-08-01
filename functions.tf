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
    TRIGGER_DATAPROC_TOPIC = google_pubsub_topic.trigger_dataproc_topic.id
  }
  labels = local.labels
  
}

data "archive_file" "trigger_dataproc_zip" {
    type = "zip"
    source_dir = "scr/functions/trigger-dataproc"
    output_path = "functions/trigger-dataproc.zip"
}

resource "google_storage_bucket_object" "trigger_dataproc_code" {
  name   = "${data.archive_file.trigger_dataproc_zip.output_path}_${data.archive_file.trigger_dataproc_zip.output_sha}.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.trigger_dataproc_zip.output_path
}

resource "google_cloudfunctions_function" "trigger_dataproc" {
  name        = "trigger-dataproc"
  description = "Triggers Dataproc workflow template for data processing pipeline"
  runtime     = "python310"
  entry_point = "main"
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource = google_pubsub_topic.trigger_dataproc_topic.id
  }
  source_archive_bucket = google_storage_bucket.function_bucket.name
  source_archive_object = google_storage_bucket_object.trigger_dataproc_code.name
  available_memory_mb   = 256
  timeout               = 540
  region                = var.region
  environment_variables = {
    GCP_PROJECT = var.project
    REGION = var.region
  }
  labels = local.labels
}

# # IAM to allow function to access Dataproc
# resource "google_project_iam_member" "trigger_dataproc_dataproc_editor" {
#   project = var.project
#   role    = "roles/dataproc.editor"
#   member  = "serviceAccount:${google_cloudfunctions_function.trigger_dataproc.service_account_email}"
# }