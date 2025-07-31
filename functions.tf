resource "google_storage_bucket" "function_bucket" {
  name     = "${var.project}-function-bucket"
  location = "${var.region}"
  force_destroy = true
}

data "archive_file" "hello_function_zip" {
    type = "zip"
    source_dir = "..scr/functions/hello_function"
    output_path = "functions/hello_function"
}

resource "google_storage_bucket_object" "hello_function_code" {
  name   = "hello_function.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.hello_function_zip.output_path
}

resource "google_cloudfunctions_function" "hello_function" {
  name        = "hello-function"
  description = "Print Hello Word"
  runtime     = "python310"
  entry_point = "main"
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource = google_pubsub_topic.hello_topic.id
  }
  source_archive_bucket = google_storage_bucket.function_bucket.name
  source_archive_object = google_storage_bucket_object.hello_function_code.name
  available_memory_mb   = 128
  region                = var.region
  environment_variables = {
    is_prd = "True"
  }
  labels = local.labels
  
}