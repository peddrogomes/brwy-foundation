resource "google_cloud_scheduler_job" "api_extract_job" {
  name             = "api-extract-scheduler-job"
  description      = "Triggers API Extract function to fetch brewery data from Open Brewery DB"
  schedule         = "0 9 * * *"
  time_zone        = "America/Sao_Paulo"
  paused           = true

  pubsub_target {
    topic_name = google_pubsub_topic.api_extract_topic.id
    data       = base64encode("{\"type\": \"all\"}")
  }
}

