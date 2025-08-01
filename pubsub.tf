resource "google_pubsub_topic" "api_extract_topic" {
  name = "api-extract-topic"
  labels = local.labels
}

resource "google_pubsub_topic" "trigger_dataproc_topic" {
  name = "trigger-dataproc-topic"
  labels = local.labels
}
