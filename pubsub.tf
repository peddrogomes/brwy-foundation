resource "google_pubsub_topic" "api_extract_topic" {
  name = "api-extract-topic${var.branch-hash}"
  labels = local.labels
}

resource "google_pubsub_topic" "trigger_dataproc_topic" {
  name = "trigger-dataproc-topic${var.branch-hash}"
  labels = local.labels
}
