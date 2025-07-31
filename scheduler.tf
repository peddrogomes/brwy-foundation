resource "google_cloud_scheduler_job" "hello_job" {
  name             = "hello-scheduler-job"
  description      = ""
  schedule         = "0 9 * * *"
  time_zone        = "America/Sao_Paulo"

  pubsub_target {
    topic_name = google_pubsub_topic.hello_topic.id
    data       = base64encode("Trigger Function")
  }
}
