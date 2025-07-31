
resource "google_storage_bucket" "lnd" {
  project = var.project
  name = "${var.data-project}-lnd"
  force_destroy = false
  uniform_bucket_level_access = true
  location = var.region
  labels = local.labels
}