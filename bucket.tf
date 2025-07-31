
resource "google_storage_bucket" "lnd" {
  project = var.project
  name = "${var.project}-lnd"
  force_destroy = false
  uniform_bucket_level_access = true
  location = var.region
  labels = local.labels
}

resource "google_storage_bucket" "raw" {
  project = var.data-project
  name = "${var.project}-raw"
  force_destroy = false
  uniform_bucket_level_access = true
  location = var.region
  labels = local.labels
}