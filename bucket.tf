resource "google_storage_bucket" "bronze" {
  project = var.project
  name = "${var.project}-bronze"
  force_destroy = false
  uniform_bucket_level_access = true
  location = var.region
  labels = local.labels
}

resource "google_storage_bucket" "silver" {
  project = var.data-project
  name = "${var.project}-silver"
  force_destroy = false
  uniform_bucket_level_access = true
  location = var.region
  labels = local.labels
}

resource "google_storage_bucket" "dataproc-bucket" {
  project = var.project
  name = "${var.project}-dataproc-code"
  force_destroy = true
  uniform_bucket_level_access = true
  location = var.region
  labels = local.labels
}

resource "google_storage_bucket" "function_bucket" {
  name     = "${var.project}-function-code"
  location = "${var.region}"
  force_destroy = true
  labels = local.labels
}