resource "google_storage_bucket" "bronze" {
  project = var.project
  name = "${var.project}-bronze${var.branch-hash}"
  force_destroy = false
  uniform_bucket_level_access = true
  location = var.region
  labels = local.labels
}

resource "google_storage_bucket" "silver" {
  project = var.data-project
  name = "${var.project}-silver${var.branch-hash}"
  force_destroy = false
  uniform_bucket_level_access = true
  location = var.region
  labels = local.labels
}

resource "google_storage_bucket" "dataproc-bucket" {
  project = var.project
  name = "${var.project}-dataproc-code${var.branch-hash}"
  force_destroy = true
  uniform_bucket_level_access = true
  location = var.region
  labels = local.labels
}

resource "google_storage_bucket" "function_bucket" {
  project = var.project
  name     = "${var.project}-function-code${var.branch-hash}"
  location = "${var.region}"
  uniform_bucket_level_access = true
  force_destroy = true
  labels = local.labels
}

resource "google_storage_bucket" "bigquery_temp" {
  project = var.project
  name = "${var.project}-bigquery-temp${var.branch-hash}"
  force_destroy = true
  uniform_bucket_level_access = true
  location = var.region
  labels = local.labels
  
  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "Delete"
    }
  }
}