terraform {
    backend "gcs" { 
      bucket  = "brwy-terraform-state"
      prefix  = "prod"
    }
}

provider "google" {
  project = var.project
  region = var.region
}