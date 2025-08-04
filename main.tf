terraform {
    backend "gcs" { 
      bucket  = "brwy-terraform-state"
      # prefix  = "prod"
    }
    required_providers {
      google = "~> 6.46.0"
    }
}

provider "google" {
  project = var.project
  region = var.region
}
