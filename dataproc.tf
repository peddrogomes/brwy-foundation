# resource "google_service_account" "dataproc-svc" {
#   account_id   = "dataproc-svc"
#   display_name = "Dataproc Service Account"
#   description  = "Service account for Dataproc cluster and workflows"
# }

resource "google_storage_bucket_object" "dataproc-code" {
  for_each = fileset("scr/dataproc", "**/*")

  name = "src/dataproc/${each.value}"
  bucket = google_storage_bucket.dataproc-bucket.name
  source = "scr/dataproc/${each.value}"
}


resource "google_dataproc_workflow_template" "brwy_pipeline" {
  name     = "brwy-pipeline-template"
  location = var.region

  parameters {
    name = "DATE"
    description = "Date parameter for processing (format: YYYY-MM-DD)"
    fields = [
        "jobs['total-load'].pysparkJob.args[0]",
        "jobs['total-transform'].pysparkJob.args[0]"
        ]
  }

  placement {
    managed_cluster {
      cluster_name = "brwy-pipeline-cluster"
      config {
        staging_bucket = google_storage_bucket.dataproc-bucket.name

        master_config {
          num_instances = 1
          machine_type  = "e2-standard-2"
          disk_config {
            boot_disk_type    = "pd-standard"
            boot_disk_size_gb = 50
          }
        }

        worker_config {
          num_instances = 2
          machine_type  = "e2-standard-2"
          disk_config {
            boot_disk_type    = "pd-standard"
            boot_disk_size_gb = 50
          }
        }

        software_config {
          image_version = "2.0.66-debian10"
        }

        gce_cluster_config {
          zone = "${var.region}-b"
          subnetwork             = var.subnet_name
        #   service_account        = google_service_account.dataproc-svc.email
          service_account_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        }
      }
    }
  }

  jobs {
    step_id = "total-load"
    pyspark_job {
      main_python_file_uri = "gs://${google_storage_bucket.dataproc-bucket.name}/src/dataproc/breweries/load/total-load.py"
      args = ["DATE"]
    }
  }

  jobs {
    step_id = "total-transform"
    pyspark_job {
      main_python_file_uri = "gs://${google_storage_bucket.dataproc-bucket.name}/src/dataproc/breweries/transform/total-transform.py"
      args = ["DATE"]
    }
    prerequisite_step_ids = ["total-load"]
  }

  labels = local.labels
}