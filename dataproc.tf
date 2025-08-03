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

resource "google_storage_bucket_object" "init-script" {
  name = "scripts/init_dataproc.sh"
  bucket = google_storage_bucket.dataproc-bucket.name
  source = "scripts/init_dataproc.sh"
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
          image_version = "2.1-debian11"
          
          # Configure Spark with BigQuery connector
          properties = {
            "spark:spark.jars.packages" = "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.0"
            # "spark:spark.sql.catalog.spark_catalog" = "com.google.cloud.spark.bigquery.v2.Spark31BigQueryTableProvider"
            "spark:spark.sql.adaptive.enabled" = "true"
            "spark:spark.sql.adaptive.coalescePartitions.enabled" = "true"
            "spark:spark.serializer" = "org.apache.spark.serializer.KryoSerializer"
            "spark:spark.dynamicAllocation.enabled" = "true"
          }
        }

        initialization_actions {
          executable_file = "gs://${google_storage_bucket.dataproc-bucket.name}/scripts/init_dataproc.sh"
          execution_timeout = "300s"
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
      args = [
        "DATE", 
        google_storage_bucket.bronze.name, 
        google_storage_bucket.silver.name
      ]
    }
  }

  jobs {
    step_id = "total-transform"
    pyspark_job {
      main_python_file_uri = "gs://${google_storage_bucket.dataproc-bucket.name}/src/dataproc/breweries/transform/total-transform.py"
      args = [
        "DATE", 
        google_storage_bucket.silver.name, 
        var.project, 
        google_bigquery_dataset.breweries_foundation.dataset_id,
        google_storage_bucket.bigquery_temp.name
      ]
    }
    prerequisite_step_ids = ["total-load"]
  }

  labels = local.labels
}