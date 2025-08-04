# BigQuery Dataset for breweries data

resource "google_bigquery_dataset" "breweries_foundation" {
  project                    = var.data-project
  location                   = var.region
  dataset_id                  = "breweries_foundation${replace(var.branch-hash, "-", "_")}"
  friendly_name              = "Breweries Foundation Dataset"
  description                = "Dataset for storing brewery data and analytics"
  labels = {
    project = var.data-project
    type    = "foundation"
  }

}

# Main breweries table with partitioning and clustering
resource "google_bigquery_table" "breweries_all_data" {
  project = var.data-project
  dataset_id = google_bigquery_dataset.breweries_foundation.dataset_id
  table_id   = "breweries_all_data"

  description = "Complete brewery data with transformations and data quality flags"
  
  # depends_on = [google_bigquery_dataset.breweries_foundation]

  time_partitioning {
    type  = "DAY"
    field = "source_date"
  }

  clustering = ["name_state", "type_brewery"]

  schema = jsonencode([
    {
      name = "id_brewery"
      type = "STRING"
      mode = "NULLABLE"
      description = "Unique brewery identifier"
    },
    {
      name = "name_brewery"
      type = "STRING"
      mode = "NULLABLE"
      description = "Brewery name"
    },
    {
      name = "type_brewery"
      type = "STRING"
      mode = "NULLABLE"
      description = "Type of brewery (micro, brewpub, etc.)"
    },
    {
      name = "address_line_1"
      type = "STRING"
      mode = "NULLABLE"
      description = "Primary address line"
    },
    {
      name = "address_line_2"
      type = "STRING"
      mode = "NULLABLE"
      description = "Secondary address line"
    },
    {
      name = "address_line_3"
      type = "STRING"
      mode = "NULLABLE"
      description = "Additional address line"
    },
    {
      name = "name_city"
      type = "STRING"
      mode = "NULLABLE"
      description = "City name"
    },
    {
      name = "name_state_province"
      type = "STRING"
      mode = "NULLABLE"
      description = "State or province name"
    },
    {
      name = "value_postal_code"
      type = "STRING"
      mode = "NULLABLE"
      description = "Postal/ZIP code"
    },
    {
      name = "name_country"
      type = "STRING"
      mode = "NULLABLE"
      description = "Country name"
    },
    {
      name = "longitude"
      type = "FLOAT"
      mode = "NULLABLE"
      description = "Longitude coordinate"
    },
    {
      name = "latitude"
      type = "FLOAT"
      mode = "NULLABLE"
      description = "Latitude coordinate"
    },
    {
      name = "phone"
      type = "STRING"
      mode = "NULLABLE"
      description = "Phone number"
    },
    {
      name = "url_website"
      type = "STRING"
      mode = "NULLABLE"
      description = "Website URL"
    },
    {
      name = "name_state"
      type = "STRING"
      mode = "NULLABLE"
      description = "State abbreviation"
    },
    {
      name = "name_street"
      type = "STRING"
      mode = "NULLABLE"
      description = "Street name"
    },
    {
      name = "processing_date"
      type = "DATE"
      mode = "NULLABLE"
      description = "Date when record was processed"
    },
    {
      name = "processing_timestamp"
      type = "TIMESTAMP"
      mode = "NULLABLE"
      description = "Timestamp when record was processed"
    },
    {
      name = "source_date"
      type = "DATE"
      mode = "NULLABLE"
      description = "Date of source data"
    },
    {
      name = "full_address"
      type = "STRING"
      mode = "NULLABLE"
      description = "Complete formatted address"
    },
    {
      name = "year"
      type = "INTEGER"
      mode = "NULLABLE"
      description = "Year from source date"
    },
    {
      name = "month"
      type = "INTEGER"
      mode = "NULLABLE"
      description = "Month from source date"
    },
    {
      name = "day"
      type = "INTEGER"
      mode = "NULLABLE"
      description = "Day from source date"
    },
    {
      name = "has_coordinates"
      type = "BOOLEAN"
      mode = "NULLABLE"
      description = "Whether brewery has valid coordinates"
    },
    {
      name = "has_contact_info"
      type = "BOOLEAN"
      mode = "NULLABLE"
      description = "Whether brewery has contact information"
    }
  ])

  labels = {
    project = var.data-project
    type    = "main-data"
  }
}

# # View: Aggregated data by brewery type
# resource "google_bigquery_table" "breweries_agg_type" {
#   dataset_id = google_bigquery_dataset.breweries_foundation.dataset_id
#   project = var.data-project
#   table_id   = "breweries_agg_type"
#   # depends_on = [google_bigquery_table.breweries_all_data]
#   description = "Aggregated brewery data by type with counts, geographic distribution, and contact info metrics"

#   view {
#     query = <<-EOF
# SELECT
#         type_brewery,
#         COUNT(*) as total_breweries,
#         COUNT(DISTINCT name_country) as countries_count,
#         COUNT(DISTINCT name_state) as states_count,
#         COUNT(DISTINCT name_city) as cities_count,
        
#         -- Geographic metrics
#         COUNTIF(has_coordinates = true) as breweries_with_coordinates,
#         ROUND(COUNTIF(has_coordinates = true) * 100.0 / COUNT(*), 2) as coordinates_percentage,
        
#         -- Contact information metrics
#         COUNTIF(has_contact_info = true) as breweries_with_contact,
#         ROUND(COUNTIF(has_contact_info = true) * 100.0 / COUNT(*), 2) as contact_info_percentage,
        
#         -- Website presence
#         COUNTIF(url_website IS NOT NULL) as breweries_with_website,
#         ROUND(COUNTIF(url_website IS NOT NULL) * 100.0 / COUNT(*), 2) as website_percentage,
        
#         -- Phone presence
#         COUNTIF(phone IS NOT NULL) as breweries_with_phone,
#         ROUND(COUNTIF(phone IS NOT NULL) * 100.0 / COUNT(*), 2) as phone_percentage,
        
#         -- Most recent data date
#         MAX(source_date) as latest_data_date,
#         MIN(source_date) as earliest_data_date,
        
#         -- Last updated
#         MAX(processing_timestamp) as last_updated
        

#       FROM `${var.project}.${google_bigquery_dataset.breweries_foundation.dataset_id}.${google_bigquery_table.breweries_all_data.table_id}`
#       GROUP BY type_brewery
#       ORDER BY total_breweries DESC
#     EOF
#     use_legacy_sql = false
#   }

#   labels = {
#     project = var.data-project
#     type    = "aggregated-view"
#     level   = "brewery-type"
#   }
# }
