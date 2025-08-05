# BigQuery Views for brewery aggregations

# View: Aggregated data by brewery type
resource "google_bigquery_table" "breweries_agg_type" {
  dataset_id = google_bigquery_dataset.breweries_foundation.dataset_id
  table_id   = "breweries_agg_type"

  description = "Aggregated brewery data by type with counts, geographic distribution, and contact info metrics"

  view {
    query = <<-EOF
      SELECT
        type_brewery,
        COUNT(*) as total_breweries,
        COUNT(DISTINCT name_country) as countries_count,
        COUNT(DISTINCT name_state) as states_count,
        COUNT(DISTINCT name_city) as cities_count,
        
        -- Geographic metrics
        COUNTIF(has_coordinates = true) as breweries_with_coordinates,
        ROUND(COUNTIF(has_coordinates = true) * 100.0 / COUNT(*), 2) as coordinates_percentage,
        
        -- Contact information metrics
        COUNTIF(has_contact_info = true) as breweries_with_contact,
        ROUND(COUNTIF(has_contact_info = true) * 100.0 / COUNT(*), 2) as contact_info_percentage,
        
        -- Website presence
        COUNTIF(url_website IS NOT NULL) as breweries_with_website,
        ROUND(COUNTIF(url_website IS NOT NULL) * 100.0 / COUNT(*), 2) as website_percentage,
        
        -- Phone presence
        COUNTIF(phone IS NOT NULL) as breweries_with_phone,
        ROUND(COUNTIF(phone IS NOT NULL) * 100.0 / COUNT(*), 2) as phone_percentage,
        
        -- Top countries by type
        ARRAY_AGG(DISTINCT name_country IGNORE NULLS ORDER BY name_country LIMIT 5) as top_countries,
        
        -- Most recent data date
        MAX(source_date) as latest_data_date,
        MIN(source_date) as earliest_data_date,
        
        -- Last updated
        MAX(processing_timestamp) as last_updated
        
      FROM `${var.project}.${google_bigquery_dataset.breweries_foundation.dataset_id}.${google_bigquery_table.breweries_all_data.table_id}`
      WHERE type_brewery IS NOT NULL
      GROUP BY type_brewery
      ORDER BY total_breweries DESC
    EOF
    use_legacy_sql = false
  }

  labels = {
    project = var.data-project
    type    = "aggregated-view"
    level   = "brewery-type"
  }
}

# View: Aggregated data by state
resource "google_bigquery_table" "breweries_agg_state" {
  dataset_id = google_bigquery_dataset.breweries_foundation.dataset_id
  table_id   = "breweries_agg_state"

  description = "Aggregated brewery data by state with type distribution, density metrics, and geographic insights"

  view {
    query = <<-EOF
      SELECT
        name_state,
        name_state_province,
        name_country,
        COUNT(*) as total_breweries,
        COUNT(DISTINCT name_city) as cities_count,
        COUNT(DISTINCT type_brewery) as brewery_types_count,
        
        -- Brewery type distribution
        COUNTIF(type_brewery = 'micro') as micro_breweries,
        COUNTIF(type_brewery = 'brewpub') as brewpub_count,
        COUNTIF(type_brewery = 'regional') as regional_breweries,
        COUNTIF(type_brewery = 'large') as large_breweries,
        COUNTIF(type_brewery = 'planning') as planning_breweries,
        COUNTIF(type_brewery = 'contract') as contract_breweries,
        COUNTIF(type_brewery = 'proprietor') as proprietor_breweries,
        COUNTIF(type_brewery = 'closed') as closed_breweries,
        
        -- Most common brewery type
        (
          SELECT type_brewery 
          FROM `${var.project}.${google_bigquery_dataset.breweries_foundation.dataset_id}.${google_bigquery_table.breweries_all_data.table_id}` sub
          WHERE sub.name_state = main.name_state AND sub.type_brewery IS NOT NULL
          GROUP BY type_brewery 
          ORDER BY COUNT(*) DESC 
          LIMIT 1
        ) as most_common_type,
        
        -- Geographic and contact metrics
        COUNTIF(has_coordinates = true) as breweries_with_coordinates,
        ROUND(COUNTIF(has_coordinates = true) * 100.0 / COUNT(*), 2) as coordinates_percentage,
        
        COUNTIF(has_contact_info = true) as breweries_with_contact,
        ROUND(COUNTIF(has_contact_info = true) * 100.0 / COUNT(*), 2) as contact_info_percentage,
        
        -- Top cities by brewery count
        ARRAY_AGG(
          STRUCT(name_city, COUNT(*) as brewery_count) 
          IGNORE NULLS 
          ORDER BY COUNT(*) DESC 
          LIMIT 5
        ) as top_cities,
        
        -- Coordinate bounds (for mapping)
        MIN(latitude) as min_latitude,
        MAX(latitude) as max_latitude,
        MIN(longitude) as min_longitude,
        MAX(longitude) as max_longitude,
        
        -- Average coordinates (center point)
        ROUND(AVG(latitude), 6) as avg_latitude,
        ROUND(AVG(longitude), 6) as avg_longitude,
        
        -- Data freshness
        MAX(source_date) as latest_data_date,
        MIN(source_date) as earliest_data_date,
        MAX(processing_timestamp) as last_updated
        
      FROM `${var.project}.${google_bigquery_dataset.breweries_foundation.dataset_id}.${google_bigquery_table.breweries_all_data.table_id}` main
      WHERE name_state IS NOT NULL
      GROUP BY name_state, name_state_province, name_country
      ORDER BY total_breweries DESC
    EOF
    use_legacy_sql = false
  }

  labels = {
    project = var.data-project
    type    = "aggregated-view"
    level   = "state"
  }
}

# View: Combined state and type aggregation (cross-tabulation)
resource "google_bigquery_table" "breweries_agg_state_type" {
  dataset_id = google_bigquery_dataset.breweries_foundation.dataset_id
  table_id   = "breweries_agg_state_type"

  description = "Cross-tabulation of brewery data by state and type with density and ranking metrics"

  view {
    query = <<-EOF
      SELECT
        name_state,
        name_state_province,
        name_country,
        type_brewery,
        COUNT(*) as brewery_count,
        COUNT(DISTINCT name_city) as cities_with_type,
        
        -- Percentage within state
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY name_state), 2) as percentage_within_state,
        
        -- Percentage within type
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY type_brewery), 2) as percentage_within_type,
        
        -- Rankings
        ROW_NUMBER() OVER (PARTITION BY name_state ORDER BY COUNT(*) DESC) as rank_within_state,
        ROW_NUMBER() OVER (PARTITION BY type_brewery ORDER BY COUNT(*) DESC) as rank_within_type,
        
        -- Geographic metrics for this state-type combination
        COUNTIF(has_coordinates = true) as breweries_with_coordinates,
        COUNTIF(has_contact_info = true) as breweries_with_contact,
        
        -- Average coordinates for this combination
        ROUND(AVG(latitude), 6) as avg_latitude,
        ROUND(AVG(longitude), 6) as avg_longitude,
        
        -- Top cities for this state-type combination
        ARRAY_AGG(
          DISTINCT name_city 
          IGNORE NULLS 
          ORDER BY name_city 
          LIMIT 10
        ) as cities_list,
        
        -- Data freshness
        MAX(source_date) as latest_data_date,
        MAX(processing_timestamp) as last_updated
        
      FROM `${var.project}.${google_bigquery_dataset.breweries_foundation.dataset_id}.${google_bigquery_table.breweries_all_data.table_id}`
      WHERE name_state IS NOT NULL AND type_brewery IS NOT NULL
      GROUP BY name_state, name_state_province, name_country, type_brewery
      HAVING COUNT(*) >= 1  -- Filter out combinations with no breweries
      ORDER BY name_state, brewery_count DESC
    EOF
    use_legacy_sql = false
  }

  labels = {
    project = var.data-project
    type    = "aggregated-view"
    level   = "state-type-cross"
  }
}

# View: Summary dashboard view with key metrics
resource "google_bigquery_table" "breweries_summary_dashboard" {
  dataset_id = google_bigquery_dataset.breweries_foundation.dataset_id
  table_id   = "breweries_summary_dashboard"

  description = "High-level summary metrics for brewery data dashboard"

  view {
    query = <<-EOF
      SELECT
        -- Overall totals
        COUNT(*) as total_breweries,
        COUNT(DISTINCT name_country) as total_countries,
        COUNT(DISTINCT name_state) as total_states,
        COUNT(DISTINCT name_city) as total_cities,
        COUNT(DISTINCT type_brewery) as total_brewery_types,
        
        -- Data quality metrics
        COUNTIF(has_coordinates = true) as breweries_with_coordinates,
        ROUND(COUNTIF(has_coordinates = true) * 100.0 / COUNT(*), 2) as coordinates_coverage_pct,
        
        COUNTIF(has_contact_info = true) as breweries_with_contact,
        ROUND(COUNTIF(has_contact_info = true) * 100.0 / COUNT(*), 2) as contact_coverage_pct,
        
        COUNTIF(url_website IS NOT NULL) as breweries_with_website,
        COUNTIF(phone IS NOT NULL) as breweries_with_phone,
        
        -- Top countries by brewery count
        ARRAY_AGG(
          STRUCT(
            name_country, 
            COUNT(*) as brewery_count,
            ROUND(COUNT(*) * 100.0 / MAX(total_count), 2) as percentage
          ) 
          ORDER BY COUNT(*) DESC 
          LIMIT 10
        ) as top_countries,
        
        -- Top brewery types
        ARRAY_AGG(
          STRUCT(
            type_brewery, 
            COUNT(*) as brewery_count,
            ROUND(COUNT(*) * 100.0 / MAX(total_count), 2) as percentage
          ) 
          ORDER BY COUNT(*) DESC 
          LIMIT 10
        ) as top_brewery_types,
        
        -- Data freshness
        MAX(source_date) as latest_data_date,
        MIN(source_date) as earliest_data_date,
        DATE_DIFF(CURRENT_DATE(), MAX(source_date), DAY) as days_since_last_update,
        MAX(processing_timestamp) as last_processed,
        
        -- Geographic bounds (for world map)
        MIN(latitude) as min_latitude,
        MAX(latitude) as max_latitude,
        MIN(longitude) as min_longitude,
        MAX(longitude) as max_longitude
        
      FROM (
        SELECT *,
               COUNT(*) OVER () as total_count
        FROM `${var.project}.${google_bigquery_dataset.breweries_foundation.dataset_id}.${google_bigquery_table.breweries_all_data.table_id}`
      )
      GROUP BY ()
    EOF
    use_legacy_sql = false
  }

  labels = {
    project = var.data-project
    type    = "aggregated-view"
    level   = "summary-dashboard"
  }
}
