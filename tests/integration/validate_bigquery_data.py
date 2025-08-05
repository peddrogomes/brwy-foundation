#!/usr/bin/env python3
"""
Script para validação detalhada dos dados do BigQuery após teste de integração.
Uso: python scripts/validate_bigquery_data.py [data-project] [test-date]
"""

import sys
from datetime import date
from google.cloud import bigquery


def validate_brewery_data(data_project, test_date=None):
    """Valida os dados carregados no BigQuery"""
    
    if test_date is None:
        test_date = date.today().strftime('%Y-%m-%d')
    
    client = bigquery.Client(project=data_project)
    table_id = f"{data_project}.brwy_data.breweries"
    
    print(f"🔍 Validating BigQuery data for date: {test_date}")
    print(f"📊 Table: {table_id}")
    print("=" * 50)
    
    # Query 1: Basic record count for the test date
    query1 = f"""
    SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT id) as unique_breweries,
        COUNT(DISTINCT brewery_type) as brewery_types,
        COUNT(DISTINCT state) as states_count
    FROM `{table_id}`
    WHERE DATE(processed_date) = '{test_date}'
    """
    
    try:
        print("📈 Query 1: Basic statistics")
        results = client.query(query1).result()
        
        for row in results:
            print(f"  ✅ Total records: {row.total_records}")
            print(f"  ✅ Unique breweries: {row.unique_breweries}")
            print(f"  ✅ Brewery types: {row.brewery_types}")
            print(f"  ✅ States represented: {row.states_count}")
            
            if row.total_records == 0:
                print(f"  ❌ No records found for date {test_date}")
                return False
            else:
                print("  ✅ Data validation passed!")
                
    except Exception as e:
        print(f"  ❌ Error executing query 1: {e}")
        return False
    
    # Query 2: Data quality checks
    query2 = f"""
    SELECT
        COUNT(*) as total_records,
        COUNT(CASE WHEN id IS NULL THEN 1 END) as null_ids,
        COUNT(CASE WHEN name IS NULL OR name = '' THEN 1 END) as null_names,
        COUNT(CASE WHEN brewery_type IS NULL THEN 1 END) as null_types,
        COUNT(CASE WHEN state IS NULL THEN 1 END) as null_states,
        MIN(processed_date) as min_processed_date,
        MAX(processed_date) as max_processed_date
    FROM `{table_id}`
    WHERE DATE(processed_date) = '{test_date}'
    """
    
    try:
        print("\n🔍 Query 2: Data quality checks")
        results = client.query(query2).result()
        
        for row in results:
            print(f"  📊 Total records: {row.total_records}")
            print(f"  🔍 Null IDs: {row.null_ids}")
            print(f"  🔍 Null names: {row.null_names}")
            print(f"  🔍 Null types: {row.null_types}")
            print(f"  🔍 Null states: {row.null_states}")
            print(f"  📅 Min processed date: {row.min_processed_date}")
            print(f"  📅 Max processed date: {row.max_processed_date}")
            
            # Validate data quality
            quality_issues = row.null_ids + row.null_names
            # More than 10% issues
            if quality_issues > row.total_records * 0.1:
                msg = f"  ⚠️ Data quality warning: {quality_issues} issues"
                print(msg)
            else:
                print("  ✅ Data quality check passed!")

    except Exception as e:
        print(f"  ❌ Error executing query 2: {e}")
        return False

    # Query 3: Brewery type distribution
    query3 = f"""
    SELECT
        brewery_type,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM `{table_id}`
    WHERE DATE(processed_date) = '{test_date}'
        AND brewery_type IS NOT NULL
    GROUP BY brewery_type
    ORDER BY count DESC
    """
    
    try:
        print("\n📊 Query 3: Brewery type distribution")
        results = client.query(query3).result()
        
        for row in results:
            print(f"  🍺 {row.brewery_type}: {row.count} ({row.percentage}%)")
            
    except Exception as e:
        print(f"  ❌ Error executing query 3: {e}")
        return False
    
    # Query 4: State distribution (top 10)
    query4 = f"""
    SELECT
        state,
        COUNT(*) as count
    FROM `{table_id}`
    WHERE DATE(processed_date) = '{test_date}'
        AND state IS NOT NULL
    GROUP BY state
    ORDER BY count DESC
    LIMIT 10
    """
    
    try:
        print("\n🗺️ Query 4: Top 10 states by brewery count")
        results = client.query(query4).result()
        
        for row in results:
            print(f"  📍 {row.state}: {row.count} breweries")
            
    except Exception as e:
        print(f"  ❌ Error executing query 4: {e}")
        return False
    
    # Query 5: Recent processing history
    query5 = f"""
    SELECT
        DATE(processed_date) as process_date,
        COUNT(*) as record_count,
        COUNT(DISTINCT id) as unique_breweries
    FROM `{table_id}`
    WHERE DATE(processed_date) >= DATE_SUB('{test_date}', INTERVAL 7 DAY)
    GROUP BY DATE(processed_date)
    ORDER BY process_date DESC
    """

    try:
        print("\n📅 Query 5: Processing history (last 7 days)")
        results = client.query(query5).result()

        for row in results:
            msg = f"  📅 {row.process_date}: {row.record_count} records, "
            msg += f"{row.unique_breweries} unique breweries"
            print(msg)

    except Exception as e:
        print(f"  ❌ Error executing query 5: {e}")
        return False

    print("\n🎉 All validation queries completed successfully!")
    return True


def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Usage: python validate_bigquery_data.py <data-project> "
              "[test-date]")
        print("Example: python validate_bigquery_data.py "
              "my-project-data-dev 2025-08-05")
        sys.exit(1)

    data_project = sys.argv[1]
    test_date = sys.argv[2] if len(sys.argv) > 2 else None

    try:
        success = validate_brewery_data(data_project, test_date)
        if success:
            print("\n✅ BigQuery validation completed successfully!")
            sys.exit(0)
        else:
            print("\n❌ BigQuery validation failed!")
            sys.exit(1)
    except Exception as e:
        print(f"\n❌ Validation failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
