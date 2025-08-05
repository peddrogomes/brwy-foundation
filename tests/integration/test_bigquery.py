#!/usr/bin/env python3
"""
BigQuery validation tests for the brewery data pipeline.
"""

import time
import logging
from google.cloud import bigquery
from .base_test import BaseIntegrationTest

logger = logging.getLogger(__name__)


class BigQueryTester(BaseIntegrationTest):
    """Tests BigQuery data validation."""

    def __init__(self, config):
        """Initialize the BigQuery tester."""
        super().__init__(config)
        
        # Initialize BigQuery client
        self.bigquery_client = bigquery.Client(
            project=config.get('data_project')
        )

    def _execute_tests(self) -> bool:
        """Run all BigQuery validation tests."""
        # Wait for data processing
        self.log_info("Waiting for data processing...")
        time.sleep(120)
        
        tests = [
            self._validate_table_exists,
            self._validate_data_loaded,
            self._validate_data_quality
        ]
        
        all_passed = True
        for test in tests:
            if not test():
                all_passed = False
                # Continue with other tests for complete validation
        
        return all_passed

    def _validate_table_exists(self) -> bool:
        """Validate that the BigQuery table exists."""
        self.log_info("Checking if BigQuery table exists...")
        
        try:
            table_id = self.config.get_resource_name('bigquery_table')
            table = self.bigquery_client.get_table(table_id)
            
            self.log_success(f"Table exists with {table.num_rows} total rows")
            self.results['table_exists'] = True
            self.results['total_rows'] = table.num_rows
            return True
            
        except Exception as e:
            self.log_error(f"Table validation failed: {e}")
            self.results['table_exists'] = False
            return False

    def _validate_data_loaded(self) -> bool:
        """Validate that data was loaded for the test date."""
        self.log_info("Checking data for test date...")
        
        table_id = self.config.get_resource_name('bigquery_table')
        test_date = self.config.get('test_date')
        
        query = f"""
        SELECT
            COUNT(*) as record_count,
            COUNT(DISTINCT id) as unique_breweries
        FROM `{table_id}`
        WHERE DATE(processed_date) = '{test_date}'
        """
        
        try:
            query_job = self.bigquery_client.query(query)
            results = query_job.result()
            
            for row in results:
                record_count = row.record_count
                unique_breweries = row.unique_breweries
                
                self.log_info(f"Records: {record_count}")
                self.log_info(f"Unique breweries: {unique_breweries}")
                
                self.results['records_for_date'] = record_count
                self.results['unique_breweries'] = unique_breweries
                
                if record_count > 0:
                    self.log_success("Data loaded successfully!")
                    return True
                else:
                    self.log_error(f"No records found for {test_date}")
                    return False
                    
        except Exception as e:
            self.log_error(f"Error querying data: {e}")
            return False

    def _validate_data_quality(self) -> bool:
        """Validate data quality metrics."""
        self.log_info("Checking data quality...")
        
        table_id = self.config.get_resource_name('bigquery_table')
        test_date = self.config.get('test_date')
        
        query = f"""
        SELECT
            COUNT(*) as total_records,
            COUNT(CASE WHEN id IS NULL THEN 1 END) as null_ids,
            COUNT(CASE WHEN name IS NULL OR name = '' THEN 1 END) as null_names
        FROM `{table_id}`
        WHERE DATE(processed_date) = '{test_date}'
        """
        
        try:
            query_job = self.bigquery_client.query(query)
            results = query_job.result()
            
            for row in results:
                total = row.total_records
                null_ids = row.null_ids
                null_names = row.null_names
                
                self.log_info(f"Total records: {total}")
                self.log_info(f"Null IDs: {null_ids}")
                self.log_info(f"Null names: {null_names}")
                
                # Quality score
                quality_issues = null_ids + null_names
                quality_score = 1 - (quality_issues / total) if total > 0 else 0
                
                self.log_info(f"Quality score: {quality_score:.2%}")
                self.results['quality_score'] = quality_score
                
                if quality_score >= 0.9:  # 90% threshold
                    self.log_success("Data quality check passed!")
                    return True
                else:
                    self.log_warning("Data quality below threshold")
                    return False
                    
        except Exception as e:
            self.log_error(f"Error checking data quality: {e}")
            return False
