#!/usr/bin/env python3
"""
API Extract function tests for the brewery data pipeline.
"""

import json
import time
import logging
from datetime import datetime, timedelta
from google.cloud import pubsub_v1
from google.cloud import logging as cloud_logging
from google.cloud import storage
from .base_test import BaseIntegrationTest

logger = logging.getLogger(__name__)


class ApiExtractTester(BaseIntegrationTest):
    """Tests the api-extract Cloud Function."""

    def __init__(self, config):
        """Initialize the API extract tester."""
        super().__init__(config)
        
        # Get credentials using the base class method
        credentials = self._get_credentials()
        project = config.get('project')
        
        # Initialize clients
        self.publisher = pubsub_v1.PublisherClient(credentials=credentials)
        self.logging_client = cloud_logging.Client(
            project=project, credentials=credentials
        )
        self.storage_client = storage.Client(
            project=project, credentials=credentials
        )

    def _execute_tests(self) -> bool:
        """Run all API extract tests."""
        tests = [
            self._trigger_function,
            self._monitor_function,
            self._verify_files
        ]
        
        for test in tests:
            if not test():
                return False
        
        return True

    def _trigger_function(self) -> bool:
        """Trigger the api-extract function via Pub/Sub."""
        self.log_info("Triggering api-extract function...")
        
        try:
            project = self.config.get('project')
            topic_name = self.config.get_resource_name('api_extract_topic')
            topic_path = self.publisher.topic_path(project, topic_name)
            
            message_data = {"type": "all", "extract_page": None}
            message = json.dumps(message_data).encode('utf-8')
            
            future = self.publisher.publish(topic_path, message)
            message_id = future.result()
            
            self.log_success(f"Published message: {message_id}")
            self.results['message_published'] = True
            return True
            
        except Exception as e:
            self.log_error(f"Failed to publish message: {e}")
            return False

    def _monitor_function(self) -> bool:
        """Monitor function logs for completion."""
        self.log_info("Monitoring function logs...")
        
        function_name = self.config.get_resource_name('api_extract_function')
        start_time = datetime.utcnow()
        timeout = timedelta(minutes=10)
        
        while datetime.utcnow() - start_time < timeout:
            filter_str = f'''
            resource.type="cloud_function"
            resource.labels.function_name="{function_name}"
            timestamp>="{start_time.isoformat()}Z"
            '''
            
            try:
                entries = list(self.logging_client.list_entries(
                    filter_=filter_str,
                    order_by='timestamp desc',
                    max_results=20
                ))
                
                for entry in entries:
                    log_text = str(entry.payload)
                    
                    if "Triggering Dataproc" in log_text:
                        self.log_success("Function completed successfully")
                        return True
                    elif "Error" in log_text:
                        self.log_error(f"Function error: {log_text}")
                        return False
                
            except Exception as e:
                self.log_warning(f"Error reading logs: {e}")
            
            time.sleep(30)
        
        self.log_error("Timeout waiting for function completion")
        return False

    def _verify_files(self) -> bool:
        """Verify files were created in bronze bucket."""
        self.log_info("Verifying files in bronze bucket...")
        
        try:
            bucket_name = self.config.get_resource_name('bronze_bucket')
            test_date = self.config.get('test_date')
            
            bucket = self.storage_client.bucket(bucket_name)
            blobs = list(bucket.list_blobs(prefix=f"{test_date}/"))
            
            if not blobs:
                self.log_error(f"No files found for date {test_date}")
                return False
            
            self.log_success(f"Found {len(blobs)} files in bronze bucket")
            self.results['file_count'] = len(blobs)
            return True
            
        except Exception as e:
            self.log_error(f"Error checking bronze bucket: {e}")
            return False
