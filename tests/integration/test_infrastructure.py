#!/usr/bin/env python3
"""
Infrastructure validation tests for the brewery data pipeline.
"""

import logging
from google.cloud import storage
from google.cloud import pubsub_v1
from .base_test import BaseIntegrationTest

logger = logging.getLogger(__name__)


class InfrastructureTester(BaseIntegrationTest):
    """Tests infrastructure components are properly deployed."""

    def __init__(self, config):
        """Initialize the infrastructure tester."""
        super().__init__(config)
        
        # Initialize clients
        self.storage_client = storage.Client(project=config.get('project'))
        self.publisher = pubsub_v1.PublisherClient()

    def _execute_tests(self) -> bool:
        """Run all infrastructure validation tests."""
        tests = [
            self._test_storage_buckets,
            self._test_pubsub_topics
        ]
        
        for test in tests:
            if not test():
                return False
        
        return True

    def _test_storage_buckets(self) -> bool:
        """Test that required storage buckets exist."""
        self.log_info("Testing storage buckets...")
        
        buckets = ['bronze_bucket', 'silver_bucket', 'functions_bucket']
        
        for bucket_type in buckets:
            bucket_name = self.config.get_resource_name(bucket_type)
            try:
                bucket = self.storage_client.bucket(bucket_name)
                # This will raise an exception if bucket doesn't exist
                bucket.reload()
                self.log_success(f"Bucket {bucket_name} exists")
                self.results[f"bucket_{bucket_type}"] = True
            except Exception as e:
                self.log_error(f"Bucket {bucket_name} not found: {e}")
                self.results[f"bucket_{bucket_type}"] = False
                return False
        
        return True

    def _test_pubsub_topics(self) -> bool:
        """Test that required Pub/Sub topics exist."""
        self.log_info("Testing Pub/Sub topics...")
        
        topics = ['api_extract_topic', 'trigger_dataproc_topic']
        project = self.config.get('project')
        
        for topic_type in topics:
            topic_name = self.config.get_resource_name(topic_type)
            try:
                topic_path = self.publisher.topic_path(project, topic_name)
                # Try to get topic metadata
                self.publisher.get_topic(request={"topic": topic_path})
                self.log_success(f"Topic {topic_name} exists")
                self.results[f"topic_{topic_type}"] = True
            except Exception as e:
                self.log_error(f"Topic {topic_name} not found: {e}")
                self.results[f"topic_{topic_type}"] = False
                return False
        
        return True
