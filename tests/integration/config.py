#!/usr/bin/env python3
"""
Configuration management for integration tests.
"""

import os
from datetime import datetime
from typing import Dict, Any


class TestConfig:
    """Manages test configuration and environment variables."""
    
    def __init__(self, args=None):
        """Initialize configuration from arguments or environment."""
        self.config = {}
        if args:
            self._load_from_args(args)
        else:
            self._load_from_env()
    
    def _load_from_args(self, args):
        """Load configuration from command line arguments."""
        self.config = {
            'project': args.project,
            'data_project': args.data_project,
            'region': args.region,
            'branch_hash': args.branch_hash,
            'test_date': args.test_date,
            'timeout': args.timeout,
            'continue_on_failure': args.continue_on_failure,
            'verbose': args.verbose
        }
    
    def _load_from_env(self):
        """Load configuration from environment variables."""
        self.config = {
            'project': os.getenv('PROJECT', ''),
            'data_project': os.getenv('DATA_PROJECT', ''),
            'region': os.getenv('REGION', 'us-east1'),
            'branch_hash': os.getenv('BRANCH_HASH', ''),
            'test_date': os.getenv('TEST_DATE', datetime.now().strftime('%Y-%m-%d')),
            'timeout': int(os.getenv('TIMEOUT', '1800')),
            'continue_on_failure': os.getenv('CONTINUE_ON_FAILURE', 'false').lower() == 'true',
            'verbose': os.getenv('VERBOSE', 'false').lower() == 'true'
        }
    
    def get(self, key: str, default=None):
        """Get configuration value."""
        return self.config.get(key, default)
    
    def get_resource_name(self, resource_type: str) -> str:
        """Get standardized resource name."""
        branch_hash = self.config['branch_hash']
        names = {
            'api_extract_topic': f"api-extract-topic{branch_hash}",
            'trigger_dataproc_topic': f"trigger-dataproc-topic{branch_hash}",
            'api_extract_function': f"api-extract{branch_hash}",
            'trigger_dataproc_function': f"trigger-dataproc{branch_hash}",
            'bronze_bucket': f"brwy-bronze{branch_hash}",
            'silver_bucket': f"brwy-silver{branch_hash}",
            'functions_bucket': f"brwy-functions{branch_hash}",
            'bigquery_table': f"{self.config['data_project']}.brwy_data.breweries"
        }
        return names.get(resource_type, f"unknown-{resource_type}")
    
    def validate(self) -> bool:
        """Validate that required configuration is present."""
        required = ['project', 'data_project', 'branch_hash']
        for key in required:
            if not self.config.get(key):
                print(f"❌ Missing required config: {key}")
                return False
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Return configuration as dictionary."""
        return self.config.copy()
    
    def __str__(self) -> str:
        """String representation of config."""
        return f"TestConfig({self.config})"
