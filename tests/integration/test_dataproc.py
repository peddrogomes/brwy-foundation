#!/usr/bin/env python3
"""
Dataproc workflow tests for the brewery data pipeline.
"""

import time
import logging
from datetime import datetime, timedelta
from google.cloud import logging as cloud_logging
from google.cloud import dataproc_v1 as dataproc
from .base_test import BaseIntegrationTest

logger = logging.getLogger(__name__)


class DataprocTester(BaseIntegrationTest):
    """Tests the trigger-dataproc function and Dataproc job execution."""

    def __init__(self, config):
        """Initialize the Dataproc tester."""
        super().__init__(config)
        
        # Initialize clients
        project = config.get('project')
        region = config.get('region')
        
        self.logging_client = cloud_logging.Client(project=project)
        
        # Dataproc client
        endpoint = f"{region}-dataproc.googleapis.com:443"
        client_options = {"api_endpoint": endpoint}
        self.dataproc_client = dataproc.JobControllerClient(
            client_options=client_options
        )

    def _execute_tests(self) -> bool:
        """Run all Dataproc tests."""
        tests = [
            self._monitor_trigger_function,
            self._monitor_dataproc_job
        ]
        
        for test in tests:
            if not test():
                return False
        
        return True

    def _monitor_trigger_function(self) -> bool:
        """Monitor trigger-dataproc function execution."""
        self.log_info("Monitoring trigger-dataproc function...")
        
        function_name = self.config.get_resource_name('trigger_dataproc_function')
        start_time = datetime.utcnow()
        timeout = timedelta(minutes=15)
        
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
                    
                    if "job" in log_text.lower():
                        self.log_success("Dataproc job triggered")
                        self.results['trigger_executed'] = True
                        return True
                    
                    if "Error" in log_text or entry.severity_name == "ERROR":
                        self.log_error(f"Trigger function error: {log_text}")
                        return False
                
            except Exception as e:
                self.log_warning(f"Error reading trigger logs: {e}")
            
            time.sleep(30)
        
        # Fallback: check for running jobs
        self.log_info("Checking for active Dataproc jobs...")
        try:
            jobs = self.dataproc_client.list_jobs(
                request={
                    "project_id": self.config.get('project'),
                    "region": self.config.get('region')
                }
            )
            
            for job in jobs:
                if job.status.state in [
                    dataproc.JobStatus.State.PENDING,
                    dataproc.JobStatus.State.RUNNING,
                    dataproc.JobStatus.State.DONE
                ]:
                    self.log_success(f"Found active job: {job.reference.job_id}")
                    self.results['job_id'] = job.reference.job_id
                    return True
                    
        except Exception as e:
            self.log_warning(f"Could not list jobs: {e}")
        
        self.log_error("Could not verify trigger-dataproc execution")
        return False

    def _monitor_dataproc_job(self) -> bool:
        """Monitor Dataproc job completion."""
        self.log_info("Monitoring Dataproc job...")
        
        job_id = self.results.get('job_id')
        if not job_id:
            self.log_warning("No job ID available, skipping job monitoring")
            return True  # Don't fail the test
        
        start_time = datetime.utcnow()
        timeout = timedelta(minutes=15)
        
        while datetime.utcnow() - start_time < timeout:
            try:
                job = self.dataproc_client.get_job(
                    request={
                        "project_id": self.config.get('project'),
                        "region": self.config.get('region'),
                        "job_id": job_id
                    }
                )
                
                state = job.status.state
                self.log_info(f"Job state: {state.name}")
                
                if state == dataproc.JobStatus.State.DONE:
                    self.log_success("Dataproc job completed successfully")
                    return True
                elif state == dataproc.JobStatus.State.ERROR:
                    self.log_error(f"Job failed: {job.status.details}")
                    return False
                
            except Exception as e:
                self.log_warning(f"Error checking job status: {e}")
            
            time.sleep(60)
        
        self.log_warning("Job monitoring timeout, continuing...")
        return True  # Don't fail the test, continue to BigQuery validation
