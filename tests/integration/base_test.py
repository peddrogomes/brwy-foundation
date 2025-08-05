#!/usr/bin/env python3
"""
Base test class for integration tests.
"""

import time
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any
from .config import TestConfig

logger = logging.getLogger(__name__)


class BaseIntegrationTest(ABC):
    """Base class for all integration tests."""
    
    def __init__(self, config: TestConfig):
        """Initialize the test with configuration."""
        self.config = config
        self.start_time = None
        self.results = {}
        self.test_name = self.__class__.__name__.replace('Tester', '').lower()
    
    def run_tests(self) -> bool:
        """Run all tests for this component."""
        self.start_time = time.time()
        logger.info(f"🧪 Starting {self.test_name} tests...")
        
        try:
            return self._execute_tests()
        except Exception as e:
            logger.error(f"❌ {self.test_name} tests crashed: {e}")
            self.results['error'] = str(e)
            return False
    
    @abstractmethod
    def _execute_tests(self) -> bool:
        """Execute the actual tests. Must be implemented by subclasses."""
        pass
    
    def get_duration(self) -> float:
        """Get test duration in seconds."""
        if self.start_time is None:
            return 0
        return time.time() - self.start_time
    
    def get_results(self) -> Dict[str, Any]:
        """Get detailed test results."""
        return self.results
    
    def log_success(self, message: str):
        """Log a success message."""
        logger.info(f"  ✅ {message}")
    
    def log_error(self, message: str):
        """Log an error message."""
        logger.error(f"  ❌ {message}")
    
    def log_warning(self, message: str):
        """Log a warning message."""
        logger.warning(f"  ⚠️ {message}")
    
    def log_info(self, message: str):
        """Log an info message."""
        logger.info(f"  📝 {message}")
