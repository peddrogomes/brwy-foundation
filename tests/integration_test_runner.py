#!/usr/bin/env python3
"""
Simplified main test runner for integration tests.
"""

import sys
import os
import argparse
import logging
from datetime import datetime

# Add the tests directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from integration.config import TestConfig
from integration.test_infrastructure import InfrastructureTester
from integration.test_api_extract import ApiExtractTester
from integration.test_dataproc import DataprocTester
from integration.test_bigquery import BigQueryTester

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_integration_tests(config: TestConfig) -> bool:
    """Run all integration tests."""
    logger.info("🚀 Starting integration test suite...")
    logger.info(f"📋 Config: {config}")
    
    # Test components in order
    test_classes = [
        InfrastructureTester,
        ApiExtractTester,
        DataprocTester,
        BigQueryTester
    ]
    
    results = {}
    start_time = datetime.now()
    
    for test_class in test_classes:
        test_name = test_class.__name__.replace('Tester', '').lower()
        logger.info(f"\n{'='*50}")
        logger.info(f"🧪 Running {test_name} tests...")
        logger.info(f"{'='*50}")
        
        tester = test_class(config)
        success = tester.run_tests()
        
        results[test_name] = {
            'success': success,
            'duration': tester.get_duration(),
            'details': tester.get_results()
        }
        
        if success:
            logger.info(f"✅ {test_name} tests passed!")
        else:
            logger.error(f"❌ {test_name} tests failed!")
            if not config.get('continue_on_failure', False):
                logger.error("🛑 Stopping tests due to failure")
                break
    
    # Print summary
    total_duration = (datetime.now() - start_time).total_seconds()
    overall_success = all(r['success'] for r in results.values())
    
    logger.info(f"\n{'='*60}")
    logger.info("📊 INTEGRATION TEST SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"⏱️  Duration: {total_duration:.2f}s")
    logger.info(f"📅 Date: {config.get('test_date')}")
    logger.info(f"🏗️  Project: {config.get('project')}")
    
    logger.info("\n📋 Results:")
    for name, result in results.items():
        status = "✅ PASS" if result['success'] else "❌ FAIL"
        duration = result['duration']
        logger.info(f"  {name}: {status} ({duration:.2f}s)")
    
    status = "✅ SUCCESS" if overall_success else "❌ FAILED"
    logger.info(f"\n🎯 Final Result: {status}")
    
    return overall_success


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run integration tests')
    
    parser.add_argument('--project', required=True, help='GCP project ID')
    parser.add_argument('--data-project', required=True, help='GCP data project ID')
    parser.add_argument('--region', default='us-central1', help='GCP region')
    parser.add_argument('--branch-hash', required=True, help='Branch hash')
    parser.add_argument('--test-date', default=datetime.now().strftime('%Y-%m-%d'), help='Test date')
    parser.add_argument('--timeout', type=int, default=1800, help='Timeout in seconds')
    parser.add_argument('--continue-on-failure', action='store_true', help='Continue on failure')
    parser.add_argument('--verbose', action='store_true', help='Verbose logging')

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_arguments()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    config = TestConfig(args)
    
    if not config.validate():
        sys.exit(1)
    
    try:
        success = run_integration_tests(config)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.error("🛑 Tests interrupted")
        sys.exit(130)
    except Exception as e:
        logger.error(f"💥 Test runner crashed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
