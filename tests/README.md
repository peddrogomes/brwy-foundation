# Integration Tests

This directory contains the integration test suite for the brewery data pipeline.

## Structure

```
tests/
├── integration_test_runner.py    # Main test runner
├── integration/
│   ├── __init__.py
│   ├── test_infrastructure.py    # Infrastructure validation tests
│   ├── test_api_extract.py       # API Extract function tests
│   ├── test_dataproc.py         # Dataproc workflow tests
│   ├── test_bigquery.py         # BigQuery validation tests
│   ├── monitor_integration_test.sh     # Monitoring script
│   └── validate_bigquery_data.py       # Standalone BigQuery validator
└── README.md                     # This file
```

## Running Tests

### Via GitHub Action (Recommended)

1. Go to **Actions** > **Integration Test** in GitHub
2. Click **Run workflow**
3. Configure parameters as needed

### Manual Execution

```bash
cd tests
python integration_test_runner.py \
  --project "your-project-dev" \
  --data-project "your-data-project-dev" \
  --region "us-east1" \
  --branch-hash "-abc12345" \
  --verbose
```

### Required Arguments

- `--project`: GCP project ID for infrastructure
- `--data-project`: GCP project ID for BigQuery data
- `--branch-hash`: Unique hash for resource isolation
- `--region`: GCP region (default: us-central1)

### Optional Arguments

- `--test-date`: Date for testing (default: today)
- `--timeout`: Timeout in seconds (default: 1800)
- `--continue-on-failure`: Continue tests even if one fails
- `--verbose`: Enable verbose logging

## Test Components

### 1. Infrastructure Tests (`test_infrastructure.py`)
- Validates storage buckets exist
- Checks Pub/Sub topics are created
- Verifies Cloud Functions are deployed

### 2. API Extract Tests (`test_api_extract.py`)
- Triggers the api-extract function via Pub/Sub
- Monitors function logs for completion
- Verifies JSON files are created in bronze bucket

### 3. Dataproc Tests (`test_dataproc.py`)
- Monitors trigger-dataproc function execution
- Tracks Dataproc job status via API
- Validates job completion

### 4. BigQuery Tests (`test_bigquery.py`)
- Checks table existence
- Validates data was loaded for test date
- Performs data quality checks
- Calculates quality score

## Monitoring


### Standalone BigQuery Validation

```bash
cd tests/integration
python validate_bigquery_data.py your-data-project-dev 2025-08-05
```

## Adding New Tests

To add a new test component:

1. Create a new test file in `tests/integration/test_your_component.py`
2. Implement a class with the following interface:

```python
class YourComponentTester:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.start_time = None
        self.results = {}

    def run_tests(self) -> bool:
        """Run all tests, return True if all pass"""
        pass

    def get_duration(self) -> float:
        """Return test duration in seconds"""
        pass

    def get_results(self) -> Dict[str, Any]:
        """Return detailed test results"""
        pass
```

3. Add your tester to `integration_test_runner.py`:

```python
from integration.test_your_component import YourComponentTester

# In _setup_test_components method:
self.test_components = [
    # ... existing components ...
    ('your_component', YourComponentTester(self.config))
]
```

## Environment Variables

The tests use the following environment variables from the GitHub Action:

- `GOOGLE_CREDENTIALS`: Service account credentials JSON
- Project and configuration details are passed as command-line arguments

## Error Handling

- Tests fail fast by default (stop on first failure)
- Use `--continue-on-failure` to run all tests regardless of failures
- Detailed error messages and logs are provided
- Test summary shows duration and status for each component

## Security

- Tests use isolated environments with unique branch hashes
- No impact on production resources
- Optional cleanup removes all test resources
- Credentials are handled securely via GitHub secrets
