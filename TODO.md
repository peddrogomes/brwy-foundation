✅ **COMPLETED** - Need to create a template to run the total-load and total-transform steps, the files are in the src/dataproc folder.

✅ **COMPLETED** - The template should be created in dataproc.tf.
✅ **COMPLETED** - Also need to create the trigger-dataproc function that will receive the steps to be executed and the date, and trigger dataproc.
✅ **COMPLETED** - Pub/Sub integration between api-extract and trigger-dataproc

## Completed implementations:

### 1. Dataproc Workflow Template (dataproc.tf)
- Created `google_dataproc_workflow_template` named "brwy-pipeline-template"
- Configured to execute `total-load` and `total-transform` jobs in sequence
- Template accepts `DATE` parameter that is passed to Python scripts
- Added service account with necessary permissions

### 2. trigger-dataproc Function
- Created Cloud Function triggered via Pub/Sub
- Receives parameters: `steps` (list of steps) and `date` (date for processing)
- Entry point configured for `trigger_dataproc_pubsub`
- Maintained HTTP version for manual testing (`trigger_dataproc_http`)

### 3. Pub/Sub Integration
- Created `trigger-dataproc-topic` topic in pubsub.tf
- Updated `api-extract` function to publish message to topic when all pages are processed
- Configured IAM permissions to allow communication between functions
- Added `TRIGGER_DATAPROC_TOPIC` environment variable in api-extract

### 4. Improved Python Scripts
- `total-load.py` and `total-transform.py` now accept date as parameter
- Implemented basic structure for processing

### 5. Additional Infrastructure
- Dataproc bucket for staging
- Service account with adequate permissions
- Configured IAM roles
- Pub/Sub communication between functions

### 6. Complete flow implemented:
1. **api-extract** extracts data from API and saves to GCS
2. When all pages are processed, **api-extract** publishes message to `trigger-dataproc-topic` topic
3. **trigger-dataproc** is activated by Pub/Sub and triggers Dataproc Workflow Template
4. **Dataproc** executes `total-load` and `total-transform` steps in sequence