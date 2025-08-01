from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
import json
import os
from datetime import datetime
import logging
import base64


project_id = os.environ.get('GCP_PROJECT')
region = os.environ.get('REGION')
template_name = 'brwy-pipeline-template'


def main(event, context):
    """
    Cloud Function to trigger Dataproc Workflow Template via Pub/Sub
    Receives parameters: steps (list of steps) and date (date for processing)
    """
    
    logging.getLogger().setLevel(logging.INFO)

    if 'data' in event:
        message = base64.b64decode(event['data']).decode('utf-8')
        try:
            message_data = json.loads(message)
            steps = message_data.get('steps')
            date = message_data.get('date')

        except json.JSONDecodeError as e:
            error_msg = (f"Error decoding JSON message: {message}. "
                         f"Error: {str(e)}")
            logging.info(error_msg)
            raise Exception(error_msg)
    else:
        error_msg = "No message received in trigger, forcing stop"
        logging.info(error_msg)
        raise Exception(error_msg)
    
    logging.info(f"Received steps: {steps}, date: {date}")
    
    try:
        # Dataproc client
        client = dataproc.WorkflowTemplateServiceClient()
        
        # Configure workflow job
        workflow_template_name = f"projects/{project_id}/regions/{region}/workflowTemplates/{template_name}"
        
        # Parameters for template
        parameters = {
            'DATE': date
        }
        
        # Submit workflow
        operation = client.instantiate_workflow_template(
            request={
                "name": workflow_template_name,
                "parameters": parameters
            }
        )
        
        logging.info(f"Workflow started successfully: {operation.name}")
        
    except Exception as e:
        error_msg = (f"Error starting workflow: {str(e)}")
        logging.info(error_msg)
        raise Exception(error_msg)
    
    return 'OK'

