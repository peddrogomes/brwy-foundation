from google.cloud import dataproc_v1 as dataproc
import json
import os
import logging
import base64


project_id = os.environ.get('GCP_PROJECT')
region = os.environ.get('REGION')
template_name = os.environ.get('DATAPROC_TEMPLATE_NAME')

logging.getLogger().setLevel(logging.INFO)

def main(event, context):
    """
    Cloud Function to trigger Dataproc Workflow Template via Pub/Sub
    Receives parameters: steps (list of steps) and date (date for processing)
    """

    if 'data' in event:
        try:
            message = base64.b64decode(event['data']).decode('utf-8')
            message_data = json.loads(message)
            steps = message_data.get('steps')
            date = message_data.get('date')

        except json.JSONDecodeError as e:
            error_msg = (f"Error decoding JSON message: {message}. "
                         f"Error: {str(e)}")
            logging.error(error_msg)
            raise Exception(error_msg)
        except Exception as e:
            error_msg = f"Error processing message: {str(e)}"
            logging.error(error_msg)
            raise Exception(error_msg)
    else:
        error_msg = "No message received in trigger, forcing stop"
        logging.error(error_msg)
        raise Exception(error_msg)
    
    logging.info(f"Received steps: {steps}, date: {date}")
    
    try:
        # Dataproc client with regional endpoint
        endpoint = f"{region}-dataproc.googleapis.com:443"
        client_options = {"api_endpoint": endpoint}
        client = dataproc.WorkflowTemplateServiceClient(
            client_options=client_options
        )
        
        # Configure workflow job
        workflow_template_name = (
            f"projects/{project_id}/regions/{region}/"
            f"workflowTemplates/{template_name}"
        )
        
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
        
        logging.info(f"Workflow started successfully: {operation.result()}")
        
    except Exception as e:
        error_msg = (f"Error starting workflow: {str(e)}")
        logging.error(error_msg)
        raise Exception(error_msg)
    
    return 'OK'

