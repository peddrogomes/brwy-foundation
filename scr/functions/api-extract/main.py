import requests
import json
import math
import base64
import os
import logging
from google.cloud import pubsub_v1
from google.cloud import storage
from google.cloud import firestore
from datetime import datetime


breweries_per_page = 200
VALID_EXTRACT_TYPES = ['all', 'by_type', 'by_state']

# Environment variables
PUBSUB_TOPIC = os.environ.get('PUBSUB_TOPIC')
GCS_BUCKET_BRONZE = os.environ.get('GCS_BUCKET_BRONZE')
TRIGGER_DATAPROC_TOPIC = os.environ.get('TRIGGER_DATAPROC_TOPIC')

# Initialize clients
publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client()
firestore_client = firestore.Client()

logging.getLogger().setLevel(logging.INFO)

def main(event, context):
    """
    Extracts brewery data from Open Brewery DB API
    and saves as JSON files to Cloud Storage bucket.
    """
    
    error_msg = None

    if 'data' in event:
        try:
            message = base64.b64decode(event['data']).decode('utf-8')
            message_data = json.loads(message)
            extract_type = message_data.get('type', '')
            extract_page = message_data.get('extract_page', None)

        except json.JSONDecodeError as e:
            error_msg = (f"Error decoding JSON message: {message}. "
                         f"Error: {str(e)}")
            logging.error(error_msg)
            raise Exception(error_msg)
        except Exception as e:
            error_msg = (f"Error processing message: {message}. "
                         f"Error: {str(e)}")
            logging.error(error_msg)
            raise Exception(error_msg)
    else:
        if error_msg:
            raise
        error_msg = "No message received in trigger, forcing stop"
        logging.error(error_msg)
        raise Exception(error_msg)

    # Validate extract type
    if extract_type not in VALID_EXTRACT_TYPES:
        error_msg = (f"Invalid extraction type: {extract_type}. "
                     f"Valid types: {', '.join(VALID_EXTRACT_TYPES)}")
        logging.error(error_msg)
        raise Exception(error_msg)

    logging.info(f"Requested extraction type: {extract_type}")
    # Process based on extract type
    if extract_type == 'all':
        extract_all_breweries(extract_page)
    elif extract_type == 'by_type':
        pass
    elif extract_type == 'by_state':
        pass
    
    return 'OK'


def extract_all_breweries(extract_page: int = None):
    """Extract all breweries from the API"""
    
    date = datetime.now().strftime("%Y-%m-%d")
    error_msg = None

    if extract_page is None:
        # Extract metadata from the Open Brewery DB API
        try:
            meta_url = "https://api.openbrewerydb.org/v1/breweries/meta"
            meta_response = requests.get(meta_url)
            
            if meta_response.status_code == 200:
                meta_data = meta_response.json()
                total_breweries = meta_data["total"]
                total_extract_pages = math.ceil(
                    total_breweries / breweries_per_page)
                logging.info(f"Total breweries: {total_breweries}")
                logging.info(f"Total pages: {total_extract_pages}")
            else:
                error_msg = (f"Error accessing breweries metadata endpoint: "
                            f"{meta_response.status_code}")
                logging.error(error_msg)
                raise Exception(error_msg)

        except Exception as e:
            if error_msg:
                raise

            error_msg = f"Error fetching metadata: {str(e)}"
            logging.error(error_msg)
            raise Exception(error_msg)
        
        initialize_extraction_job(date, total_extract_pages)
        
        try:

            # Publish messages to Pub/Sub for each page
            for page in range(1, total_extract_pages + 1):
                message_data = {
                    "type": "all",
                    "extract_page": page
                }
                message_json = json.dumps(message_data)
                message_bytes = message_json.encode('utf-8')
                
                future = publisher.publish(PUBSUB_TOPIC, message_bytes)
                logging.info(f"Published message for page {page}: "
                        f"{future.result()}")
        
        except Exception as e:
            error_msg = (
            f"Error publishing extraction page messages to Pub/Sub: {str(e)}"
            )
            logging.error(error_msg)
            raise Exception(error_msg)
        
        return 'OK'
    
    if extract_page:

        error_msg = None
        try:
            api_url = (f"https://api.openbrewerydb.org/v1/breweries?"
                       f"page={extract_page}&per_page={breweries_per_page}")
            
            response = requests.get(api_url)

            if response.status_code == 200:
                breweries = response.json()
            else:
                error_msg = (
                    f"Error accessing breweries API: {response.status_code}"
                )
                logging.info(error_msg)
                raise Exception(error_msg)
              
        except Exception as e:
            log_page_save_and_check_completion(
                    extract_page, date, 'failed')
            
            if error_msg:
                raise
            
            error_msg = (
                f"Error during extraction for page {extract_page}: {str(e)}"
            )
            logging.error(error_msg)
            raise Exception(error_msg)
        
        save_to_gcs(breweries, extract_page, date)
            
        # Log successful completion
        log_page_save_and_check_completion(
            extract_page, date, 'completed')
        
        return 'OK'

def save_to_gcs(data: str, page_number: int, date: str):
    """Save data to Google Cloud Storage"""
    try:
        bucket = storage_client.bucket(GCS_BUCKET_BRONZE)
        
        filename = f"{date}/page_{page_number}.json"
        
        # Create blob and upload
        blob = bucket.blob(filename)
        blob.upload_from_string(
            json.dumps(data, indent=2, ensure_ascii=False),
            content_type='application/json'
        )
        
        logging.info(f"Data saved to gs://{GCS_BUCKET_BRONZE}/{filename}")
            
    except Exception as e:
        error_msg = f"Error saving to GCS: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)

# Use transaction to ensure atomicity
@firestore.transactional
def update_and_check(transaction, job_doc_ref, page_number, status, date):
    try:
        # Get the current job document
        job_doc = job_doc_ref.get(transaction=transaction)

        if not job_doc.exists:
            logging.info(
                f"Job document for {date} does not exist. "
                "Cannot log page save."
            )
            return False

        job_data = job_doc.to_dict()
        total_pages = job_data.get('total_pages')
        # Dict format
        completed_pages = job_data.get('completed_pages', {})
        dataproc_triggered = job_data.get('dataproc_triggered', False)

        # Add current page to completed pages if not already there
        page_key = str(page_number)
        if page_key not in completed_pages:
            completed_pages[page_key] = {
                'page_number': page_number,
                'processed_at': datetime.now().isoformat(),
                'status': status
            }

            # Update the document
            transaction.update(job_doc_ref, {
                'completed_pages': completed_pages,
                'last_update': datetime.now()
            })

            logging.info(
                f"Page {page_number} logged with status "
                f"'{status}'. Progress: {len(completed_pages)}/"
                f"{total_pages}"
            )
        elif completed_pages[page_key]['status'] != status:
            # Update status if it has changed
            completed_pages[page_key]['status'] = status
            completed_pages[page_key]['last_updated'] = (
                datetime.now().isoformat()
            )

            transaction.update(job_doc_ref, {
                'completed_pages': completed_pages,
                'last_update': datetime.now()
            })

            logging.info(f"Page {page_number} status updated to '{status}'")

        # Only check for completion if all pages have 'completed' status
        completed_count = sum(
            1 for page_data in completed_pages.values()
            if page_data.get('status') == 'completed'
        )

        # Check if all pages are completed and dataproc hasn't been triggered yet
        if completed_count == total_pages and not dataproc_triggered:
            # Mark dataproc as triggered to prevent multiple calls
            transaction.update(job_doc_ref, {
                'dataproc_triggered': True,
                'dataproc_trigger_time': datetime.now()
            })

            logging.info(
                f"All {total_pages} pages completed. Triggering Dataproc..."
            )
            return True

        return False
    except Exception as e:

        error_msg = f"Error in update_and_check: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)

def log_page_save_and_check_completion(page_number: int, date: str, status='completed'):
    """Log page save to Firestore and check if all pages are completed"""
    try:
        # Reference to the extraction job document
        job_doc_ref = firestore_client.collection(
            'extraction_jobs').document(date)
        
        # Execute the transaction
        transaction = firestore_client.transaction(max_attempts=30)
      
    except Exception as e:
        error_msg = f"Error logging page save to Firestore: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)
    
    should_trigger_dataproc = update_and_check(transaction, job_doc_ref, page_number, status, date)

    if should_trigger_dataproc:
        trigger_dataproc()


def initialize_extraction_job(date: str, total_pages: int):
    """Initialize the extraction job document in Firestore"""
    try:
        job_doc_ref = firestore_client.collection(
            'extraction_jobs').document(date)
        
        # Always create/overwrite the document for reprocessing
        job_data = {
            'date': date,
            'total_pages': total_pages,
            'completed_pages': {},  # Clean dict for reprocessing
            'dataproc_triggered': False,
            'created_at': datetime.now(),
            'last_update': datetime.now()
        }
        
        job_doc_ref.set(job_data)
        logging.info(f"Initialized/reset extraction job for {date} with "
                     f"{total_pages} pages")
            
    except Exception as e:
        error_msg = f"Error initializing extraction job in Firestore: {str(e)}"
        logging.info(error_msg)
        raise Exception(error_msg)


def trigger_dataproc():
    """Trigger Dataproc workflow via Pub/Sub"""
    logging.info("Triggering Dataproc job...")
    
    try:
        date = datetime.now().strftime("%Y-%m-%d")
        
        # Prepare message for trigger-dataproc function
        message_data = {
            "steps": ["total-load", "total-transform"],
            "date": date
        }
        
        message_json = json.dumps(message_data)
        message_bytes = message_json.encode('utf-8')
        
        # Publish message to trigger-dataproc topic
        future = publisher.publish(TRIGGER_DATAPROC_TOPIC, message_bytes)
        logging.info(f"Dataproc trigger message published: {future.result()}")
            
    except Exception as e:
        error_msg = f"Error triggering Dataproc: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)

