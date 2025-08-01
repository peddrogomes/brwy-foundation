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
GCS_BUCKET_LANDING = os.environ.get('GCS_BUCKET_LANDING')

# Configure logging
logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

logger = logging.getLogger(__name__)

# Initialize clients
publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client()
firestore_client = firestore.Client()


def main(event, context):

    if 'data' in event:
        message = base64.b64decode(event['data']).decode('utf-8')
        try:
            message_data = json.loads(message)
            extract_type = message_data.get('type', '')
            extract_page = message_data.get('extract_page', None)

        except json.JSONDecodeError as e:
            error_msg = (f"Error decoding JSON message: {message}. "
                         f"Error: {str(e)}")
            logger.error(error_msg)
            raise Exception(error_msg)
    else:
        error_msg = "No message received in trigger, forcing stop"
        logger.error(error_msg)
        raise Exception(error_msg)

    logger.info(f"Requested extraction type: {extract_type}")

    # Validate extract type
    if extract_type not in VALID_EXTRACT_TYPES:
        error_msg = (f"Invalid extraction type: {extract_type}. "
                     f"Valid types: {', '.join(VALID_EXTRACT_TYPES)}")
        logger.error(error_msg)
        raise Exception(error_msg)

    # Process based on extract type
    if extract_type == 'all':
        extract_all_breweries(extract_page)
    elif extract_type == 'by_type':
        pass
    elif extract_type == 'by_state':
        pass
    
    return 'OK'


def extract_all_breweries(extract_page):
    """Extract all breweries from the API"""
    
    date = datetime.now().strftime("%Y-%m-%d")

    if extract_page is None:
        # Extract metadata from the Open Brewery DB API
        meta_url = "https://api.openbrewerydb.org/v1/breweries/meta"
        meta_response = requests.get(meta_url)
        if meta_response.status_code == 200:
            meta_data = meta_response.json()
            total_breweries = meta_data["total"]
            total_extract_pages = math.ceil(
                total_breweries / breweries_per_page)
            logger.info(f"Total breweries: {total_breweries}")
            logger.info(f"Total pages: {total_extract_pages}")
        else:
            error_msg = (f"Error accessing meta endpoint: "
                         f"{meta_response.status_code}")
            logger.error(error_msg)
            raise Exception(error_msg)

        # Initialize extraction job in Firestore
        
        initialize_extraction_job(date, total_extract_pages)

        # Publish messages to Pub/Sub for each page
        for page in range(1, total_extract_pages + 1):
            message_data = {
                "type": "all",
                "extract_page": page
            }
            message_json = json.dumps(message_data)
            message_bytes = message_json.encode('utf-8')
            
            future = publisher.publish(PUBSUB_TOPIC, message_bytes)
            logger.info(f"Published message for page {page}: "
                        f"{future.result()}")

        return 'OK'
    
    if extract_page:
        api_url = (f"https://api.openbrewerydb.org/v1/breweries?"
                   f"page={extract_page}&per_page={breweries_per_page}")
        
        response = requests.get(api_url)

        if response.status_code == 200:
            breweries = response.json()
        else:
            error_msg = f"Error accessing API: {response.status_code}"
            logger.error(error_msg)
            raise Exception(error_msg)
        
        save_to_gcs(breweries, extract_page, date)

        log_page_save_and_check_completion(extract_page, date)


def save_to_gcs(data: str, page_number: int, date: str):
    """Save data to Google Cloud Storage"""
    try:
        bucket = storage_client.bucket(GCS_BUCKET_LANDING)
        
        filename = f"{date}/page_{page_number}.json"
        
        # Create blob and upload
        blob = bucket.blob(filename)
        blob.upload_from_string(
            json.dumps(data, indent=2, ensure_ascii=False),
            content_type='application/json'
        )
        
        logger.info(f"Data saved to gs://{GCS_BUCKET_LANDING}/{filename}")
            
    except Exception as e:
        error_msg = f"Error saving to GCS: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def log_page_save_and_check_completion(page_number, date):
    """Log page save to Firestore and check if all pages are completed"""
    try:
        # Reference to the extraction job document
        job_doc_ref = firestore_client.collection(
            'extraction_jobs').document(date)
        
        # Use transaction to ensure atomicity
        @firestore.transactional
        def update_and_check(transaction):
            # Get the current job document
            job_doc = job_doc_ref.get(transaction=transaction)
            
            if not job_doc.exists:
                logger.warning(f"Job document for {date} does not exist. "
                               "Cannot log page save.")
                return False
            
            job_data = job_doc.to_dict()
            total_pages = job_data.get('total_pages')
            completed_pages = job_data.get('completed_pages', [])
            dataproc_triggered = job_data.get('dataproc_triggered', False)
            
            # Add current page to completed pages if not already there
            if page_number not in completed_pages:
                completed_pages.append(page_number)
                
                # Update the document
                transaction.update(job_doc_ref, {
                    'completed_pages': completed_pages,
                    'last_update': datetime.now()
                })
                
                logger.info(f"Page {page_number} logged as completed. "
                            f"Progress: {len(completed_pages)}/{total_pages}")
            
            # Check if all pages are completed and dataproc hasn't been
            # triggered yet
            if (len(completed_pages) == total_pages and
                    not dataproc_triggered):
                
                # Mark dataproc as triggered to prevent multiple calls
                transaction.update(job_doc_ref, {
                    'dataproc_triggered': True,
                    'dataproc_trigger_time': datetime.now()
                })
                
                logger.info(f"All {total_pages} pages completed. "
                            "Triggering Dataproc...")
                return True
            
            return False
        
        # Execute the transaction
        transaction = firestore_client.transaction()
        should_trigger_dataproc = update_and_check(transaction)
        
        # Trigger dataproc outside of transaction if needed
        if should_trigger_dataproc:
            trigger_dataproc()
            logger.info("Dataproc triggered successfully after all pages "
                        "completed.")
            
    except Exception as e:
        error_msg = f"Error logging page save to Firestore: {str(e)}"
        logger.error(error_msg)
        # Don't raise exception here to avoid failing the main process


def initialize_extraction_job(date, total_pages):
    """Initialize the extraction job document in Firestore"""
    try:
        job_doc_ref = firestore_client.collection(
            'extraction_jobs').document(date)
        
        # Check if document already exists
        job_doc = job_doc_ref.get()
        
        if not job_doc.exists:
            job_data = {
                'date': date,
                'total_pages': total_pages,
                'completed_pages': [],
                'dataproc_triggered': False,
                'created_at': datetime.now(),
                'last_update': datetime.now()
            }
            
            job_doc_ref.set(job_data)
            logger.info(f"Initialized extraction job for {date} with "
                        f"{total_pages} pages")
        else:
            logger.info(f"Extraction job for {date} already exists")
            
    except Exception as e:
        error_msg = f"Error initializing extraction job in Firestore: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)
    
def trigger_dataproc():
    logger.info("Triggering Dataproc job...")
    #TODO: Trigger dataproc logic
