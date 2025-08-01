import requests
import json
import math
import base64
import os
from google.cloud import pubsub_v1
from google.cloud import storage
from datetime import datetime

breweries_per_page = 200
VALID_EXTRACT_TYPES = ['all', 'by_type', 'by_state']

# Environment variables
PUBSUB_TOPIC = os.environ.get('PUBSUB_TOPIC')
GCS_BUCKET_LANDING = os.environ.get('GCS_BUCKET_LANDING')

# Initialize clients
publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client()


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
            print(error_msg)
            raise Exception(error_msg)
    else:
        error_msg = "No message received in trigger, forcing stop"
        print(error_msg)
        raise Exception(error_msg)

    print(f"Requested extraction type: {extract_type}")

    # Validate extract type
    if extract_type not in VALID_EXTRACT_TYPES:
        error_msg = (f"Invalid extraction type: {extract_type}. "
                     f"Valid types: {', '.join(VALID_EXTRACT_TYPES)}")
        print(error_msg)
        raise Exception(error_msg)

    # Process based on extract type
    if extract_type == 'all':
        extract_all_breweries(extract_page)
    elif extract_type == 'by_type':
        pass
    elif extract_type == 'by_state':
        pass


def save_to_gcs(data, page_number):
    """Save data to Google Cloud Storage"""
    try:
        bucket = storage_client.bucket(GCS_BUCKET_LANDING)
        
        # Create filename with timestamp and page number
        date = datetime.now().strftime("%Y-%m-%d")
        
        filename = f"{date}/page_{page_number}.json"
        
        # Create blob and upload
        blob = bucket.blob(filename)
        blob.upload_from_string(
            json.dumps(data, indent=2, ensure_ascii=False),
            content_type='application/json'
        )
        
        print(f"Data saved to gs://{GCS_BUCKET_LANDING}/{filename}")
        
    except Exception as e:
        error_msg = f"Error saving to GCS: {str(e)}"
        print(error_msg)
        raise Exception(error_msg)


def extract_all_breweries(extract_page):
    """Extract all breweries from the API"""

    if extract_page is None:
        # Extract metadata from the Open Brewery DB API
        meta_url = "https://api.openbrewerydb.org/v1/breweries/meta"
        meta_response = requests.get(meta_url)
        if meta_response.status_code == 200:
            meta_data = meta_response.json()
            total_breweries = meta_data["total"]
            total_extract_pages = math.ceil(
                total_breweries / breweries_per_page)
            print(f"Total breweries: {total_breweries}")
            print(f"Total pages: {total_extract_pages}")
        else:
            error_msg = (f"Error accessing meta endpoint: "
                         f"{meta_response.status_code}")
            print(error_msg)
            raise Exception(error_msg)

        # Publish messages to Pub/Sub for each page
        for page in range(1, total_extract_pages + 1):
            message_data = {
                "type": "all",
                "extract_page": page
            }
            message_json = json.dumps(message_data)
            message_bytes = message_json.encode('utf-8')
            
            future = publisher.publish(PUBSUB_TOPIC, message_bytes)
            print(f"Published message for page {page}: {future.result()}")

        return 'OK'
    
    if extract_page:
        api_url = (f"https://api.openbrewerydb.org/v1/breweries?"
                   f"page={extract_page}&per_page={breweries_per_page}")
        
        response = requests.get(api_url)

        if response.status_code == 200:
            breweries = response.json()
        else:
            error_msg = f"Error accessing API: {response.status_code}"
            print(error_msg)
            raise Exception(error_msg)
        
        # Save data to GCS bucket
        save_to_gcs(breweries, extract_page)