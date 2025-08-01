import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from google.cloud import bigquery, storage


def main():
    """
    Brewery data transformation script
    Receives date as parameter: python total-transform.py YYYY-MM-DD
    """
    
    # Check if date was passed as parameter
    if len(sys.argv) > 1:
        date_param = sys.argv[1]
        print(f"Processing transformations for date: {date_param}")
    else:
        date_param = datetime.now().strftime('%Y-%m-%d')
        print(f"No date provided, using current date: {date_param}")
    
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName(f"Breweries Transform - {date_param}") \
        .getOrCreate()
    
    try:
        print(f"Starting transformation process for {date_param}")
        
        # Here would be implemented the data transformation logic
        # For example: read data from raw bucket, apply transformations
        # and save to processed bucket
        
        print(f"Transformation completed successfully for {date_param}")
        
    except Exception as e:
        print(f"Error during transformation process: {str(e)}")
        raise e
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
