import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from google.cloud import bigquery, storage


def main():
    """
    Brewery data load script
    Receives date as parameter: python total-load.py YYYY-MM-DD
    """
    
    # Check if date was passed as parameter
    if len(sys.argv) > 1:
        date_param = sys.argv[1]
        print(f"Processing data for date: {date_param}")
    else:
        date_param = datetime.now().strftime('%Y-%m-%d')
        print(f"No date provided, using current date: {date_param}")
    
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName(f"Breweries Load - {date_param}") \
        .getOrCreate()
    
    try:
        print(f"Starting load process for {date_param}")
        
        # Here would be implemented the data load logic
        # For example: read data from landing bucket and load to raw bucket
        
        print(f"Load completed successfully for {date_param}")
        
    except Exception as e:
        print(f"Error during load process: {str(e)}")
        raise e
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
