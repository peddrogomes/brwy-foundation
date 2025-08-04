import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, concat_ws, to_date, year, month, dayofmonth
)
from datetime import datetime
from google.cloud import bigquery

date_param = sys.argv[1]
silver_bucket_arg = sys.argv[2]
project_id = sys.argv[3]
dataset_id = sys.argv[4]
temp_bucket = sys.argv[5]


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def clean_brewery_data(df):
    """
    Apply data cleaning and transformations to brewery data
    """
    logging.info("Starting data cleaning transformations")
    
    # Create full address field
    df = df \
        .withColumn("full_address",
                    concat_ws(", ",
                              col("address_line_1"),
                              col("address_line_2"),
                              col("address_line_3"),
                              col("name_city"),
                              col("name_state"),
                              col("value_postal_code")))
    
    # Add date components for partitioning
    df = df \
        .withColumn("source_date",
                    to_date(col("source_date"), "yyyy-MM-dd")) \
        .withColumn("year", year(col("source_date"))) \
        .withColumn("month", month(col("source_date"))) \
        .withColumn("day", dayofmonth(col("source_date")))
    
    # Add has_coordinates flag
    df = df \
        .withColumn("has_coordinates",
                    when((col("latitude").isNotNull()) &
                         (col("longitude").isNotNull()), True)
                    .otherwise(False))
    
    # Add has_contact_info flag
    df = df \
        .withColumn("has_contact_info",
                    when((col("phone").isNotNull()) |
                         (col("url_website").isNotNull()), True)
                    .otherwise(False))
    
    logging.info("Data cleaning transformations completed")
    return df


def delete_partition(project_id, dataset_id, table_name, source_date):
    """
    Delete existing partition data for the given date
    """
    try:
        client = bigquery.Client(project=project_id)
        
        delete_query = f"""
        DELETE FROM `{project_id}.{dataset_id}.{table_name}`
        WHERE DATE(source_date) = '{source_date}'
        """
        
        logging.info(f"Executing partition deletion for date: {source_date}")
        job = client.query(delete_query)
        job.result()  # Wait for the job to complete
        
        logging.info(f"Successfully deleted partition data for {source_date}")
        
    except Exception as e:
        error_msg = f"Could not delete partition data: {str(e)}"
        logging.warning(error_msg)
        raise Exception(error_msg)


def load_to_bigquery(df, project_id, dataset_id, table_name, source_date):
    """
    Load DataFrame to BigQuery table
    """
    logging.info(f"Loading data to BigQuery: "
                 f"{project_id}.{dataset_id}.{table_name}")
    
    # Delete existing partition data before loading new data
    delete_partition(project_id, dataset_id, table_name, source_date)
    
    try:
        # Configure BigQuery options for optimized loading
        df.write \
            .format("bigquery") \
            .option("table", f"{project_id}.{dataset_id}.{table_name}") \
            .option("writeMethod", "indirect") \
            .option("temporaryGcsBucket", temp_bucket) \
            .option("partitionField", "source_date") \
            .option("partitionType", "DAY") \
            .option("clusteredFields", "name_state,type_brewery") \
            .option("createDisposition", "CREATE_NEVER") \
            .mode("append") \
            .save()
    except Exception as e:
        error_msg = f"Error loading data to BigQuery: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)
    
    logging.info("Data successfully loaded to BigQuery table")


def transform_brewery_data(spark, silver_bucket, project_id,
                           dataset_id, date_param):
    """
    Main transformation function
    """
    # Define input path for silver bucket data
    input_path = f"gs://{silver_bucket}/breweries/date={date_param}"
    
    logging.info(f"Reading Parquet files from: {input_path}")
    
    try:
        # Read data from silver bucket
        df = spark.read.parquet(input_path)
        
        logging.info(f"Initial record count: {df.count()}")
        
        # Apply data cleaning and transformations
        df_transformed = clean_brewery_data(df)
        
        # Data quality validation
        final_count = df_transformed.count()
        logging.info(f"Final record count after transformations: "
                     f"{final_count}")
        
        # Check for required fields
        null_brewery_ids = df_transformed.filter(
            col("id_brewery").isNull()).count()
        if null_brewery_ids > 0:
            logging.warning(f"Found {null_brewery_ids} records with "
                            f"null brewery IDs")
        
    except Exception as e:
        error_msg = f"Error during transformation: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)
            
    # Load to BigQuery
    load_to_bigquery(df_transformed, project_id, dataset_id,
                        "breweries_all_data", date_param)
    
    logging.info("Transformation process completed successfully")
    return final_count


def main():
    """
    Brewery data transformation script
    """
    # Validate date format
    try:
        datetime.strptime(date_param, '%Y-%m-%d')
    except ValueError:
        error_msg = f"Error: Date must be in YYYY-MM-DD format: {date_param}"
        logging.error(error_msg)
        raise Exception(error_msg)

    logging.info(f"Processing transformations for date: {date_param}")
    logging.info(f"Silver bucket: {silver_bucket_arg}")
    logging.info(f"Project ID: {project_id}")
    logging.info(f"Dataset ID: {dataset_id}")
    logging.info(f"Temporary bucket: {temp_bucket}")

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName(f"Breweries Transform - {date_param}") \
        .getOrCreate()

    logging.info("Starting brewery data transformation process")
    
    # Execute transformation
    record_count = transform_brewery_data(
        spark, silver_bucket_arg, project_id, dataset_id, date_param
    )
    
    logging.info(f"Transformation completed successfully. "
                 f"Records processed: {record_count}")

    spark.stop()


if __name__ == "__main__":
    main()
