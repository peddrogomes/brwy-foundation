import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp, lit
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType)
from datetime import datetime

date_param = sys.argv[1]
bronze_bucket_arg = sys.argv[2]
silver_bucket_arg = sys.argv[3]


def define_brewery_schema():
    """
    Define the schema for brewery data based on the JSON structure
    """
    return StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("brewery_type", StringType(), True),
        StructField("address_1", StringType(), True),
        StructField("address_2", StringType(), True),
        StructField("address_3", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state_province", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("phone", StringType(), True),
        StructField("website_url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("street", StringType(), True)
    ])


def rename_columns_to_standard(df):
    """
    Rename columns to standardized format with prefixes
    """
    column_mapping = {
        "id": "id_brewery",
        "name": "name_brewery",
        "brewery_type": "type_brewery",
        "address_1": "address_line_1",
        "address_2": "address_line_2",
        "address_3": "address_line_3",
        "city": "name_city",
        "state_province": "name_state_province",
        "postal_code": "value_postal_code",
        "country": "name_country",
        "longitude": "longitude",
        "latitude": "latitude",
        "phone": "phone",
        "website_url": "url_website",
        "state": "name_state",
        "street": "name_street"
    }
    
    # Apply column renaming
    for old_name, new_name in column_mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    
    return df


def load_brewery_data(spark, bronze_bucket, silver_bucket, date_param):
    """
    Load brewery data from bronze bucket JSON files and save as Parquet
    in silver bucket
    """
    # Define input and output paths
    input_path = f"gs://{bronze_bucket}/{date_param}/*.json"
    output_path = f"gs://{silver_bucket}/breweries/date={date_param}"
    
    logging.info(f"Reading JSON files from: {input_path}")
    
    # Define schema
    brewery_schema = define_brewery_schema()
    
    try:
        # Read JSON files from bronze bucket
        df = spark.read \
            .option("multiline", "true") \
            .schema(brewery_schema) \
            .json(input_path)
        
        # Rename columns to standardized format
        df_renamed = rename_columns_to_standard(df)
        
        # Add processing metadata
        df_with_metadata = df_renamed \
            .withColumn("processing_date", current_date()) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("source_date", lit(date_param))
        
        # Data quality checks
        initial_count = df_with_metadata.count()
        logging.info(f"Total records loaded: {initial_count}")
        
        # Remove duplicates based on brewery id
        df_clean = df_with_metadata.dropDuplicates(["id_brewery"])
        final_count = df_clean.count()
        
        if initial_count != final_count:
            duplicates_removed = initial_count - final_count
            logging.info(f"Removed {duplicates_removed} duplicate records")

        # Save as Parquet with partitioning by source_date
        logging.info(f"Writing Parquet files to: {output_path}")
        df_clean.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)
        
        logging.info(f"Successfully processed {final_count} brewery records")
        return final_count
        
    except Exception as e:
        error_msg = f"Error processing brewery data: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)


def main():
    """
    Brewery data load script
    """

    # Validate date format
    try:
        datetime.strptime(date_param, '%Y-%m-%d')
    except ValueError:
        error_msg = "Error: Date must be in YYYY-MM-DD format"
        logging.error(error_msg)
        raise Exception(error_msg)

    logging.info(f"Processing data for date: {date_param}")

    logging.info(f"Bronze bucket: {bronze_bucket_arg}")
    logging.info(f"Silver bucket: {silver_bucket_arg}")

    # Initialize Spark Session with optimized configuration
    spark = SparkSession.builder \
        .appName(f"Breweries Total Load - {date_param}") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    logging.info(f"Starting brewery data load process for {date_param}")
    
    # Load brewery data from bronze to silver
    record_count = load_brewery_data(
        spark, bronze_bucket_arg, silver_bucket_arg, date_param)

    logging.info(
        f"Brewery data load completed successfully for {date_param}")
    logging.info(f"Total records processed: {record_count}")

    spark.stop()
    return 'OK'


if __name__ == "__main__":
    main()
