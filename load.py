from google.cloud import storage, bigquery, logging
import pandas as pd
import json

# Configure Logging
logging_client = logging.Client()
logger = logging_client.logger("assignment_load")

def log_info(message):
    logger.log_text(message, severity="INFO")

def log_error(message):
    logger.log_text(message, severity="ERROR")

#  Initialize GCS and BigQuery Clients
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# GCS & BigQuery Details
GCS_BUCKET_NAME = "assignmentdbxgcp"
BQ_PROJECT_ID = "luminous-wharf-450412-p2"
BQ_DATASET_NAME = "Assignment"
BQ_TABLE_NAME = "load_strike_data"
BQ_METADATA_TABLE = "metadata_table"

# Return the existing list of JSON files in GCS
def list_files_in_gcs(bucket_name, prefix=""):
    try:
        bucket = storage_client.bucket(bucket_name)
        files = [blob.name for blob in bucket.list_blobs(prefix=prefix) if blob.name.endswith(".json")]
        log_info(f"Found {len(files)} JSON files in GCS bucket: {bucket_name}.")
        return files
    except Exception as e:
        log_error(f"Error listing files from GCS: {e}")
        return []

# Get the list of already processed files from BigQuery metadata table
def get_processed_files():
    try:
        query = f"SELECT file_name FROM `{BQ_PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_METADATA_TABLE}`"
        query_job = bigquery_client.query(query)
        processed_files = {row.file_name for row in query_job.result()}
        log_info(f"Retrieved {len(processed_files)} processed files from metadata table.")
        return processed_files
    except Exception as e:
        log_error(f"Error fetching processed files: {e}")
        return set()

# Process JSON files & Load Data to BigQuery
def process_and_load_json_file(bucket_name, dataset_name, table_name, file_path):
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        json_data = blob.download_as_bytes().decode("utf-8")

        try:
            # Convert JSON array to Pandas DataFrame
            data = json.loads(json_data)
            df = pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame([data])
        except json.JSONDecodeError:
            log_error(f"Invalid JSON format in {file_path}. Skipping file.")
            return

        # Convert columns to appropriate data types
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        if "etl_timestamp" in df.columns:
            df["etl_timestamp"] = pd.to_datetime(df["etl_timestamp"], errors="coerce")

        table_id = f"{BQ_PROJECT_ID}.{dataset_name}.{table_name}"

        # Define schema explicitly
        schema = [
            bigquery.SchemaField("date", "TIMESTAMP"),
            bigquery.SchemaField("number_of_strikes", "INTEGER"),
            bigquery.SchemaField("center_point_geom", "GEOGRAPHY"),
            bigquery.SchemaField("source_url", "STRING"),
            bigquery.SchemaField("etl_timestamp", "TIMESTAMP")
        ]

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        # Load DataFrame into BigQuery
        load_job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()  # Wait for job completion

        log_info(f"Successfully loaded {df.shape[0]} rows from {file_path} into {table_id}.")

    except Exception as e:
        log_error(f"ERROR processing {file_path}: {e}")

# Mark files as processed in BigQuery metadata table
def mark_file_as_processed(file_name):
    try:
        table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_METADATA_TABLE}"
        rows_to_insert = [{"file_name": file_name}]
        errors = bigquery_client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            log_error(f"Error inserting metadata for {file_name}: {errors}")
        else:
            log_info(f"Marked {file_name} as processed.")
    except Exception as e:
        log_error(f"Error marking {file_name} as processed: {e}")

# Main function to process all new files
def main():
    log_info("Starting file processing job...")

    all_files = list_files_in_gcs(GCS_BUCKET_NAME)
    processed_files = get_processed_files()
    
    new_files = [file for file in all_files if file not in processed_files]
    if not new_files:
        log_info("No new files to process.")
        return

    for file_path in new_files:
        process_and_load_json_file(GCS_BUCKET_NAME, BQ_DATASET_NAME, BQ_TABLE_NAME, file_path)
        mark_file_as_processed(file_path)

    log_info("All new files processed successfully!")

if __name__ == "__main__":
    main()
