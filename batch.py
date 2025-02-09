from google.cloud import bigquery, storage, logging
import pandas as pd
import json
from datetime import datetime, timezone, timedelta

#Configure Logging
logging_client = logging.Client()
logger = logging_client.logger("assignment_fetch")

def log_info(message):
    logger.log_text(message, severity="INFO")

def log_error(message):
    logger.log_text(message, severity="ERROR")

# Initialize Clients
bigquery_client = bigquery.Client()
storage_client = storage.Client()

# Remove Duplicates
def remove_duplicates(df):
    return df.drop_duplicates()

# Handle Missing Data
def handle_missing_values(df):
    return df.dropna()

# Fetch the latest ETL timestamp from GCS
def get_latest_etl_timestamp_from_gcs(metadata_bucket, metadata_blob_name):
    bucket = storage_client.bucket(metadata_bucket)
    blob = bucket.blob(metadata_blob_name)

    if not blob.exists():
        log_info(f"No metadata file found in {metadata_bucket}/{metadata_blob_name}. Returning None.")
        return None

    try:
        data = json.loads(blob.download_as_text())
        return pd.to_datetime(data["latest_etl_timestamp"])
    except Exception as e:
        log_error(f"Error reading metadata file: {str(e)}")
        return None

# Save the latest ETL timestamp to GCS
def save_latest_etl_timestamp_to_gcs(metadata_bucket, metadata_blob_name, latest_timestamp):
    bucket = storage_client.bucket(metadata_bucket)
    blob = bucket.blob(metadata_blob_name)

    try:
        data = {"latest_etl_timestamp": latest_timestamp}
        blob.upload_from_string(json.dumps(data), content_type="application/json")
        log_info(f"Updated latest_etl_timestamp in {metadata_bucket}/{metadata_blob_name}: {data}")
    except Exception as e:
        log_error(f"Error saving latest_etl_timestamp to GCS: {str(e)}")

# Fetch data from BigQuery in batches
def fetch_batches(starttime, endtime):
    log_info("Starting batch data extraction from BigQuery.")

    query = """
        SELECT * FROM `bigquery-public-data.noaa_lightning.lightning_strikes`
        WHERE etl_timestamp BETWEEN @start_time and @end_time
        ORDER BY etl_timestamp
        LIMIT 100
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", starttime),
            bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", endtime),
        ],
        priority=bigquery.QueryPriority.BATCH
    )

    try:
        query_job = bigquery_client.query(query, job_config=job_config)
        results = query_job.result()
        batch_data = [dict(row) for row in results]

        if batch_data:
            df = pd.DataFrame(batch_data)
            df_removed_dup = remove_duplicates(df)
            df_clean = handle_missing_values(df_removed_dup)

            log_info(f"Fetched batch with {len(df_clean)} records.")
            return df_clean
        else:
            log_info("No more data available.")
            return None

    except Exception as e:
        log_error(f"Error fetching batch from BigQuery: {str(e)}")
        return None

# Upload partitioned data to GCS
def upload_partition_to_gcs(partition_data, bucket, folder_path, file_counter):
    file_name = f"{folder_path}/partition_{file_counter}.json"
    blob = bucket.blob(file_name)

    try:
        blob.upload_from_string(json.dumps(partition_data, default=str), content_type="application/json")
        log_info(f"Uploaded {file_name} to GCS.")
    except Exception as e:
        log_error(f"Error uploading {file_name} to GCS: {str(e)}")

# Partition data before uploading to GCS
def partition_and_upload_to_gcs(df, bucket_name, folder_name, max_size_kb=5):
    bucket = storage_client.bucket(bucket_name)
    folder_path = f"{folder_name}"
    current_partition = []
    current_size = 0
    file_counter = 0

    log_info(f"Partitioning and uploading {len(df)} records to GCS.")

    for _, row in df.iterrows():
        # Convert all timestamps to string before converting to JSON
        row_dict = row.to_dict()
        for key, value in row_dict.items():
            if isinstance(value, pd.Timestamp):
                row_dict[key] = value.strftime("%Y-%m-%d %H:%M:%S.%f UTC")  # Convert to string

        row_json = json.dumps(row_dict)  # Convert row to JSON string
        row_size_kb = len(row_json.encode("utf-8")) / 1024  # Convert to KB

        if current_size + row_size_kb > max_size_kb:
            upload_partition_to_gcs(current_partition, bucket, folder_path, file_counter)
            current_partition = []
            current_size = 0
            file_counter += 1

        current_partition.append(row_dict)
        current_size += row_size_kb

    if current_partition:
        upload_partition_to_gcs(current_partition, bucket, folder_path, file_counter)
# Main function
def main():
    BUCKET_NAME = "assignmentdbxgcp"
    METADATA_BUCKET = "assignment_metadata_gcp"
    METADATA_BLOB_NAME = "latest_etl_timestamp.json"

    latest_etl_timestamp = get_latest_etl_timestamp_from_gcs(METADATA_BUCKET, METADATA_BLOB_NAME)

    if latest_etl_timestamp:
        start_timestamp = latest_etl_timestamp + timedelta(seconds=1)
    else:
        start_timestamp = datetime(2022, 10, 13, 0, 0, 0, 695188, tzinfo=timezone.utc)  # Default start_time
        log_info(f"Starting from default timestamp: {start_timestamp}")

    end_timestamp = (start_timestamp + timedelta(days=60)).strftime("%Y-%m-%d %H:%M:%S.%f UTC")
    start_timestamp = start_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f UTC")

    batch_df = fetch_batches(start_timestamp, end_timestamp)

    if batch_df is not None and not batch_df.empty:
        new_latest_timestamp = batch_df["etl_timestamp"].max().tz_convert("UTC").strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        folder_path = new_latest_timestamp
        partition_and_upload_to_gcs(batch_df, BUCKET_NAME, folder_path)
        log_info(f"Latest timestamp updated to {new_latest_timestamp}")
        save_latest_etl_timestamp_to_gcs(METADATA_BUCKET, METADATA_BLOB_NAME, new_latest_timestamp)
    else:
        log_info("No new data received in batch.")

if __name__ == "__main__":
    main()
