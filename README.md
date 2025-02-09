
**Before running the script, ensure the following:**


1.You have a Google Cloud account.

2.Authenticate your account in cli 

gcloud auth login

This will prompt you to browser and you can login to your GCP account.
3.Ensure your project is set

gcloud config set project [ProjectID]

4.You have enabled APIs for BigQuery and Google Cloud Storage (GCS).


**The first script batch.py**  <!-- H1 -->
**Overview**
This script extracts data from Google BigQuery in batches based on timestamps, processes it to remove duplicates and handle missing values, and uploads partitioned data to Google Cloud Storage (GCS). It also maintains metadata in GCS to track the latest extracted timestamp.
**Workflow**
Initialize Logging and Clients

Connects to Google Cloud BigQuery, Storage, and Logging.
Logs operations for debugging and tracking.
Retrieve Latest ETL Timestamp

Fetches the last processed timestamp from a metadata file stored in GCS.
If no timestamp exists, a default timestamp is used.
Extract Data from BigQuery

Queries BigQuery to fetch new records based on the latest ETL timestamp.
Orders the data by etl_timestamp and limits the batch size.
Data Cleaning

Removes duplicate records.
Handles missing values by dropping them.
Partition and Upload Data to GCS

Splits data into small partitions based on size constraints (default: 5KB).
Uploads each partition as a JSON file to GCS.
Update Metadata in GCS

Updates the metadata file with the latest extracted timestamp.
Ensures that the next run continues from the last fetched timestamp.


**Modify the following variables in main() as needed:**

BUCKET_NAME → Destination bucket for data storage.
METADATA_BUCKET → Bucket storing metadata information.
METADATA_BLOB_NAME → File tracking the latest processed timestamp.
max_size_kb → Maximum partition size per file upload.





**The second script load.py**  <!-- H1 -->
**Overview**
This script automates the process of fetching JSON files from Google Cloud Storage (GCS), loading them into a BigQuery table, and maintaining metadata to track processed files. It ensures that only new files are loaded, avoiding duplicate processing.

**Workflow**
Initialize Logging and Clients

Sets up Google Cloud Logging, Storage, and BigQuery clients.
Logs operations for monitoring and debugging.
List Files in GCS

Retrieves a list of JSON files available in the specified GCS bucket.
Fetch Processed Files from BigQuery Metadata Table

Queries BigQuery to get a list of already processed files.
Ensures only new files are processed.
Read Files from GCS

Downloads JSON files and converts them into a pandas DataFrame.
Load Data into BigQuery

Defines a schema for the BigQuery table.
Loads data from DataFrame into the target BigQuery table.
Uses WRITE_APPEND to ensure new records are added.
Mark Processed Files in Metadata Table

Once successfully loaded, the file name is inserted into the metadata table.
Prevents reprocessing of the same file in future runs.


**Modify the following variables in main() as needed:**

GCS_BUCKET_NAME → Name of the GCS bucket storing JSON files.
BQ_PROJECT_ID → Google Cloud Project ID.
BQ_DATASET_NAME → BigQuery dataset name.
BQ_TABLE_NAME → BigQuery table name for storing processed data.
BQ_METADATA_TABLE → Metadata table to track processed files.
