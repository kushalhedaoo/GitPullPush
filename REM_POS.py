from google.cloud import storage, bigquery

MM_PROC_DB = "int"  # Replace with your actual database name
MM_PROC_CP = "uspfactsalespos"  # Replace with your actual procedure name

def check_and_call_procedure(bucket_name, filename, bq_client):
    # Check if the filename contains '_replace'
    print("The filename is: ",filename)
    if '_replace' not in filename:
        # Call the mm_proc_call procedure
        mm_proc_call(bq_client)
    else:
        print(f"Skipping file {filename} as it not contains '_replace'.")

def uspfactsalesPOS(filename):
    # Replace this with the implementation of your existing procedure
    # Add the logic to process the file as needed
    print(f"Processing file {filename}")

def mm_proc_call(bq_client):
    try:
        print('Calling uspfactsalespos')
        query = f'''CALL {MM_PROC_DB}.{MM_PROC_CP}()'''
        
        query_job = bq_client.query(query)
        query_job.result()
        print("uspfactsalespos DONE")
    except Exception as e:
        print(f"Error calling the procedure: {e}")

def main(bucket_name, bq_project, bq_dataset,folder_name):
    # Initialize the GCS client
    gcs_client = storage.Client()

    # Get the GCS bucket
    bucket = gcs_client.get_bucket(bucket_name)
    print("The name of bucket in main: ",bucket)
    # Initialize the BigQuery client
    bq_client = bigquery.Client(project=bq_project)
    print("the bq_client is initialised: ")

    # List all objects in the GCS bucket
    file=None
    blobs = bucket.list_blobs(prefix=folder_name)
    # Iterate over each object in the GCS bucket
    for blob in blobs:
        # Get the filename from the GCS object
        if not blob.name.endswith('/'):
            file=blob.name
# Check and call the procedure if needed
        if file is not None:
            check_and_call_procedure(bucket_name, file, bq_client)

if __name__ == "__main__":
    # Replace 'your_bucket_name' with the actual name of your GCS bucket
    your_bucket_name = "dw-sm-poc-usecase3"
    folder_name='CSV_testfiles'
    
    # Replace 'your_project_id' with your actual BigQuery project ID
    your_project_id = "datawarehouse-datametica-poc"
    
    # Replace 'your_dataset_id' with your actual BigQuery dataset ID
    your_dataset_id = "int"

    # Call the main function
    main(your_bucket_name, your_project_id, your_dataset_id,folder_name)
