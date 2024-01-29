from google.cloud import storage
from google.cloud.exceptions import NotFound



def gcs_connection(logging,project_id:str,bucket_name:str):
    logging.info(f"Connecting to the Bucket for project_id : {project_id} and bucket_name : {bucket_name}")
    try:
        storage_client=storage.Client(project=project_id)
        bucket=storage_client.bucket(bucket_name=bucket_name)
        logging.info(f"gcs object craeted successfully..!")
    except Exception as e:
        logging.error(f"Exception while connecting to the Bucket for project_id : {project_id} and bucket_name : {bucket_name}")
        logging.error(f"Exception : {e}")
    return bucket