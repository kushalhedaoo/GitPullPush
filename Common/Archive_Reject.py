from google.cloud import storage
from datetime import datetime




def copy_blob(status,bucket_name,file_name,destination_bucket_name,csv_path_regex,archive_path,rejected_path,task_name):
    print('inside archival')
    file_source_path = csv_path_regex + file_name
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(file_source_path)
    destination_bucket = storage_client.bucket(destination_bucket_name)
    current_date = datetime.now().strftime("%Y%m%d")
    
    if status.lower() == 'success':
        destination_folder_name = f'{archive_path}{task_name}_{current_date}/'
        destination_blob_name_with_date = destination_folder_name + file_name
        destination_folder_blob = destination_bucket.blob(destination_folder_name)
        destination_folder_blob.upload_from_string('')
        new_blob = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name_with_date
        )
        print('Done with Copy') 
        print(f'File {file_name} copied to {destination_blob_name_with_date} in the archive folder.')
        print('-*-'*30)
        if source_blob.name.endswith('.csv'):
            print(f"DELETING SOURCE_BLOD : {source_blob.name}")
            source_blob.delete()
            print(f"{source_blob.name} DELETED")
        print('-*-'*30)
    else:
        destination_folder_name = f'{rejected_path}{task_name}_{current_date}/'
        destination_blob_name_with_date = destination_folder_name + file_name
        destination_folder_blob = destination_bucket.blob(destination_folder_name)
        destination_folder_blob.upload_from_string('')
        new_blob = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name_with_date
        )
        print('Done with Copy') 
        print(f'File {file_name} copied to {destination_blob_name_with_date} in the rejected folder.')
        print('-*-'*30)
        if source_blob.name.endswith('.csv'):
            print(f"DELETING SOURCE_BLOD : {source_blob.name}")
            source_blob.delete()
            print(f"{source_blob.name} DELETED")
        print('-*-'*30)