from google.cloud import storage

def download_file_from_bucket( bucket_name, source_blob_name, destination_file_name):
    """Downloads a file from a Google Cloud Storage bucket to your local machine."""
    
    # Initialize the GCS client
    # client = storage.Client(project=project_id)
    client = storage.Client.from_service_account_json('credentials.json')
    
    # Connect to your bucket
    bucket = client.bucket(bucket_name)
    
    # Target the specific file inside the bucket
    blob = bucket.blob(source_blob_name)
    
    # Download the file to your local directory
    blob.download_to_filename(destination_file_name)
    
    print(f"Success! Downloaded '{source_blob_name}' to '{destination_file_name}'.")

if __name__ == "__main__":
    # Your specific Google Cloud details
    PROJECT_ID = "gen-lang-client-0829420978"
    BUCKET_NAME = "data-connect-27316"
    
    # The name of the file currently sitting inside your bucket
    FILE_NAME_IN_BUCKET = "custom-message-001.txt"
    
    # What you want to name the file when it saves to your local folder
    LOCAL_SAVE_NAME = "downloaded-message.txt"
    
    download_file_from_bucket(BUCKET_NAME, FILE_NAME_IN_BUCKET, LOCAL_SAVE_NAME)