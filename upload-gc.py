from google.cloud import storage

def upload_string_to_bucket(bucket_name, destination_blob_name, data_string):
    """Uploads a string of data to a Google Cloud Storage bucket."""
    
    # Initialize the GCS client with your specific project
    # client = storage.Client(project=project_id)
    client = storage.Client.from_service_account_json('credentials.json')
    
    # Connect to your bucket
    bucket = client.bucket(bucket_name)
    
    # Create a new "blob" (which is what GCS calls a file)
    blob = bucket.blob(destination_blob_name)
    
    # Upload the string data
    blob.upload_from_string(data_string)
    
    print(f"Success! Data uploaded to '{destination_blob_name}' in bucket '{bucket_name}'.")

if __name__ == "__main__":
    # Your specific Google Cloud details
    PROJECT_ID = "gen-lang-client-0829420978"
    BUCKET_NAME = "data-connect-27316"
    
    # The name the file will have once inside the bucket
    FILE_NAME = "custom-message-002.txt"
    
    # The actual data you want to push
    DATA = "Hello from Python! This data just arrived and should trigger the Pub/Sub topic."
    
    upload_string_to_bucket(BUCKET_NAME, FILE_NAME, DATA)