import os
from google.cloud import storage

# Set up Google Cloud Storage
client = storage.Client()

# Assuming you've set your bucket name as an environment variable
bucket_name = os.environ.get("MY_GCS_BUCKET")
if not bucket_name:
    raise ValueError("Bucket name not set in environment variables!")

bucket = client.bucket(bucket_name)

# Specify the local directory containing your CSV files
local_directory = './local_csv_data'

# Loop through each CSV file and upload to GCS
for filename in os.listdir(local_directory):
    if filename.endswith('.csv'):
        local_path = os.path.join(local_directory, filename)
        
        # Create a blob and upload the file's content
        blob = bucket.blob(filename)
        with open(local_path, 'rb') as f:
            blob.upload_from_file(f)
        print(f"Uploaded {filename} to {bucket_name}.")

print("All CSV files have been transferred.")
