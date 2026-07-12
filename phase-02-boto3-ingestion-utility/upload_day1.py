import os
from datetime import datetime
import re
import sys
from dotenv import load_dotenv  # Loaded from the python-dotenv package
from s3_ingest import S3IngestClient

def main(filepath):
    # 1. Look for the '.env' file and inject its values into the OS environment variables
    load_dotenv()
    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("Environment variable 'S3_BUCKET_NAME' is missing! Check your .env file.")

    # 2. Instantiate our custom client wrapper (Boto3 picks up the environment variables here)
    client = S3IngestClient()

 
    # Target structured S3 Key format required for Phase 3: raw/<dataset_name>/dt=YYYY-MM-DD/filename
    #s3_key = f"raw/{dataset_name}/date={ingestion_date}/{local_file}"
    
    basename = os.path.basename(filepath)
    match = re.search(r'\d{8}', basename)
    raw_date = match.group(0)
    dt = datetime.strptime(raw_date, "%Y%m%d").strftime("%Y-%m-%d")
    s3_key = f"raw/orders/date={dt}/{basename}"

    # Automatically generate a dummy CSV data file if it doesn't exist locally yet
    if not os.path.exists(filepath):
        raise ValueError(f"File does not exist{filepath}")
    # 4. Perform ingestion actions
    print("\n--- Executing File Ingestion ---")
    client.upload_file(file_path=filepath, bucket=bucket_name, key=s3_key)

    # 5. Check and verify data 
    print("\n--- Verifying Ingested Objects ---")
    objects = client.list_objects(bucket=bucket_name, prefix=f"raw/orders/date={dt}/")
    print(f"raw/orders/{dt}/")
    for obj in objects:
        print(f" Found in S3: {obj['Key']} ({obj['Size']} bytes)")

    print("\n--- Generating Secure Presigned Sharing Link ---")
    url = client.generate_presigned_url(bucket=bucket_name, key=s3_key, expiration=1800)
    print(f"Presigned Access URL:\n{url}\n")

    
if __name__ == "__main__":
    filepath = sys.argv[1]
    main(filepath)