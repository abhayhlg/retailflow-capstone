import time
import random
from botocore.exceptions import ClientError, BotoCoreError
import boto3
from .logger import logger  # Import the central logger

class S3IngestClient:
    def __init__(self, region_name=None):
        try:
            # Credentials are implicitly picked up from environment variables here
            self.s3_client = boto3.client('s3', region_name=region_name)
            logger.info("S3IngestClient initialized successfully.")
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Failed to initialize S3 Client: {e}")
            raise

    def _execute_with_retry(self, operation_name, s3_func, *args, **kwargs):
        max_retries = 4
        base_delay = 1.0
        
        for attempt in range(max_retries + 1):
            try:
                return s3_func(*args, **kwargs)
            except (ClientError, BotoCoreError) as error:
                is_retryable = False
                if isinstance(error, ClientError):
                    error_code = error.response.get('Error', {}).get('Code', 'Unknown')
                    status_code = error.response.get('ResponseMetadata', {}).get('HTTPStatusCode', 500)
                    if status_code >= 500 or error_code in ['Throttling', 'SlowDown']:
                        is_retryable = True
                else:
                    is_retryable = True

                if attempt == max_retries or not is_retryable:
                    logger.error(f"Operation '{operation_name}' failed permanently. Error: {error}")
                    raise error

                delay = base_delay * (2 ** attempt)
                jittered_delay = random.uniform(0, delay)
                
                logger.warning(
                    f"S3 operation '{operation_name}' failed. "
                    f"Retrying attempt {attempt + 1}/{max_retries} in {jittered_delay:.2f}s... Error: {error}"
                )
                time.sleep(jittered_delay)

    def upload_file(self, file_path, bucket, key):
        logger.info(f"Starting upload: local '{file_path}' -> s3://{bucket}/{key}")
        return self._execute_with_retry("upload_file", self.s3_client.upload_file, Filename=file_path, Bucket=bucket, Key=key)

    def download_file(self, bucket, key, download_path):
        logger.info(f"Starting download: s3://{bucket}/{key} -> local '{download_path}'")
        return self._execute_with_retry("download_file", self.s3_client.download_file, Bucket=bucket, Key=key, Filename=download_path)

    def list_objects(self, bucket, prefix=""):
        logger.info(f"Listing objects in s3://{bucket} with prefix '{prefix}'")
        response = self._execute_with_retry("list_objects", self.s3_client.list_objects_v2, Bucket=bucket, Prefix=prefix)
        return response.get('Contents', [])

    def generate_presigned_url(self, bucket, key, expiration=3600, client_method='get_object'):
        logger.info(f"Generating presigned URL for s3://{bucket}/{key}")
        return self._execute_with_retry("generate_presigned_url", self.s3_client.generate_presigned_url, ClientMethod=client_method, Params={'Bucket': bucket, 'Key': key}, ExpiresIn=expiration)