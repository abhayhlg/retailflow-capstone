import unittest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError
from s3_ingest.client import S3IngestClient

class TestS3IngestRetryLogic(unittest.TestCase):

    @patch('s3_ingest.client.time.sleep')  # Mock sleep so the test runs instantly
    @patch('s3_ingest.client.boto3.client') # Mock the base boto3 client configuration
    def test_upload_retry_success_eventually(self, mock_boto_client, mock_sleep):
        """
        Scenario 1: Tests that if an upload encounters a transient 'SlowDown' (503) 
        error twice, it retries and successfully finishes on the 3rd attempt.
        """
        # Arrange: Setup mock client instance
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Simulate a transient AWS throttling/retryable error
        retryable_error = ClientError(
            error_response={
                'Error': {'Code': 'SlowDown', 'Message': 'Please reduce your request rate.'},
                'ResponseMetadata': {'HTTPStatusCode': 503}
            },
            operation_name='PutObject'
        )

        # Set side_effect: 2 Failures followed by 1 Success (None)
        mock_s3.upload_file.side_effect = [retryable_error, retryable_error, None]

        # Act: Run the method
        client = S3IngestClient()
        client.upload_file("orders_20260712.csv", "my-test-bucket", "raw/orders/dt=2026-07-12/orders_20260712.csv")

        # Assert: Initial attempt + 2 retries = 3 calls total
        self.assertEqual(mock_s3.upload_file.call_count, 3)
        # Should have backed off/slept exactly twice
        self.assertEqual(mock_sleep.call_count, 2)

    @patch('s3_ingest.client.time.sleep')
    @patch('s3_ingest.client.boto3.client')
    def test_upload_retry_exhaustion(self, mock_boto_client, mock_sleep):
        """
        Scenario 2: Tests that if an error persists continuously, the loop stops 
        after reaching max_retries (4 retries, 5 total attempts) and raises the exception.
        """
        # Arrange
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        server_error = ClientError(
            error_response={
                'Error': {'Code': 'InternalError', 'Message': 'An internal server error occurred.'},
                'ResponseMetadata': {'HTTPStatusCode': 500}
            },
            operation_name='PutObject'
        )

        # Force the mock to always fail with a 500 server error
        mock_s3.upload_file.side_effect = server_error

        # Act & Assert: Verify that it eventually bubbles up the ClientError
        client = S3IngestClient()
        with self.assertRaises(ClientError):
            client.upload_file("orders_20260712.csv", "my-test-bucket", "raw/orders/dt=2026-07-12/orders_20260712.csv")

        # Initial attempt (1) + max_retries (4) = 5 total calls
        self.assertEqual(mock_s3.upload_file.call_count, 5)
        # Should have slept 4 times before giving up
        self.assertEqual(mock_sleep.call_count, 4)

    @patch('s3_ingest.client.time.sleep')
    @patch('s3_ingest.client.boto3.client')
    def test_non_retryable_error_fails_immediately(self, mock_boto_client, mock_sleep):
        """
        Scenario 3: Tests that non-retryable errors (like 403 Access Denied) 
        fail instantly without wasting time on retries.
        """
        # Arrange
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # 403 Forbidden is a structural credential issue, not transient
        fatal_error = ClientError(
            error_response={
                'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'},
                'ResponseMetadata': {'HTTPStatusCode': 403}
            },
            operation_name='PutObject'
        )
        mock_s3.upload_file.side_effect = fatal_error

        # Act & Assert
        client = S3IngestClient()
        with self.assertRaises(ClientError):
            client.upload_file("orders_20260712.csv", "my-test-bucket", "raw/orders/dt=2026-07-12/orders_20260712.csv")

        # It should stop immediately on the 1st attempt
        self.assertEqual(mock_s3.upload_file.call_count, 1)
        # It should never back off or sleep
        self.assertEqual(mock_sleep.call_count, 0)

if __name__ == '__main__':
    unittest.main()