import boto3
import json
from backend_streaming.constants import (
    LOCAL_SQS_URL, 
    DEFAULT_MAX_MESSAGES, 
    DEFAULT_WAIT_TIME, 
    DEFAULT_VISIBILITY_TIMEOUT
)

class SQSError(Exception):
    """Base exception for SQS operations"""
    pass


class LocalSQSClient:
    def __init__(self, sqs_url: str = LOCAL_SQS_URL):
        """Initialize SQS client"""
        self.sqs_url = sqs_url
        self.sqs_client = None
        self._connect()
        
    def _connect(self) -> None:
        """Establish SQS connection"""
        try:
            self.sqs_client = boto3.client('sqs', region_name='us-east-1')
        except Exception as e:
            raise SQSError(f"Failed to initialize SQS client: {str(e)}") from e

    def send_message(self, data: dict) -> int:
        """Send message to SQS queue and return response status code"""
        self._ensure_connection()
        try:
            response = self.sqs_client.send_message(
                QueueUrl=self.sqs_url,
                MessageBody=json.dumps(data)
            )
            return response['ResponseMetadata']['HTTPStatusCode']
        except Exception as e:
            raise SQSError(f"Failed to send message: {str(e)}") from e
        
    def receive_messages(
        self,
        max_messages: int = DEFAULT_MAX_MESSAGES,
        wait_time: int = DEFAULT_WAIT_TIME,
        visibility_timeout: int = DEFAULT_VISIBILITY_TIMEOUT
    ) -> list:
        """
        Receive messages from SQS queue
        Args:
            max_messages: Maximum number of messages to receive (1-10)
            wait_time: Long polling wait time in seconds
            visibility_timeout: Time in seconds message is invisible after receipt
        Returns:
            list: List of received messages
        """
        self._ensure_connection()
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=self.sqs_url,
                MaxNumberOfMessages=min(max_messages, 10),  # SQS limit is 10
                WaitTimeSeconds=wait_time,
                AttributeNames=['All'],
                MessageAttributeNames=['All'],
                VisibilityTimeout=visibility_timeout
            )
            return response.get('Messages', [])
        except Exception as e:
            raise SQSError(f"Failed to receive messages: {str(e)}") from e
        
    def delete_message(self, receipt_handle: str) -> bool:
        """Delete a message from the queue after processing"""
        self._ensure_connection()
        try:
            self.sqs_client.delete_message(
                QueueUrl=self.sqs_url,
                ReceiptHandle=receipt_handle
            )
            return True
        except Exception as e:
            raise SQSError(f"Failed to delete message: {str(e)}") from e
        
    def get_num_messages(self) -> int:
        """Get the number of messages in the queue"""
        self._ensure_connection()
        try:
            response = self.sqs_client.get_queue_attributes(
                QueueUrl=self.sqs_url,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            return int(response['Attributes']['ApproximateNumberOfMessages'])
        except Exception as e:
            raise SQSError(f"Failed to get number of messages: {str(e)}") from e
        
    def purge_queue(self) -> int:
        """Purge all messages from the queue and returns the number of messages purged"""
        self._ensure_connection()
        message_count = self.get_num_messages()
        if message_count > 0:
            self.sqs_client.purge_queue(QueueUrl=self.sqs_url)
            return message_count
        else:
            return 0
        
    def _ensure_connection(self) -> None:
        """Ensure SQS connection is established"""
        if not self.sqs_client:
            self._connect()