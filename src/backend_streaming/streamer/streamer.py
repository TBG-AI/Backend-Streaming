import logging
from typing import Optional

from shared.messaging.sqs import LocalSQSClient 
from datetime import datetime

logger = logging.getLogger(__name__)

class SingleGameStreamer:
    def __init__(
        self,  
        game_id: int,   
        sqs_client: Optional[LocalSQSClient] = None,
    ):
        self.game_id = game_id
        # NOTE: this is by default NOT a FIFO queue
        self.sqs = sqs_client or LocalSQSClient()
    
    def send_message(self, message_type: str):
        """
        Send message to SQS with appropriate type: 'update', 'stop'
        """
        # NOTE: 'update' message not really needed, but it's here for clarity
        data = {
            'game_id': self.game_id,
            'type': message_type,
            'timestamp': datetime.now().isoformat()
        }
        logger.info(f"Game {self.game_id} sending: {data}")
        response_code = self.sqs.send_message(data)
        
        if response_code != 200:
            # TODO: handle error appropriately
            pass