import logging
from typing import Dict, Set

from db.core.base import SQLClient
from datetime import datetime

from ..utils.time import time_config
from ..constants import HTTP_200_STATUS

logger = logging.getLogger(__name__)

class SingleGameStreamer:
    def __init__(
        self,  
        game_id: int,   
        db_client: SQLClient,
        sqs_client,
    ):
        self.first_push = True
        self.game_id = game_id
        self.db = db_client
        self.sqs = sqs_client
    
    def send_message(self, is_eog: bool = False):
        """
        Send message to SQS with appropriate type.
        Types: 'start' (first push), 'ongoing' (regular updates), 'stop' (game end)
        """
        if is_eog:
            message_type = 'stop'
        elif self.first_push:
            message_type = 'start'
            # chaning to False to only trigger once
            self.first_push = False
        else:
            message_type = 'ongoing'
            
        # Not sending any specific query information here. Instead, the responsibility is on the receiver.
        # NOTE: this is much more robust to potentially missed messages. 
        data = {
            'game_id': self.game_id,
            'type': message_type,
            'timestamp': datetime.now().isoformat()
        }
        logger.info(f"Game {self.game_id} sending: {data}")
        response_code = self.sqs.send_message(data)
        
        if response_code != HTTP_200_STATUS:
            logger.error(f"Game {self.game_id} SQS response: {response_code}")