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
        """
        This is a wrapper to capture events for a SINGLE game.  
        Acquires batch of events -> processes -> sends to SQS
        """
        self.game_id = game_id
        self.db = db_client
        self.sqs = sqs_client

        self.first_push = True
        self.start_time = None
        # Track last processed time
        # NOTE: this will keep incrementing regardless of event type changes
        self.last_time = 0  
     
    def handle_update(self, changed_times: Set[int], current_time: int):
        """Handle updates from the provider"""
        assert self.last_time <= current_time, "Current time can't be less than last time"
        data = self._format_data(changed_times, current_time)
        self._send_and_log_message(data)
        # NOTE: order is important!
        self.last_time = current_time
    
    def handle_game_end(self):
        """Handle end of game signal"""
        final_data = {
            'game_id': self.game_id,
            'type': 'stop'
        }
        self._send_and_log_message(final_data)

    def _format_data(self, changed_times: Set[int], current_time: int) -> dict:
        """
        Format data for SQS sending with message type logic.
        Importantly, we dictate the start time based on the data received.
        """
        if self.first_push:
            message_type = 'start'
            self.first_push = False
            self.start_time = 0
        elif changed_times:
            message_type = 'reset'
            self.start_time = min(changed_times)
        else:
            message_type = 'ongoing'
            self.start_time = self.last_time
            
        return {
            'game_id': self.game_id,
            'type': message_type,
            'start_time': self.start_time,
            'end_time': current_time,
            'timestamp': datetime.now().isoformat()
        }
 
    def _send_and_log_message(self, data: dict) -> None:
        """Helper method to send message to SQS and log the response"""
        logger.info(f"Game {self.game_id} sending: {data}")
        response_code = self.sqs.send_message(data)
        
        if response_code != HTTP_200_STATUS:
            logger.error(f"Game {self.game_id} SQS response: {response_code}")
        