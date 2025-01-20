import logging
import json
import time
from typing import Dict
from db.core.factory import DatabaseClientFactory as db_factory
from db.core.base import SQLClient

from ..config.time import time_config
from ..providers.base import BaseProvider
from ..providers.local.local import LocalDataProvider
from ..constants import HTTP_200_STATUS

logger = logging.getLogger(__name__)

class SingleGameStreamer:
    def __init__(
        self,  
        game_id: int,   
        provider: BaseProvider,
        db_client: SQLClient,
        sqs_client,
    ):
        """
        Instance to capture events for a SINGLE game. 
        Acquires batch of events -> processes -> sends to SQS
        """
        self.first_push = True
        self.game_id = game_id
        self.db = db_client
        self.sqs = sqs_client
        self.provider = provider
     
    def run(self):
        """
        Run the game instance. This method:
            - establishes connection to data provider
            - acquires data
            - processes and sends data to SQS
        """
        iter = 0
        while not self.provider.is_finished():
            # NOTE: assuming events are passed in order
            events = json.loads(self.provider.get_live_events())
            if events:
                start_id, end_id = self.provider.process_events(events)
                data = self._format_data(start_id, end_id)
                self._send_and_log_message(data)
                
        # After the loop ends, send stop message
        # NOTE: Assumes all data is guaranteed processed here. 
        final_data = {'game_id': self.game_id, 'type': 'stop'}
        self._send_and_log_message(final_data)
        
        # hacky way to wait for main instance to process the EOG message
        time.sleep(10)

    def _format_data(self, start_id: int, end_id: int) -> dict:
        """
        Format data for SQS sending.
        """
        # format message for SQS sending
        type_str = 'start' if self.first_push else 'ongoing'
        self.first_push = False
        data = {
            'game_id': self.game_id,
            'type': type_str,
            'start_id': start_id,
            'end_id': end_id
        } 
        return data
 
    def _send_and_log_message(self, data: dict) -> None:
        """Helper method to send message to SQS and log the response"""
        logger.info(f"Game {self.game_id} sending: {data}")
        response_code = self.sqs.send_message(data)
        
        if response_code != HTTP_200_STATUS:
            logger.error(f"Game {self.game_id} SQS response: {response_code}")
        