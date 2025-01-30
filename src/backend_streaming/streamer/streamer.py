import logging
import json
from typing import Optional, Dict, List

from shared.src.shared.messaging.sqs import LocalSQSClient 
from datetime import datetime

from backend_streaming.providers.opta.infra.models import MatchProjectionModel

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
    
    def send_message(self, message_type: str, events: List[MatchProjectionModel] = None):
        """
        Send message to SQS with appropriate type and payload.
        """ 
        # serialize to json-serializable format
        payload = [
            model.to_dict() for model in events
        ] if events else None

        data = {
            'game_id': self.game_id,
            'type': message_type,
            'payload': payload,
            'timestamp': datetime.now().isoformat()
        }
        logger.info(f"Game {self.game_id} sending: {data}")
        # NOTE: sqs client handels json serialization
        response_code = self.sqs.send_message(data)
        
        if response_code != 200:
            # TODO: handle error appropriately
            pass

if __name__ == "__main__":
    # TODO: send few messages to Games service.
    # try out of order messages too
    from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository  
    from backend_streaming.providers.opta.infra.db import get_session

    repo = MatchProjectionRepository(session_factory=get_session)
    streamer = SingleGameStreamer(game_id='cgrtk6bfvu2ctp1rjs34g2r6c')

    events = repo.get_match_state(match_id="cgrtk6bfvu2ctp1rjs34g2r6c")
    streamer.sqs.purge_queue()
    streamer.send_message(message_type="stop", events=events)