
import json
import pika
from typing import Optional, Dict, List

from shared.src.shared.messaging.sqs import LocalSQSClient 
from datetime import datetime

from backend_streaming.providers.opta.infra.models import MatchProjectionModel

import logging
logger = logging.getLogger(__name__)

RABBITMQ_URL = 'amqp://guest:guest@localhost:5672/'
QUEUE_NAME = 'game_events'

class SingleGameStreamer:
    def __init__(
        self, 
        game_id: str, 
        url: str = RABBITMQ_URL,
        queue_name: str = QUEUE_NAME
    ):
        self.game_id = game_id
        self.url = url
        self.queue_name = queue_name

        # TODO: use aio-pika for async?
        self.connection = pika.BlockingConnection(pika.URLParameters(self.url))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)

    def send_message(
        self, 
        message_type: str, 
        events: List[MatchProjectionModel] = None
    ):
        # need to convert to json serializable object
        payload = [model.to_dict() for model in events] if events else None

        properties = pika.BasicProperties(
            # TODO: app_id is kinda irrelevant but including for consistency
            app_id='single_game_streamer',
            content_type='application/json',
            headers={
                'game_id': self.game_id,
                'message_type': message_type,
                'timestamp': datetime.now().isoformat()
            }
        )
        self.channel.basic_publish(
            exchange='',
            routing_key=self.queue_name,
            body=json.dumps(payload),
            properties=properties
    )

    def close(self):
        self.connection.close()



if __name__ == "__main__":
    from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository  
    from backend_streaming.providers.opta.infra.db import get_session

    # get the events
    game_id = 'ci0mj3nznl2mswxmit5tdiwic'
    repo = MatchProjectionRepository(session_factory=get_session)
    events = repo.get_match_state(match_id=game_id)

    # set up streamer
    rabbitmq_url = 'amqp://guest:guest@localhost:5672/'
    queue_name = 'game_events'
    message_type = 'stop'
    streamer = SingleGameStreamer(game_id, rabbitmq_url, queue_name)
    
    # send the events
    print(f"==> \n Sending {len(events)} events to RabbitMQ...")
    print(f"==> message type: {message_type}")
    streamer.send_message(message_type=message_type, events=events)
    
  