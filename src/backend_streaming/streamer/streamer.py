import os
import json
import aio_pika

from typing import List
from pathlib import Path
from datetime import datetime
from backend_streaming.providers.opta.infra.models import MatchProjectionModel

# TODO: implement better logging here!
import logging
logger = logging.getLogger(__name__)

QUEUE_NAME = 'game_events'

class SingleGameStreamer:
    
    PROGRESS_MESSAGE_TYPE = 'update'
    STOP_MESSAGE_TYPE = 'stop'

    def __init__(
        self, 
        game_id: str, 
        url: str = os.getenv('RABBITMQ_URL'),
        queue_name: str = QUEUE_NAME
    ):
        self.game_id = game_id
        self.url = url
        self.queue_name = queue_name
        # TODO: these are RabbitMQ connection objects? 
        # If so, should keep dependencies to be injected instead of fixing design
        self.connection = None
        self.channel = None

    async def connect(self):
        """Establish connection and channel"""
        if not self.connection:
            self.connection = await aio_pika.connect_robust(self.url)
            self.channel = await self.connection.channel()
            await self.channel.declare_queue(self.queue_name)

    async def send_message(
        self, 
        message_type: str, 
        events: List[MatchProjectionModel] = None
    ):
        """Send message to RabbitMQ queue"""
        if not self.channel:
            await self.connect()

        # payload must be a json serializable object
        if isinstance(events[0], dict):
            payload = events
        else:
            payload = [model.to_dict() for model in events]

        message = aio_pika.Message(
            body=json.dumps(payload).encode(),
            app_id='single_game_streamer',
            content_type='application/json',
            headers={
                'game_id': self.game_id,
                'message_type': message_type,
                'timestamp': datetime.now().isoformat()
            }
        )
        await self.channel.default_exchange.publish(
            message,
            routing_key=self.queue_name
        )

    async def close(self):
        """Close connection"""
        if self.connection:
            await self.connection.close()
            self.connection = None
            self.channel = None


if __name__ == "__main__":
    import asyncio
    async def main():
        # get the events
        game_id = 'ci0mj3nznl2mswxmit5tdiwic'
        with open('tests/data/test_events.json') as f:
            # TODO: ok to just return this json?
            events = json.load(f)

        # set up streamer
        rabbitmq_url = 'amqp://guest:guest@localhost:5672/'
        queue_name = 'game_events'
        message_type = 'stop'
        
        streamer = SingleGameStreamer(game_id, rabbitmq_url, queue_name)
        try:
            await streamer.connect()
            print(f"==> Sending {len(events)} events to RabbitMQ with type: {message_type}")
            await streamer.send_message(message_type=message_type, events=events)
        finally:
            await streamer.close()

    # Run the async main function
    asyncio.run(main())  