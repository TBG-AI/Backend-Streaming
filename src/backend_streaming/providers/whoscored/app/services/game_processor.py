import json
import asyncio
import soccerdata as sd
from typing import List
from datetime import datetime, timedelta

from backend_streaming.providers.whoscored.domain.ws import setup_whoscored
from backend_streaming.streamer.streamer import SingleGameStreamer
from backend_streaming.providers.opta.infra.models import MatchProjectionModel
from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository
from backend_streaming.providers.opta.infra.db import get_session
from backend_streaming.providers.whoscored.app.services.scraper import SingleGamesScraper

class GameProcessor:
    def __init__(self, game_id: int, poll_interval: int = 30):
        self.game_id = game_id
        self.poll_interval = poll_interval
        self.running = True
        
        # Initialize scraper and streamer
        self.scraper = SingleGamesScraper(setup_whoscored())
        self.streamer = SingleGameStreamer(
            game_id=str(game_id),
            url="amqp://guest:guest@localhost:5672/",
            queue_name="game_events"
        )
        
    async def process(self):
        """
        Continuously process game events until the game is finished
        """
        try:
            await self.streamer.connect()
            
            last_event_count = 0
            no_new_events_count = 0
            
            while self.running:
                try:
                    # Fetch and process events
                    events = self.scraper.fetch_events(ws_game_id=self.game_id)
                    current_event_count = len(events)
                    
                    # If we have new events, stream them
                    if current_event_count > last_event_count:
                        print(f"Found {current_event_count - last_event_count} new events")
                        await self.streamer.send_message(
                            message_type="update",
                            events=events
                        )
                        no_new_events_count = 0
                    else:
                        no_new_events_count += 1
                        print(f"No new events found. Count: {no_new_events_count}")
                    
                    last_event_count = current_event_count
                    
                    # Check if we should stop processing
                    # For example, if no new events for 30 minutes (60 polls * 30 seconds)
                    if no_new_events_count >= 60:
                        print("No new events for 30 minutes. Sending stop message.")
                        await self.streamer.send_message(
                            message_type="stop",
                            events=events
                        )
                        self.running = False
                        break
                    
                    # Wait before next poll
                    await asyncio.sleep(self.poll_interval)
                    
                except Exception as e:
                    print(f"Error during event processing: {str(e)}")
                    # Maybe wait longer on error
                    await asyncio.sleep(self.poll_interval * 2)
                    
        except Exception as e:
            print(f"Fatal error processing game {self.game_id}: {str(e)}")
            raise
        finally:
            await self.streamer.close()
            print(f"Finished processing game {self.game_id}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--game-id', type=int, required=True)
    parser.add_argument('--poll-interval', type=int, default=30,
                       help='Interval in seconds between event polls')
    args = parser.parse_args()
    
    processor = GameProcessor(args.game_id, args.poll_interval)
    asyncio.run(processor.process()) 