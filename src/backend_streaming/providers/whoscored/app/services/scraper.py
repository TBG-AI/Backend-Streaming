# this is the service that fetches the events using the soccerdata package
import json
import soccerdata as sd
import time
from pathlib import Path

from typing import List
from backend_streaming.providers.whoscored.domain.ws import setup_whoscored
from backend_streaming.streamer.streamer import SingleGameStreamer
# NOTE: borrowing opta infra models since we're mapping to opta
from backend_streaming.providers.opta.infra.models import MatchProjectionModel
from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository
from backend_streaming.providers.opta.infra.db import get_session
from datetime import datetime
from backend_streaming.utils.logging import setup_logger
import sys

class SingleGamesScraper:
    # Get project root directory (assuming consistent project structure)
    PROJECT_ROOT = Path(__file__).parents[5]  # Go up 5 levels from scraper.py to reach project root
    
    # Use absolute paths for mappings
    # TODO: this is very dirty...
    PLAYER_MAPPINGS = json.load(open(PROJECT_ROOT / 'backend_streaming/mappings/player_ids.json'))
    TEAM_MAPPINGS = json.load(open(PROJECT_ROOT / 'backend_streaming/mappings/team_ids.json'))
    MATCH_MAPPINGS = json.load(open(PROJECT_ROOT / 'backend_streaming/mappings/ws_to_opta_match_ids.json'))
    UNFOUND_ID = 'aaaaaaaaaaaaaaaaaaaaaaaaa'

    STOP_MESSAGE_TYPE = 'stop'
    # TODO: currently not using with SingleGameStreamer since selenium can't support multiple live games
    # fix webdriver complexities???
    PROGRESS_MESSAGE_TYPE = 'update'

    def __init__(self, scraper):
        self.scraper = scraper
        self.repo = MatchProjectionRepository(session_factory=get_session)
    
    def fetch_events(self, ws_game_id: str) -> List[MatchProjectionModel]:
        """
        Fetch all whoscored events and format to MatchProjectionModel instances.
        Saves to table and returns the list of MatchProjectionModel instances.
        """
        # TODO: sometimes scraper gets empty events. Make sure to flag this so we can run manually!!!!
        print("...starting fetch events")
        int_game_id = int(ws_game_id)
        events = self.scraper.read_events(
            match_id=int_game_id, 
            output_fmt="raw", 
            force_cache=True,
            live=True 
        )
        if not events:
            print(f"No events found for game {ws_game_id}")
            return []
        print("...finished fetching events")
        projections = [self._convert_to_projection(event) for event in events[int_game_id]]
        print("...finished converting to projections")
        self.repo.save_match_state(projections)
        print("...finished saving to db")
        return projections
    
    # async def stream_events(
    #     self, 
    #     events: List[MatchProjectionModel]
    # ):
    #     """
    #     Publishing events to RabbitMQ. 
    #     For now, assuming we're only streaming at the end of the game.
    #     """
    #     try:
    #         await self.streamer.connect()
    #         await self.streamer.send_message(message_type=self.STOP_MESSAGE_TYPE, events=events)
    #     finally:
    #         await self.streamer.close()
        
    def _convert_to_projection(self, event: dict) -> MatchProjectionModel:
        """
        Convert a single event to a MatchProjectionModel instance
        """
        # Transform qualifiers to the desired format
        transformed_qualifiers = []
        if event.get('qualifiers', {}):
            for qualifier in event['qualifiers']:
                transformed_qualifier = {
                    'qualifierId': qualifier['type']['value'],
                    'value': qualifier.get('value', '')
                }
                transformed_qualifiers.append(transformed_qualifier)

        # Get corresponding Opta IDs from mappings
        ws_match_id = str(event.get('matchId'))
        ws_player_id = str(event.get('playerId')) if event.get('playerId') else None
        ws_team_id = str(event.get('teamId')) if event.get('teamId') else None

        # TODO: need better conversion here. 
        # will need to constantly update the list of mappings for new ids 
        projection = {
            'match_id': self.MATCH_MAPPINGS.get(ws_match_id) or self.UNFOUND_ID,  # Convert to Opta match ID
            'event_id': event['id'],
            'local_event_id': event.get('eventId'),
            'type_id': event.get('type', {}).get('value'),
            'period_id': event.get('period', {}).get('value'),
            'time_min': event.get('minute'),
            'time_sec': event.get('second'),
            'player_id': self.PLAYER_MAPPINGS.get(ws_player_id) or self.UNFOUND_ID,  # Convert to Opta player ID
            'contestant_id': self.TEAM_MAPPINGS.get(ws_team_id) or self.UNFOUND_ID,  # Convert to Opta team ID
            'player_name': None,  # You'll need to get this from a separate mapping
            'outcome': event.get('outcomeType', {}).get('value'),
            'x': event.get('x'),
            'y': event.get('y'),
            'qualifiers': transformed_qualifiers,
            'time_stamp': None,
            'last_modified': None
        }
        return MatchProjectionModel().deserialize(projection)
    

def process_game(game_id: str):
    """Process a single game in its own process"""
    logger = setup_logger(game_id)
    
    try:
        logger.info(f"Starting game at {datetime.now()}")
        scraper = SingleGamesScraper(setup_whoscored(game_id=game_id))
        
        # TODO: hacky way to emulate a live game
        for i in range(10):
            events = scraper.fetch_events(ws_game_id=game_id)
            logger.info(f"Fetch {i+1}/10: Found {len(events)} events")
            logger.debug(f"Timestamp: {datetime.now()}")  # More detailed timing info at debug level
            time.sleep(5)
            
    except Exception as e:
        logger.error(f"Error processing game: {str(e)}", exc_info=True)  # Include stack trace
        return game_id, 0

if __name__ == "__main__":
    # Handle command line argument
    if len(sys.argv) != 2:
        print("Usage: python scraper.py <game_id>")
        sys.exit(1)
    
    game_id = sys.argv[1]
    process_game(game_id)