# this is the service that fetches the events using the soccerdata package
import json
import soccerdata as sd
from typing import List
from backend_streaming.providers.whoscored.domain.ws import setup_whoscored
from backend_streaming.streamer.streamer import SingleGameStreamer
# NOTE: borrowing opta infra models since we're mapping to opta
from backend_streaming.providers.opta.infra.models import MatchProjectionModel
from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository
from backend_streaming.providers.opta.infra.db import get_session

class SingleGameScraper:
    # TODO: don't do this per class instantiation. pass them in instead!
    # if the mappings are not found, the fields will get populated with None. 
    PLAYER_MAPPINGS = json.load(open('src/backend_streaming/mappings/player_ids.json'))
    TEAM_MAPPINGS = json.load(open('src/backend_streaming/mappings/team_ids.json'))
    MATCH_MAPPINGS = json.load(open('src/backend_streaming/mappings/ws_to_opta_match_ids.json'))
    UNFOUND_ID = 'aaaaaaaaaaaaaaaaaaaaaaaaa'

    STOP_MESSAGE_TYPE = 'stop'
    # TODO: currently not using with SingleGameStreamer since selenium can't support multiple live games
    # fix webdriver complexities???
    PROGRESS_MESSAGE_TYPE = 'update'

    RABBITMQ_URL='amqp://guest:guest@localhost:5672/'
    QUEUE_NAME='game_events'

    def __init__(self, game_id: str):
        self.game_id = game_id
        self.scraper = setup_whoscored()
        self.repo = MatchProjectionRepository(session_factory=get_session)
        self.streamer = SingleGameStreamer(
            game_id=self.MATCH_MAPPINGS.get(str(game_id)) or self.UNFOUND_ID, 
            url=self.RABBITMQ_URL, 
            queue_name=self.QUEUE_NAME
        )

    def fetch_events(self) -> List[MatchProjectionModel]:
        """
        Fetch all whoscored events and format to MatchProjectionModel instances.
        Saves to table and returns the list of MatchProjectionModel instances.
        """
        events = self.scraper.read_events(
            match_id=self.game_id, 
            output_fmt="raw", 
            force_cache=True, 
        )
        projections = [self._convert_to_projection(event) for event in events[self.game_id]]
        # import ipdb; ipdb.set_trace()
        self.repo.save_match_state(projections)
        return projections
    
    async def stream_events(
        self, 
        events: List[MatchProjectionModel]
    ):
        """
        Publishing events to RabbitMQ. 
        For now, assuming we're only streaming at the end of the game.
        """
        try:
            await self.streamer.connect()
            await self.streamer.send_message(message_type=self.STOP_MESSAGE_TYPE, events=events)
        finally:
            await self.streamer.close()
        
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
        ws_match_id = str(self.game_id)
        ws_player_id = str(event.get('playerId')) if event.get('playerId') else None
        ws_team_id = str(event.get('teamId')) if event.get('teamId') else None

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


if __name__ == "__main__":
    import asyncio
    game_id = 1821192
    scraper = SingleGameScraper(game_id)
    projections = scraper.fetch_events()
    asyncio.run(scraper.stream_events(projections))   
