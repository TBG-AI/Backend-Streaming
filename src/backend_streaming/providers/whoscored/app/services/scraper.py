# this is the service that fetches the events using the soccerdata package
from typing import List
from backend_streaming.providers.opta.infra.models import MatchProjectionModel
from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository
from backend_streaming.providers.opta.infra.db import get_session
from backend_streaming.providers.whoscored.infra.logs.logger import setup_game_logger
from backend_streaming.providers.whoscored.domain.mappings import WhoScoredToOptaMappings
from backend_streaming.providers.whoscored.infra.repos.file_mapping_repo import FileMappingRepository
from backend_streaming.providers.whoscored.infra.config import MAPPINGS_DIR
from backend_streaming.providers.whoscored.domain.ws import WhoScored
from backend_streaming.providers.whoscored.infra.config import PLAYER_MAPPING_TYPE, TEAM_MAPPING_TYPE, MATCH_MAPPING_TYPE

# TODO: need to add contestant_ids and player_ids to the mappings.

class SingleGamesScraper:
    """
    Scrapes and processes WhoScored game events.
    Maps WhoScored IDs to Opta IDs using predefined mappings.
    """    
    def __init__(self, whoscored_client: WhoScored):
        """
        Initialize scraper with WhoScored client and mappings.
        
        Args:
            whoscored_client: Initialized WhoScored client
        """
        self.client = whoscored_client
        self.proj_repo = MatchProjectionRepository(session_factory=get_session)
        
        # Initialize mappings with file repository
        mapping_repository = FileMappingRepository(MAPPINGS_DIR)
        self.mappings = WhoScoredToOptaMappings.create(mapping_repository)
        self.logger = setup_game_logger(self.client.game_id)
        
    def fetch_events(self, ws_game_id: str) -> List[MatchProjectionModel]:
        """
        Fetch and process WhoScored events.
        
        Args:
            ws_game_id: WhoScored game ID
            
        Returns:
            List[MatchProjectionModel]: Processed event models
        """
        int_game_id = int(ws_game_id)
        events = self.client.read_events(
            match_id=int_game_id,
            output_fmt="raw",
            force_cache=True,
            live=True
        )
        
        if not events:
            self.logger.warning(f"No events found for game {ws_game_id}")
            return []
            
        projections = [
            self._convert_to_projection(event) 
            for event in events[int_game_id]
        ]
        self.proj_repo.save_match_state(projections)
        return projections
        
    def _convert_to_projection(self, event: dict) -> MatchProjectionModel:
        """
        Convert WhoScored event to Opta projection format.
        
        Args:
            event: Raw WhoScored event
            
        Returns:
            MatchProjectionModel: Converted event in Opta format
        """
        transformed_qualifiers = self._transform_qualifiers(
            event.get('qualifiers', {})
        )
        
        # Map IDs to Opta format
        # TODO: if there are unmapped player or team ids, we need to also update the player and team tables!!!!
        ws_player_id = str(event.get('playerId')) if event.get('playerId') else None
        ws_team_id = str(event.get('teamId')) if event.get('teamId') else None
        projection = {
            'match_id': self.mappings.get_or_create_mapping(MATCH_MAPPING_TYPE, self.client.game_id),
            'player_id': self.mappings.get_or_create_mapping(PLAYER_MAPPING_TYPE, ws_player_id),
            'contestant_id': self.mappings.get_or_create_mapping(TEAM_MAPPING_TYPE, ws_team_id),
            'event_id': event['id'],
            'local_event_id': event.get('eventId'),
            'type_id': event.get('type', {}).get('value'),
            'period_id': event.get('period', {}).get('value'),
            'time_min': event.get('minute'),
            'time_sec': event.get('second'),
            'player_name': None,
            'outcome': event.get('outcomeType', {}).get('value'),
            'x': event.get('x'),
            'y': event.get('y'),
            'qualifiers': transformed_qualifiers,
            'time_stamp': None,
            'last_modified': None
        }
        
        return MatchProjectionModel().deserialize(projection)
            
    def _transform_qualifiers(self, qualifiers: dict) -> List[dict]:
        """
        Transform WhoScored qualifiers to Opta format.
        
        Args:
            qualifiers: WhoScored qualifiers
            
        Returns:
            List[dict]: Transformed qualifiers in Opta format
        """
        transformed = []
        for qualifier in qualifiers:
            transformed.append({
                'qualifierId': qualifier['type']['value'],
                'value': qualifier.get('value', '')
            })
        return transformed