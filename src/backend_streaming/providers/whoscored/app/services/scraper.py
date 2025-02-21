import json
from typing import List, Dict, Tuple, Optional

from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository
from backend_streaming.providers.opta.infra.db import get_session
from backend_streaming.providers.whoscored.infra.config.logger import setup_game_logger
from backend_streaming.providers.whoscored.domain.mappings import WhoScoredToOptaMappings
from backend_streaming.providers.whoscored.infra.repos.file_mapping_repo import FileMappingRepository
from backend_streaming.providers.whoscored.infra.config.config import paths, mappings

# TODO: The mapping functionality is unnecessarily complex...

class SingleGameScraper:
    """
    Scrapes and processes WhoScored game events.
    Maps WhoScored IDs to Opta IDs using predefined mappings.
    """    
    def __init__(self, game_id: str):
        """
        Initialize scraper with WhoScored client and mappings.
        
        Args:
            whoscored_client: Initialized WhoScored client
        """
        self.game_id = game_id
        self.logger = setup_game_logger(self.game_id)
        self.mapping_repo = WhoScoredToOptaMappings.create(FileMappingRepository(paths.mappings_dir))
        self.proj_repo = MatchProjectionRepository(session_factory=get_session, logger=self.logger)
        
    def fetch_events(self) -> dict:
        """
        Process game from raw pagesource.
        Steps:
            1. Read raw pagesource
            2. Convert to JSON and save
            3. Extract events
            4. Convert to projections and save
        """
        # 1. Read raw pagesource
        raw_file = paths.raw_pagesources_dir / f"{self.game_id}.txt"
        if not raw_file.exists():
            raise FileNotFoundError(f"Raw pagesource not found for game {self.game_id}")
            
        self.logger.info(f"Reading raw pagesource for game {self.game_id}")
        raw_content = raw_file.read_text()
        
        # 2. Convert to JSON 
        json_data = self._format_pagesource(raw_content)
        
        # TODO: move properly to the cache I'm creating!
        json_file = paths.game_sources_dir / f"{self.game_id}.json"
        self.logger.info(f"Saving formatted JSON for game {self.game_id}")
        with open(json_file, 'w') as f:
            json.dump(json_data, f, indent=2)
        
        # 3. Extract events
        if "events" not in json_data:
            raise ValueError(f"No events found in game {self.game_id}")
        
        events = json_data["events"]
        self.logger.info(f"Extracted {len(events)} events from game {self.game_id}")
        return self.save_projections(events)
        
    def save_projections(self, events: List[dict]):
        """
        Converting events to projections and saving them to match_projections table.
        """
        projections = []
        for event in events:
            try:
                projection = self._convert_to_projection(event)
                projections.append(projection)
            except ValueError as e:
                # Log the missing player mapping
                self.logger.warning(f"Undetected player {event.get('playerId')} for event {event['id']}")
                continue
        try:
            if projections:
                self.proj_repo.save_match_state(projections)
        except Exception as e:
            self.logger.error(f"Failed to save projections: {e}")
            raise
            
        return projections
        
    def _convert_to_projection(self, event: dict) -> dict:
        transformed_qualifiers = self._transform_qualifiers(event.get('qualifiers', {}))
        match_id, team_id, player_id = self._get_mappings(event)
        
        projection = {
            'match_id': match_id,
            'player_id': player_id,
            'contestant_id': team_id,
            'event_id': int(event['id']),
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
        return projection    
        # return MatchProjectionModel().deserialize(projection)
    
    def _get_mappings(self, event: dict) -> Tuple[str, str, Optional[str]]:
        """
        Get mappings for team, match, and player.
        Team and match mappings must exist, player mappings might not..
        """
        # Team and match mappings must exist    
        match_id = self.mapping_repo.get_mapping(mappings.MATCH, self.game_id)
        assert match_id is not None, f"Match mapping not found for {self.game_id}"

        ws_team_id = str(event.get('teamId')) if event.get('teamId') else None
        team_id = self.mapping_repo.get_mapping(mappings.TEAM, ws_team_id)
        assert team_id is not None, f"Team mapping not found for {ws_team_id}"
        
        # Player mapping might not exist
        ws_player_id = str(event.get('playerId')) if event.get('playerId') else None
        player_id = None
        if ws_player_id:
            # Let ValueError propagate up for handling in save_projections
            player_id = self.mapping_repo.get_mapping(mappings.PLAYER, ws_player_id)
        
        return match_id, team_id, player_id
    
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
    
    def _format_pagesource(self, raw_content: str) -> Dict:
        """
        Format raw pagesource content into proper JSON.
        Very basic checks include:
            - Remove trailing comma if it exists
            - Add closing brace if missing
            - Check for required fields
        """
        # Remove trailing comma if it exists
        if raw_content.rstrip().endswith(','):
            raw_content = raw_content.rstrip().rstrip(',')
        
        # Add closing brace if missing
        if raw_content.count('{') > raw_content.count('}'):
            raw_content = raw_content + '}'
            
        try:
            data = json.loads(raw_content)
            
            # Validate required fields
            required_fields = ["playerIdNameDictionary", "events", "home", "away"]
            missing_fields = [field for field in required_fields if field not in data]
            
            if missing_fields:
                raise ValueError(f"Missing required fields: {missing_fields}")
            return data
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format: {e}")


