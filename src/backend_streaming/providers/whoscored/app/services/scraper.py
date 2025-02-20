# this is the service that fetches the events using the soccerdata package
import re
import json
from pathlib import Path
from typing import List, Dict, Tuple, Optional

from backend_streaming.providers.opta.infra.models import MatchProjectionModel
from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository
from backend_streaming.providers.opta.infra.db import get_session
from backend_streaming.providers.whoscored.infra.logs.logger import setup_game_logger, setup_provider_logger
from backend_streaming.providers.whoscored.domain.mappings import WhoScoredToOptaMappings
from backend_streaming.providers.whoscored.infra.repos.file_mapping_repo import FileMappingRepository
from backend_streaming.providers.whoscored.domain.ws import WhoScored
from backend_streaming.providers.whoscored.infra.config import (
    PLAYER_MAPPING_TYPE, 
    TEAM_MAPPING_TYPE, 
    MATCH_MAPPING_TYPE, 
    RAW_PAGESOURCES_DIR, 
    GAME_SOURCES_DIR, 
    MAPPINGS_DIR,
    MATCH_NAMES_TYPE
)

# TODO: The mapping functionality is unnecessarily complex...

class SingleGameScraper:
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
        if self.client:
            self.game_id = self.client.game_id
        
        self.mapping_repo = WhoScoredToOptaMappings.create(FileMappingRepository(MAPPINGS_DIR))
        self.logger = setup_game_logger(self.game_id)
        self.proj_repo = MatchProjectionRepository(session_factory=get_session, logger=self.logger)
        
    def fetch_events(self) -> List[MatchProjectionModel]:
        """
        Fetch and process WhoScored events.
        
        Args:
            ws_game_id: WhoScored game ID
            
        Returns:
            List[MatchProjectionModel]: Processed event models
        """
        ws_game_id = int(self.game_id)
        events = self.client.read_events(
            match_id=ws_game_id,
            output_fmt="raw",
            force_cache=True,
            live=True
        )
        # NOTE: flagging so we can rerun with the ManualGameScraper
        if not events:
            self.logger.warning(f"No events found for game_id: {ws_game_id}. Match_name: {self.mapping_repo.get_mapping(MATCH_NAMES_TYPE, ws_game_id)}")
            return []
            
        return self.save_projections(events[ws_game_id])
    
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
        return projection    
        # return MatchProjectionModel().deserialize(projection)
    
    def _get_mappings(self, event: dict) -> Tuple[str, str, Optional[str]]:
        """
        Get mappings for team, match, and player.
        Team and match mappings must exist, player mappings might not..
        """
        # Team and match mappings must exist    
        match_id = self.mapping_repo.get_mapping(MATCH_MAPPING_TYPE, self.game_id)
        assert match_id is not None, f"Match mapping not found for {self.game_id}"

        ws_team_id = str(event.get('teamId')) if event.get('teamId') else None
        team_id = self.mapping_repo.get_mapping(TEAM_MAPPING_TYPE, ws_team_id)
        assert team_id is not None, f"Team mapping not found for {ws_team_id}"
        
        # Player mapping might not exist
        ws_player_id = str(event.get('playerId')) if event.get('playerId') else None
        player_id = None
        if ws_player_id:
            # Let ValueError propagate up for handling in save_projections
            player_id = self.mapping_repo.get_mapping(PLAYER_MAPPING_TYPE, ws_player_id)
        
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
    

########################
# Manual Game Scraper #
########################

class ManualGameScraper(SingleGameScraper):
    """
    Fall back method to scrape games from the pagesource manually
    """
    def __init__(self, game_id: str):
        self.game_id = game_id
        # Pass None as we don't need a WhoScored client
        super().__init__(None)  
        

    def fetch_events(self) -> None:
        """
        Process game from raw pagesource.
        Steps:
            1. Read raw pagesource
            2. Convert to JSON and save
            3. Extract events
            4. Convert to projections and save
        """
        # 1. Read raw pagesource
        raw_file = RAW_PAGESOURCES_DIR / f"{self.game_id}.txt"
        if not raw_file.exists():
            raise FileNotFoundError(f"Raw pagesource not found for game {self.game_id}")
            
        self.logger.info(f"Reading raw pagesource for game {self.game_id}")
        raw_content = raw_file.read_text()
        
        # 2. Convert to JSON 
        json_data = self._format_pagesource(raw_content)
        
        # TODO: move properly to the cache I'm creating!
        json_file = GAME_SOURCES_DIR / f"{self.game_id}.json"
        self.logger.info(f"Saving formatted JSON for game {self.game_id}")
        with open(json_file, 'w') as f:
            json.dump(json_data, f, indent=2)
        
        # 3. Extract events
        if "events" not in json_data:
            raise ValueError(f"No events found in game {self.game_id}")
        
        events = json_data["events"]
        self.logger.info(f"Extracted {len(events)} events from game {self.game_id}")
        return self.save_projections(events)
        
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
        
if __name__ == "__main__":
    games_unable_to_scrape = ["1821421", "1821424"]
    for game_id in games_unable_to_scrape:
        scraper = ManualGameScraper(game_id)
        scraper.fetch_events()

    # # loader = OptaLoader(
    # #     root=GAME_SOURCES_DIR,
    # #     parser="whoscored",
    # #     feeds={"whoscored": str(Path("ENG-Premier League_2425/1821424.json"))},
    # # )
    # # import ipdb; ipdb.set_trace()
    # # import soccerdata as sd
    # # ws = sd.WhoScored(leagues="ENG-Premier League", seasons='24-25')
    # # events = ws.read_events(
    # #     output_fmt='loader', 
    # #     match_id=1821164, 
    # #     force_cache=True,
    # #     live=True
    # # )
    # # import ipdb; ipdb.set_trace()
    # from backend_streaming.providers.whoscored.domain.ws import setup_whoscored
    # game_id = 1821340
    # scraper = SingleGameScraper(setup_whoscored(game_id=game_id))
    # scraper.fetch_events()
            

