import json
import string
import random
from typing import List, Dict, Tuple, Optional
from collections import defaultdict

# using MatchProjectionRepository originally defined for Opta provider
from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository
from backend_streaming.providers.opta.infra.db import get_session
from backend_streaming.providers.whoscored.infra.config.logger import setup_game_logger
from backend_streaming.providers.whoscored.infra.repos.mapping_repo import MappingRepository
from backend_streaming.providers.whoscored.infra.config.config import paths, mapping_paths, mapping_types

# TODO: The mapping functionality is unnecessarily complex...

class SingleGameScraper:
    """
    Scrapes and processes WhoScored game events.
    Maps WhoScored IDs to Opta IDs using predefined mappings.
    """    
    HOME_KEYWORD = "home"
    AWAY_KEYWORD = "away"
    FORMATION_KEYWORD = "formation"
    PLAYER_IDS_KEYWORD = "playerIds"
    JERSEY_NUMBERS_KEYWORD = "jerseyNumbers"
    PLAYER_NAME_DICTIONARY_KEYWORD = "playerIdNameDictionary"
    TEAM_ID_KEYWORD = "teamId"
    
    def __init__(self, game_id: str):
        """
        Initialize scraper with WhoScored client and mappings.
        
        Args:
            whoscored_client: Initialized WhoScored client
        """
        self.game_id = game_id
        self.logger = setup_game_logger(self.game_id)
        self.proj_repo = MatchProjectionRepository(session_factory=get_session, logger=self.logger)
        self.mapping_repo = MappingRepository(
            paths=mapping_paths,
            mapping_types=mapping_types,
            logger=self.logger
        )
        # init paths
        self._init_mappings()

        # NOTE: this will be populated after the first fetch
        self.json_data = None

    def _init_mappings(self):
        """
        Initialize paths for the scraper.
        """
        self.ws_to_opta_mapping = self.mapping_repo.load(mapping_types.MATCH)
        self.player_mappings = self.mapping_repo.load(mapping_types.PLAYER)
        self.team_mappings = self.mapping_repo.load(mapping_types.TEAM)

    def fetch_events(self) -> dict:
        """
        Process game from raw pagesource.
        Steps:
            1. Read raw pagesource
            2. Convert to JSON and save
            3. Extract events and lineup data
            4. Convert to projections and save
        """
        # Read raw pagesource
        raw_file = paths.raw_pagesources_dir / f"{self.game_id}.txt"
        if not raw_file.exists():
            raise FileNotFoundError(f"Raw pagesource not found for game {self.game_id}")
            
        self.logger.info(f"Reading raw pagesource for game {self.game_id}")
        raw_content = raw_file.read_text()
        
        # Convert to JSON 
        json_data = self._format_pagesource(raw_content)
        
        # NOTE: There is a LOT more information that you can potentially use from this single json source. 
        # Properly investigate this. For now, I'm just using the events and lineup info.
        json_file = paths.parsed_page_sources_dir / f"{self.game_id}.json"
        with open(json_file, 'w') as f:
            json.dump(json_data, f, indent=2)
        
        # Extract events
        self.json_data = json_data
        if "events" not in json_data:
            self.logger.warning(f"No events found in game {self.game_id}")
            return []
        
        self.logger.info(f"Extracted {len(json_data['events'])} events from game {self.game_id}")
        return json_data['events']
    
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
        
    def extract_lineup(self) -> dict:
        """
        Extract lineup data from json_data and saves to file.
        Returns a dictionary with player IDs separated by team.
        """
        if self.json_data is None:
            raise ValueError(f"No events found in game {self.game_id}")
        if self.HOME_KEYWORD not in self.json_data or self.AWAY_KEYWORD not in self.json_data:
            raise ValueError(f"No lineup data found in game {self.game_id}")
                
        # save lineup as json
        home_lineup, away_lineup = self._format_lineup_data(self.json_data, self.game_id)
        with open(paths.lineups_dir / f"{self.game_id}.json", 'w') as f:
            json.dump({self.HOME_KEYWORD: home_lineup, self.AWAY_KEYWORD: away_lineup}, f, indent=2)

        # Extract all player IDs from both teams' formations
        team_player_ids = defaultdict(list)
        for team in [self.HOME_KEYWORD, self.AWAY_KEYWORD]:
            if self.FORMATION_KEYWORD in self.json_data[team] and self.json_data[team][self.FORMATION_KEYWORD]:
                formation = self.json_data[team][self.FORMATION_KEYWORD][0]
                team_player_ids[team] = formation.get(self.PLAYER_IDS_KEYWORD, [])

        return team_player_ids

    def update_player_mappings(self) -> dict:
        """
        Update player mappings for players that don't exist in the mapping.
        Returns a dictionary of new player mappings.
        """
        updated = False 
        new_player_mappings = {}
        player_info = self._extract_player_info()

        for player_id, (player_name, jersey_number, ws_team_id) in player_info.items():
            if player_id not in self.player_mappings:
                updated = True
                # Generate random Opta-like ID
                opta_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=random.randint(20, 25)))
                self.player_mappings[player_id] = opta_id
                
                # Split name into first and last name
                name_parts = player_name.split(maxsplit=1)
                first_name = name_parts[0]
                last_name = name_parts[1] if len(name_parts) > 1 else ""
                
                # Create player data dictionary
                player_data = {
                    'player_id': opta_id,
                    'team_id': self.team_mappings[ws_team_id],
                    'first_name': first_name,
                    'last_name': last_name,
                    'short_first_name': first_name,
                    'short_last_name': last_name,
                    'match_name': player_name,
                    'shirt_number': jersey_number
                }
                
                # Store mapping for return
                new_player_mappings[player_id] = player_data
                # Insert player into database
                self.mapping_repo.insert_player_data(
                    session=get_session(),
                    **player_data
                )

        if updated:
            self.logger.info(f"Added {len(new_player_mappings)} new player mappings")
            self.mapping_repo.save(mapping_types.PLAYER, self.player_mappings)
        
        return new_player_mappings


    ###################################
    # Class specific helper functions #
    ###################################

    def _extract_player_info(self) -> dict:
        """
        Only called if player mappings are not found.
        Extracts the minimal information to fill in the players table with the the new players
        """
        player_info = {}
        for team_key in [self.HOME_KEYWORD, self.AWAY_KEYWORD]:
            team_data = self.json_data[team_key]
            formation = team_data[self.FORMATION_KEYWORD][0]
            player_ids = formation[self.PLAYER_IDS_KEYWORD]
            jersey_numbers = formation[self.JERSEY_NUMBERS_KEYWORD]
            ws_team_id = str(team_data[self.TEAM_ID_KEYWORD])
            player_name_dict = self.json_data[self.PLAYER_NAME_DICTIONARY_KEYWORD]
            
            # Map player IDs to their jersey numbers
            for player_id, jersey_number in zip(player_ids, jersey_numbers):
                str_player_id = str(player_id)
                
                # NOTE: this assertion is just a sanity check.
                # If this fails, it means that WhoScored's json_data is not properly formatted.
                assert str_player_id in player_name_dict, f"Player name dictionary does not contain player {str_player_id}"
                player_name = player_name_dict[str_player_id]
                player_info[str_player_id] = [player_name, jersey_number, ws_team_id]

        return player_info

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
    
    @staticmethod
    def _format_lineup_data(json_data: dict, game_id: int) -> tuple[dict, dict]:
        """
        Convert both teams' formation data into the required lineup info format.
        Takes the first formation entry (initial lineup) for each team.
        
        Returns:
            tuple[dict, dict]: (home_lineup, away_lineup)
        """
        def _format_team(team_data: dict) -> dict:
            formation = team_data['formations'][0]
            return {
                "data_type": "whoscored",
                "game_id": game_id,
                "lineup_info": {
                    "team_id": team_data['teamId'],
                    "formation_id": formation['formationId'],
                    "formation_name": formation['formationName'],
                    "formation_slots": formation['formationSlots'],
                    "player_ids": formation['playerIds'],
                    "formation_positions": [
                        {
                            "vertical": pos["vertical"],
                            "horizontal": pos["horizontal"]
                        }
                        for pos in formation['formationPositions']
                    ],
                    "captain_id": formation['captainPlayerId']
                }
            }
        
        home_lineup = _format_team(json_data["home"])
        away_lineup = _format_team(json_data["away"])
        
        return home_lineup, away_lineup


