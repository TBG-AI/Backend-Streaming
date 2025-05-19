import os
import json
import string
import random
from typing import List, Dict, Tuple, Optional, Union
from datetime import datetime

from backend_streaming.providers.opta.infra.db import get_session
from backend_streaming.providers.whoscored.infra.config.logger import setup_game_logger
from backend_streaming.providers.whoscored.infra.repos.file_repo import FileRepository
from backend_streaming.providers.whoscored.infra.repos.scraper_repo import ScraperRepository
# NOTE: using MatchProjectionRepository originally defined for Opta provider
from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository
from backend_streaming.providers.whoscored.infra.config.config import paths, type_to_paths

# TODO: The mapping functionality is unnecessarily complex...

class SingleGameScraper:
    """
    Scrapes and processes WhoScored game events.
    Maps WhoScored IDs to Opta IDs using predefined mappings.
    """    
    HOME_KEYWORD = "home"
    AWAY_KEYWORD = "away"
    FORMATION_KEYWORD = "formations"
    PLAYER_IDS_KEYWORD = "playerIds"
    JERSEY_NUMBERS_KEYWORD = "jerseyNumbers"
    PLAYER_NAME_DICTIONARY_KEYWORD = "playerIdNameDictionary"
    TEAM_ID_KEYWORD = "teamId"
    EVENTS_KEYWORD = "events"
    
    def __init__(self, game_id: str):
        """
        Initialize scraper with WhoScored client and mappings.
        
        Args:
            whoscored_client: Initialized WhoScored client
        """
        # SingleGameScraper is only used for manual fetches
        self._is_manual_scraper = True

        # NOTE: for manual fetches, game_id is a path to the file
        assert isinstance(game_id, str), f"game_id must be a string, got {type(game_id)}"
        self.game_id = game_id
        self.logger = setup_game_logger(self.game_id)
        self.proj_repo = MatchProjectionRepository(session_factory=get_session, logger=self.logger)
        self.scraper_repo = ScraperRepository(logger=self.logger)
        self.file_repo = FileRepository(
            paths=paths,
            type_to_paths=type_to_paths,
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
        self.ws_to_opta_mapping = self.file_repo.load('match')
        self.player_mappings = self.file_repo.load('player')
        self.team_mappings = self.file_repo.load('team')

    def fetch_events(self, page_source: Optional[str] = None) -> dict:
        """
        Process game from raw pagesource and save to JSON.
        Will also save as match projection rows to update db and send via streamer.
        """
        if not page_source:
            # Read raw pagesource from local file
            # TODO: need to change to load from db instead of local file
            page_source = self.file_repo.load(
                file_type='raw_pagesources', 
                is_txt=True, 
                file_name=f"{self.game_id}.txt"
            )
        # NOTE: There is a LOT of information stored in the json_data (not just events). 
        # Therefore, saving as an attribute to use elsewhere
        self.json_data = self._format_pagesource(page_source)
        # TODO: instead of saving locally, save to db.
        # self.file_repo.save(
        #     file_type='parsed_page_sources', 
        #     data=self.json_data, 
        #     file_name=f"{self.game_id}.json"
        # )
        # Extract events
        if "events" not in self.json_data:
            self.logger.warning(f"No events found in game {self.game_id}")
            return []
        
        self.logger.info(f"Extracted {len(self.json_data['events'])} events from game {self.game_id}")
        return self.json_data['events']
    
    def get_score(self) -> dict:
        """
        Get the score from the json_data and parse it into home and away scores.
        Returns:
            dict: Dictionary containing 'home_score' and 'away_score' as integers
        """
        score_str = self.json_data['score']
        # Split the string by ':' and strip whitespace
        score_parts = [part.strip() for part in score_str.split(':')]
        
        # Convert to integers
        try:
            home_score = int(score_parts[0])
            away_score = int(score_parts[1])
        except (ValueError, IndexError):
            raise ValueError(f"Failed to parse score string: {score_str}")
        
        return {
            'home_score': home_score,
            'away_score': away_score
        }
    
    def save_projections(self, events: List[dict]) -> List[dict]:
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
                # NOTE: this player info should immdiately get updated in the 
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
        Extract lineup data from json_data and saves to file
        Returns:
            a dictionary with player IDs separated by team. 
            If convert_to_opta is True, will convert the player IDs to Opta IDs using the player_mappings dictionary.
        """              
        # save lineup as json
        home_lineup, away_lineup = self._format_lineup_data()
        lineup_info = {
            self.HOME_KEYWORD: home_lineup,
            self.AWAY_KEYWORD: away_lineup
        }
        # TODO: instead of saving locally, save to db.
        # self.file_repo.save(
        #     file_type='lineups',
        #     data=lineup_info,
        #     file_name=f"{self.game_id}.json"
        # )
        return lineup_info

    def update_player_data(self) -> List[Dict[str, Union[str, int]]]:
        """
        Update player data for players that don't exist in the database.
        NOTE: if a new player is found, the player_mappings dictionary is updated.
        """
        all_player_data = []
        player_info = self._extract_player_info()
        for player_id, (player_name, jersey_number, ws_team_id) in player_info.items():
            first_name, last_name = self._format_names(player_name)
            if player_id in self.player_mappings:
                opta_id = self.player_mappings[player_id]
            else:
                # new player found. create opta id and update mapping
                opta_id = self._create_opta_id()
                self.player_mappings[player_id] = opta_id

            # updating player data each time to keep up to date.
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
            all_player_data.append(player_data)
            self.scraper_repo.insert_player_data(
                session=get_session(),
                **player_data
            )

        self.logger.info(f"Updated {len(all_player_data)} players")
        # TODO: instead of saving locally, save to db. Keeping this for now though since we need proper mappings
        self.file_repo.save("player", data=self.player_mappings)
        return all_player_data


    ###################################
    # Class specific helper functions #
    ###################################

    def _extract_player_info(self) -> Dict[str, Tuple[str, str, str]]:
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

    def _convert_to_projection(
        self, 
        event: dict
    ) -> dict:
        """
        Creating a dictionary entry that can be converted to a MatchProjectionModel.
        Need to map the WhoScored ids to an Opta ids.
        """
        def _transform_qualifiers(qualifiers: dict) -> List[dict]:
            """
            Transform WhoScored qualifiers to Opta format.
            """
            transformed = []
            for qualifier in qualifiers:
                transformed.append({
                    'qualifierId': qualifier['type']['value'],
                    'value': qualifier.get('value', '')
                })
            return transformed
        
        # NOTE: some events don't have a teamId or playerId
        ws_team_id = event.get('teamId', None)
        ws_player_id = event.get('playerId', None)

        projection = {
            'match_id': self.ws_to_opta_mapping[self.game_id],
            'player_id': self.player_mappings[str(ws_player_id)] if ws_player_id else None,
            'contestant_id': self.team_mappings[str(ws_team_id)] if ws_team_id else None,
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
            'qualifiers': _transform_qualifiers(event.get('qualifiers', {})),
            # NOTE: use these to see if it was actually updated
            'time_stamp': datetime.now().isoformat(),
            'last_modified': datetime.now().isoformat()
        }
        return projection    
    
    def _create_opta_id(self) -> str:
        """
        Create a random Opta-like ID.
        """
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=random.randint(20, 25)))
    
    def _format_names(self, name: str) -> Tuple[str, str]:
        """
        Format name into first and last name.
        """
        name_parts = name.split(maxsplit=1)
        first_name = name_parts[0]
        last_name = name_parts[1] if len(name_parts) > 1 else ""
        return first_name, last_name
    
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
            required_fields = [self.PLAYER_NAME_DICTIONARY_KEYWORD, self.EVENTS_KEYWORD, self.HOME_KEYWORD, self.AWAY_KEYWORD]
            missing_fields = [field for field in required_fields if field not in data]
            if missing_fields:
                raise ValueError(f"Missing required fields: {missing_fields}")
            return data
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format: {e}")
    
    def _format_lineup_data(self) -> tuple[dict, dict]:
        """
        Convert both teams' formation data into the required lineup info format.
        Takes the first formation entry (initial lineup) for each team.
        
        Returns:
            tuple[dict, dict]: (home_lineup, away_lineup)
        """
        def _format_team(team_data: dict) -> dict:
            formation = team_data['formations'][0]
            
            player_ids = []
            for idx in range(len(formation['playerIds'])):
                if int(formation['formationSlots'][idx]) == 0:
                    pass
                else:
                    player_ids.append(formation['playerIds'][idx])
            opta_player_ids = [
                self.player_mappings.get(str(pid), str(pid))
                for pid in player_ids
            ]
            return {
                "data_type": "opta",
                "game_id": self.ws_to_opta_mapping[self.game_id],
                "lineup_info": {
                    "team_id": self.team_mappings[str(team_data['teamId'])],
                    "formation_id": formation['formationId'],
                    "formation_name": formation['formationName'],
                    "formation_slots": formation['formationSlots'],
                    "player_ids": opta_player_ids,
                    "formation_positions": [
                        {
                            "vertical": pos["vertical"],
                            "horizontal": pos["horizontal"]
                        }
                        for pos in formation['formationPositions']
                    ],
                    "captain_id": self.player_mappings[str(formation['captainPlayerId'])] 
                }
            }

        
        
        home_lineup = _format_team(self.json_data["home"])
        away_lineup = _format_team(self.json_data["away"])
        return home_lineup, away_lineup

