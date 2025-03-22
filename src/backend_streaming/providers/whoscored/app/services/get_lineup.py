import json

from typing import Dict, Any
from backend_streaming.providers.whoscored.infra.config.config import paths
from backend_streaming.providers.whoscored.infra.repos.lineup_repo import LineupRepository

class GetLineupService:
    def __init__(self, game_id: str):
        self.ws_to_opta_match_mapping = json.load(open(paths.ws_to_opta_match_mapping_path, 'r'))
        self.opta_to_ws_match_mapping = {v: k for k, v in self.ws_to_opta_match_mapping.items()}
        self.team_mapping = json.load(open(paths.team_mapping_path, 'r'))
        self.player_mapping = json.load(open(paths.player_mapping_path, 'r'))

        self.ws_game_id = self.opta_to_ws_match_mapping[game_id]
        self.opta_game_id = game_id
        self.lineup_repo = LineupRepository(self.ws_game_id)

    def get_lineup(self, convert_to_opta: bool = False) -> Dict[str, Dict[str, Any]]:
        """
        Get formatted lineup data for both teams.
        
        Returns:
            Dict containing home and away lineup information
        """
        try:
            lineups = self.lineup_repo.get_lineup()
            if convert_to_opta:
                lineups = self._convert_to_opta(lineups)
            return lineups
        except FileNotFoundError as e:
            # You might want to log this error
            return {
                "error": str(e),
                "game_id": self.ws_game_id
            }
        
    def _convert_to_opta(self, lineup_data: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:        
        # Convert IDs for both home and away teams
        for team_type in ['home', 'away']:
            team_data = lineup_data[team_type]
            if team_data['data_type'] == 'whoscored':
                # Convert game ID (we already have this mapping from __init__)
                team_data['game_id'] = self.ws_to_opta_match_mapping.get(
                    str(team_data['game_id']), team_data['game_id']
                )
                
                # Convert team ID
                team_data['lineup_info']['team_id'] = self.team_mapping.get(
                    str(team_data['lineup_info']['team_id']),
                    team_data['lineup_info']['team_id']
                )
                
                # Convert player IDs
                player_ids = team_data['lineup_info']['player_ids']
                team_data['lineup_info']['player_ids'] = [
                    self.player_mapping.get(str(pid), pid)
                    for pid in player_ids
                ]
                
                # Convert captain ID if present
                if team_data['lineup_info'].get('captain_id'):
                    team_data['lineup_info']['captain_id'] = self.player_mapping.get(
                        str(team_data['lineup_info']['captain_id']),
                        team_data['lineup_info']['captain_id']
                    )
                
                # Change data_type to indicate conversion is done
                team_data['data_type'] = 'opta'
        
        return lineup_data
        