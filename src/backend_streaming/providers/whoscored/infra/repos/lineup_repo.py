import json
from typing import Dict, Any

from backend_streaming.providers.whoscored.infra.config.config import paths

class LineupRepository:
    def __init__(self, game_id: int):
        self.game_id = game_id
        self.lineup_path = paths.lineups_dir / f"{self.game_id}.json"

    def get_lineup(self) -> Dict[str, Any]:
        """
        Get both home and away lineups for a game. Currently, just a simple load from internal json.
        Returns:
            Dict containing 'home' and 'away' lineup data
        """
        try:
            with open(self.lineup_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"No lineup data found for game {self.game_id}")
