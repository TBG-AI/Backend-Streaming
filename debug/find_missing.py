import json
from pathlib import Path
from sqlalchemy.orm import Session
from typing import List
from backend_streaming.providers.opta.infra.models import PlayerModel
from backend_streaming.providers.opta.infra.db import get_session

def find_missing_players(player_ids: List[str]) -> List[str]:
    """
    Given a list of player IDs, return the IDs that are not present in the players table.
    
    :param player_ids: List of player IDs to check.
    :return: List of player IDs that are not found in the database.
    """
    missing_players = []
    session: Session = get_session()
    
    try:
        # Query the database for existing player IDs
        existing_ids = set(id_[0] for id_ in session.query(PlayerModel.player_id).filter(PlayerModel.player_id.in_(player_ids)).all())
        
        # Determine which player IDs are missing
        missing_players = [player_id for player_id in player_ids if player_id not in existing_ids]
        
    except Exception as e:
        print(f"An error occurred while querying the database: {e}")
    finally:
        session.close()
    
    return missing_players

def load_player_ids_from_json(file_path: str) -> List[str]:
    """
    Load player IDs from a JSON file, extracting the values.
    
    :param file_path: Path to the JSON file containing player IDs.
    :return: List of player IDs.
    """
    with open(file_path, 'r') as file:
        data = json.load(file)
    return list(data.values())

# Load player IDs from JSON file
json_file_path = Path("src/backend_streaming/providers/mappings/player_ids.json")
player_ids_to_check = load_player_ids_from_json(json_file_path)

# Find missing players
missing_players = find_missing_players(player_ids_to_check)
print(f"Missing players: {missing_players}")