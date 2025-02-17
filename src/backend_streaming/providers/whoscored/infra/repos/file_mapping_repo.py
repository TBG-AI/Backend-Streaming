from pathlib import Path
import json
import logging
from typing import Dict
from backend_streaming.providers.whoscored.domain.mappings import MappingRepository
from datetime import datetime
from sqlalchemy.orm import Session
from backend_streaming.providers.opta.infra.models import PlayerModel, TeamModel
from backend_streaming.providers.opta.infra.db import get_session

# TODO: change this to a database!

class FileMappingRepository(MappingRepository):
    """File-based implementation of WhoScored mapping storage"""
    def __init__(self, mappings_dir: Path):
        self.mappings_dir = mappings_dir
        self.logger = logging.getLogger(__name__)
        
    def load(self, mapping_type: str) -> Dict[str, str]:
        try:
            mapping_file = self.mappings_dir / f"{mapping_type}_ids.json"
            self.logger.debug(f"Loading mappings from: {mapping_file}")
            
            with open(mapping_file) as f:
                return json.load(f)
                
        except FileNotFoundError as e:
            self.logger.warning(
                f"Mapping file not found: {e.filename}. Creating new."
            )
            return {}
            
    def save(self, mapping_type: str, data: Dict[str, str]) -> None:
        try:
            self.mappings_dir.mkdir(parents=True, exist_ok=True)
            mapping_file = self.mappings_dir / f"{mapping_type}_ids.json"
            
            self.logger.debug(f"Saving mappings to: {mapping_file}")
            with open(mapping_file, 'w') as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Failed to save mappings: {e}")
            raise

    def insert_team_id(self, team_id: str) -> None:
        """adding new team to db"""
        pass

    def insert_player_id(self, player_id: str, team_id: str) -> None:
        """
        insert new player to db
        """      
        session = get_session()
        try:
            # Create and add all player models
            session.add(
                PlayerModel(
                    player_id=player_id,
                    first_name="PLACEHOLDER",
                    last_name=f"PLACEHOLDER",
                    short_first_name="PLACEHOLDER",
                    short_last_name="PLACEHOLDER",
                    gender="PLACEHOLDER",
                    match_name=f"PLACEHOLDER",
                    nationality="PLACEHOLDER",
                    nationality_id="PLACEHOLDER",
                    position="PLACEHOLDER",
                    type="PLACEHOLDER",
                    date_of_birth="PLACEHOLDER",
                    place_of_birth="PLACEHOLDER",
                    country_of_birth="PLACEHOLDER",
                    country_of_birth_id="PLACEHOLDER",
                    height=0,
                    weight=0,
                    foot="PLACEHOLDER",
                    shirt_number=0,
                    status="active",
                    active="true",
                    team_id=team_id,
                    team_name="PLACEHOLDER",
                    last_updated=datetime.utcnow().isoformat()
                )
            )
            session.commit()
            
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()
