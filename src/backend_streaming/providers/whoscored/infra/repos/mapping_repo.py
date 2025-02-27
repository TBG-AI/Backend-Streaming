from pathlib import Path
import json
import logging
from typing import Dict
from backend_streaming.providers.whoscored.domain.mappings import MappingRepository
from datetime import datetime
from sqlalchemy.orm import Session
from backend_streaming.providers.opta.infra.models import PlayerModel, TeamModel
from backend_streaming.providers.opta.infra.db import get_session
from backend_streaming.providers.whoscored.infra.config.config import MappingPaths, MappingTypes

# TODO: change this to a database!

class MappingRepository:
    """
    File-based repository for storing and retrieving mappings
    """
    def __init__(
            self, 
            paths: MappingPaths, 
            mapping_types: MappingTypes, 
            logger: logging.Logger
        ):
        self.paths = paths
        self.mapping_types = mapping_types
        self.logger = logger
        
    def _get_path(self, mapping_type: str) -> Path:
        """Get the correct path based on mapping type"""
        if mapping_type == self.mapping_types.PLAYER:
            return self.paths.players
        elif mapping_type == self.mapping_types.TEAM:
            return self.paths.teams
        elif mapping_type == self.mapping_types.MATCH:
            return self.paths.matches
        else:
            raise ValueError(f"Unknown mapping type: {mapping_type}")

    def load(self, mapping_type: str) -> Dict[str, str]:
        """
        Load mappings based on mapping type
        """
        try:
            mapping_file = self._get_path(mapping_type)  
            with open(mapping_file) as f:
                return json.load(f)
                
        except FileNotFoundError as e:
            self.logger.warning(f"Mapping file not found: {e.filename}. Creating new.")
            return {}
            
    def save(self, mapping_type: str, data: Dict[str, str]) -> None:
        """
        Save mappings based on mapping type
        """
        try:
            mapping_file = self._get_path(mapping_type)
            with open(mapping_file, 'w') as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Failed to save mappings: {e}")
            raise

    def insert_team_id(self, team_id: str) -> None:
        """
        This method is never called since teams don't change for a given season.
        Just including for completeness sake.
        """
        pass

    def insert_player_data(self, session: Session, **player_data) -> None:
        """
        insert new player to db with name information
        """      
        self.logger.info(f"Inserting new player information: {player_data['player_id']} - {player_data['match_name']}")
        try:
            # Set defaults for optional fields
            default_data = {
                'gender': 'M',
                'nationality': 'PLACEHOLDER',
                'nationality_id': 'PLACEHOLDER',
                'position': 'PLACEHOLDER',
                'type': 'PLACEHOLDER',
                'date_of_birth': 'PLACEHOLDER',
                'place_of_birth': 'PLACEHOLDER',
                'country_of_birth': 'PLACEHOLDER',
                'country_of_birth_id': 'PLACEHOLDER',
                'height': 0,
                'weight': 0,
                'foot': 'PLACEHOLDER',
                'status': 'active',
                'active': 'true',
                'team_name': 'PLACEHOLDER',
                'last_updated': datetime.utcnow().isoformat()
            }
            # Merge provided data with defaults
            player_data = {**default_data, **player_data}
            session.add(PlayerModel(**player_data))
            session.commit()
            
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()
