from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Dict, Optional
from backend_streaming.providers.whoscored.infra.config.config import mappings

class MappingRepository(ABC):
    """Abstract base class for WhoScored-to-Opta ID mapping storage"""
    @abstractmethod
    def load(self, mapping_type: str) -> Dict[str, str]:
        """Load mappings from storage"""
        pass
    
    @abstractmethod
    def save(self, mapping_type: str, data: Dict[str, str]) -> None:
        """Save mappings to storage"""
        pass

    @abstractmethod
    def insert_player_id(self, player_id: str, team_id: str) -> None:
        """adding new player to db"""
        pass

    @abstractmethod
    def insert_team_id(self, team_id: str) -> None:
        """adding new team to db"""
        pass
    

@dataclass
class WhoScoredToOptaMappings:
    """
    Manages ID mappings between WhoScored and Opta formats.
    Used for converting scraped WhoScored data to Opta format.
    """
    # NOTE: these should be the same as the json file names in the mappings directory
    player_ids: Dict[str, str]
    team_ids: Dict[str, str]
    ws_to_opta_match_ids: Dict[str, str]
    ws_match_ids: Dict[str, str]
    repository: MappingRepository
    
    @classmethod
    def create(cls, repository: MappingRepository) -> 'WhoScoredToOptaMappings':
        """Factory method to create mappings instance"""
        return cls(
            player_ids=repository.load(mappings.PLAYER),
            team_ids=repository.load(mappings.TEAM),
            ws_to_opta_match_ids=repository.load(mappings.MATCH),
            ws_match_ids=repository.load(mappings.MATCH_NAMES),
            repository=repository
        )

    def get_mapping(self, mapping_type: str, ws_id: str) -> Optional[str]:
        """
        Get existing Opta ID for WhoScored ID.
        If no mapping is found, flag it. We will update the mappings table later.
        """
        if not ws_id:
            return None

        if isinstance(ws_id, int):
            ws_id = str(ws_id)

        mappings = getattr(self, f"{mapping_type}_ids")
        if ws_id not in mappings:
            return None
            # print(f"No mapping found for {ws_id} in {mapping_type}")
            # raise ValueError(f"No mapping found for {ws_id} in {mapping_type}")
        return mappings[ws_id]
    
    def insert_player_id(self, player_id: str) -> None:
        """
        Insert new player ID and create placeholder entry in database.
        """
        self.repository.insert_player_id(player_id, team_id)
        
        
