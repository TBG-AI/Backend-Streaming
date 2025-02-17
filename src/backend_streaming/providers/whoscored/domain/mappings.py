from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional
from abc import ABC, abstractmethod
import uuid
from backend_streaming.providers.whoscored.infra.config import PLAYER_MAPPING_TYPE, TEAM_MAPPING_TYPE, MATCH_MAPPING_TYPE

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

@dataclass
class WhoScoredToOptaMappings:
    """
    Manages ID mappings between WhoScored and Opta formats.
    Used for converting scraped WhoScored data to Opta format.
    """
    player_ids: Dict[str, str]
    team_ids: Dict[str, str]
    ws_to_opta_match_ids: Dict[str, str]
    repository: MappingRepository
    
    @classmethod
    def create(cls, repository: MappingRepository) -> 'WhoScoredToOptaMappings':
        """Factory method to create mappings instance"""
        return cls(
            player_ids=repository.load(PLAYER_MAPPING_TYPE),
            team_ids=repository.load(TEAM_MAPPING_TYPE),
            ws_to_opta_match_ids=repository.load(MATCH_MAPPING_TYPE),
            repository=repository
        )

    def get_or_create_mapping(self, mapping_type: str, ws_id: str) -> Optional[str]:
        """
        Get existing Opta ID or create new one for WhoScored ID.
        
        Args:
            mapping_type: Type of ID ('player', 'team', or 'match')
            ws_id: WhoScored ID to map
            
        Returns:
            Optional[str]: Corresponding Opta ID
        """
        if not ws_id:
            return None
            
        mappings = getattr(self, f"{mapping_type}_ids")
        
        if ws_id not in mappings:
            import ipdb; ipdb.set_trace()
            new_id = str(uuid.uuid4())
            mappings[ws_id] = new_id
            self.repository.save(mapping_type, mappings)
            return new_id
            
        return mappings[ws_id]
