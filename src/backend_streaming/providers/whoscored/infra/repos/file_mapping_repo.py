from pathlib import Path
import json
import logging
from typing import Dict
from backend_streaming.providers.whoscored.domain.mappings import MappingRepository

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