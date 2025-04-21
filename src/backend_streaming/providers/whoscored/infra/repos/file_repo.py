from pathlib import Path
import json
import logging
from typing import Dict, Optional
from backend_streaming.providers.whoscored.infra.config.config import PathConfig, TypeToPaths

# TODO: change this to a database!

class FileRepository:
    """
    File-based repository for storing and retrieving internal json files.
    NOTE: all mappings are included as json files for now
    """
    def __init__(
            self, 
            paths: PathConfig, 
            type_to_paths: TypeToPaths, 
            logger: Optional[logging.Logger] = None
        ):
        self.paths = paths
        self.type_to_paths = type_to_paths
        self.logger = logger or logging.getLogger(__name__)
        
    def _get_path(self, file_type: str, file_name: Optional[str] = None) -> Path:
        """Get the correct path based on file type"""
        if file_type == 'player':
            return self.type_to_paths.PLAYER
        elif file_type == 'team':
            return self.type_to_paths.TEAM
        elif file_type == 'match':
            return self.type_to_paths.MATCH
        elif file_type == "standard_team_name":
            return self.type_to_paths.STANDARD_TEAM_NAME
        elif file_type == "competition":
            return self.type_to_paths.COMPETITION
        elif file_type == "tournament":
            return self.type_to_paths.TOURNAMENT
        
        # NOTE: these directories require file name since they are game specific
        elif file_name:
            if file_type == 'raw_pagesources':
                return self.type_to_paths.RAW_PAGESOURCES / file_name
            elif file_type == 'lineups':
                return self.type_to_paths.LINEUPS / file_name
            elif file_type == 'parsed_page_sources':
                return self.type_to_paths.PARSED_PAGE_SOURCES / file_name
            elif file_type == "payloads":
                return self.type_to_paths.PAYLOADS / file_name
        else:
            raise ValueError(f"Unknown file type: {file_type}")

    def load(
        self, 
        file_type: str, 
        is_txt: bool = False, 
        file_name: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Load mappings based on mapping type
        """
        try:
            file_path = self._get_path(file_type, file_name)  
            if is_txt:
                return file_path.read_text()
            else:
                with open(file_path) as f:
                    return json.load(f)
                
        except FileNotFoundError as e:
            self.logger.warning(f"File not found: {e.filename}. Returning empty dict.")
            return {}
            
    def save(
        self, 
        file_type: str, 
        data: Dict[str, str], 
        file_name: Optional[str] = None
    ) -> None:
        """
        Save mappings based on mapping type
        """
        try:
            file_path = self._get_path(file_type, file_name)
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Failed to save mappings: {e}")
            raise

    @staticmethod
    def reverse(mapping: Dict[str, str]) -> Dict[str, str]:
        """
        Reverse the mapping
        """
        return {v: k for k, v in mapping.items()}