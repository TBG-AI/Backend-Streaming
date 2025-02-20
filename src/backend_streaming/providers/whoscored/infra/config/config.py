from pathlib import Path
from datetime import timedelta
from dataclasses import dataclass

@dataclass(frozen=True)
class PathConfig:
    """Configuration for all project paths"""
    project_root: Path
    
    @property
    def src_dir(self) -> Path:
        return self.project_root / "src" / "backend_streaming"
    
    @property
    def providers_dir(self) -> Path:
        return self.src_dir / "providers"
    
    @property
    def whoscored_dir(self) -> Path:
        return self.providers_dir / "whoscored"
    
    @property
    def mappings_dir(self) -> Path:
        return self.providers_dir / "mappings"
    
    @property
    def manual_scraping_dir(self) -> Path:
        return self.whoscored_dir / "infra" / "data" / "manual"
    
    @property
    def logs_dir(self) -> Path:
        return self.whoscored_dir / "infra" / "logs"
    
    @property
    def raw_pagesources_dir(self) -> Path:
        return self.manual_scraping_dir / "raw"
    
    @property
    def game_sources_dir(self) -> Path:
        return self.manual_scraping_dir / "parsed"
    
    @property
    def game_logs_dir(self) -> Path:
        return self.manual_scraping_dir / "logs"
    
    @property
    def ws_to_opta_match_mapping_path(self) -> Path:
        return self.mappings_dir / "ws_to_opta_match_ids.json"

@dataclass(frozen=True)
class MappingConfig:
    """Configuration for mapping types"""
    PLAYER = 'player'
    TEAM = 'team'
    MATCH = 'ws_to_opta_match'
    MATCH_NAMES = 'ws_match'

@dataclass(frozen=True)
class TimeConfig:
    """Configuration for timing parameters"""
    MAX_DURATION: timedelta = timedelta(hours=3)
    POLL_INTERVAL: int = 60  # 1 minute

def find_project_root() -> Path:
    """Find the project root by looking for marker files"""
    current = Path(__file__).resolve()
    for parent in [current, *current.parents]:
        if (parent / "pyproject.toml").exists() or (parent / ".git").exists():
            return parent
    raise FileNotFoundError("Could not find project root")

# Create configuration instances
paths = PathConfig(project_root=find_project_root())
mappings = MappingConfig()
timing = TimeConfig()
