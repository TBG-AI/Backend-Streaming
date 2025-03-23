from pathlib import Path
from datetime import timedelta
from dataclasses import dataclass

@dataclass(frozen=True)
class PathConfig:
    """Configuration for all project paths"""
    project_root: Path
    
    def ensure_directories_exist(self) -> None:
        """Create all required directories if they don't exist."""
        directories = [
            self.src_dir,
            self.providers_dir,
            self.whoscored_dir,
            self.mappings_dir,
            self.manual_scraping_dir,
            self.logs_dir,
            self.raw_pagesources_dir,
            self.lineups_dir,
            self.parsed_page_sources_dir,
            self.game_logs_dir,
            self.payloads_dir,
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
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
        return self.manual_scraping_dir / "raw_page_sources"
    
    @property
    def lineups_dir(self) -> Path:
        return self.manual_scraping_dir / "lineups"
    
    @property
    def parsed_page_sources_dir(self) -> Path:
        return self.manual_scraping_dir / "parsed_page_sources"
    
    @property
    def game_logs_dir(self) -> Path:
        return self.manual_scraping_dir / "logs"
    
    @property
    def payloads_dir(self) -> Path:
        return self.manual_scraping_dir / "payloads"
    
    @property
    def ws_to_opta_match_mapping_path(self) -> Path:
        return self.mappings_dir / "ws_to_opta_match_ids.json"
    
    @property
    def team_mapping_path(self) -> Path:
        return self.mappings_dir / "team_ids.json"
    
    @property
    def player_mapping_path(self) -> Path:
        return self.mappings_dir / "player_ids.json"
 

@dataclass(frozen=True)
class TypeToPaths:
    """Types of files and their paths"""
    paths: PathConfig

    @property
    def PLAYER(self) -> Path:
        return self.paths.player_mapping_path
    
    @property
    def TEAM(self) -> Path:
        return self.paths.team_mapping_path
    
    @property
    def MATCH(self) -> Path:
        return self.paths.ws_to_opta_match_mapping_path

    @property
    def RAW_PAGESOURCES(self) -> Path:
        return self.paths.raw_pagesources_dir
    
    @property
    def LINEUPS(self) -> Path:
        return self.paths.lineups_dir
    
    @property
    def PARSED_PAGE_SOURCES(self) -> Path:
        return self.paths.parsed_page_sources_dir
    
    @property
    def GAME_LOGS(self) -> Path:
        return self.paths.game_logs_dir
    
    @property
    def PAYLOADS(self) -> Path:
        return self.paths.payloads_dir

# @dataclass(frozen=True)
# class TimeConfig:
#     """Configuration for timing parameters"""
#     MAX_DURATION: timedelta = timedelta(hours=3)
#     POLL_INTERVAL: int = 60  # 1 minute

def find_project_root() -> Path:
    """Find the project root by looking for marker files"""
    current = Path(__file__).resolve()
    for parent in [current, *current.parents]:
        if (parent / "pyproject.toml").exists() or (parent / ".git").exists():
            return parent
    raise FileNotFoundError("Could not find project root")

# Create configuration instances
paths = PathConfig(project_root=find_project_root())
paths.ensure_directories_exist()
type_to_paths = TypeToPaths(paths=paths)
