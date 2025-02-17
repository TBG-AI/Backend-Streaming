from pathlib import Path
from datetime import timedelta

# Get project root using a marker file (e.g., pyproject.toml or .git)
def find_project_root() -> Path:
    """Find the project root by looking for marker files"""
    current = Path(__file__).resolve()
    for parent in [current, *current.parents]:
        if (parent / "pyproject.toml").exists() or (parent / ".git").exists():
            return parent
    raise FileNotFoundError("Could not find project root")

PROJECT_ROOT = find_project_root()
MAPPINGS_DIR = PROJECT_ROOT / "src" / "backend_streaming" / "providers" / "mappings"

PLAYER_MAPPING_TYPE = 'player'
TEAM_MAPPING_TYPE = 'team'
MATCH_MAPPING_TYPE = 'ws_to_opta_match'

MAX_DURATION = timedelta(hours=3)

# Add these to your existing config
DEFAULT_TEAM_VALUES = {
    'name': 'PLACEHOLDER_TEAM',
    'short_name': 'PLACEHOLDER',
    'official_name': 'PLACEHOLDER_TEAM',
    'code': 'PLC',
    'type': 'placeholder',
    'team_type': 'placeholder',
    'country_id': 'placeholder',
    'country': 'placeholder',
    'status': 'active',
    'city': 'placeholder',
    'postal_address': 'placeholder',
    'address_zip': 'placeholder',
    'founded': 'placeholder',
    'last_updated': None
}

DEFAULT_PLAYER_VALUES = {
    'first_name': 'PLACEHOLDER',
    'last_name': 'PLAYER',
    'short_first_name': 'PLC',
    'short_last_name': 'PLR',
    'gender': 'M',
    'match_name': 'PLACEHOLDER_PLAYER',
    'nationality': 'placeholder',
    'nationality_id': 'placeholder',
    'position': 'placeholder',
    'type': 'placeholder',
    'date_of_birth': 'placeholder',
    'place_of_birth': 'placeholder',
    'country_of_birth': 'placeholder',
    'country_of_birth_id': 'placeholder',
    'height': 0,
    'weight': 0,
    'foot': 'placeholder',
    'shirt_number': 0,
    'status': 'active',
    'active': 'true',
    'team_name': 'PLACEHOLDER_TEAM',
    'last_updated': None
}