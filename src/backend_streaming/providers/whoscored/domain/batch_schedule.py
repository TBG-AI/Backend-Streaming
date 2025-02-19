from datetime import datetime, timedelta
from pathlib import Path
import json
from typing import Optional
from dataclasses import dataclass

@dataclass
class BatchSchedule:
    """Simple tracker for WhoScored batch scheduling periods"""
    batch_end: datetime
    
    # Single file to track current batch
    # TODO (PRODUCTION): kind of hacky way to save files
    SCHEDULE_FILE = Path(__file__).parents[2] / "batch_schedule" / "current_batch.json"

    @classmethod
    def get_current(cls) -> Optional['BatchSchedule']:
        """Get current batch schedule - only method needed by monitor"""
        if not cls.SCHEDULE_FILE.exists():
            return None
            
        try:
            with open(cls.SCHEDULE_FILE) as f:
                data = json.load(f)
                return cls(batch_end=datetime.fromisoformat(data['batch_end']))
        except (json.JSONDecodeError, KeyError, FileNotFoundError):
            return None

    def save(self) -> None:
        """Save current batch end date"""
        self.SCHEDULE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(self.SCHEDULE_FILE, 'w') as f:
            json.dump({'batch_end': self.batch_end.isoformat()}, f) 