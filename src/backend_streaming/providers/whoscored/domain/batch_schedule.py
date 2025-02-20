from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
from typing import Optional
from dataclasses import dataclass

@dataclass
class BatchSchedule:
    """Simple tracker for WhoScored batch scheduling periods"""
    batch_start: datetime
    batch_end: datetime
    notified: bool = False  # Track if we've notified about this batch
    
    # Single file to track current batch
    # TODO (PRODUCTION): kind of hacky way to save files
    # NOTE: make sure this runs daily to check all batch files
    SCHEDULE_FILE = Path(__file__).parents[2] / "batch_schedule" / f"production_schedule.json"

    @classmethod
    def get_current(cls) -> Optional['BatchSchedule']:
        """Get current batch schedule - only method needed by monitor"""
        if not cls.SCHEDULE_FILE.exists():
            return None
            
        try:
            with open(cls.SCHEDULE_FILE) as f:
                history = json.load(f)
                
                # Handle empty history
                if not history:
                    return None
                
                # Find first unnotified batch
                unnotified_batch = next(
                    (batch for batch in history if not batch.get('notified', False)),
                    None
                )
                
                # If all batches are notified, get the most recent one
                batch = unnotified_batch or history[-1]
                
                # Create BatchSchedule from batch
                return cls(
                    batch_start=datetime.fromisoformat(batch.get('batch_start', '')),
                    batch_end=datetime.fromisoformat(batch['batch_end']),
                    notified=batch.get('notified', False)
                )
                
        except (json.JSONDecodeError, KeyError, FileNotFoundError, IndexError) as e:
            print(f"Error reading batch schedule: {e}")
            return None

    def save(self) -> None:
        """Append current batch end date to history"""
        self.SCHEDULE_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing history
        history = []
        if self.SCHEDULE_FILE.exists():
            with open(self.SCHEDULE_FILE, 'r') as f:
                try:
                    data = json.load(f)
                    # Convert old format to new format if needed
                    if isinstance(data, dict):
                        history = [data]
                    else:
                        history = data
                except json.JSONDecodeError:
                    history = []  # Start fresh if file is corrupted
        
        # Add new batch to history with notification status
        history.append({
            'batch_start': self.batch_start.isoformat(),
            'batch_end': self.batch_end.isoformat(),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'notified': self.notified
        })
        
        # Write updated history back to file
        with open(self.SCHEDULE_FILE, 'w') as f:
            json.dump(history, f, indent=2) 