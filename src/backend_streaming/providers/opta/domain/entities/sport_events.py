# Directory: src/backend_streaming/providers/opta/domain/entities/sport_events.py
from dataclasses import dataclass, field
from typing import Dict, Optional

@dataclass
class Qualifier:
    qualifier_id: int
    value: Optional[str] = None

@dataclass
class EventInMatch:
    feed_event_id: int       # The "global event ID" from your JSON (e.g. 2762916859)
    local_event_id: int      # The "eventId" from your JSON (e.g. 230)
    type_id: int
    period_id: int
    time_min: int
    time_sec: int
    contestant_id: str
    player_id: str
    player_name: str
    outcome: Optional[int] = None
    x: Optional[float] = None
    y: Optional[float] = None
    # We can keep qualifiers in a dict keyed by 'qualifierId' for easy lookup
    qualifiers: Dict[int, Qualifier] = field(default_factory=dict)
    time_stamp: Optional[str] = None       # "2024-12-30T20:07:18.992Z"
    last_modified: Optional[str] = None    # "2024-12-31T03:28:08Z"
