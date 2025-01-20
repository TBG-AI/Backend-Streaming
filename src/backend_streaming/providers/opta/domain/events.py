# Directory: src/backend_streaming/providers/opta/domain/events.py
import datetime
from dataclasses import dataclass, field
from typing import Optional, Dict
from backend_streaming.providers.opta.domain.entities.sport_events import Qualifier

@dataclass(frozen=True)
class DomainEvent:
    domain_event_id: str     # For event sourcing (UUID)
    aggregate_id: str        # match_id
    occurred_on: datetime.datetime
    # possibly more

@dataclass(frozen=True)
class GlobalEventAdded(DomainEvent):
    feed_event_id: int       # "id" from the feed
    local_event_id: int      # "eventId" from the feed
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
    qualifiers: Dict[int, Qualifier] = field(default_factory=dict)
    time_stamp: Optional[str] = None       # "2024-12-30T20:07:18.992Z"
    last_modified: Optional[str] = None    # "2024-12-31T03:28:08Z"

@dataclass(frozen=True)
class EventTypeChanged(DomainEvent):
    feed_event_id: int
    old_type_id: int
    new_type_id: int

@dataclass(frozen=True)
class QualifiersChanged(DomainEvent):
    feed_event_id: int
    new_qualifiers: Dict[int, Optional[str]]
    """
    new_qualifiers is a dict {qualifierId: value}, 
    representing the entire updated set of qualifiers for this event.
    """

