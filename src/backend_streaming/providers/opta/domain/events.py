from dataclasses import dataclass, field
from typing import Optional, Dict
import datetime

@dataclass(frozen=True)
class DomainEvent:
    domain_event_id: str
    aggregate_id: str   # match_id
    # NOTE: this name is slightly misleading... 
    # It's not when the event occurred, but when the DomainEvent was created.
    occurred_on: datetime.datetime

@dataclass(frozen=True)
class GlobalEventAdded(DomainEvent):
    """
    Fired when a brand-new event is discovered in the feed.
    """
    feed_event_id: int
    local_event_id: int
    type_id: int
    period_id: int
    time_min: int
    time_sec: int
    contestant_id: Optional[str] = None
    player_id: Optional[str] = None
    player_name: Optional[str] = None
    outcome: Optional[int] = None
    x: Optional[float] = None
    y: Optional[float] = None
    qualifiers: Dict[int, str] = field(default_factory=dict)
    time_stamp: Optional[str] = None
    last_modified: Optional[str] = None

    def __repr__(self) -> str:
        time = f"{self.time_min}:{self.time_sec:02d}"
        player = f", player={self.player_name}" if self.player_name else ""
        pos = f", pos=({self.x},{self.y})" if self.x is not None and self.y is not None else ""
        return f"GlobalEventAdded(id={self.feed_event_id}, type={self.type_id}, time={time}{player}{pos})"

@dataclass(frozen=True)
class EventEdited(DomainEvent):
    """
    Fired when an existing event is modified in any way
    (typeId, x/y, outcome, qualifiers, etc.).
    
    'changed_fields' is a dict where the key is the field name (e.g. "type_id"),
    and the value is the *new* value. 'old_fields' optionally stores the old values,
    if you want to track them for analytics.
    """
    feed_event_id: int
    changed_fields: Dict[str, any] = field(default_factory=dict)
    old_fields: Dict[str, any] = field(default_factory=dict)

    def __repr__(self) -> str:
        changes = ', '.join(f"{k}={v}" for k, v in self.changed_fields.items())
        return f"EventEdited(event={self.feed_event_id}, changes=[{changes}])"

