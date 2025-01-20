# Directory: src/backend_streaming/providers/opta/domain/entities/sport_events.py
from dataclasses import dataclass, field
from typing import Dict, Optional, List

@dataclass
class Qualifier:
    qualifier_id: int
    value: Optional[str] = None

    def to_dict(self) -> Dict:
        """Convert Qualifier to dictionary format for JSON storage"""
        return {
            'qualifier_id': self.qualifier_id,
            'value': self.value
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'Qualifier':
        """Create Qualifier instance from dictionary data"""
        return cls(
            qualifier_id=int(data['qualifierId']),
            value=data.get('value')
        )

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
    qualifiers: List[Qualifier] = field(default_factory=list)
    time_stamp: Optional[str] = None       # "2024-12-30T20:07:18.992Z"
    last_modified: Optional[str] = None    # "2024-12-31T03:28:08Z"

    def to_dict(self) -> Dict:
        """Convert EventInMatch to dictionary format for JSON storage"""
        return {
            'feed_event_id': self.feed_event_id,
            'local_event_id': self.local_event_id,
            'type_id': self.type_id,
            'period_id': self.period_id,
            'time_min': self.time_min,
            'time_sec': self.time_sec,
            'contestant_id': self.contestant_id,
            'player_id': self.player_id,
            'player_name': self.player_name,
            'outcome': self.outcome,
            'x': self.x,
            'y': self.y,
            'qualifiers': [q.to_dict() for q in self.qualifiers],
            'time_stamp': self.time_stamp,
            'last_modified': self.last_modified
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'EventInMatch':
        """Create EventInMatch instance from dictionary data"""
        return cls(
            feed_event_id=data['id'],
            local_event_id=data['eventId'],
            type_id=data['typeId'],
            period_id=data['periodId'],
            time_min=data['timeMin'],
            time_sec=data['timeSec'],
            contestant_id=data.get('contestantId'),
            player_id=data.get('playerId'),
            player_name=data.get('playerName'),
            outcome=data.get('outcome'),
            x=data.get('x'),
            y=data.get('y'),
            qualifiers=cls.map_qualifiers_from_dict(data.get('qualifier')),
            time_stamp=data.get('timeStamp'),
            last_modified=data.get('lastModified')
        )

    @classmethod
    def map_qualifiers_from_dict(cls, data: Optional[List[Dict]]) -> List[Qualifier]:
        """Create qualifiers dictionary from JSON data"""
        if not data:
            return []

        qualifiers = []
        for qualifier_data in data:
            qualifier = Qualifier.from_dict(qualifier_data)
            qualifiers.append(qualifier)
        return qualifiers
