from dataclasses import dataclass, field
from typing import Dict, Optional, List

@dataclass
class Qualifier:
    qualifier_id: int
    value: Optional[str] = None

    def to_dict(self) -> Dict:
        """Convert Qualifier to a dict using 'qualifierId' to match feed JSON."""
        return {
            'qualifierId': self.qualifier_id,
            'value': self.value
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'Qualifier':
        """Create Qualifier from feed-like dict, expecting 'qualifierId' & 'value' keys."""
        return cls(
            qualifier_id=int(data['qualifierId']),  # the feed uses 'qualifierId'
            value=data.get('value')
        )

    @staticmethod
    def qualifiers_are_equal(old_quals: List['Qualifier'], new_quals: List['Qualifier']) -> bool:
        """
        Compare two lists of Qualifier objects for equality.
        """
        if len(old_quals) != len(new_quals):
            return False

        old_map = {q.qualifier_id: q.value for q in old_quals}
        new_map = {q.qualifier_id: q.value for q in new_quals}
        if not old_map == new_map:
            print(f"Qualifiers are not equal: {old_map} != {new_map}")
        return old_map == new_map


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
    # We store the qualifiers as a list of Qualifier objects
    qualifiers: List[Qualifier] = field(default_factory=list)
    time_stamp: Optional[str] = None       # e.g. "2024-12-30T20:07:18.992Z"
    last_modified: Optional[str] = None    # e.g. "2024-12-31T03:28:08Z"

    def to_dict(self) -> Dict:
        """
        Convert EventInMatch to dictionary format (mimicking feed structure).
        """
        return {
            'id': self.feed_event_id,
            'eventId': self.local_event_id,
            'typeId': self.type_id,
            'periodId': self.period_id,
            'timeMin': self.time_min,
            'timeSec': self.time_sec,
            'contestantId': self.contestant_id,
            'playerId': self.player_id,
            'playerName': self.player_name,
            'outcome': self.outcome,
            'x': self.x,
            'y': self.y,
            # Convert each Qualifier using its to_dict() => expecting 'qualifierId'
            'qualifier': self.map_qualifiers_to_dict(),
            'timeStamp': self.time_stamp,
            'lastModified': self.last_modified
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'EventInMatch':
        """
        Create EventInMatch instance from feed-like JSON dict.
        We expect keys like 'id', 'eventId', 'qualifier' (list of qualifiers).
        """
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
        """
        Convert the 'qualifier' list from the feed (each item has 'qualifierId' & 'value')
        into a list of Qualifier objects.
        """
        if not data:
            return []
        return [Qualifier.from_dict(qd) for qd in data]
    
    def map_qualifiers_to_dict(self) -> List[Dict]:
        """
        Convert the qualifiers list to a list of dicts (mimicking feed structure).
        """
        return [q.to_dict() for q in self.qualifiers]
