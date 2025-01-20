from typing import List
from backend_streaming.providers.opta.domain.events import DomainEvent

class EventStore:
    """Interface/base class for an event store."""
    def load_events(self, aggregate_id: str) -> List[DomainEvent]:
        raise NotImplementedError
    
    def save_events(self, aggregate_id: str, new_events: List[DomainEvent]) -> None:
        raise NotImplementedError