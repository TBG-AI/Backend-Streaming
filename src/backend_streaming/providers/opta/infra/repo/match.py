# Directory: src/backend_streaming/providers/opta/infra/repo/match_repo.py

from backend_streaming.providers.opta.domain.aggregates.match_aggregate import MatchAggregate
from typing import List
from backend_streaming.providers.opta.domain.events import DomainEvent

class MatchRepository:
    def __init__(self, event_store):
        self.event_store = event_store

    def load(self, match_id: str) -> MatchAggregate:
        agg = MatchAggregate(match_id)
        stored_events = self.event_store.load_events(match_id)
        for evt in stored_events:
            agg.apply(evt)
        return agg

    def save(self, agg: MatchAggregate):
        new_events = agg.get_uncommitted_events()
        if not new_events:
            return
        # simple insertion. no filtering. 
        self.event_store.save_events(agg.match_id, new_events)
        # NOTE: can't clear uncommitted events here, because we need to update the projections
        
    def delete(self, match_id: str):
        self.event_store.delete_events(match_id)

