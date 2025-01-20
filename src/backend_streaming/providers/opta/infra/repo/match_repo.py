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
        self.event_store.save_events(agg.match_id, new_events)
        agg.clear_uncommitted_events()

