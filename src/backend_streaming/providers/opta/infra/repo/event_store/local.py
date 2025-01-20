
import os
import json
from typing import List, Dict
from uuid import UUID
from datetime import datetime

from backend_streaming.providers.opta.domain.events import (
    DomainEvent,
    GlobalEventAdded,
    EventTypeChanged,
    QualifiersChanged
)
from backend_streaming.providers.opta.domain.entities.sport_events import Qualifier
from backend_streaming.providers.opta.infra.repo.event_store.base import EventStore

class LocalFileEventStore(EventStore):
    """
    An in-memory event store that also persists to a local JSON file.
    Format on disk (domain_events.json):
    {
      "match-123": [
         {"event_type": "GlobalEventAdded", "domain_event_id": "...", "occurred_on": "...", "payload": {...}},
         {"event_type": "QualifiersChanged", "domain_event_id": "...", "occurred_on": "...", "payload": {...}}
      ],
      "match-XYZ": [ ... ]
    }
    """
    def __init__(self, filename: str = "domain_events.json"):
        self.filename = filename
        self._storage: Dict[str, List[dict]] = {}  # raw dict for all aggregates
        self._load_from_file()                     # on startup, load everything
        

    def load_events(self, aggregate_id: str) -> List[DomainEvent]:
        """
        Return a list of DomainEvent objects for this match_id, in chronological order.
        """
        raw_list = self._storage.get(aggregate_id, [])
        events = []
        for row in raw_list:
            event_type = row["event_type"]
            occurred_on = datetime.fromisoformat(row["occurred_on"])
            domain_event_id = row["domain_event_id"]
            
            payload = row["payload"]
            evt = self._deserialize_event(
                event_type, domain_event_id, aggregate_id, occurred_on, payload
            )
            events.append(evt)
        return events

    def save_events(self, aggregate_id: str, new_events: List[DomainEvent]) -> None:
        """
        Append new events in memory + persist entire dictionary to the JSON file.
        """
        if not new_events:
            return

        if aggregate_id not in self._storage:
            self._storage[aggregate_id] = []

        for evt in new_events:
            row = {
                "event_type": type(evt).__name__,  # e.g. "GlobalEventAdded"
                "domain_event_id": evt.domain_event_id,
                "occurred_on": evt.occurred_on.isoformat(),
                "payload": self._serialize_event(evt)
            }
            self._storage[aggregate_id].append(row)

        self._save_to_file()
        
    def delete_events(self, aggregate_id: str) -> None:
        if aggregate_id in self._storage:
            del self._storage[aggregate_id]
        self._save_to_file()

    # ------------- Internal JSON handling -------------
    def _load_from_file(self):
        if not os.path.exists(self.filename):
            self._storage = {}
            return
        with open(self.filename, "r", encoding="utf-8") as f:
            self._storage = json.load(f)

    def _save_to_file(self):
        with open(self.filename, "w", encoding="utf-8") as f:
            json.dump(self._storage, f, indent=2)

    # ------------- Serialization / Deserialization ----
    def _serialize_event(self, evt: DomainEvent) -> dict:
        """Convert a domain event object to a dict for JSON storage."""
        if isinstance(evt, GlobalEventAdded):
            return {
                "feed_event_id": evt.feed_event_id,
                "local_event_id": evt.local_event_id,
                "type_id": evt.type_id,
                "period_id": evt.period_id,
                "time_min": evt.time_min,
                "time_sec": evt.time_sec,
                "contestant_id": evt.contestant_id,
                "player_id": evt.player_id,
                "player_name": evt.player_name,
                "outcome": evt.outcome,
                "x": evt.x,
                "y": evt.y,
                # We store qualifiers as {qid: val}
                "qualifiers": evt.qualifiers,
                "time_stamp": evt.time_stamp,
                "last_modified": evt.last_modified
            }
        elif isinstance(evt, EventTypeChanged):
            return {
                "feed_event_id": evt.feed_event_id,
                "old_type_id": evt.old_type_id,
                "new_type_id": evt.new_type_id
            }
        elif isinstance(evt, QualifiersChanged):
            return {
                "feed_event_id": evt.feed_event_id,
                "new_qualifiers": evt.new_qualifiers
            }
        else:
            # If you add more event types, handle them or do a general fallback
            return evt.__dict__

    def _deserialize_event(self,
                           event_type: str,
                           domain_event_id: str,
                           aggregate_id: str,
                           occurred_on: datetime,
                           payload: dict) -> DomainEvent:
        """Convert the JSON row back into the correct DomainEvent object."""
        if event_type == "GlobalEventAdded":
            # Rebuild the qualifiers as a dict of {qid: Qualifier(...)}

            return GlobalEventAdded(
                domain_event_id=domain_event_id,
                aggregate_id=aggregate_id,
                occurred_on=occurred_on,

                feed_event_id=payload["feed_event_id"],
                local_event_id=payload["local_event_id"],
                type_id=payload["type_id"],
                period_id=payload["period_id"],
                time_min=payload["time_min"],
                time_sec=payload["time_sec"],
                contestant_id=payload["contestant_id"],
                player_id=payload["player_id"],
                player_name=payload["player_name"],
                outcome=payload["outcome"],
                x=payload["x"],
                y=payload["y"],
                qualifiers=payload["qualifiers"],
                time_stamp=payload["time_stamp"],
                last_modified=payload["last_modified"]
            )
        elif event_type == "EventTypeChanged":
            return EventTypeChanged(
                domain_event_id=domain_event_id,
                aggregate_id=aggregate_id,
                occurred_on=occurred_on,
                feed_event_id=payload["feed_event_id"],
                old_type_id=payload["old_type_id"],
                new_type_id=payload["new_type_id"]
            )
        elif event_type == "QualifiersChanged":
            return QualifiersChanged(
                domain_event_id=domain_event_id,
                aggregate_id=aggregate_id,
                occurred_on=occurred_on,
                feed_event_id=payload["feed_event_id"],
                new_qualifiers=payload["new_qualifiers"]
            )
        else:
            raise ValueError(f"Unknown event type: {event_type}")
