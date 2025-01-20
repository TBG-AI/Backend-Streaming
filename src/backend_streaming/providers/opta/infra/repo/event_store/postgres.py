# src/backend_streaming/providers/opta/infra/repo/event_store_repo.py

from typing import List
from datetime import datetime
from sqlalchemy.orm import Session

from backend_streaming.providers.opta.domain.entities.sport_events import Qualifier
from backend_streaming.providers.opta.domain.events import DomainEvent, GlobalEventAdded, EventTypeChanged, QualifiersChanged
from backend_streaming.providers.opta.infra.models import DomainEventModel
from backend_streaming.providers.opta.infra.repo.event_store.base import EventStore


class PostgresEventStore(EventStore):
    def __init__(self, session_factory: callable):
        """
        session_factory should be something like:
            session_factory = sessionmaker(bind=engine)
        so we can create new Sessions on demand.
        """
        self.session_factory = session_factory

    def load_events(self, aggregate_id: str) -> List[DomainEvent]:
        """
        Load all events for a given aggregate, ordered by occurred_on ascending.
        """
        session: Session = self.session_factory()
        try:
            rows = session.query(DomainEventModel) \
                          .filter_by(aggregate_id=aggregate_id) \
                          .order_by(DomainEventModel.occurred_on.asc()) \
                          .all()
            events = []
            for row in rows:
                event = self._deserialize_event(
                    event_type=row.event_type,
                    domain_event_id=row.domain_event_id,
                    aggregate_id=row.aggregate_id,
                    occurred_on=row.occurred_on,
                    payload=row.payload
                )
                events.append(event)
            return events
        finally:
            session.close()

    def save_events(self, aggregate_id: str, new_events: List[DomainEvent]) -> None:
        """
        Insert new domain events into the 'domain_events' table (append-only).
        """
        if not new_events:
            return
        session: Session = self.session_factory()
        try:
            for evt in new_events:
                row = DomainEventModel(
                    domain_event_id=evt.domain_event_id,
                    aggregate_id=aggregate_id,
                    event_type=type(evt).__name__,  # e.g. 'GlobalEventAdded'
                    occurred_on=evt.occurred_on,
                    payload=self._serialize_event(evt)
                )
                session.add(row)
            session.commit()
        finally:
            session.close()

    # -------------- Internal serialization/deserialization --------------

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
                "qualifiers": {q_id: q.value for (q_id, q) in evt.qualifiers.items()},
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
            qual_dict = {
                int(qid): Qualifier(int(qid), val)
                for qid, val in payload["qualifiers"].items()
            }
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
                qualifiers=qual_dict,
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
