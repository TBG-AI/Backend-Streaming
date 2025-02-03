from typing import List
from datetime import datetime
from sqlalchemy.orm import Session

from backend_streaming.providers.opta.domain.events import (
    DomainEvent,
    GlobalEventAdded,
    EventEdited
)
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
        Load all deserialized domain events for a given aggregate ID.
        NOTE: all attributes accessible with .__dict__ to view all fields. Otherwise, you only see the __repr__ fields
        """
        session: Session = self.session_factory()
        try:
            rows = (
                session.query(DomainEventModel)
                .filter_by(aggregate_id=aggregate_id)
                .order_by(DomainEventModel.occurred_on.asc())
                .all()
            )
            return self._bulk_deserialize_events(rows)
        finally:
            session.close()

    def save_events(self, aggregate_id: str, new_events: List[DomainEvent]) -> None:
        if not new_events:
            return

        session: Session = self.session_factory()
        try:
            for evt in new_events:
                row = DomainEventModel(
                    domain_event_id=evt.domain_event_id,
                    aggregate_id=aggregate_id,
                    occurred_on=evt.occurred_on,
                    # Choosing to save event specific information separately in the payload. 
                    # This makes it easier to query for specific event types.
                    event_type=type(evt).__name__, 
                    payload=self._serialize_event(evt)
                )
                session.add(row)
            session.commit()
        finally:
            session.close()

    def delete_events(self, aggregate_id: str) -> None:
        session: Session = self.session_factory()
        try:
            session.query(DomainEventModel).filter_by(aggregate_id=aggregate_id).delete()
            session.commit()
        finally:
            session.close()

    # -------------- Internal serialization/deserialization --------------

    def _bulk_deserialize_events(self, rows: List[DomainEventModel]) -> List[DomainEvent]:
        """Deserialize a list of DomainEventModel objects into a list of DomainEvent objects."""
        return [
            self._deserialize_event(
                event_type=row.event_type,
                domain_event_id=row.domain_event_id,
                aggregate_id=row.aggregate_id,
                occurred_on=row.occurred_on,
                payload=row.payload
            ) for row in rows
        ]

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
                "qualifiers": evt.qualifiers,
                "time_stamp": evt.time_stamp,
                "last_modified": evt.last_modified
            }
        elif isinstance(evt, EventEdited):
            return {
                "feed_event_id": evt.feed_event_id,
                "changed_fields": evt.changed_fields,
                "old_fields": evt.old_fields
            }
        else:
            return evt.__dict__

    def _deserialize_event(
        self,
        event_type: str,
        domain_event_id: str,
        aggregate_id: str,
        occurred_on: datetime,
        payload: dict
    ) -> DomainEvent:
        """Convert the JSON row back into the correct DomainEvent object."""
        if event_type == "GlobalEventAdded":
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
        elif event_type == "EventEdited":
            return EventEdited(
                domain_event_id=domain_event_id,
                aggregate_id=aggregate_id,
                occurred_on=occurred_on,
                feed_event_id=payload["feed_event_id"],
                changed_fields=payload["changed_fields"],
                old_fields=payload["old_fields"]
            )
        else:
            raise ValueError(f"Unknown event type: {event_type}")
