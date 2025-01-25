from typing import Dict, Any, Union
from uuid import uuid4
from datetime import datetime

from backend_streaming.providers.opta.domain.events import (
    DomainEvent,
    GlobalEventAdded,
    EventEdited
)
from backend_streaming.providers.opta.domain.entities.sport_events import EventInMatch, Qualifier

class MatchAggregate:
    def __init__(self, match_id: str):
        self.match_id = match_id

        # Key = feed_event_id, Value = EventInMatch
        self.events: Dict[int, EventInMatch] = {}
        
        # Domain events that haven't been persisted yet
        self._uncommitted_events: list[DomainEvent] = []

    # ------------------------------------------------------------------
    # APPLY methods: these mutate the aggregator's in-memory state
    # ------------------------------------------------------------------

    def apply(self, evt: DomainEvent):
        """
        Base "apply" method that routes to the correct specific handler.
        """
        if isinstance(evt, GlobalEventAdded):
            self._apply_global_event_added(evt)
        elif isinstance(evt, EventEdited):
            self._apply_event_edited(evt)
        # else: you could log or ignore if you have more event types

    def _apply_global_event_added(self, evt: GlobalEventAdded):
        """
        Add a brand-new event to the aggregator's in-memory `events` dict.
        """
        new_event = EventInMatch(
            feed_event_id=evt.feed_event_id,
            local_event_id=evt.local_event_id,
            type_id=evt.type_id,
            period_id=evt.period_id,
            time_min=evt.time_min,
            time_sec=evt.time_sec,
            contestant_id=evt.contestant_id,
            player_id=evt.player_id,
            player_name=evt.player_name,
            outcome=evt.outcome,
            x=evt.x,
            y=evt.y,
            qualifiers=EventInMatch.map_qualifiers_from_dict(evt.qualifiers),   # dict => may convert to domain objects if needed
            time_stamp=evt.time_stamp,
            last_modified=evt.last_modified
        )
        self.events[evt.feed_event_id] = new_event

    def _apply_event_edited(self, evt: EventEdited):
        """
        Update an existing event with new field values.
        
        We'll loop through `evt.changed_fields` dict and set them on the aggregator's in-memory object.
        """
        existing = self.events.get(evt.feed_event_id)
        if not existing:
            # Possibly log a warning or skip if aggregator doesn't have that event
            return
        
        # For each field that changed, set the new value on the in-memory event
        for field_name, new_value in evt.changed_fields.items():
            # Example: if field_name == "type_id", do existing.type_id = new_value
            # We'll do a generic setattr if the attribute exists
            if hasattr(existing, field_name):
                if field_name == "qualifiers":
                    new_qualifiers = EventInMatch.map_qualifiers_from_dict(new_value)
                    setattr(existing, field_name, new_qualifiers)
                else:
                    setattr(existing, field_name, new_value)
            else:
                raise ValueError(f"Field {field_name} not found in EventInMatch When editing event {evt.feed_event_id}")

    # ------------------------------------------------------------------
    # PUBLIC "HANDLE" methods:
    #   Code calls these to generate domain events and record them.
    # ------------------------------------------------------------------

    def handle_new_event(self, event: EventInMatch):
        """
        The aggregator sees a brand-new feed event. 
        We create a `GlobalEventAdded` domain event, record it.
        """
        domain_evt = GlobalEventAdded(
            domain_event_id=str(uuid4()),
            aggregate_id=self.match_id,
            occurred_on=datetime.utcnow(),

            feed_event_id=event.feed_event_id,
            local_event_id=event.local_event_id,
            type_id=event.type_id,
            period_id=event.period_id,
            time_min=event.time_min,
            time_sec=event.time_sec,
            contestant_id=event.contestant_id,
            player_id=event.player_id,
            player_name=event.player_name,
            outcome=event.outcome,
            x=event.x,
            y=event.y,
            qualifiers=event.map_qualifiers_to_dict(),
            time_stamp=event.time_stamp,
            last_modified=event.last_modified
        )
        self._record(domain_evt)

    def handle_event_edited(
        self,
        feed_event_id: int,
        changed_fields: Dict[str, Any],
        old_fields: Dict[str, Any] = None
    ):
        """
        The aggregator sees an existing event has changed. 
        `changed_fields` might contain updates to `type_id`, `x`, `y`, `qualifiers`, etc.
        `old_fields` is optional if we want to track the old values for analytics.
        """
        domain_evt = EventEdited(
            domain_event_id=str(uuid4()),
            aggregate_id=self.match_id,
            occurred_on=datetime.utcnow(),
            feed_event_id=feed_event_id,
            changed_fields=changed_fields,
            old_fields=old_fields or {}
        )
        self._record(domain_evt)

    # ------------------------------------------------------------------
    # COMMIT / TRACKING
    # ------------------------------------------------------------------

    def _record(self, domain_event: DomainEvent):
        """
        1. Apply the domain event to update aggregator state
        2. Keep track of uncommitted domain events
        """
        self.apply(domain_event)
        self._uncommitted_events.append(domain_event)

    def get_uncommitted_events(self) -> list[DomainEvent]:
        return self._uncommitted_events

    def clear_uncommitted_events(self):
        self._uncommitted_events.clear()

