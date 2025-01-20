# Directory: src/backend_streaming/providers/opta/domain/match_aggregate.py
from typing import Dict, Optional
from uuid import uuid4
from datetime import datetime
from backend_streaming.providers.opta.domain.events import DomainEvent, GlobalEventAdded, EventTypeChanged, QualifiersChanged
from backend_streaming.providers.opta.domain.entities.sport_events import EventInMatch, Qualifier

class MatchAggregate:
    def __init__(self, match_id: str):
        self.match_id = match_id
        # Key = feed_event_id (API 'id'), Value = EventInMatch
        self.events: Dict[int, EventInMatch] = {}
        self._uncommitted_events: list[DomainEvent] = []

    def apply(self, evt: DomainEvent):
        # Route to the correct handler method
        if isinstance(evt, GlobalEventAdded):
            self._apply_global_event_added(evt)
        elif isinstance(evt, EventTypeChanged):
            self._apply_event_type_changed(evt)
        elif isinstance(evt, QualifiersChanged):
            self._apply_qualifiers_changed(evt)

    def _apply_global_event_added(self, evt: GlobalEventAdded):
        # Construct new in-memory event from domain event
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
            # Convert the domain event's qualifiers (dict or list) into a dict
            qualifiers=evt.qualifiers,
            time_stamp=evt.time_stamp,
            last_modified=evt.last_modified
        )
        self.events[evt.feed_event_id] = new_event

    def _apply_event_type_changed(self, evt: EventTypeChanged):
        event_in_match = self.events.get(evt.feed_event_id)
        if event_in_match:
            event_in_match.type_id = evt.new_type_id

    def _apply_qualifiers_changed(self, evt: QualifiersChanged):
        e = self.events.get(evt.feed_event_id)
        if e:
            # Overwrite the entire qualifiers dictionary
            e.qualifiers = {
                qid: Qualifier(qid, val) 
                for qid, val in evt.new_qualifiers.items()
            }
            
    # ------------------
    # Public "handle" methods
    # ------------------

    def handle_new_event(self, data: dict):
        """
        data is the raw dict from the API, e.g. {
          "id": 2762916859,
          "eventId": 230,
          "typeId": 1,
          ...
          "qualifier": [ {"qualifierId":212, "value":"6.7"}, ... ]
        }
        """
        domain_evt = GlobalEventAdded(
            domain_event_id=str(uuid4()),
            aggregate_id=self.match_id,
            occurred_on=datetime.utcnow(),

            feed_event_id=data["id"],
            local_event_id=data["eventId"],
            type_id=data["typeId"],
            period_id=data["periodId"],
            time_min=data["timeMin"],
            time_sec=data["timeSec"],
            contestant_id=data.get("contestantId", ""),
            player_id=data.get("playerId", ""),
            player_name=data.get("playerName", ""),
            outcome=data.get("outcome"),
            x=data.get("x"),
            y=data.get("y"),
            qualifiers=self._build_qualifier_dict(data.get("qualifier", [])),
            time_stamp=data.get("timeStamp"),
            last_modified=data.get("lastModified")
        )
        self._record(domain_evt)

    def handle_type_changed(self, feed_event_id: int, old_type: int, new_type: int):
        domain_evt = EventTypeChanged(
            domain_event_id=str(uuid4()),
            aggregate_id=self.match_id,
            occurred_on=datetime.utcnow(),
            feed_event_id=feed_event_id,
            old_type_id=old_type,
            new_type_id=new_type
        )
        self._record(domain_evt)

    def handle_qualifiers_changed(self, feed_event_id: int, new_qualifiers: Dict[int, Optional[str]]):
        domain_evt = QualifiersChanged(
            domain_event_id=str(uuid4()),
            aggregate_id=self.match_id,
            occurred_on=datetime.utcnow(),
            feed_event_id=feed_event_id,
            new_qualifiers=new_qualifiers
        )
        self._record(domain_evt)

    def _record(self, domain_event: DomainEvent):
        self.apply(domain_event)
        self._uncommitted_events.append(domain_event)

    def get_uncommitted_events(self) -> list[DomainEvent]:
        return self._uncommitted_events

    def clear_uncommitted_events(self):
        self._uncommitted_events.clear()

    def _build_qualifier_dict(self, qualifiers_list: list[dict]) -> Dict[int, Qualifier]:
        result = {}
        for q in qualifiers_list:
            q_id = q["qualifierId"]
            val = q.get("value")
            result[q_id] = Qualifier(q_id, val)
        return result
