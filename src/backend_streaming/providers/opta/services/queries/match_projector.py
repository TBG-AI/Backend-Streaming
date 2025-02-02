from backend_streaming.providers.opta.domain.events import DomainEvent, GlobalEventAdded, EventEdited

class MatchProjection:
    """
    A read model that stores the current state of a match
    for fast queries, without having to replay all events.
    """
    def __init__(self):
        # In-memory store: { match_id: { "events_by_id": { feed_event_id: {...fields...} }, ... } }
        # NOTE: a single match state is collection of events
        self._match_states = {}

    def project(self, evt: DomainEvent):
        """
        Apply the incoming domain event to update the read model.
        """
        match_id = evt.aggregate_id
        
        # Ensure an entry for this match_id
        if match_id not in self._match_states:
            self._match_states[match_id] = {
                "events_by_id": {},
                # other fields if needed, e.g. "score": None, "contestants": {}, etc.
            }

        match_state = self._match_states[match_id]
        
        if isinstance(evt, GlobalEventAdded):
            # Insert a brand-new event in the read model
            match_state["events_by_id"][evt.feed_event_id] = {
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
                "last_modified": evt.last_modified,
            }

        elif isinstance(evt, EventEdited):
            # Update an existing event's fields
            event_entry = match_state["events_by_id"].get(evt.feed_event_id)
            if event_entry:
                # For each changed field, overwrite the old value
                for field_name, new_value in evt.changed_fields.items():
                    event_entry[field_name] = new_value

    def get_current_match_state(self, match_id: str) -> dict:
        """
        Return the read model state for a match: events, or other summary info.
        """
        return self._match_states.get(match_id, {})
