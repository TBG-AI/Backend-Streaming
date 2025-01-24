from backend_streaming.providers.opta.domain.events import DomainEvent, GlobalEventAdded, EventTypeChanged, QualifiersChanged

class MatchProjection:
    """
    A read model that stores the current state of a match
    for fast queries, without having to replay all events.
    """
    def __init__(self):
        # You can store this in memory, or in a DB, or anywhere else.
        # For demonstration, let's just keep it in a dictionary:
        self._match_states = {}  # { match_id: { 'events': [...], 'score': ..., etc. } }

    def project(self, evt: DomainEvent):
        """
        Apply the incoming domain event to update the read model.
        Depending on the event type, we'll update the read model differently.
        """
        # extract the match_id from the event
        match_id = evt.aggregate_id
        
        # Make sure there's an entry for this match_id
        # NOTE: this is when project is first called
        if match_id not in self._match_states:
            self._match_states[match_id] = {
                "events_by_id": {},
                # store other fields as needed, e.g. "score": None, "contestants": {}, etc.
            }

        match_state = self._match_states[match_id]
        if isinstance(evt, GlobalEventAdded):
            # Insert the event in the read model
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
                "last_modified": evt.last_modified
            }

        # TODO: these get merged in... therefore, the games service needs to know when this happens. 
        # ideally, it should be "pending" until we know a merge won't happen any more. 
        elif isinstance(evt, EventTypeChanged):
            # Update the event's type in the read model
            event_entry = match_state["events_by_id"].get(evt.feed_event_id)
            if event_entry:
                event_entry["type_id"] = evt.new_type_id

        elif isinstance(evt, QualifiersChanged):
            # Update qualifiers in the read model
            event_entry = match_state["events_by_id"].get(evt.feed_event_id)
            if event_entry:
                event_entry["qualifiers"] = evt.new_qualifiers
    
    def get_current_match_state(self, match_id: str) -> dict:
        """
        Return the entire read model state for a match,
        or some subset (like a scoreboard, event list, etc.).
        """
        return self._match_states.get(match_id, {})
