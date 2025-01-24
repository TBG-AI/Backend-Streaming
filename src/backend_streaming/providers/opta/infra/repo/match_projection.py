from backend_streaming.providers.opta.infra.models import MatchProjectionModel
from backend_streaming.providers.opta.domain.entities.sport_events import Qualifier


class MatchProjectionRepository:
    """Responsible for persisting and retrieving the read model in a table."""
    def __init__(self, session_factory):
        self.session_factory = session_factory

    def save_current_state(self, match_id: str, event_data: dict):
        """
        Upsert the match projection row for a given event in a match.
        
        Args:
            match_id: The ID of the match
            event_data: Dictionary containing event data from the read model
        """
        # Create ORM model from event data
        mp = MatchProjectionModel(
            match_id=match_id,
            event_id=event_data["event_id"],
            local_event_id=event_data["local_event_id"],
            type_id=event_data["type_id"],
            period_id=event_data["period_id"],
            time_min=event_data["time_min"],
            time_sec=event_data["time_sec"],
            contestant_id=event_data["contestant_id"],
            player_id=event_data["player_id"],
            player_name=event_data["player_name"],
            outcome=event_data["outcome"],
            x=event_data["x"],
            y=event_data["y"],
            qualifiers=event_data["qualifiers"],
            time_stamp=event_data["time_stamp"],
            # TODO: this field will probably dictate some event change nuances
            last_modified=event_data["last_modified"]
        )
        # one transaction per event since READ models update current state.
        session = self.session_factory()
        try:
            session.merge(mp)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def get_match_state(self, match_id: str):
        """
        Return all projected events for this match as a list of MatchProjectionModel objects.
        """
        session = self.session_factory()
        try:
            return (session.query(MatchProjectionModel)
                          .filter_by(match_id=match_id)
                          .all())
        finally:
            session.close()
