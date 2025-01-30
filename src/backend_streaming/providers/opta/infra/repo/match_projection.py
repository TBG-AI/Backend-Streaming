from typing import List

from backend_streaming.providers.opta.infra.models import MatchProjectionModel
from backend_streaming.providers.opta.domain.entities.sport_events import Qualifier


class MatchProjectionRepository:
    """Responsible for persisting and retrieving the read model in a table."""
    def __init__(self, session_factory):
        self.session_factory = session_factory

    def save_current_state(self, mp: MatchProjectionModel):
        """
        Upsert the match projection row for a given event in a match.
        """
        session = self.session_factory()
        try:
            session.merge(mp)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def get_match_state(self, match_id: str) -> List[MatchProjectionModel]:
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
