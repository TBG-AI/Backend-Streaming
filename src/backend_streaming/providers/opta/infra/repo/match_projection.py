from backend_streaming.providers.opta.infra.models import MatchProjectionModel

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
            # If (match_id, event_id) is unique, you can do a merge or custom upsert logic:
            existing = (session.query(MatchProjectionModel)
                                .filter_by(match_id=mp.match_id, event_id=mp.event_id)
                                .one_or_none())
            if existing:
                # update existing row
                existing.local_event_id = mp.local_event_id
                existing.type_id = mp.type_id
                existing.period_id = mp.period_id
                existing.time_min = mp.time_min
                existing.time_sec = mp.time_sec
                existing.player_id = mp.player_id
                existing.contestant_id = mp.contestant_id
                existing.player_name = mp.player_name
                existing.outcome = mp.outcome
                existing.x = mp.x
                existing.y = mp.y
                existing.qualifiers = mp.qualifiers
                existing.time_stamp = mp.time_stamp
                existing.last_modified = mp.last_modified
            else:
                # insert new row
                session.add(mp)

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
