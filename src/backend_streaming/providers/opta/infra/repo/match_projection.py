import os
from typing import List, Optional
from backend_streaming.providers.opta.infra.models import MatchProjectionModel
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

import logging


class MatchProjectionRepository:
    """
    Responsible for persisting and retrieving the read model in a table.
    """
    def __init__(self, session_factory, logger: Optional[logging.Logger] = None):
        self.session_factory = session_factory
        self.logger = logger or logging.getLogger(__name__)

    # NOTE: currently deprecated!
    def _convert_to_orm_model(
        self, 
        match_id: str, 
        feed_event_id: int,
        event_entry: dict
    ) -> MatchProjectionModel:
        """
        Convert a single event entry into an ORM model. 
        """
        return MatchProjectionModel(
            match_id=match_id,
            event_id=feed_event_id,
            local_event_id=event_entry["local_event_id"],
            type_id=event_entry["type_id"],
            period_id=event_entry["period_id"],
            time_min=event_entry["time_min"],
            time_sec=event_entry["time_sec"],
            contestant_id=event_entry["contestant_id"],
            player_id=event_entry["player_id"],
            player_name=event_entry["player_name"],
            outcome=event_entry["outcome"],
            x=event_entry["x"],
            y=event_entry["y"],
            qualifiers=event_entry["qualifiers"], # JSON/BLOB field
            time_stamp=event_entry["time_stamp"],
            last_modified=event_entry["last_modified"]
        )

    def save_match_state(self, projections: List[dict]):    
        session = self.session_factory()
        try: 
            # Track seen event IDs and build list of unique models
            seen_events = {}
            unique_models = []
            
            for projection in projections:
                if projection['event_id'] in seen_events:
                    self.logger.warning(f"Duplicate event for event id: {projection['event_id']}")
                    continue
                
                seen_events[projection['event_id']] = projection
                unique_models.append(projection)
            
            # TODO: this is being triggered but why don't I see the inserts?
            self.logger.info(f"upserting {len(unique_models)} projections")
            stmt = insert(MatchProjectionModel).values(unique_models)
            stmt = stmt.on_conflict_do_update(
                index_elements=['event_id'],  # Use primary key instead of named constraint
                set_=stmt.excluded
            )
            session.execute(stmt)
            session.commit()
            
        except Exception as e:
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

    @classmethod
    async def load_events_by_ids(cls, session: Session, event_ids: List[int]) -> List[MatchProjectionModel]:
        """
        Load events by their IDs from the database.
        """
        try:
            rows = (
                session.query(MatchProjectionModel)
                .filter(MatchProjectionModel.event_id.in_(event_ids))
                .all()
            )
            # TODO: deserialize rows into MatchProjectionModel objects
            return rows

        finally:
            session.close()

    

