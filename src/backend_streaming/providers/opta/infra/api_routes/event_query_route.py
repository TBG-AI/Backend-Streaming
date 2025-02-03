from typing import List
from fastapi import APIRouter, HTTPException
from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository
from backend_streaming.providers.opta.infra.db import get_session
from pydantic import BaseModel

import logging
logger = logging.getLogger(__name__)

router = APIRouter()


# defining custom class since this is a post method
class EventIdsRequest(BaseModel):
    event_ids: List[int]

@router.post("/get_events")
async def get_events(request: EventIdsRequest) -> List[dict]:
    """
    Given the event_ids in request body, query and return the events from the database
    """
    try:        
        # Load events by their IDs
        events = await MatchProjectionRepository.load_events_by_ids(
            session=get_session(),
            event_ids=request.event_ids
        )        
        # conversion to dict to stay consistent with streamer's send_message method
        # NOTE: calling MatchProjectionModel's to_dict method.
        return [event.to_dict() for event in events]

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve events: {str(e)}"
        )
    

if __name__ == "__main__":
    pass