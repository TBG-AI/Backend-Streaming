from typing import List
from fastapi import APIRouter, HTTPException
from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository
from backend_streaming.providers.opta.infra.db import get_session
from pydantic import BaseModel

import logging
# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create console handler if you want to see logs in terminal
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

# Add handler to logger
logger.addHandler(ch)

router = APIRouter()

# defining custom class since this is a post method
class EventIdsRequest(BaseModel):
    event_ids: List[int]

@router.post("/get_events_by_ids")
async def get_events_by_ids(request: EventIdsRequest) -> List[dict]:
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
    
@router.get("/get_events_by_game_id")
async def get_events_by_game_id(game_id: str) -> List[dict]:
    """
    Given the game_id, query and return the events from the database.
    Used by the manual verification service. 
    """
    try:
        # Create repository with session factory
        repo = MatchProjectionRepository(get_session, logger)
        
        # Get events
        events = repo.get_match_state(game_id)
        logger.info(f"Retrieved {len(events)} events for game {game_id}")
        if not events:
            raise HTTPException(
                status_code=404,
                detail={
                    "message": f"No events found for game {game_id}",
                    "error_code": "EVENTS_NOT_FOUND",
                    "action_required": True,
                    "action_type": "RUN_SCRIPT"
                }
            )

        # Convert to dict for JSON response
        return [event.to_dict() for event in events]
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving events: {str(e)}"
        )




if __name__ == "__main__":
    pass