# TODO: currenty throwing all routes in one file. modularize this later.
import os
import logging
import uuid

from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, HTTPException
from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository
from backend_streaming.providers.opta.infra.db import get_session
from pydantic import BaseModel
from backend_streaming.providers.whoscored.infra.repos.file_repo import FileRepository
from backend_streaming.providers.whoscored.app.services.run_scraper import parse_game_txt, save_game_txt, process_game
from backend_streaming.providers.whoscored.app.services.update_fixtures import process_fixtures
from backend_streaming.providers.whoscored.app.services.scraper import SingleGameScraper
from backend_streaming.providers.whoscored.infra.config.config import paths, type_to_paths


router = APIRouter()

class EventIdsRequest(BaseModel):
    event_ids: List[int]


class ParseGameTxtRequest(BaseModel):
    """
    Represents a request to parse a game txt file.
    """
    game_txt: str
    send_via_stream: bool = False

class ScrapeFixturesRequest(BaseModel):
    """
    Represents a request to scrape the schedule for a given league and season.
    """
    fixtures_dict: dict


def get_file_repository(logger: Optional[logging.Logger] = None) -> FileRepository:
    return FileRepository(
        paths=paths,
        type_to_paths=type_to_paths,
        logger=logger
    )


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
        print(events)
        # conversion to dict to stay consistent with streamer's send_message method
        # NOTE: calling MatchProjectionModel's to_dict method.
        return [event.to_dict() for event in events]

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve events: {str(e)}"
        )
    
    
@router.post("/fetch_game_manually")
async def fetch_game_manually(request: ParseGameTxtRequest) -> dict:
    """
    Given the event_ids in request body, query and return the events from the database
    """
    try:
        match_id, match_centre_data = parse_game_txt(request.game_txt)
        # TODO: use db instead of local file
        # save_game_txt(match_id, match_centre_data)
        scraper = SingleGameScraper(match_id)
        print(f"========== calling process_game with match_id: {match_id} ==========")
        result = await process_game(
            game_id=match_id, 
            scraper=scraper,
            match_centre_data=match_centre_data,
            # NOTE: by default, this is false
            send_via_stream=request.send_via_stream
        )
        print(f"opta_game_id: {result['opta_game_id']}")
        return {
            "opta_game_id": result['opta_game_id'],
            # just returning the last payload since this method is designed to be used
            # to get the events at the end of the game.
            "payload": result['payloads'][-1]
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch game: {str(e)}"
        )
    
    
@router.post("/scrape_fixtures")
async def scrape_fixtures(request: ScrapeFixturesRequest) -> dict:
    """
    Given the league and season, scrape the schedule for the given month.
    This is all the fixtures for EPL 24-25 season, April 2025: https://www.whoscored.com/tournaments/23400/data/?d=202504
    """
    # Get file repository
    file_repo = get_file_repository()
    return process_fixtures(file_repo, request.fixtures_dict)

@router.get("/get_events_by_game_id")
async def get_events_by_game_id(game_id: str) -> List[dict]:
    """
    Given the game_id, query and return the events from the database.
    Used by the manual verification service. 
    """
    try:
        # Create repository with session factory
        repo = MatchProjectionRepository(get_session)
        events = repo.get_match_state(game_id)
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

