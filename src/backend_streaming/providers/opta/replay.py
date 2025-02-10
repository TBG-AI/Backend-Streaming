# Directory: src/backend_streaming/providers/opta/replay.py
# This file is used to replay the events from the event store.
from dotenv import load_dotenv
load_dotenv('.env')

import time
import json
import asyncio

from datetime import datetime, timedelta
from typing import List, Dict, Tuple


from backend_streaming.config.time import time_config, TimeConfig
from backend_streaming.providers.opta.services.queries.match_projector import MatchProjection
from backend_streaming.providers.opta.infra.models import DomainEventModel
from backend_streaming.providers.opta.services.opta_provider import SingleGameStreamer
from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository
# src/backend_streaming/providers/opta/infra/models.py
from backend_streaming.providers.opta.infra.repo.event_store.postgres import PostgresEventStore
from analysis.constants import SNAPSHOTS_DIR, GAME_TO_ID_MAPPING

def _convert_to_datetime(string: str) -> datetime:
    return datetime.strptime(string, '%Y-%m-%dT%H:%M:%S.%f')

def set_config(match_id: str, speed: float = 500) -> Tuple[List[DomainEventModel], TimeConfig]:
    """
    Get the domain events and set the time configuration.
    """
    try:
        # events = event_store.load_events(match_id)
        file_path = f"{SNAPSHOTS_DIR}/domain_events/{GAME_TO_ID_MAPPING[match_id]}.json"
        with open(file_path, 'r') as f:
            events = json.load(f)
            events = [DomainEventModel(**event) for event in events]
            # a little bit hacky but trying to just use the static json files
            events = PostgresEventStore(session_factory=None)._bulk_deserialize_events(events)
        
    except Exception as e:
        raise e
    
    # NOTE: assuming the first event is the start of the game
    time_config.jump_to(_convert_to_datetime(events[0].occurred_on))
    time_config.set_speed(speed)
    return events, time_config

def replay_events(
    events: List[DomainEventModel], 
    current_time: datetime, 
    is_eog: bool,
) -> Tuple[List[DomainEventModel], int]:
    """
    Returns a batch of domain events based until the current time.
    # NOTE: all events after the game are processed at once. This is bc we could have events updated 24h later...
    """
    for i, event in enumerate(events):
        # process all events that happened after the game at once.
        if is_eog:
            break

        # get the time attribute for when the event was added
        # NOTE: assuming that domain event creation (in our system) roughly aligns with when the event was streamed
        # This is true unless our system went down. 
        if _convert_to_datetime(event.occurred_on) > current_time:
            return events[:i], i
        
    # final return
    return events, len(events)


async def stream_read_model(
    match_id: str,
    speed: float = 500,
    push_interval: int = 30
):
    """
    Reconstructs the READ model from the event store and streams the read states.
    NOTE: This simulates the event stream which could go past the end of the match (opta makes updates after)
    """
    projector = MatchProjection()
    projector_repo = MatchProjectionRepository(session_factory=None)
    streamer = SingleGameStreamer(game_id=match_id)
    events, time_config = set_config(match_id, speed)

    # streaming events based on the time passed
    remaining = events
    while remaining:
        time.sleep(push_interval / speed)     
        curr_time = time_config.now()
        print("time: ", time_config.get_time_passed())

        batch, i = replay_events(
            events=remaining, 
            current_time=curr_time, 
            is_eog=time_config.get_time_passed() >= timedelta(hours=2)
        )
        remaining = remaining[i:]

        # Update in-memory read model
        for evt in batch:
            projector.project(evt)

        # Extract current state and stream the data
        match_state = projector.get_current_match_state(match_id)
        match_state_read = [
            projector_repo._convert_to_orm_model(match_id, feed_event_id, event_entry)
            for feed_event_id, event_entry in match_state.get("events_by_id", {}).items()
        ]
        try:
            message_type = "update" if remaining else "stop"
            await streamer.send_message(message_type=message_type, events=match_state_read)
        except Exception as e:
            raise(f"Error sending message: {e}")

    # close the streamer
    await streamer.close()
        

def main():
    matches = GAME_TO_ID_MAPPING
    # Display available matches
    print("\nAvailable matches:")
    print("================")
    for idx, (match_id, name) in enumerate(matches.items(), 1):
        print(f"{idx}. {name}")

    # Get user choice for match
    while True:
        try:
            choice = int(input("\nSelect match number: "))
            if 1 <= choice <= len(matches):
                match_id = list(matches.keys())[choice - 1]
                break
            print(f"Please enter a number between 1 and {len(matches)}")
        except ValueError:
            print("Please enter a valid number")

    # Get simulation speed
    while True:
        try:
            speed = float(input("\nEnter simulation speed (default: 500): ") or "500")
            if speed > 0:
                break
            print("Speed must be greater than 0")
        except ValueError:
            print("Please enter a valid number")

    # Get push interval
    while True:
        try:
            push_interval = int(input("\nEnter push interval in seconds (default: 30): ") or "30")
            if push_interval > 0:
                break
            print("Interval must be greater than 0")
        except ValueError:
            print("Please enter a valid number")

    print(f"\nStarting replay for: {matches[match_id]}")
    print(f"Speed: {speed}x")
    print(f"Push interval: {push_interval} seconds")
    print("================")

    asyncio.run(stream_read_model(match_id, speed, push_interval))

if __name__ == "__main__":
    main()
