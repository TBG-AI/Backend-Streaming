# Directory: src/backend_streaming/providers/opta/replay.py
# This file is used to replay the events from the event store.

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Tuple

from backend_streaming.config.time import time_config
from src.backend_streaming.providers.opta.services.queries.match_projector import MatchProjection
from src.backend_streaming.providers.opta.services.opta_provider import SingleGameStreamer
from src.backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository
from src.backend_streaming.providers.opta.infra.repo.event_store.postgres import PostgresEventStore
from backend_streaming.providers.opta.infra.db import get_session
from analysis.get_snapshot import get_formatted_name
from analysis.constants import SNAPSHOTS_DIR, GAME_TO_ID_MAPPING
    
    
def set_config(match_id: str, speed: float = 500) -> Tuple[List[Dict], datetime]:
    """
    Get the domain events and set the time configuration.
    """
    try:
        event_store = PostgresEventStore(session_factory=get_session)
        events = event_store.load_events(match_id)
        
    except Exception as e:
        raise e
    
    # Set simulation start time to first event time
    start_time = events[0].occurred_on
    time_config.jump_to(start_time)
    time_config.set_speed(speed)
    return events, start_time


def replay_events(
    events: List[Dict], 
    last_push_time: datetime, 
    push_interval: int = 30
) -> Tuple[List[Dict], datetime]:
    """
    Returns a batch of domain events based on the push interval and the new last_push_time.
    """
    current_batch = []
    new_last_push_time = last_push_time
    
    for event in events:
        event_time = event.occurred_on
        # If we've passed the push interval, return current batch
        if event_time - last_push_time >= timedelta(seconds=push_interval):
            return current_batch, event_time
        
        current_batch.append(event)
        new_last_push_time = event_time
    
    # Return any remaining events in the final batch
    return current_batch, new_last_push_time


def stream_read_model(
    match_id: str,
    speed: float = 500,
    push_interval: int = 30
):
    """
    Reconstructs the READ model from the event store and streams the read states.
    """
    projector = MatchProjection()
    projector_repo = MatchProjectionRepository(session_factory=None)
    streamer = SingleGameStreamer(game_id=match_id)
    events, start_time = set_config(match_id, speed)
    last_push_time = start_time

    remaining_events = events
    while remaining_events:
        # Get next batch of events
        batch, last_push_time = replay_events(remaining_events, last_push_time, push_interval)
        remaining_events = remaining_events[len(batch):]  # Remove processed events
        
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
            streamer.send_message(message_type="ongoing", events=match_state_read)
        except Exception as e:
            raise(f"Error sending message: {e}")

    # Send final stop message
    try:
        streamer.send_message(message_type="stop")
    except Exception as e:
        raise(f"Error sending stop message: {e}")
    finally:
        streamer.close()

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

    stream_read_model(match_id, speed, push_interval)

if __name__ == "__main__":
    main()
