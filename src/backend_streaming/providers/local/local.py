import json
import time
from typing import Tuple

from backend_streaming.config.time import time_config
from backend_streaming.constants import GAMES_DIR
from .utils import get_event_time, get_events_for_game
from ..base import BaseProvider

# NOTE: defined here since only used in local provider
EVENTS_PER_PUSH = 100
EVENTS_PER_SECOND = 0.5

EVENT_PUSH_INTERVAL = (EVENTS_PER_PUSH / EVENTS_PER_SECOND) / time_config.SIMULATION_SPEED  
SIMULATE_PROCESSING = 0.5 / time_config.SIMULATION_SPEED
SIMULATE_INSERTION = 0.5 / time_config.SIMULATION_SPEED


class LocalDataProvider(BaseProvider):
    def __init__(self, game_id: int):
        """ 
        Reads events from local db and simulates a real-time stream.
        """
        self.game_id = game_id
        self.events = get_events_for_game(game_id)
        self.i = 0
        self.start_time = None

    def is_finished(self) -> bool:
        """
        Returns true if all events have been processed.
        """
        return self.i >= len(self.events)

    def get_live_events(self) -> json:
        """
        Get events from the game stream based on elapsed time.
        Returns events that occurred during the current time window.
        """
        if self.start_time is None:
            self.start_time = time_config.now()
        # Simulate real-time delay for push
        time.sleep(EVENT_PUSH_INTERVAL)
        
        
        # Calculate time window
        elapsed_seconds = (time_config.now() - self.start_time).total_seconds()
        # Find events that occurred in this time window
        prev_i = self.i
        while (
            self.i < len(self.events) and
            get_event_time(self.events[self.i]) <= elapsed_seconds
        ):
            self.i += 1    

        return json.dumps(self.events[prev_i: self.i])

    def process_events(self, events: list[dict]) -> Tuple[int, int]:
        """
        Process a batch of events and return filters for querying.
        The processed state is stored in the events table.
        """
        # NOTE: official data providers will have different raw data but the processed data will be the same
        # refer to events table for details on fields to include
        event_ids = [event['event_id'] for event in events]
        time.sleep(SIMULATE_PROCESSING)
        # TODO: when inserting, make sure events are sorted by timestamp
        time.sleep(SIMULATE_INSERTION) 
        return min(event_ids), max(event_ids)
