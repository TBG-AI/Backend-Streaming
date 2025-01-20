# Directory: src/backend_streaming/providers/opta/services/opta_provider.py
import json
import time
import logging
from typing import List, Dict, Optional
from datetime import datetime

from backend_streaming.providers.base import BaseProvider
from backend_streaming.providers.opta.infra.oath import get_auth_headers
from backend_streaming.providers.opta.infra.api import get_match_events
from backend_streaming.providers.opta.constants import EPL_TOURNAMENT_ID
from backend_streaming.providers.opta.domain.value_objects.sport_event_enums import EventType
from backend_streaming.utils.logging import setup_logger

from backend_streaming.providers.opta.infra.db import get_session

# Domain/Infrastructure imports:
from backend_streaming.providers.opta.services.queries.match_projector import MatchProjection
from backend_streaming.providers.opta.domain.aggregates.match_aggregate import MatchAggregate
from backend_streaming.providers.opta.infra.repo.event_store.local import EventStore, LocalFileEventStore
from backend_streaming.providers.opta.infra.repo.match import MatchRepository
from backend_streaming.providers.opta.infra.repo.event_store.postgres import PostgresEventStore
from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository  

class OptaStreamer:
    def __init__(self, 
                 match_id: str,
                 tournament_id: str = EPL_TOURNAMENT_ID,
                 log_file: Optional[str] =  None,
                 event_store_filename: Optional[str] = None,
                 event_store: Optional[EventStore] = None,
                 match_projection: Optional[MatchProjection] = None,
                 match_projection_repo: Optional[MatchProjectionRepository] = None):
        """
        We'll pass in a match_id that we want to track.
        We'll keep a local file event store, so we can replay domain events across runs.
        """
        self.access_token = get_auth_headers()
        self.headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        self.tournament_id = tournament_id
        
        if log_file is None:
            log_file = f"{match_id}.log"
        
        if event_store_filename is None:
            event_store_filename = f"{match_id}.json"
        
        self.logger = setup_logger(__name__, log_file=log_file)
        
        
        self.event_store = event_store or LocalFileEventStore(filename=event_store_filename)
        self.match_repo = MatchRepository(self.event_store)
        
        # TODO: Implement match projection repo? (Not yet used -- Ignore for now)
        self.match_projection_repo = match_projection_repo or MatchProjectionRepository(session_factory=get_session)
        self.match_projection = match_projection or MatchProjection()
        
        # Load aggregator from existing domain events
        self.agg = self.match_repo.load(match_id)  # replays everything
        self.match_id = match_id

        # We'll track if we detect the match ended
        self.finished = False

    def run_live_stream(self, interval: int = 30):
        """
        Continuously poll the API for new raw events, integrate them into our aggregator,
        and persist them as domain events.
        """
        self.logger.info(f"Starting streaming for match={self.match_id}...")

        while not self.finished:
            try:
                # 1) Fetch raw data from Opta
                raw_data = get_match_events(self.match_id)
                live_data = raw_data.get("liveData", {})
                raw_events = live_data.get("event", [])
                
                # 2) For each raw event, see if it's new or changed
                self._process_raw_events(raw_events)

                # 3) Save aggregator => persists new domain events
                self.match_repo.save(self.agg)

            except Exception as e:
                self.logger.error(f"Error fetching events for match {self.match_id}: {e}", exc_info=True)

            # Sleep for the polling interval
            if not self.finished:
                time.sleep(interval)

        self.logger.info(f"Match {self.match_id} is finished. Exiting stream.")

    def _process_raw_events(self, raw_events: List[Dict]):
        """
        Compare each raw event with aggregator state, detect changes,
        emit domain events via aggregator handle methods.
        Also detect if the match ended.
        """
        for ev in raw_events:
            feed_event_id = ev["id"]

            # If aggregator doesn't have it yet, it's new
            if feed_event_id not in self.agg.events:
                self.agg.handle_new_event(ev)
                self.logger.info(f"New event {feed_event_id} added to aggregator.")
            else:
                # Possibly detect type change
                existing = self.agg.events[feed_event_id]
                old_type = existing.type_id
                new_type = ev["typeId"]

                if old_type != new_type or ev["lastModified"] != existing.last_modified: # Check last modified
                    self.agg.handle_type_changed(feed_event_id, old_type, new_type)
                    self.agg.handle_qualifiers_changed(feed_event_id, ev["qualifier"])
                    self.logger.info(f"Event {feed_event_id} type changed to {new_type}.")
                    
                

            # Also see if this event is an 'END' with period=2 => match finished
            if ev["typeId"] == EventType.END and ev.get("periodId") == 2:
                self.finished = True
                self.logger.info(f"Match {self.match_id} ended.")


if __name__ == "__main__":
    event_store = PostgresEventStore(session_factory=get_session)
    provider = OptaStreamer(match_id="c0i4chcg41suds6581fj8k7bo", event_store=event_store)
    provider.run_live_stream()
