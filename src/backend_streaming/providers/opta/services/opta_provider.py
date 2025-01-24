# Directory: src/backend_streaming/providers/opta/services/opta_provider.py
import time
from typing import List, Dict, Optional, Callable, Set, Tuple

from backend_streaming.streamer.streamer import SingleGameStreamer
from backend_streaming.providers.opta.infra.oath import get_auth_headers
from backend_streaming.providers.opta.infra.api import get_match_events
from backend_streaming.providers.opta.constants import EPL_TOURNAMENT_ID
from backend_streaming.providers.opta.domain.value_objects.sport_event_enums import EventType
from backend_streaming.utils.logging import setup_logger

from backend_streaming.providers.opta.infra.db import get_session

# Domain/Infrastructure imports:
from backend_streaming.providers.opta.domain.entities.sport_events import EventInMatch
from backend_streaming.providers.opta.services.queries.match_projector import MatchProjection
from backend_streaming.providers.opta.domain.aggregates.match_aggregate import MatchAggregate
from backend_streaming.providers.opta.infra.repo.event_store.local import EventStore, LocalFileEventStore
from backend_streaming.providers.opta.infra.repo.match import MatchRepository
from backend_streaming.providers.opta.infra.repo.event_store.postgres import PostgresEventStore
from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository  
from backend_streaming.providers.opta.infra.models import MatchProjectionModel
from backend_streaming.providers.opta.domain.events import DomainEvent, GlobalEventAdded, EventTypeChanged, QualifiersChanged

class OptaStreamer:
    def __init__(
        self, 
        match_id: str,
        streamer: SingleGameStreamer,
        tournament_id: str = EPL_TOURNAMENT_ID,
        log_file: Optional[str] =  None,
        event_store_filename: Optional[str] = None,
        event_store: Optional[EventStore] = None,
        match_projection: Optional[MatchProjection] = None,
        match_projection_repo: Optional[MatchProjectionRepository] = None,
        fetch_events_func: Optional[Callable] = None, # Depends on the provider (Mock, API, etc.)
    ):
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
        
        # TODO: unused?
        if event_store_filename is None:
            event_store_filename = f"{match_id}.json"
        
        self.logger = setup_logger(__name__, log_file=log_file)
        
        
        self.event_store = event_store or PostgresEventStore(session_factory=get_session)
        self.match_repo = MatchRepository(self.event_store)
        
        self.match_projection_repo = match_projection_repo or MatchProjectionRepository(session_factory=get_session)
        self.match_projection = match_projection or MatchProjection()
        
        # Load aggregator from existing domain events
        self.agg: MatchAggregate = self.match_repo.load(match_id)  # replays everything
        self.match_id = match_id
        
        # Fetch events from Opta API
        self.fetch_events_func = fetch_events_func or get_match_events

        # We'll track if we detect the match ended
        self.finished = False

        # this is the streamer wrapper
        self.streamer = streamer
        self.last_event_id = None

    def run_live_stream(self, interval: int = 30):
        """
        Continuously poll the API for new raw events, integrate them into our aggregator,
        and persist them as domain events.
        """
        self.logger.info(f"Starting streaming for match={self.match_id}...")

        while not self.finished:
            try:
                # fetch raw data from Opta
                raw_data = self.fetch_events_func(self.match_id)
                live_data = raw_data.get("liveData", {})
                raw_events = live_data.get("event", [])
                
                # maintain the READ / WRITE models
                # NOTE: all these functions run a loop over the batch. Order should be persistent.
                if raw_events:
                    changed_times, curr_time = self._process_raw_events(raw_events)
                    self.match_repo.save(self.agg)
                    self._update_projections()
                    self.agg.clear_uncommitted_events()
                    # sending to Games service
                    self.streamer.handle_update(changed_times, curr_time)

            except Exception as e:
                self.logger.error(f"Error fetching events for match {self.match_id}: {e}", exc_info=True)

            # Sleep for the polling interval (TODO: tier 13??)
            if not self.finished:
                time.sleep(interval)
            else:
                break
        
        # signal the Games service that the match has ended
        self.streamer.handle_game_end()
        self.logger.info(f"Match {self.match_id} is finished. Exiting stream.")

    def _process_raw_events(self, raw_events: List[Dict]) -> Tuple[Set[int], int]:
        """
        Compare each raw event with aggregator state, detect changes,
        emit domain events via aggregator handle methods.
        Also detect if the match ended.
        """
        # this is to track event types that can change
        changed_times = set()

        for raw_ev in raw_events:
            ev: EventInMatch = EventInMatch.from_dict(raw_ev)
            ev_time = self._get_event_time_in_seconds(ev)
            feed_event_id = ev.feed_event_id
            
            # avoid doing the same processing for previous raw data. 
            # NOTE: necessary because raw_data keeps growing since events are accumulating in the feed.
            if self._is_new_event(feed_event_id):
                self.agg.handle_new_event(ev)
                self.logger.info(f"New event {feed_event_id} added to aggregator.")
            else:
                existing = self.agg.events[feed_event_id]
                if self._has_event_changed(existing, ev):
                    changed_times.add(ev_time)
                    # TODO: unify all handle methods to update properly
                    # TODO: last_modified wasn't updated properly to the new one
                    self.agg.handle_type_changed(feed_event_id, existing.type_id, ev.type_id)
                    self.agg.handle_qualifiers_changed(feed_event_id, ev.qualifiers)
                    self.logger.info(f"Event {feed_event_id} type changed to {ev.type_id}.")
                    
            if self._is_match_end(ev):
                self.finished = True
                self.logger.info(f"Match {self.match_id} ended.")
        
        return changed_times, self._get_max_time(raw_events)

    def _update_projections(self):
        """
        Read uncommitted domain events from aggregator, apply them
        to in-memory projection, then persist each event's state
        to the database via MatchProjectionRepository (upsert).
        """
        uncommitted = self.agg.get_uncommitted_events()
        for domain_evt in uncommitted:
            # Update the in-memory projection
            self.match_projection.project(domain_evt)
            # Get current state
            match_state = self.match_projection.get_current_match_state(self.match_id)
            event_entry = match_state.get("events_by_id", {}).get(domain_evt.feed_event_id)
            # Shouldn't happen if the projection is in sync. TODO: make this a proper test
            if not event_entry:
                return  
            # Persist current state
            self.logger.info(f"Upserting event {domain_evt.feed_event_id} into DB...")
            self.match_projection_repo.save_current_state(
                match_id=self.match_id,
                event_data=event_entry
            )


    ################################################
    # single line helper functions for readability #
    ################################################

    def _is_new_event(self, feed_event_id: str) -> bool:
        return feed_event_id not in self.agg.events
    
    def _has_event_changed(self, existing_event, new_event) -> bool:
        """Check if an existing event has changed"""
        return (existing_event.type_id != new_event.type_id or 
                new_event.last_modified != existing_event.last_modified)
    
    def _is_match_end(self, event: EventInMatch) -> bool:
        """Check if this event signals the end of the match"""
        return event.type_id == EventType.END.value and event.period_id == 2
    
    def _get_event_time_in_seconds(self, event: EventInMatch) -> int:
        """Convert event time to total seconds from EventInMatch object"""
        return event.time_min * 60 + event.time_sec
    
    def _get_max_time(self, raw_events: List[Dict]) -> int:
        """Get the maximum game time from raw events"""
        return max(
            raw_ev['timeMin'] * 60 + raw_ev['timeSec'] 
            for raw_ev in raw_events
        )


if __name__ == "__main__":
    event_store = PostgresEventStore(session_factory=get_session)
    provider = OptaStreamer(match_id="cbggpny9iygsfce7xf6wycb9w", event_store=event_store)
    provider.run_live_stream()
