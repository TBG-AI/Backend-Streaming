# Directory: src/backend_streaming/providers/opta/services/opta_provider.py
import json
import time
import asyncio
import logging
from typing import List, Dict, Optional, Callable
from datetime import datetime
from dataclasses import fields

from backend_streaming.providers.base import BaseProvider
from backend_streaming.providers.opta.infra.oath import get_auth_headers
from backend_streaming.providers.opta.infra.api import get_match_events
from backend_streaming.providers.opta.constants import EPL_TOURNAMENT_ID
from backend_streaming.providers.opta.domain.value_objects.sport_event_enums import EventType
from backend_streaming.utils.logging import setup_logger

from backend_streaming.providers.opta.infra.db import get_session

# Domain/Infrastructure imports:
from backend_streaming.providers.opta.domain.entities.sport_events import EventInMatch, Qualifier
from backend_streaming.providers.opta.services.queries.match_projector import MatchProjection
from backend_streaming.providers.opta.domain.aggregates.match_aggregate import MatchAggregate
from backend_streaming.providers.opta.infra.repo.event_store.local import EventStore, LocalFileEventStore
from backend_streaming.providers.opta.infra.repo.match import MatchRepository
from backend_streaming.providers.opta.infra.repo.event_store.postgres import PostgresEventStore
from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository  
from backend_streaming.providers.opta.infra.models import MatchProjectionModel
from backend_streaming.providers.opta.domain.events import DomainEvent

class OptaStreamer:
    def __init__(self, 
                 match_id: str,
                 tournament_id: str = EPL_TOURNAMENT_ID,
                 log_file: Optional[str] =  None,
                 event_store_filename: Optional[str] = None,
                 event_store: Optional[EventStore] = None,
                 match_projection: Optional[MatchProjection] = None,
                 match_projection_repo: Optional[MatchProjectionRepository] = None,
                 fetch_events_func: Optional[Callable] = None # Depends on the provider (Mock, API, etc.)
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

    async def run_live_stream(self, interval: int = 30):
        """
        Continuously poll the API for new raw events, integrate them into our aggregator,
        and persist them as domain events.
        """
        self.logger.info(f"Starting streaming for match={self.match_id}...")

        while not self.finished:
            try:
                # 1) Fetch raw data from Opta
                raw_data = await self.fetch_events_func(self.match_id)
                live_data = raw_data.get("liveData", {})
                raw_events = live_data.get("event", [])
                
                # 2) Process raw events in aggregator
                self._process_raw_events(raw_events)

                # 3) Persist aggregator => commits new domain events
                self.match_repo.save(self.agg)

                # 4) Update DB match projection from aggregator's uncommitted events
                self._update_projections()

                # 5) Clear uncommitted
                self.agg.clear_uncommitted_events()

            except Exception as e:
                self.logger.error(f"Error fetching events for match {self.match_id}: {e}", exc_info=True)

            # Sleep for the polling interval
            if not self.finished:
                await asyncio.sleep(interval)
            else:
                break

        self.logger.info(f"Match {self.match_id} is finished. Exiting stream.")

    def _process_raw_events(self, raw_events: List[Dict]):
        """
        Compare each raw event with aggregator state, detect changes,
        emit domain events via aggregator handle methods.
        Also detect if the match ended.
        """
        for ev in raw_events:
            new: EventInMatch = EventInMatch.from_dict(ev)
            feed_event_id = new.feed_event_id
            
            # If aggregator doesn't have it yet, it's new
            if feed_event_id not in self.agg.events:
                self.agg.handle_new_event(new)
                self.logger.info(f"New event {feed_event_id} added to aggregator.")
            else:
                # Possibly detect type change
                existing = self.agg.events[feed_event_id]
                changed_fields, old_fields = self._compare_event_fields(existing, new)

                if changed_fields:
                    # create your domain event for editing
                    self.agg.handle_event_edited(
                        feed_event_id=feed_event_id,
                        changed_fields=changed_fields,
                        old_fields=old_fields
                        )
                    
                

            # Also see if this event is an 'END' with period=2 => match finished
            if new.type_id == EventType.END.value and new.period_id == 2:
                self.finished = True
                self.logger.info(f"Match {self.match_id} ended.")

    def _update_projections(self):
        """
        Read uncommitted domain events from aggregator, apply them
        to in-memory projection, then persist each event's state
        to the database via MatchProjectionRepository (upsert).
        """
        uncommitted = self.agg.get_uncommitted_events()
        for domain_evt in uncommitted:
            # Update the in-memory read model first
            self.match_projection.project(domain_evt)

            # Then upsert each event in the DB
            self.logger.info(f"Upserting event {domain_evt.feed_event_id} into DB...")
            self._upsert_match_projection(domain_evt)

    def _upsert_match_projection(
        self,
        evt: DomainEvent,
    ):
        """
        Construct a MatchProjectionModel from the current in-memory read-model
        for the event in question, then upsert via repository.
        """
        # 1) Look up the event data from the in-memory read model:
        match_state = self.match_projection.get_current_match_state(self.match_id)
        event_entry = match_state.get("events_by_id", {}).get(evt.feed_event_id)
        if not event_entry:
            return  # Shouldn't happen if the projection is in sync.

        # 2) Create or fill out the ORM model
        mp = MatchProjectionModel(
            match_id=self.match_id,
            event_id=evt.feed_event_id,
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

        # 3) Upsert
        self.match_projection_repo.save_current_state(mp)
        
    def _compare_event_fields(self, existing: EventInMatch, new_event: EventInMatch):
        """
        Compare all dataclass fields except feed_event_id/local_event_id.
        If qualifiers differ, do a custom comparison.
        Return (changed_fields, old_fields).
        """
        changed_fields = {}
        old_fields = {}

        # Get all field names from the dataclass
        field_names = [f.name for f in fields(EventInMatch)]
        # Exclude feed_event_id/local_event_id if they never change
        field_names_to_check = [
            fname for fname in field_names
            if fname not in ("feed_event_id", "local_event_id")
        ]

        for field_name in field_names_to_check:
            old_val = getattr(existing, field_name, None)
            new_val = getattr(new_event, field_name, None)

            if field_name == "qualifiers":
                # Compare them with our helper function
                print(f"old_val: {old_val}")
                print(f"new_val: {new_val}")
                if not Qualifier.qualifiers_are_equal(old_val, new_val):
                    changed_fields[field_name] = new_val
                    old_fields[field_name] = old_val
            else:
                # Normal direct comparison for other fields
                if old_val != new_val:
                    changed_fields[field_name] = new_val
                    old_fields[field_name] = old_val

        return changed_fields, old_fields

if __name__ == "__main__":
    event_store = PostgresEventStore(session_factory=get_session)
    provider = OptaStreamer(match_id="cdvojt8rvxgk077kd9bvyj3f8", event_store=event_store)
    provider.run_live_stream()
