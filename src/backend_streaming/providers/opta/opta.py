# Directory: src/backend_streaming/providers/opta/opta.py
# import json
# import time
# import logging
# from typing import List, Dict, Optional
# from datetime import datetime

# from backend_streaming.providers.base import BaseProvider
# from backend_streaming.providers.opta.oath import get_auth_headers
# from backend_streaming.providers.opta.api import get_match_events
# from backend_streaming.providers.opta.constants import EPL_TOURNAMENT_ID
# from backend_streaming.providers.opta.domain.event_types.event_enums import EventType
# from backend_streaming.utils.logging import setup_logger

# # Domain/Infrastructure imports:
# from backend_streaming.providers.opta.domain.match_aggregate import MatchAggregate
# from backend_streaming.providers.opta.infra.event_store import LocalFileEventStore
# from backend_streaming.providers.opta.infra.repo.match_repo import MatchRepository

# class OptaProvider:
#     def __init__(self, 
#                  match_id: str,
#                  tournament_id: str = EPL_TOURNAMENT_ID,
#                  log_file: Optional[str] =  None,
#                  event_store_filename: Optional[str] = None):
#         """
#         We'll pass in a match_id that we want to track.
#         We'll keep a local file event store, so we can replay domain events across runs.
#         """
#         self.access_token = get_auth_headers()
#         self.headers = {
#             'Authorization': f'Bearer {self.access_token}',
#             'Content-Type': 'application/json'
#         }
#         self.tournament_id = tournament_id
        
#         if log_file is None:
#             log_file = f"{match_id}.log"
        
#         if event_store_filename is None:
#             event_store_filename = f"{match_id}.json"
        
#         self.logger = setup_logger(__name__, log_file=log_file)
        
#         # We use a file-based event store + repository
#         self.event_store = LocalFileEventStore(filename=event_store_filename)
#         self.match_repo = MatchRepository(self.event_store)

#         # Load aggregator from existing domain events
#         self.agg = self.match_repo.load(match_id)  # replays everything
#         self.match_id = match_id

#         # We'll track if we detect the match ended
#         self.finished = False

#     def run_live_stream(self, interval: int = 30):
#         """
#         Continuously poll the API for new raw events, integrate them into our aggregator,
#         and persist them as domain events.
#         """
#         self.logger.info(f"Starting streaming for match={self.match_id}...")

#         while not self.finished:
#             try:
#                 # 1) Fetch raw data from Opta
#                 raw_data = get_match_events(self.match_id)
#                 live_data = raw_data.get("liveData", {})
#                 raw_events = live_data.get("event", [])
                
#                 # 2) For each raw event, see if it's new or changed
#                 self._process_raw_events(raw_events)

#                 # 3) Save aggregator => persists new domain events
#                 self.match_repo.save(self.agg)

#             except Exception as e:
#                 self.logger.error(f"Error fetching events for match {self.match_id}: {e}", exc_info=True)

#             # Sleep for the polling interval
#             time.sleep(interval)

#         self.logger.info(f"Match {self.match_id} is finished. Exiting stream.")

#     def _process_raw_events(self, raw_events: List[Dict]):
#         """
#         Compare each raw event with aggregator state, detect changes,
#         emit domain events via aggregator handle methods.
#         Also detect if the match ended.
#         """
#         for ev in raw_events:
#             feed_event_id = ev["id"]

#             # If aggregator doesn't have it yet, it's new
#             if feed_event_id not in self.agg.events:
#                 self.agg.handle_new_event(ev)
#             else:
#                 # Possibly detect type change
#                 existing = self.agg.events[feed_event_id]
#                 old_type = existing.type_id
#                 new_type = ev["typeId"]

#                 if old_type != new_type:
#                     self.agg.handle_type_changed(feed_event_id, old_type, new_type)

#                 # Possibly detect qualifier changes or additional logic

#             # Also see if this event is an 'END' with period=2 => match finished
#             if ev["typeId"] == EventType.END and ev.get("periodId") == 2:
#                 self.finished = True



# if __name__ == "__main__":
#     provider = OptaProvider(match_id="c0i4chcg41suds6581fj8k7bo")
#     provider.run_live_stream()
