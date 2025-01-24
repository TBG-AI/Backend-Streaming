# tests/test_scheduler_e2e.py

import pytest
import asyncio
import datetime
from datetime import timezone, timedelta
from unittest.mock import patch

from backend_streaming.providers.opta.services.opta_scheduler import (
    schedule_matches_for_tournament
)
from backend_streaming.providers.opta.services.opta_provider import OptaStreamer
from backend_streaming.providers.opta.infra.repo.event_store.postgres import PostgresEventStore
from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository
from backend_streaming.providers.opta.infra.models import MatchProjectionModel
from backend_streaming.providers.opta.constants import EPL_TOURNAMENT_ID

@pytest.mark.asyncio
async def test_scheduler_e2e_with_mock_db(seeded_db, mock_get_events, mocker):
    """
    End-to-end style test:
      1) Mock get_tournament_schedule to return matches that start soon.
      2) Run schedule_matches_for_tournament to schedule tasks.
      3) Let them run briefly.
      4) Verify data is stored in the 'seeded_db' via normal streaming logic.
      5) Use mock db, mock fetch_events_func, and mock start_stream to test the scheduler.

    We'll do a minimal approach: each scheduled match starts near 'now', so
    the "10 minutes before" logic results in an immediate or short delay.
    """

    # 1) Patch get_tournament_schedule to produce 2 matches that start in 30 seconds
    mock_get_schedule = mocker.patch(
        "backend_streaming.providers.opta.services.opta_scheduler.get_tournament_schedule"
    )
    now_utc = datetime.datetime.now(timezone.utc)
    
    # Match 1: starts in 10 minutes and 5 seconds from now
    # Match 2: starts in 10 seconds from now
    # Since streaming starts 10 minutes before match time:
    # - Match 1's stream should start in ~5 seconds
    # - Match 2's stream should have started ~9 minutes and 50 seconds ago
    match_time_1 = now_utc + timedelta(minutes=10, seconds=5)
    match_time_2 = now_utc + timedelta(minutes=10, seconds=5)

    fake_schedule = {
        "matchDate": [
            {
                "match": [
                    {
                        "id": "scheduler-e2e-match-A",
                        "date": match_time_1.strftime("%Y-%m-%dZ"),
                        "time": match_time_1.strftime("%H:%M:%SZ"),
                    },
                    {
                        "id": "scheduler-e2e-match-B",
                        "date": match_time_2.strftime("%Y-%m-%dZ"),
                        "time": match_time_2.strftime("%H:%M:%SZ"),
                    },
                ]
            }
        ]
    }
    mock_get_schedule.return_value = fake_schedule

    # 2) We also need to ensure that when 'start_stream' is called, we use an OptaStreamer
    #    that references our 'seeded_db' for event_store / match_projection. 
    #    By default, your start_stream might create a new OptaStreamer without using seeded_db.
    #    We can patch 'OptaStreamer' or 'start_stream' to inject these.

    # Option A) Patch the entire 'start_stream' function to create a custom streamer with seeded_db
    # Option B) Patch 'OptaStreamer.__init__' so it uses our seeded_db.

    # Let's do Option B: patch OptaStreamer so each time it's created, it uses our seeded_db-based repos.

    original_init = OptaStreamer.__init__
    def patched_init(self, match_id, *args, **kwargs):
        # Force event_store & projection_repo to point at seeded_db
        session_factory = lambda: seeded_db
        event_store = PostgresEventStore(session_factory)
        match_proj_repo = MatchProjectionRepository(session_factory)

        original_init(self,
            match_id=match_id,
            event_store=event_store,
            match_projection_repo=match_proj_repo,
            fetch_events_func=mock_get_events,
            *args, **kwargs
        )

    mocker.patch.object(OptaStreamer, "__init__", patched_init)

    # 3) Now run the scheduler function
    await schedule_matches_for_tournament(EPL_TOURNAMENT_ID, interval=1.0)

    # We won't run the 'while True' loop. The tasks have been scheduled with create_task.
    # Let's wait a few seconds so if the code "starts immediately," it has time to insert DB data.
    await asyncio.sleep(10)

    # 4) Check the DB
    # Because your streaming logic calls run_live_stream (which presumably fetches some events
    # or uses mock fetch_events_func?), the aggregator might insert into match_projection.

    # Let's see what's in the match_projection table for 'scheduler-e2e-match-A' and 'scheduler-e2e-match-B'
    finalA = seeded_db.query(MatchProjectionModel).filter_by(match_id="scheduler-e2e-match-A").all()
    finalB = seeded_db.query(MatchProjectionModel).filter_by(match_id="scheduler-e2e-match-B").all()
    print(f"[Final] A: {finalA}")
    print(f"[Final] B: {finalB}")

    
    assert len(finalA) > 0, "Expected at least some data for matchA"
    assert len(finalB) > 0, "Expected at least some data for matchB"
    assert True, "Scheduler E2E test completed without error."
