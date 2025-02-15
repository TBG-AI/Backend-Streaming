import pytest
import asyncio
from backend_streaming.providers.opta.services.opta_provider import OptaStreamer
from backend_streaming.providers.opta.infra.repo.event_store.postgres import PostgresEventStore
from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository
from backend_streaming.providers.opta.infra.models import MatchProjectionModel

@pytest.mark.asyncio
async def test_multiple_matches_concurrently(seeded_db, mock_get_events):
    """
    Runs two matches in parallel using the same mock_get_events fixture.
    Each match will proceed through the same sequence of events, but on its own timeline.
    """

    session_factory = lambda: seeded_db
    event_storeA = PostgresEventStore(session_factory)
    event_storeB = PostgresEventStore(session_factory)

    proj_repoA = MatchProjectionRepository(session_factory)
    proj_repoB = MatchProjectionRepository(session_factory)

    # Create streamers for matchA and matchB
    streamerA = OptaStreamer(
        match_id="matchA",
        fetch_events_func=mock_get_events,
        event_store=event_storeA,
        match_projection_repo=proj_repoA,
    )
    streamerB = OptaStreamer(
        match_id="matchB",
        fetch_events_func=mock_get_events,
        event_store=event_storeB,
        match_projection_repo=proj_repoB,
    )

    # Launch them concurrently
    taskA = asyncio.create_task(streamerA.run_live_stream(interval=0.5))
    taskB = asyncio.create_task(streamerB.run_live_stream(interval=0.5))

    # Let them run for some time (partial checks)
    await asyncio.sleep(1.0)

    # Partial check for matchA
    partialA = seeded_db.query(MatchProjectionModel).filter_by(match_id="matchA").all()
    partialB = seeded_db.query(MatchProjectionModel).filter_by(match_id="matchB").all()
    print("[Partial] matchA:", partialA)
    print("[Partial] matchB:", partialB)

    # We expect each match to have at least 1 or 2 events by now
    assert len(partialA) >= 1, "matchA should have partial events"
    assert len(partialB) >= 1, "matchB should have partial events"

    # Wait for both to finish
    await asyncio.gather(taskA, taskB)

    # Final checks
    finalA = seeded_db.query(MatchProjectionModel).filter_by(match_id="matchA").all()
    finalB = seeded_db.query(MatchProjectionModel).filter_by(match_id="matchB").all()

    print(f"[Final] matchA: {type(finalA)} - {finalA}")
    print(f"[Final] matchB: {type(finalB)} - {finalB}")
    
    domain_eventsA = event_storeA.load_events('matchA')
    domain_eventsB = event_storeB.load_events('matchB')
    print(f"[Domain] matchA: {[evt for evt in domain_eventsA]}")
    print(f"[Domain] matchB: {[evt for evt in domain_eventsB]}")

    assert len(finalA) == 4, "Expected matchA to have 4 final events (example)"
    assert len(finalB) == 4, "Expected matchB to have 4 final events (example)"

    assert len(domain_eventsA) == 5, "Expected matchA to have 5 domain events"
    assert len(domain_eventsB) == 5, "Expected matchB to have 5 domain events"
