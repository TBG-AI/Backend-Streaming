# tests/test_opta_streamer.py

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from backend_streaming.providers.opta.domain.value_objects.sport_event_enums import EventType
from backend_streaming.providers.opta.infra.models import Base, TeamModel, PlayerModel, MatchProjectionModel

from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository
from backend_streaming.providers.opta.infra.repo.event_store.postgres import PostgresEventStore
from backend_streaming.providers.opta.services.opta_provider import OptaStreamer

# ======================================
# 1. Configure test engine & create DB
# ======================================
@pytest.fixture(scope="session")
def test_engine():
    """
    Creates an in-memory SQLite engine (or could be a test Postgres DB).
    Creates all tables once for the test session, then drops them at the end.
    """
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(bind=engine)  # create all tables
    yield engine
    Base.metadata.drop_all(bind=engine)    # teardown - drop all tables

# ======================================
# 2. Transactional Session Fixture
# ======================================
@pytest.fixture
def session(test_engine):
    """
    For each test:
      - Begin a transaction
      - Yield a Session bound to that transaction
      - Roll back after test finishes
    """
    connection = test_engine.connect()
    trans = connection.begin()
    SessionLocal = sessionmaker(bind=connection)
    db_session = SessionLocal()

    try:
        yield db_session
    finally:
        db_session.close()
        trans.rollback()
        connection.close()
        
@pytest.fixture
def mock_get_session(session):
    # Return a function that yields the existing test session
    return lambda: session

# ======================================
# 3. Seed Data Fixture
# ======================================
@pytest.fixture
def seeded_db(session):
    # Insert teams
    home_team = TeamModel(team_id="Home", name="Mock Home Team")
    away_team = TeamModel(team_id="Away", name="Mock Away Team")
    session.add(home_team)
    session.add(away_team)
    
    # Insert players
    p1 = PlayerModel(player_id="p1", first_name="Player", last_name="One", team_id="Home")
    p2 = PlayerModel(player_id="p2", first_name="Player", last_name="Two", team_id="Away")
    p3 = PlayerModel(player_id="p3", first_name="Player", last_name="Three", team_id="Home")
    session.add_all([p1, p2, p3])
    
    # Commit so the data is visible for the test
    session.commit()
    return session

# ======================================
# 4. Mock events function
# ======================================
idx = 0
def mock_get_events(match_id: str):
    global idx
    events_sequence = [
        # First call - Initial match events
        [
            {
                "id": 1001,
                "eventId": 1,
                "typeId": 34,  # Pass
                "periodId": 1,
                "timeMin": 0,
                "timeSec": 0,
                "contestantId": "Home",
                "playerId": "p1",
                "playerName": "Player One",
                "outcome": 1,  # Successful
                "x": 50.0,
                "y": 50.0,
                "qualifier": [
                    {"qualifierId": 140, "value": "p3"}  # Pass recipient
                ],
                "timeStamp": "2024-03-20T19:00:00.000Z",
                "lastModified": "2024-03-20T19:00:00Z",
            },
            {
                "id": 1002,
                "eventId": 2,
                "typeId": 1,  # Shot
                "periodId": 1,
                "timeMin": 2,
                "timeSec": 15,
                "contestantId": "Home",
                "playerId": "p3",
                "playerName": "Player Three",
                "outcome": 0,  # Missed
                "x": 85.0,
                "y": 45.0,
                "qualifier": [
                    {"qualifierId": 233, "value": "High"}  # Shot placement
                ],
                "timeStamp": "2024-03-20T19:02:15.000Z",
                "lastModified": "2024-03-20T19:02:15Z",
            }
        ],
        # Second call - Updated event plus new corner
        [
            # eventID 2 type id changed from 1 to 3
            {
                "id": 1001,
                "eventId": 1,
                "typeId": 34,
                "periodId": 1,
                "timeMin": 0,
                "timeSec": 0,
                "contestantId": "Home",
                "playerId": "p1",
                "playerName": "Player One",
                "outcome": 1,
                "x": 50.0,
                "y": 50.0,
                "qualifier": [
                    {"id": "q1", "qualifierId": 140, "value": "p3"}
                ],
                "timeStamp": "2024-03-20T19:00:00.000Z",
                "lastModified": "2024-03-20T19:00:00Z",
            },
            {
                "id": 1002,
                "eventId": 2,
                "typeId": 3,
                "periodId": 1,
                "timeMin": 2,
                "timeSec": 15,
                "contestantId": "Home",
                "playerId": "p3",
                "playerName": "Player Three",
                "outcome": 0,
                "x": 85.0,
                "y": 45.0,
                "qualifier": [
                    {"id": "q2", "qualifierId": 233, "value": "High"}
                ],
                "timeStamp": "2024-03-20T19:02:15.000Z",
                "lastModified": "2024-03-20T19:02:15Z",
            },
            # Changed from corner to throw-in (typeId: 65)
            {
                "id": 1003,
                "eventId": 11,
                "typeId": 65,  # Throw-in instead of corner
                "periodId": 1,
                "timeMin": 77,
                "timeSec": 0,
                "contestantId": "Away",
                "playerId": "p2",
                "playerName": "Player Two",
                "outcome": 1,
                "x": 100.0,
                "y": 0.0,
                "qualifier": [
                    {"id": "q3", "qualifierId": 236, "value": "Long"}
                ],
                "timeStamp": "2024-03-20T19:05:00.000Z",
                "lastModified": "2024-03-20T19:05:00Z",
            }
        ],
        # Third call - Match end
        [
            # Previous events remain unchanged...
            {
                "id": 9999,
                "eventId": 999,
                "typeId": EventType.END.value,  # Match end
                "periodId": 2,  # Full time
                "timeMin": 90,
                "timeSec": 0,
                "outcome": None,
                "x": None,
                "y": None,
                "qualifier": [{"id": "q4", "qualifierId": 209}],
                "timeStamp": "2024-03-20T20:45:00Z",
                "lastModified": "2024-03-20T20:45:00Z",
            }
        ]
    ]
    
    current_events = events_sequence[idx % len(events_sequence)]
    idx += 1
    return {"liveData": {"event": current_events}}


# ======================================
# 5. Actual Tests
# ======================================

def test_opta_streamer_with_mock_1(seeded_db):
    """
    Uses the 'seeded_db' fixture (which inserts teams/players),
    checks the aggregator in-memory, then confirms rows in match_projection.
    """
    # 1) Create a custom PostgresEventStore but backed by the test session
    from backend_streaming.providers.opta.infra.repo.event_store.postgres import PostgresEventStore
    test_event_store = PostgresEventStore(session_factory=lambda: seeded_db)

    # 2) Create a MatchProjectionRepository that also uses the test session
    from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository
    mock_match_projection_repo = MatchProjectionRepository(session_factory=lambda: seeded_db)

    # 3) Pass both into OptaStreamer
    streamer = OptaStreamer(
        match_id="test-match",
        fetch_events_func=mock_get_events,
        event_store=test_event_store,  
        match_projection_repo=mock_match_projection_repo
    )
    streamer.run_live_stream(interval=0.3)

    # 4) Assert on the in-memory database
    print("aaa")
    match_projections = seeded_db.query(MatchProjectionModel).all()
    print("match_projections: ", match_projections)
    assert len(match_projections) == 4, "Expected 4 events inserted into match_projection"

    e4321 = seeded_db.query(MatchProjectionModel).filter_by(event_id=1003).one()
    assert e4321.player_id == "p2"


def test_opta_streamer_with_mock_2(seeded_db):
    """
    Same as above, but verifying we can use the repository directly.
    """
      # 1) Create a custom PostgresEventStore but backed by the test session
    from backend_streaming.providers.opta.infra.repo.event_store.postgres import PostgresEventStore
    test_event_store = PostgresEventStore(session_factory=lambda: seeded_db)

    # 2) Create a MatchProjectionRepository that also uses the test session
    from backend_streaming.providers.opta.infra.repo.match_projection import MatchProjectionRepository
    mock_match_projection_repo = MatchProjectionRepository(session_factory=lambda: seeded_db)

    streamer = OptaStreamer(
        match_id="test-match",
        fetch_events_func=mock_get_events,
        match_projection_repo=mock_match_projection_repo,
        event_store=test_event_store
    )
    streamer.run_live_stream(interval=0.3)

    stored_projections = mock_match_projection_repo.get_match_state("test-match")
    print("stored_projections: ", stored_projections)
    assert len(stored_projections) == 4
    
    event_store = test_event_store.load_events("test-match")
    print("event_store: ", [type(e) for e in event_store])
    assert len(event_store) == 6

