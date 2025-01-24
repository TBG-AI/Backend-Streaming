# tests/conftest.py

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from backend_streaming.providers.opta.infra.models import Base, TeamModel, PlayerModel
from backend_streaming.providers.opta.domain.value_objects.sport_event_enums import EventType

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
def seeded_db(session):
    """
    Inserts some default teams/players into the DB.
    Returns the session so tests can query or insert more if they want.
    """
    # Insert teams
    home_team = TeamModel(team_id="Home", name="Mock Home Team")
    away_team = TeamModel(team_id="Away", name="Mock Away Team")
    session.add_all([home_team, away_team])
    
    # Insert players
    p1 = PlayerModel(player_id="p1", first_name="Player", last_name="One", team_id="Home")
    p2 = PlayerModel(player_id="p2", first_name="Player", last_name="Two", team_id="Away")
    p3 = PlayerModel(player_id="p3", first_name="Player", last_name="Three", team_id="Home")
    session.add_all([p1, p2, p3])

    session.commit()
    return session

@pytest.fixture
def mock_get_events():
    """
    Returns a function that mimics fetching real-time events.
    Events IDs are now generated based on match_id for uniqueness.
    """
    idx_map = {}
    
    def generate_event_ids(match_id: str, base_id: int) -> tuple[int, int]:
        """Generate unique event IDs based on match_id"""
        # Use the sum of ASCII values of match_id as an offset
        offset = sum(ord(c) for c in match_id) * 1000
        return offset + base_id, base_id

    async def _mock_get_events(match_id: str):
        nonlocal idx_map
        if match_id not in idx_map:
            idx_map[match_id] = 0
            
        events_sequence = [
            # First call - initial match events
            [
                {
                    "id": generate_event_ids(match_id, 1001)[0],
                    "eventId": generate_event_ids(match_id, 1)[1],
                    "typeId": 34,  # Pass
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
                        {"qualifierId": 140, "value": "p3"}  # Pass recipient
                    ],
                    "timeStamp": "2024-03-20T19:00:00.000Z",
                    "lastModified": "2024-03-20T19:00:00Z",
                },
                {
                    "id": generate_event_ids(match_id, 1002)[0],
                    "eventId": generate_event_ids(match_id, 2)[1],
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
            # Second call - updated event plus new throw-in
            [
                {
                    "id": generate_event_ids(match_id, 1001)[0],  # Same ID as first event
                    "eventId": generate_event_ids(match_id, 1)[1],
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
                    "id": generate_event_ids(match_id, 1002)[0],  # Same ID as second event
                    "eventId": generate_event_ids(match_id, 2)[1],
                    "typeId": 3,  # changed from shot to something else
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
                {
                    "id": generate_event_ids(match_id, 1003)[0],
                    "eventId": generate_event_ids(match_id, 11)[1],
                    "typeId": 65,  # throw-in
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
            # Third call - match end
            [
                {
                    "id": generate_event_ids(match_id, 9999)[0],
                    "eventId": generate_event_ids(match_id, 999)[1],
                    "typeId": EventType.END.value,
                    "periodId": 2,
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
        
        batch = events_sequence[idx_map[match_id] % len(events_sequence)]
        idx_map[match_id] += 1
        return {"liveData": {"event": batch}}

    return _mock_get_events
