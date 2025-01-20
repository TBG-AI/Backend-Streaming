#scripts/sddp_demo.py
import os
import time
import json
import hashlib
import logging
import requests
from websocket import create_connection, WebSocketConnectionClosedException
from datetime import datetime, timedelta, timezone

logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------------
#                        C O N F I G   V A R I A B L E S
# ---------------------------------------------------------------------------

OUTLET_KEY = os.environ.get("STATS_PERFORM_OUTLET_KEY", "1n6ieqbe52syk1o2dvueifegcc")
SECRET_KEY = os.environ.get("STATS_PERFORM_SECRET_KEY", "1a5t6y9wozyvc1fz0kbog6j4h9")

OAUTH_URL = f"https://oauth.performgroup.com/oauth/token/{OUTLET_KEY}?_fmt=json&_rt=b"
SDAPI_BASE_URL = "https://api.performfeeds.com"
SDDP_WS_URL = "wss://sddp-soccer.performgroup.io"

# For demonstration: the MA1 endpoint for fixtures
MA1_ENDPOINT = f"{SDAPI_BASE_URL}/soccerdata/match/{OUTLET_KEY}?_rt=b&_fmt=json"

# For demonstration: use the MA28DP "matchStream" feed. 
# If you actually want MA3DP or another feed, adjust here:
SDDP_FEED_NAME = "matchStream"

MAX_RETRIES = 5
BASE_DELAY_SEC = 2


# ---------------------------------------------------------------------------
#                          O A U T H   F L O W
# ---------------------------------------------------------------------------

def generate_oauth_hash(outlet_key, secret_key):
    """
    Build the SHA512 hash of: OUTLET_KEY + TIMESTAMP + SECRET_KEY.
    Return (hash_value, timestamp_in_ms).
    """
    timestamp = int(time.time() * 1000)  # current UNIX time in ms
    combo = f"{outlet_key}{timestamp}{secret_key}"
    unique_hash = hashlib.sha512(combo.encode("utf-8")).hexdigest()
    return unique_hash, timestamp


def fetch_access_token():
    """
    Generate the SHA512 hash and post to the OAuth endpoint.
    Return the 'access_token'.
    """
    unique_hash, timestamp = generate_oauth_hash(OUTLET_KEY, SECRET_KEY)

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {unique_hash}",
        "Timestamp": str(timestamp),
    }
    data = {
        "grant_type": "client_credentials",
        "scope": "b2b-feeds-auth",
    }

    logging.info("Requesting OAuth token...")
    resp = requests.post(OAUTH_URL, headers=headers, data=data, timeout=15)
    resp.raise_for_status()

    j = resp.json()
    logging.info(f"OAuth response: {j}")
    token = j["access_token"]
    logging.info(f"Successfully obtained access_token from OAuth. Token: {token[:10]}...")
    return token


# ---------------------------------------------------------------------------
#                           S D A P I   C A L L
# ---------------------------------------------------------------------------

def get_fixture_uuids(access_token, limit=5):
    """
    Calls the MA1 feed, filters for EPL, and returns a list of fixture UUIDs.
    By default, returns up to 'limit' fixtures.
    
    If you'd like to refine "recent" logic (e.g., ignoring past matches or
    only near-future matches), implement additional date/time checks below.
    """
    headers = {"Authorization": f"Bearer {access_token}"}

    logging.info(f"Fetching fixtures from MA1: {MA1_ENDPOINT}")
    resp = requests.get(MA1_ENDPOINT, headers=headers, timeout=15)
    resp.raise_for_status()

    data = resp.json()
    matches = data.get("match", [])
    logging.info(f"Matches: {matches}")
    if not matches:
        logging.warning("No fixtures found in 'matches'.")
        return []

    # Filter for EPL competition only
    epl_matches = [
        m for m in matches 
        if m["matchInfo"]["competition"].get("competitionCode") == "EPL"
    ]
    if not epl_matches:
        logging.warning("No EPL fixtures found in 'matches'.")
        return []

    # Get current time and 7 days from now in UTC
    now = datetime.now(timezone.utc)
    seven_days_later = now + timedelta(days=7)

    # Filter for matches within next 7 days
    epl_matches = [
        m for m in epl_matches
        if datetime.strptime(m["matchInfo"]["date"], "%Y-%m-%dZ").replace(tzinfo=timezone.utc) <= seven_days_later
        and datetime.strptime(m["matchInfo"]["date"], "%Y-%m-%dZ").replace(tzinfo=timezone.utc) >= now
    ]

    # Sort by date
    epl_matches = sorted(
        epl_matches, 
        key=lambda x: datetime.strptime(x["matchInfo"]["date"], "%Y-%m-%dZ").replace(tzinfo=timezone.utc)
    )
    epl_matches = epl_matches[:limit]

    fixture_uuids = [m["matchInfo"]["id"] for m in epl_matches]
    logging.info(f"Selected EPL fixture UUIDs: {fixture_uuids}")
    return fixture_uuids


def get_match_events(access_token, fixture_uuid):
    """
    Fetches match events for a specific fixture from the SDAPI matchevent feed.
    
    Args:
        access_token (str): Valid OAuth access token
        fixture_uuid (str): The fixture UUID to fetch events for
        
    Returns:
        dict: JSON response containing match events data
    """
    # Construct the matchevent endpoint URL with the correct path structure
    events_endpoint = (
        f"{SDAPI_BASE_URL}/soccerdata/matchevent/{OUTLET_KEY}/{fixture_uuid}"
        "?_rt=b&_fmt=json"
    )
    
    headers = {"Authorization": f"Bearer {access_token}"}
    
    logging.info(f"Fetching match events from: {events_endpoint}")
    resp = requests.get(events_endpoint, headers=headers, timeout=15)
    resp.raise_for_status()
    
    data = resp.json()
    if not data:
        logging.warning(f"No match events found for fixture {fixture_uuid}")
        return {}
        
    logging.info(f"Successfully retrieved match events for fixture {fixture_uuid}")
    return data


# ---------------------------------------------------------------------------
#                 S D D P   ( D E L T A   P U S H )   W E B S O C K E T
# ---------------------------------------------------------------------------

def create_outlet_auth_message(outlet_key):
    """
    The message needed once the websocket is open to authenticate your outlet key.
    """
    return json.dumps({
        "outlet": {
            "OutletKeyService": {
                "outletKey": outlet_key
            }
        }
    })


def create_subscribe_message(
    fixture_uuid, 
    feed_name=SDDP_FEED_NAME,
    language="en-gb",
    content_type="autoLtc",
    snapshot=False
):
    """
    Build a "subscribe" message for SDDP.

    For the matchStream (MA28DP) feed, you typically pass feed=[{"feed":..., "lang":..., "contentType":...}] 
    so you can subscribe to multiple content types or languages. 
    For a simpler example, we do just one feed, one language, one content type.
    
    If you want the 'snapshot' feature, you can add "name": "snapShot" instead, or 
    set "subscribe+snapShot" combined if your use-case wants a historical catch-up.
    """
    # If you want to request snapshot data from the start, there's a separate approach. 
    # The snippet below is a minimal subscribe example for "autoLtc" commentary, 
    # but you can adapt to multiple contentTypes or "manualLtc"/"insight" etc.

    feed_obj = {
        "feed": feed_name, 
        "lang": language, 
        "contentType": content_type
    }

    # The "name" can be "subscribe" or "snapShot" or "subscribeSnapshot" depending on your needs.
    # We'll keep it as "subscribe" for live updates:
    message_content = {
        "name": "subscribe",
        "fixtureUuid": fixture_uuid,
        "feed": [feed_obj]
    }

    return json.dumps({"content": message_content})


def log_to_file(msg):
    """
    Helper to log an incoming WS message to a file, labeled by fixture UUID.
    Prepends the local time we *received* the message.

    We'll look for the fixture UUID in the message:
      msg["content"]["liveData"]["matchDetails"]["id"]
    If not found, we'll store in "unknown_fixture.log".
    """
    time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # Convert entire message to JSON string
    raw_str = json.dumps(msg, ensure_ascii=False)

    # Attempt to parse fixture UUID from the message
    try:
        fixture_uuid = msg["content"]["liveData"]["matchDetails"]["id"]
    except KeyError:
        fixture_uuid = "unknown_fixture"

    # Build the log filename
    log_filename = f"logs/fixture_{fixture_uuid}.log"
    os.makedirs(os.path.dirname(log_filename), exist_ok=True)

    # Append a line with local timestamp + raw JSON
    with open(log_filename, "a", encoding="utf-8") as f:
        f.write(f"[{time_str}] {raw_str}\n")


def run_sddp_websocket(fixtures_list, token):
    """
    Connect to SDDP WebSocket once, authorize the outlet, then subscribe to all
    fixtures in `fixtures_list`. Keep reading messages, logging them to files.
    Retry logic on disconnection, up to MAX_RETRIES.
    """
    attempt = 0
    while True:
        try:
            logging.info(f"Attempting to connect to SDDP: {SDDP_WS_URL} (attempt #{attempt+1})")
            ws = create_connection(SDDP_WS_URL, header={"Authorization": f"Bearer {token}"})
            logging.info("WebSocket open. Sending outlet authorization message...")

            # 1) Send outlet auth
            outlet_auth_msg = create_outlet_auth_message(OUTLET_KEY)
            ws.send(outlet_auth_msg)

            # 2) Read messages in a loop
            subscribed_yet = False
            while True:
                raw_msg = ws.recv()  # Blocks until message or disconnection
                if not raw_msg:
                    raise WebSocketConnectionClosedException("No data. Possibly disconnected.")

                msg = json.loads(raw_msg)
                logging.info(f"Received WS message:\n{json.dumps(msg, indent=2)}")

                # Always log to file (including 'outlet' or 'is_subscribed' messages):
                log_to_file(msg)

                # Check if the message says "is_authorised"
                if (
                    "outlet" in msg and 
                    msg["outlet"].get("msg") == "is_authorised" and 
                    not subscribed_yet
                ):
                    logging.info("Outlet is authorised. Now subscribing to each fixture...")
                    subscribed_yet = True

                    # Subscribe to each fixture:
                    for fx_id in fixtures_list:
                        sub_msg = create_subscribe_message(
                            fixture_uuid=fx_id,
                            feed_name=SDDP_FEED_NAME, 
                            language="en-gb",
                            content_type="autoLtc"
                        )
                        logging.info(f"Subscribing to fixture {fx_id} with feed={SDDP_FEED_NAME}")
                        ws.send(sub_msg)

                # Add more logic for "is_subscribed", "is_unsubscribed", 
                # or application-specific handling of match data...

        except (WebSocketConnectionClosedException, ConnectionRefusedError) as e:
            logging.warning(f"WebSocket disconnected: {e}")
            attempt += 1

            if MAX_RETRIES is not None and attempt > MAX_RETRIES:
                logging.error("Max retries reached. Giving up on WebSocket.")
                break

            # Exponential backoff
            delay = BASE_DELAY_SEC * (2 ** (attempt - 1))
            logging.info(f"Will attempt reconnect in {delay:.1f} seconds...")
            time.sleep(delay)
            continue

        except Exception as e:
            logging.exception("Unexpected error in WebSocket loop:", exc_info=e)
            break
        else:
            # If we exit the read loop normally (no exception), decide if you want to reconnect or end.
            logging.info("WebSocket loop ended normally.")
            break

    logging.info("WebSocket client is stopping.")


# ---------------------------------------------------------------------------
#                               M A I N
# ---------------------------------------------------------------------------

def get_tournament_calendar(access_token):
    """
    Fetches active and authorized tournaments from the OT2 feed.
    """
    endpoint = (
        f"{SDAPI_BASE_URL}/soccerdata/tournamentcalendar/{OUTLET_KEY}"
        "/active/authorized?_rt=b&_fmt=json"
    )
    
    headers = {"Authorization": f"Bearer {access_token}"}
    
    logging.info(f"Fetching tournament calendar from: {endpoint}")
    resp = requests.get(endpoint, headers=headers, timeout=15)
    resp.raise_for_status()
    
    data = resp.json()
    logging.info(f"Retrieved tournament calendar data")
    return data

def get_tournament_schedule(access_token, tournament_calendar_uuid):
    """
    Fetches tournament schedule from MA0 feed for a specific tournament.
    """
    endpoint = (
        f"{SDAPI_BASE_URL}/soccerdata/tournamentschedule/{OUTLET_KEY}"
        f"?_rt=b&_fmt=json&tmcl={tournament_calendar_uuid}"
    )
    
    headers = {"Authorization": f"Bearer {access_token}"}
    
    logging.info(f"Fetching tournament schedule for {tournament_calendar_uuid}")
    resp = requests.get(endpoint, headers=headers, timeout=15)
    resp.raise_for_status()
    
    data = resp.json()
    logging.info(f"Retrieved tournament schedule data")
    return data

def get_standings(access_token, tournament_calendar_uuid):
    """
    Fetches standings from TM2 feed for a specific tournament.
    """
    endpoint = (
        f"{SDAPI_BASE_URL}/soccerdata/standings/{OUTLET_KEY}"
        f"?_rt=b&_fmt=json&tmcl={tournament_calendar_uuid}"
    )
    
    headers = {"Authorization": f"Bearer {access_token}"}
    
    logging.info(f"Fetching standings for tournament {tournament_calendar_uuid}")
    resp = requests.get(endpoint, headers=headers, timeout=15)
    resp.raise_for_status()
    
    data = resp.json()
    logging.info(f"Retrieved standings data")
    return data

def get_contestant_participation(access_token, contestant_uuid):
    """
    Fetches contestant participation data from TM16 feed.
    """
    endpoint = (
        f"{SDAPI_BASE_URL}/soccerdata/contestantparticipation/{OUTLET_KEY}"
        f"?_rt=b&_fmt=json&ctst={contestant_uuid}"
    )
    
    headers = {"Authorization": f"Bearer {access_token}"}
    
    logging.info(f"Fetching participation data for contestant {contestant_uuid}")
    resp = requests.get(endpoint, headers=headers, timeout=15)
    resp.raise_for_status()
    
    data = resp.json()
    logging.info(f"Retrieved contestant participation data")
    return data

def get_referees(access_token, tournament_calendar_uuid):
    """
    Fetches referees data from PE3 feed for a specific tournament.
    """
    endpoint = (
        f"{SDAPI_BASE_URL}/soccerdata/referees/{OUTLET_KEY}"
        f"?_rt=b&_fmt=json&tmcl={tournament_calendar_uuid}"
    )
    
    headers = {"Authorization": f"Bearer {access_token}"}
    
    logging.info(f"Fetching referees for tournament {tournament_calendar_uuid}")
    resp = requests.get(endpoint, headers=headers, timeout=15)
    resp.raise_for_status()
    
    data = resp.json()
    logging.info("Retrieved referees data")
    return data

def get_rankings(access_token, tournament_calendar_uuid):
    """
    Fetches rankings data from PE4 feed for a specific tournament.
    """
    endpoint = (
        f"{SDAPI_BASE_URL}/soccerdata/rankings/{OUTLET_KEY}"
        f"?_rt=b&_fmt=json&tmcl={tournament_calendar_uuid}"
    )
    
    headers = {"Authorization": f"Bearer {access_token}"}
    
    logging.info(f"Fetching rankings for tournament {tournament_calendar_uuid}")
    resp = requests.get(endpoint, headers=headers, timeout=15)
    resp.raise_for_status()
    
    data = resp.json()
    logging.info("Retrieved rankings data")
    return data

def get_player_contract(access_token, person_uuid):
    """
    Fetches player contract data from PE12 feed.
    """
    endpoint = (
        f"{SDAPI_BASE_URL}/soccerdata/playercontract/{OUTLET_KEY}"
        f"/{person_uuid}?_rt=b&_fmt=json"
    )
    
    headers = {"Authorization": f"Bearer {access_token}"}
    
    logging.info(f"Fetching contract data for player {person_uuid}")
    resp = requests.get(endpoint, headers=headers, timeout=15)
    resp.raise_for_status()
    
    data = resp.json()
    logging.info("Retrieved player contract data")
    return data

def get_penalties_preview(access_token, fixture_uuid):
    """
    Fetches penalties preview data from MA19 feed.
    """
    endpoint = (
        f"{SDAPI_BASE_URL}/soccerdata/matchpenaltiespreview/{OUTLET_KEY}"
        f"/{fixture_uuid}?_rt=b&_fmt=json"
    )
    
    headers = {"Authorization": f"Bearer {access_token}"}
    
    logging.info(f"Fetching penalties preview for fixture {fixture_uuid}")
    resp = requests.get(endpoint, headers=headers, timeout=15)
    resp.raise_for_status()
    
    data = resp.json()
    logging.info("Retrieved penalties preview data")
    return data

def get_pass_matrix(access_token, fixture_uuid):
    """
    Fetches pass matrix and average formations from MA4 feed.
    """
    endpoint = (
        f"{SDAPI_BASE_URL}/soccerdata/passmatrix/{OUTLET_KEY}"
        f"/{fixture_uuid}?_rt=b&_fmt=json"
    )
    
    headers = {"Authorization": f"Bearer {access_token}"}
    
    logging.info(f"Fetching pass matrix for fixture {fixture_uuid}")
    resp = requests.get(endpoint, headers=headers, timeout=15)
    resp.raise_for_status()
    
    data = resp.json()
    logging.info("Retrieved pass matrix data")
    return data

def get_possessions(access_token, fixture_uuid):
    """
    Fetches possessions data from MA5 feed.
    """
    endpoint = (
        f"{SDAPI_BASE_URL}/soccerdata/possession/{OUTLET_KEY}"
        f"/{fixture_uuid}?_rt=b&_fmt=json"
    )
    
    headers = {"Authorization": f"Bearer {access_token}"}
    
    logging.info(f"Fetching possessions for fixture {fixture_uuid}")
    resp = requests.get(endpoint, headers=headers, timeout=15)
    resp.raise_for_status()
    
    data = resp.json()
    logging.info("Retrieved possessions data")
    return data

def main():
    try:
        # 1) Fetch OAuth access_token
        token = fetch_access_token()

        # 2) Get tournament calendar first
        calendar_data = get_tournament_calendar(token)
        logging.info(f"Tournament calendar sample:\n{json.dumps(calendar_data, indent=2)}")

        # 3) If we have tournaments, get schedule and standings for first one
        if calendar_data and "competition" in calendar_data:
            tournament_uuid = calendar_data["competition"][0]["tournamentCalendar"][0]["id"]
            
            run_sddp_websocket(tournament_uuid, token)
            
            # # Get tournament schedule
            # schedule = get_tournament_schedule(token, tournament_uuid)
            # logging.info(f"Tournament schedule sample:\n{json.dumps(schedule, indent=2)}")
            
            # # Get standings
            # standings = get_standings(token, tournament_uuid)
            # logging.info(f"Standings sample:\n{json.dumps(standings, indent=2)}")

            # # Get referees and rankings for tournament
            # referees = get_referees(token, tournament_uuid)
            # logging.info(f"Referees sample:\n{json.dumps(referees, indent=2)}")
            
            # rankings = get_rankings(token, tournament_uuid)
            # logging.info(f"Rankings sample:\n{json.dumps(rankings, indent=2)}")

            # # If we have contestant data, get participation
            # if "contestant" in schedule:
            #     contestant_uuid = schedule["contestant"][0]["id"]
            #     participation = get_contestant_participation(token, contestant_uuid)
            #     logging.info(f"Contestant participation sample:\n{json.dumps(participation, indent=2)}")

            # Get fixture-specific data
            # fixture_uuids = get_fixture_uuids(token, limit=1)
            # if fixture_uuids:
            #     fixture_uuid = fixture_uuids[0]
                
            #     # Get match events
            #     events = get_match_events(token, fixture_uuid)
            #     logging.info(f"Match events sample:\n{json.dumps(events, indent=2)}")
                
            #     # Get penalties preview
            #     penalties = get_penalties_preview(token, fixture_uuid)
            #     logging.info(f"Penalties preview sample:\n{json.dumps(penalties, indent=2)}")
                
            #     # Get pass matrix
            #     pass_matrix = get_pass_matrix(token, fixture_uuid)
            #     logging.info(f"Pass matrix sample:\n{json.dumps(pass_matrix, indent=2)}")
                
            #     # Get possessions
            #     possessions = get_possessions(token, fixture_uuid)
            #     logging.info(f"Possessions sample:\n{json.dumps(possessions, indent=2)}")

            # # Get player contract if we have player data
            # if "person" in calendar_data:
            #     person_uuid = calendar_data["person"][0]["id"]
            #     contract = get_player_contract(token, person_uuid)
            #     logging.info(f"Player contract sample:\n{json.dumps(contract, indent=2)}")

    except Exception as ex:
        logging.exception("Fatal error in main:", exc_info=ex)


if __name__ == "__main__":
    main()
