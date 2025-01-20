# Directory: src/backend_streaming/providers/opta/api.py
import requests
from backend_streaming.providers.opta.infra.oath import get_auth_headers
from backend_streaming.providers.opta.constants import OUTLET_AUTH_KEY, EPL_TOURNAMENT_ID

def _make_api_request(url):
    """Helper function to make API requests with error handling"""
    headers = get_auth_headers()
    if not headers:
        return None
        
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        return None

def get_tournament_calendar():
    """Get tournament calendar data"""
    url = f"https://api.performfeeds.com/soccerdata/tournamentcalendar/{OUTLET_AUTH_KEY}?_rt=b&_fmt=json"
    return _make_api_request(url)

def get_tournament_schedule(tournament_id):
    """Get tournament schedule data"""
    url = f"https://api.performfeeds.com/soccerdata/tournamentschedule/{OUTLET_AUTH_KEY}?_rt=b&_fmt=json&tmcl={tournament_id}"
    return _make_api_request(url)
    
def get_all_fixtures(tournament_id):
    """Get all fixtures for a specific tournament"""
    url = f"https://api.performfeeds.com/soccerdata/match/{OUTLET_AUTH_KEY}?tmcl={tournament_id}&live=yes&_pgSz=1000&_fmt=json&_rt=b"
    return _make_api_request(url)

def get_match_details(match_id):
    """Get details for a specific match"""
    url = f"https://api.performfeeds.com/soccerdata/match/{OUTLET_AUTH_KEY}?fx={match_id}&_fmt=json&_rt=b"
    return _make_api_request(url)

def get_team_statistics(team_id, tournament_id):
    """Get team statistics for a specific tournament"""
    url = f"https://api.performfeeds.com/soccerdata/team/rankings/{team_id}/{tournament_id}/{OUTLET_AUTH_KEY}?_rt=b&_fmt=json"
    return _make_api_request(url)

def get_teams(tournament_id):
    """Get teams for a specific tournament"""
    url = f"https://api.performfeeds.com/soccerdata/team/{OUTLET_AUTH_KEY}?tmcl={tournament_id}&detailed=yes&_rt=b&_fmt=json"
    return _make_api_request(url)

def get_squads(tournament_id, team_id=None):
    """Get squads for a specific tournament, optionally filtered by team_id"""
    url = f"https://api.performfeeds.com/soccerdata/squads/{OUTLET_AUTH_KEY}?tmcl={tournament_id}&detailed=yes&_fmt=json&_rt=b"
    if team_id:
        url += f"&ctst={team_id}"
    return _make_api_request(url)

def get_match_events(match_id):
    """Get events for a specific match"""
    url = f"https://api.performfeeds.com/soccerdata/matchevent/{OUTLET_AUTH_KEY}/?fx={match_id}&_fmt=json&_rt=b"
    return _make_api_request(url)
    


if __name__ == "__main__": 
    teams = get_teams(EPL_TOURNAMENT_ID)
    print(teams)
    team_id = teams['contestant'][0]['id']
    
    squads = get_squads(EPL_TOURNAMENT_ID, team_id)
    print(squads)
