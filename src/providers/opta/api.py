import requests
from src.providers.opta.oath import get_auth_headers
from src.providers.opta.constants import OUTLET_AUTH_KEY

def get_tournament_calendar():
    """Get tournament calendar data"""
    url = f"https://api.performfeeds.com/soccerdata/tournamentcalendar/{OUTLET_AUTH_KEY}?_rt=b&_fmt=json"
    
    headers = get_auth_headers()
    if not headers:
        return None
        
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error getting tournament calendar: {e}")
        return None

def get_match_details(match_id):
    """Get details for a specific match"""
    url = f"https://api.performfeeds.com/soccerdata/match/{match_id}/{OUTLET_AUTH_KEY}?_rt=b&_fmt=json"
    
    headers = get_auth_headers()
    if not headers:
        return None
        
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error getting match details: {e}")
        return None

def get_team_statistics(team_id, tournament_id):
    """Get team statistics for a specific tournament"""
    url = f"https://api.performfeeds.com/soccerdata/team/rankings/{team_id}/{tournament_id}/{OUTLET_AUTH_KEY}?_rt=b&_fmt=json"
    
    headers = get_auth_headers()
    if not headers:
        return None
        
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error getting team statistics: {e}")
        return None
