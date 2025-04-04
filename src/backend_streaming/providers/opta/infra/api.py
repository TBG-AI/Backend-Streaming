# Directory: src/backend_streaming/providers/opta/infra/api.py
import requests
import aiohttp
from backend_streaming.providers.opta.infra.oath import get_auth_headers
from backend_streaming.providers.opta.constants import OUTLET_AUTH_KEY, EPL_TOURNAMENT_ID

async def _make_api_request(url):
    """Helper function to make async API requests with error handling"""
    headers = get_auth_headers()
    if not headers:
        return None
        
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                return await response.json()
    except aiohttp.ClientError as e:
        print(f"Error making API request: {e}")
        return None

async def get_tournament_calendar():
    """Get tournament calendar data"""
    url = f"https://api.performfeeds.com/soccerdata/tournamentcalendar/{OUTLET_AUTH_KEY}?_rt=b&_fmt=json"
    return await _make_api_request(url)

async def get_tournament_schedule(tournament_id):
    """Get tournament schedule data"""
    url = f"https://api.performfeeds.com/soccerdata/tournamentschedule/{OUTLET_AUTH_KEY}?_rt=b&_fmt=json&tmcl={tournament_id}"
    return await _make_api_request(url)
    
async def get_all_fixtures(tournament_id):
    """Get all fixtures for a specific tournament"""
    url = f"https://api.performfeeds.com/soccerdata/match/{OUTLET_AUTH_KEY}?tmcl={tournament_id}&live=yes&_pgSz=1000&_fmt=json&_rt=b"
    return await _make_api_request(url)

async def get_match_details(match_id):
    """Get details for a specific match"""
    url = f"https://api.performfeeds.com/soccerdata/match/{OUTLET_AUTH_KEY}?fx={match_id}&_fmt=json&_rt=b"
    return await _make_api_request(url)

async def get_team_statistics(team_id, tournament_id):
    """Get team statistics for a specific tournament"""
    url = f"https://api.performfeeds.com/soccerdata/team/rankings/{team_id}/{tournament_id}/{OUTLET_AUTH_KEY}?_rt=b&_fmt=json"
    return await _make_api_request(url)

async def get_teams(tournament_id):
    """Get teams for a specific tournament"""
    url = f"https://api.performfeeds.com/soccerdata/team/{OUTLET_AUTH_KEY}?tmcl={tournament_id}&detailed=yes&_rt=b&_fmt=json"
    return await _make_api_request(url)

async def get_squads(tournament_id, team_id=None):
    """Get squads for a specific tournament, optionally filtered by team_id"""
    url = f"https://api.performfeeds.com/soccerdata/squads/{OUTLET_AUTH_KEY}?tmcl={tournament_id}&detailed=yes&_fmt=json&_rt=b"
    if team_id:
        url += f"&ctst={team_id}"
    return await _make_api_request(url)

async def get_match_events(match_id):
    """Get events for a specific match"""
    url = f"https://api.performfeeds.com/soccerdata/matchevent/{OUTLET_AUTH_KEY}/?fx={match_id}&_fmt=json&_rt=b"
    return await _make_api_request(url)
    


if __name__ == "__main__": 
    import asyncio
    
    async def main():
        # Get teams and first team's ID
        teams = await get_teams(EPL_TOURNAMENT_ID)
        print(teams)
        team_id = teams['contestant'][0]['id']
        
        # Get squad information
        squads = await get_squads(EPL_TOURNAMENT_ID, team_id)
        print(squads)
        
        # Get tournament schedule to find match_id
        schedule = await get_tournament_schedule(EPL_TOURNAMENT_ID)
        if schedule and "matchDate" in schedule:
            # Get first available match ID from the schedule
            match_id = schedule["matchDate"][0]["match"][0]["id"]
            print(f"Found match ID: {match_id}")
            
            # Get match events
            events = await get_match_events(match_id)
            events = events.get("liveData", {}).get("event", [])
            print(f"Match events: {events[:3]}")
        
    asyncio.run(main())
