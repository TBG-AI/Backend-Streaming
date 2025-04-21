import uuid
from datetime import datetime
from typing import Dict, List, Any, Tuple

from backend_streaming.providers.whoscored.infra.repos.file_repo import FileRepository
from backend_streaming.providers.opta.infra.db import get_session

def save_teams_to_db(
    scraper_repo, 
    matches_info: List[Dict[str, Any]]
) -> None:
    """
    Save team information to the database using the scraper repository.
    
    Args:
        scraper_repo: Repository for database operations
        matches_info: List of formatted match information containing team data
    """
    
    # Create a set to track processed team IDs to avoid duplicates
    processed_teams = set()
    
    for match in matches_info:
        # Process home team if not already processed
        home_id = match["home_contestant_id"]
        if home_id not in processed_teams:
            processed_teams.add(home_id)
            home_name = match.get("home_contestant_name")
            team_data = {
                'team_id': home_id,
                'name': home_name,
                'short_name': home_name,
                'official_name': home_name,
                'code': home_name[:3].upper() if home_name != "PLACEHOLDER" else "PLACEHOLDER",
                'type': 'club',
                'team_type': 'default',
                'status': 'active',
                'country': match.get("home_contestant_country", "PLACEHOLDER"),
                'country_id': "PLACEHOLDER",
                'city': "PLACEHOLDER",
                'postal_address': "PLACEHOLDER",
                'address_zip': "PLACEHOLDER",
                'founded': "PLACEHOLDER",
                'last_updated': datetime.now().isoformat(),
            }
            
            session = get_session()
            try:
                scraper_repo.insert_team_data(session, **team_data)
            except Exception as e:
                print(f"Error saving home team data: {e}")
        
        # Process away team if not already processed
        away_id = match["away_contestant_id"]
        if away_id not in processed_teams:
            processed_teams.add(away_id)
            away_name = match.get("away_contestant_name")
            team_data = {
                'team_id': away_id,
                'name': away_name,
                'short_name': away_name,
                'official_name': away_name,
                'code': away_name[:3].upper() if away_name != "PLACEHOLDER" else "PLACEHOLDER",
                'type': 'club',
                'team_type': 'default',
                'status': 'active',
                'country': match.get("away_contestant_country", "PLACEHOLDER"),
                'country_id': "PLACEHOLDER",
                'city': "PLACEHOLDER",
                'postal_address': "PLACEHOLDER",
                'address_zip': "PLACEHOLDER",
                'founded': "PLACEHOLDER",
                'last_updated': datetime.now().isoformat(),
            }
            
            session = get_session()
            try:
                scraper_repo.insert_team_data(session, **team_data)
            except Exception as e:
                print(f"Error saving away team data: {e}")

def extract_competition_and_tournament_data(
    tournament_data: Dict[str, Any],
    competition_id: str,
    tournament_id: str
) -> Dict[str, Dict[str, Any]]:
    """
    Extract competition and tournament model data from fixture data.
    
    Args:
        tournament_data: Raw tournament data from WhoScored
        competition_id: Mapped competition ID
        tournament_id: Mapped tournament ID
        
    Returns:
        Dictionary containing competition and tournament model data
    """
    # Extract tournament info from first tournament in the list
    tournament = tournament_data.get("tournaments", [])[0]
    for field in ["tournamentName", "regionName", "sex", "regionId", "stageId", "seasonName"]:
        assert field in tournament, f"Required field '{field}' missing from tournament data"
    
    # Extract competition data
    competition_data = {
        "competition_id": competition_id,
        "name": tournament.get("tournamentName"),
        "known_name": tournament.get("tournamentName"),
        "country": tournament.get("regionName"),
        "competition_format": "League",  # Default, can be overridden
        "type": "men" if tournament.get("sex") == 1 else "women",
        "last_updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "country_id": tournament.get("regionId"),
        "competition_code": tournament.get("stageId"),
        "country_code": None,
    }
    
    # Extract tournament data
    tournament_data = {
        "tournament_id": tournament_id,
        "competition_id": competition_id,
        "name": tournament.get("seasonName"),
        "start_date": None,  # Not directly available
        "end_date": None,    # Not directly available
        "active": "yes",
        "last_updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "includes_standings": "yes"
    }
    
    return {
        "competition_model": competition_data,
        "tournament_model": tournament_data
    }

def load_and_update_mappings(
    file_repo: FileRepository,
    tournament_data: Dict[str, Any]
) -> Tuple[str, str, bool, Dict[str, Dict[str, Any]]]:
    """
    Load existing mappings and update them if new competitions/tournaments are found.
    Also extracts model data for competition and tournament.
    
    Args:
        file_repo: Repository to access mapping files
        tournament_data: Tournament data containing IDs
        
    Returns:
        Tuple of (competition_id, tournament_id, was_updated, model_data)
    """
    competition_mapping = file_repo.load('competition')
    tournament_mapping = file_repo.load('tournament')
    
    mappings_updated = False
    
    # Extract IDs from the tournament data
    tournament = tournament_data.get("tournaments", [])[0]
    competition_id = str(tournament.get("tournamentId"))
    tournament_id = str(tournament.get("seasonId"))
    
    # Generate UUIDs for new competitions/tournaments
    if competition_id not in competition_mapping:
        # Generate a new UUID with competition_id as prefix
        new_uuid = competition_id + str(uuid.uuid4()).replace('-', '')[:20]
        competition_mapping[competition_id] = new_uuid
        # Save the updated mapping
        file_repo.save('competition', competition_mapping)
        mappings_updated = True
        print(f"Generated new competition ID mapping: {competition_id} -> {new_uuid}")
    
    if tournament_id not in tournament_mapping:
        # Generate a new UUID
        new_uuid = str(uuid.uuid4()).replace('-', '')[:24]
        tournament_mapping[tournament_id] = new_uuid
        # Save the updated mapping
        file_repo.save('tournament', tournament_mapping)
        mappings_updated = True
        print(f"Generated new tournament ID mapping: {tournament_id} -> {new_uuid}")
    
    # Get mapped IDs
    mapped_competition_id = competition_mapping[competition_id]
    mapped_tournament_id = tournament_mapping[tournament_id]
    
    # Extract model data
    competition_and_tournament_data = extract_competition_and_tournament_data(
        tournament_data, 
        mapped_competition_id, 
        mapped_tournament_id
    )
    
    return (
        mapped_competition_id,
        mapped_tournament_id,
        mappings_updated,
        competition_and_tournament_data
    )

def update_team_and_match_mappings(
    file_repo: FileRepository,
    matches: List[Dict[str, Any]]
) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str], bool]:
    """
    Update team and match mappings for all matches, creating new UUIDs as needed.
    
    Args:
        file_repo: Repository to access mapping files
        matches: List of match data
        
    Returns:
        Tuple of (match_mapping, team_mapping, team_names, was_updated)
    """
    ws_to_opta_mapping = file_repo.load('match')
    team_mappings = file_repo.load('team')
    standard_team_names = file_repo.load('standard_team_name')
    
    mappings_updated = False
    new_team_mappings = {}
    
    for match in matches:
        match_id = str(match.get("id"))
        home_team_id = str(match.get("homeTeamId"))
        away_team_id = str(match.get("awayTeamId"))
        home_team_name = match.get("homeTeamName")
        away_team_name = match.get("awayTeamName")
        
        # Generate and save mapping for match if it doesn't exist
        if match_id not in ws_to_opta_mapping:
            new_match_uuid = str(uuid.uuid4()).replace('-', '')[:26]
            ws_to_opta_mapping[match_id] = new_match_uuid
            mappings_updated = True
            print(f"Generated new match ID mapping: {match_id} -> {new_match_uuid}")
        
        # Generate and save mapping for teams if they don't exist
        if home_team_id not in team_mappings:
            new_team_uuid = str(uuid.uuid4()).replace('-', '')[:26]
            team_mappings[home_team_id] = new_team_uuid
            mappings_updated = True
            print(f"Generated new team ID mapping: {home_team_id} -> {new_team_uuid}")
            # this is just to return
            new_team_mappings[home_team_id] = home_team_name
        
        if away_team_id not in team_mappings:
            new_team_uuid = str(uuid.uuid4()).replace('-', '')[:26]
            team_mappings[away_team_id] = new_team_uuid
            mappings_updated = True
            print(f"Generated new team ID mapping: {away_team_id} -> {new_team_uuid}")
        
        # Add standard team names if they don't exist
        if home_team_name not in standard_team_names:
            standard_team_names[home_team_name] = home_team_name
            mappings_updated = True
            print(f"Added new standard team name: {home_team_name}")
            
        if away_team_name not in standard_team_names:
            standard_team_names[away_team_name] = away_team_name
            mappings_updated = True
            print(f"Added new standard team name: {away_team_name}")
    
    return ws_to_opta_mapping, team_mappings, standard_team_names, mappings_updated, new_team_mappings

def format_matches_info(
    matches: List[Dict[str, Any]],
    match_mapping: Dict[str, str],
    team_mapping: Dict[str, str],
    team_names: Dict[str, str],
    competition_id: str,
    tournament_id: str
) -> List[Dict[str, Any]]:
    """
    Format the matches information into the required format.
    
    Args:
        matches: List of match data
        match_mapping: Mapping of match IDs
        team_mapping: Mapping of team IDs
        team_names: Mapping of team names
        competition_id: Opta competition ID
        tournament_id: Opta tournament ID
        
    Returns:
        List of formatted match information
    """
    NUMBER_OF_PERIODS = 2
    PERIOD_LENGTH = 45
    VAR = "1"
    
    matches_info = []
    
    for match in matches:
        match_id = str(match.get("id"))
        # home
        home_team_id = str(match.get("homeTeamId"))
        home_team_name = match.get("homeTeamName")
        home_team_country = match.get("homeTeamCountryName")

        # away
        away_team_id = str(match.get("awayTeamId"))
        away_team_name = match.get("awayTeamName")
        away_team_country = match.get("awayTeamCountryName")
        # Parse datetime
        dt = datetime.strptime(match["startTimeUtc"], "%Y-%m-%dT%H:%M:%SZ")
        
        # Build match info with all required fields
        matches_info.append({
            "match_id": match_mapping[match_id],
            "date": dt.date().isoformat(),
            "time": dt.time().strftime("%H:%M:%S"),
            "competition_id": competition_id,
            "tournament_id": tournament_id,
            "number_of_periods": NUMBER_OF_PERIODS,
            "period_length": PERIOD_LENGTH,
            "var": VAR,
            # home
            "home_contestant_id": team_mapping[home_team_id],  
            "home_contestant_name": team_names[home_team_name],
            "home_contestant_country": home_team_country,
            # away
            "away_contestant_id": team_mapping[away_team_id], 
            "away_contestant_name": team_names[away_team_name],
            "away_contestant_country": away_team_country,
        })
    
    return matches_info

def process_fixtures(
    file_repo: FileRepository,
    fixtures_data: Dict[str, Any],
    scraper_repo=None  # Add the scraper_repo parameter
) -> Dict[str, Any]:
    """
    Process fixture data, create needed mappings, and format match information.
    
    Args:
        file_repo: Repository to access mapping files 
        fixtures_data: Complete fixtures data dictionary
        scraper_repo: Repository for saving team data to the database
        
    Returns:
        Dictionary containing formatted match information and model data
    """
    # Load and update competition/tournament mappings
    competition_id, tournament_id, _, extract_competition_and_tournament_data = load_and_update_mappings(
        file_repo, fixtures_data
    )
    
    # Extract all matches
    matches = []
    for tournament in fixtures_data.get("tournaments", []):
        matches.extend(tournament.get("matches", []))
    
    # Update team and match mappings
    match_mapping, team_mapping, team_names, mappings_updated, new_team_mappings = update_team_and_match_mappings(
        file_repo, matches
    )
    
    # Save updated mappings if needed
    if mappings_updated:
        file_repo.save('match', match_mapping)
        file_repo.save('team', team_mapping)
        file_repo.save('standard_team_name', team_names)
    
    # Format match information
    matches_info = format_matches_info(
        matches, match_mapping, team_mapping, team_names, 
        competition_id, tournament_id
    )

    # Save teams to database if scraper_repo is provided
    if scraper_repo:
        save_teams_to_db(scraper_repo, matches_info)
    
    # Return both match info and model data
    return {
        "additional_teams": new_team_mappings,
        "all_matches": matches_info,
        "meta": extract_competition_and_tournament_data
    }
