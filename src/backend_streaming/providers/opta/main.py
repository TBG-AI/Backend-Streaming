# Directory: src/backend_streaming/providers/opta/main.py

import logging

from backend_streaming.providers.opta.constants import EPL_TOURNAMENT_ID
from backend_streaming.providers.opta.infra.api import get_teams, get_squads
from backend_streaming.providers.opta.infra.db import init_db
from backend_streaming.providers.opta.infra.db import get_session
from backend_streaming.providers.opta.infra.repo.team_player import TeamPlayerRepository
from backend_streaming.providers.opta.domain.entities.teams import Team
from backend_streaming.providers.opta.domain.entities.players import Player
from backend_streaming.providers.opta.infra.repo.event_store.postgres import PostgresEventStore

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
)

def run_example():
    session = get_session()
    repo = TeamPlayerRepository(session=session)
    
    # Fetch teams
    teams = []
    for team in get_teams(EPL_TOURNAMENT_ID)['contestant']:
        logger.info(f"Team name: {team['name']}")
        logger.info(f"Team keys: {', '.join(sorted(team.keys()))}")
        new_team = Team.from_dict(team)
        teams.append(new_team)
    
    squads = get_squads(EPL_TOURNAMENT_ID)
    for squad in squads['squad']:
        team_id = squad['contestantId']
        team: Team = next((t for t in teams if t.team_id == team_id), None)
        if team:
            for player in squad['person']:
                logger.info(f"player: {player}")
                logger.info(f"Player name: {player['matchName']}")
                logger.info(f"Player keys: {', '.join(sorted(player.keys()))}")
                new_player = Player.from_dict(player)
                new_player.assign_to_team(team)
                team.add_player(new_player)
    
    # Save all teams and their players to the database
    for team in teams:
        # Save the team
        repo.save_team(team)
        # Save all players in the team
        for player in team.players:
            repo.save_player(player)
    
    # Example of retrieving data (for verification)
    fetched_team = repo.get_team_by_id(teams[0].team_id)
    print(f"Fetched Team: {fetched_team}")
    players = repo.get_players_by_team_id(teams[0].team_id)
    print(f"Number of players in team: {len(players)}")
    
def run_example_with_event_store():
    session = get_session()
    event_store = PostgresEventStore(session=session)
    
    # Fetching Events 
    
    


def main():
    # Create tables if they don't already exist
    init_db()

    # Then proceed with the rest of your application
    print("Database initialized. Starting the application...")
    
    

if __name__ == "__main__":
    main()
    run_example()
