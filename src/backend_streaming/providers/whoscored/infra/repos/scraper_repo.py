from datetime import datetime
from backend_streaming.providers.opta.infra.models import PlayerModel, TeamModel
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

class ScraperRepository:
    """
    Repository for scraping data from Whoscored.
    """
    def __init__(self, logger):
        self.logger = logger

    def insert_team_data(self, session: Session, **team_data) -> None:
        """
        Insert or update team data in the database.
        """
        self.logger.info(f"Upserting team information: opta id {team_data['team_id']} - team name {team_data['name']}")
        try:
            # Create an upsert statement
            stmt = insert(TeamModel).values(team_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['team_id'],  # Assuming 'team_id' is the primary key
                set_={key: stmt.excluded[key] for key in team_data if key != 'team_id'}
            )

            session.execute(stmt)
            session.commit()
            
        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to insert or update team data: {e}")
            raise
        finally:
            session.close()

    def insert_player_data(self, session: Session, **player_data) -> None:
        """
        Insert or update player data in the database.
        """
        self.logger.info(f"Upserting player information: opta id {player_data['player_id']} - player name {player_data['match_name']}")
        try:
            # Set defaults for optional fields
            default_data = {
                'gender': 'M',
                'nationality': 'PLACEHOLDER',
                'nationality_id': 'PLACEHOLDER',
                'position': 'PLACEHOLDER',
                'type': 'PLACEHOLDER',
                'date_of_birth': 'PLACEHOLDER',
                'place_of_birth': 'PLACEHOLDER',
                'country_of_birth': 'PLACEHOLDER',
                'country_of_birth_id': 'PLACEHOLDER',
                'height': 0,
                'weight': 0,
                'foot': 'PLACEHOLDER',
                'status': 'active',
                'active': 'true',
                'team_name': 'PLACEHOLDER',
                'last_updated': datetime.utcnow().isoformat()
            }
            # Merge provided data with defaults
            player_data = {**default_data, **player_data}

            # Create an upsert statement
            stmt = insert(PlayerModel).values(player_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['player_id'],  # Assuming 'player_id' is the primary key or unique index
                set_={key: stmt.excluded[key] for key in player_data if key != 'player_id'}
            )

            session.execute(stmt)
            session.commit()
            
        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to insert or update player data: {e}")
            raise
        finally:
            session.close()

    def insert_team_data(self, session: Session, **team_data) -> None:
        """
        Insert or update team data in the database.
        """
        self.logger.info(f"Upserting team information: opta id {team_data['team_id']} - team name {team_data['name']}")
        try:
            # Create an upsert statement
            stmt = insert(TeamModel).values(team_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['team_id'],  # Assuming 'team_id' is the primary key
                set_={key: stmt.excluded[key] for key in team_data if key != 'team_id'}
            )

            session.execute(stmt)
            session.commit()
            
        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to insert or update team data: {e}")
            raise
        finally:
            session.close()
