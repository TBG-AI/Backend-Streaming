# Directory: src/backend_streaming/providers/opta/infra/repo/team_player_repo.py
from sqlalchemy.orm import Session
from backend_streaming.providers.opta.domain.entities.teams import Team
from backend_streaming.providers.opta.domain.entities.players import Player
from backend_streaming.providers.opta.infra.models import TeamModel, PlayerModel

class TeamPlayerRepository:
    def __init__(self, session: Session):
        self.session = session

    def save_team(self, team: Team) -> None:
        team_model = TeamModel(
            team_id=team.team_id,
            name=team.name,
            short_name=team.short_name,
            official_name=team.official_name,
            code=team.code,
            type=team.type,
            team_type=team.team_type,
            country_id=team.country_id,
            country=team.country,
            status=team.status,
            city=team.city,
            postal_address=team.postal_address,
            address_zip=team.address_zip,
            founded=team.founded,
            last_updated=team.last_updated
        )
        self.session.merge(team_model)
        self.session.commit()

    def save_player(self, player: Player) -> None:
        player_model = PlayerModel(
            player_id=player.player_id,
            first_name=player.first_name,
            last_name=player.last_name,
            short_first_name=player.short_first_name,
            short_last_name=player.short_last_name,
            gender=player.gender,
            match_name=player.match_name,
            nationality=player.nationality,
            nationality_id=player.nationality_id,
            position=player.position,
            type=player.type,
            date_of_birth=player.date_of_birth,
            place_of_birth=player.place_of_birth,
            country_of_birth=player.country_of_birth,
            country_of_birth_id=player.country_of_birth_id,
            height=player.height,
            weight=player.weight,
            foot=player.foot,
            shirt_number=player.shirt_number,
            status=player.status,
            active=player.active,
            team_id=player.team_id,
            team_name=player.team_name,
            last_updated=player.last_updated
        )
        self.session.merge(player_model)
        self.session.commit()

    def get_team_by_id(self, team_id: str) -> Team:
        team_model = self.session.query(TeamModel).filter_by(team_id=team_id).first()
        if not team_model:
            return None
        
        team = Team.from_dict({
            'id': team_model.team_id,
            'name': team_model.name,
            'shortName': team_model.short_name,
            'officialName': team_model.official_name,
            'code': team_model.code,
            'type': team_model.type,
            'teamType': team_model.team_type,
            'countryId': team_model.country_id,
            'country': team_model.country,
            'status': team_model.status,
            'city': team_model.city,
            'postalAddress': team_model.postal_address,
            'addressZip': team_model.address_zip,
            'founded': team_model.founded,
            'lastUpdated': team_model.last_updated
        })
        return team

    def get_player_by_id(self, player_id: str) -> Player:
        player_model = self.session.query(PlayerModel).filter_by(player_id=player_id).first()
        if not player_model:
            return None
        
        player = Player.from_dict({
            'id': player_model.player_id,
            'firstName': player_model.first_name,
            'lastName': player_model.last_name,
            'shortFirstName': player_model.short_first_name,
            'shortLastName': player_model.short_last_name,
            'gender': player_model.gender,
            'matchName': player_model.match_name,
            'nationality': player_model.nationality,
            'nationalityId': player_model.nationality_id,
            'position': player_model.position,
            'type': player_model.type,
            'dateOfBirth': player_model.date_of_birth,
            'placeOfBirth': player_model.place_of_birth,
            'countryOfBirth': player_model.country_of_birth,
            'countryOfBirthId': player_model.country_of_birth_id,
            'height': player_model.height,
            'weight': player_model.weight,
            'foot': player_model.foot,
            'shirtNumber': player_model.shirt_number,
            'status': player_model.status,
            'active': player_model.active,
            'teamId': player_model.team_id,
            'teamName': player_model.team_name,
            'lastUpdated': player_model.last_updated
        })
        return player

    def get_players_by_team_id(self, team_id: str) -> list[Player]:
        player_models = self.session.query(PlayerModel).filter_by(team_id=team_id).all()
        return [self.get_player_by_id(player_model.player_id) for player_model in player_models]