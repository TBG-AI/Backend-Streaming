# Directory: src/backend_streaming/providers/opta/domain/entities/players.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING


if TYPE_CHECKING:
    from .teams import Team


@dataclass
class Player:
    """
    Example player data:
    {
        'id': '19m0uqmf1otcekssb7tbrl1m2',
        'firstName': 'Cheick Oumar',
        'lastName': 'Doucouré',
        'shortFirstName': 'Cheick',
        'shortLastName': 'Doucouré',
        'gender': 'Male',
        'matchName': 'C. Doucouré',
        'nationality': 'Mali',
        'nationalityId': 'd0v29nncgikdswlxpdq553zz',
        'position': 'Midfielder',
        'type': 'player',
        'dateOfBirth': '2000-01-08',
        'placeOfBirth': 'Bamako',
        'countryOfBirth': 'Mali',
        'countryOfBirthId': 'd0v29nncgikdswlxpdq553zz',
        'height': 180,
        'weight': 73,
        'foot': 'right',
        'shirtNumber': 28,
        'status': 'active',
        'active': 'yes'
    }
    """
    # Required fields (no defaults)
    player_id: str
    first_name: str
    last_name: str
    nationality: str
    nationality_id: str
    type: str
    active: str
    
    # Optional fields (with defaults)
    gender: Optional[str] = None
    position: Optional[str] = None
    match_name: Optional[str] = None
    short_first_name: Optional[str] = None
    short_last_name: Optional[str] = None
    date_of_birth: Optional[str] = None
    place_of_birth: Optional[str] = None
    country_of_birth: Optional[str] = None
    country_of_birth_id: Optional[str] = None
    height: Optional[int] = None
    weight: Optional[int] = None
    foot: Optional[str] = None
    shirt_number: Optional[int] = None
    status: Optional[str] = None
    team_id: Optional[str] = None
    team_name: Optional[str] = None
    last_updated: Optional[str] = None
    team: Optional[Team] = field(default=None, repr=False)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Player':
        """Create a Player instance from a dictionary."""
        return cls(
            player_id=data['id'],
            first_name=data['firstName'],
            last_name=data['lastName'],
            gender=data.get('gender'),
            match_name=data.get('matchName'),
            nationality=data['nationality'],
            nationality_id=data['nationalityId'],
            position=data.get('position'),
            type=data['type'],
            date_of_birth=data.get('dateOfBirth'),
            place_of_birth=data.get('placeOfBirth'),
            country_of_birth=data.get('countryOfBirth'),
            country_of_birth_id=data.get('countryOfBirthId'),
            height=data.get('height'),
            weight=data.get('weight'),
            foot=data.get('foot'),
            shirt_number=data.get('shirtNumber'),
            status=data.get('status'),
            active=data['active'],
            team_id=data.get('teamId', ''),
            team_name=data.get('teamName', ''),
            last_updated=data.get('lastUpdated', ''),
            short_first_name=data.get('shortFirstName'),
            short_last_name=data.get('shortLastName')
        )

    def to_dict(self) -> dict:
        """Convert Player instance to a dictionary."""
        return {
            'id': self.player_id,
            'firstName': self.first_name,
            'lastName': self.last_name,
            'shortFirstName': self.short_first_name,
            'shortLastName': self.short_last_name,
            'gender': self.gender,
            'matchName': self.match_name,
            'nationality': self.nationality,
            'nationalityId': self.nationality_id,
            'position': self.position,
            'type': self.type,
            'dateOfBirth': self.date_of_birth,
            'placeOfBirth': self.place_of_birth,
            'countryOfBirth': self.country_of_birth,
            'countryOfBirthId': self.country_of_birth_id,
            'height': self.height,
            'weight': self.weight,
            'foot': self.foot,
            'shirtNumber': self.shirt_number,
            'status': self.status,
            'active': self.active,
            'teamId': self.team_id,
            'teamName': self.team_name,
            'lastUpdated': self.last_updated
        }

    def assign_to_team(self, team: 'Team') -> None:
        """Assign this player to a team."""
        if self.team:
            self.team.remove_player(self)
        self.team = team
        self.team_id = team.team_id
        self.team_name = team.name
        team.add_player(self)

    def remove_from_team(self) -> None:
        """Remove this player from their current team."""
        if self.team:
            self.team.remove_player(self)
            self.team = None
            self.team_id = None
            self.team_name = None
    
    