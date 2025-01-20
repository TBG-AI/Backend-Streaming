# Directory: src/backend_streaming/providers/opta/domain/entities/teams.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .players import Player


@dataclass
class Team:
    """
    Example team data:
    {
        'id': '1c8m2ko0wxq1asfkuykurdr0y',
        'name': 'Crystal Palace',
        'shortName': 'Crystal Palace',
        'officialName': 'Crystal Palace FC',
        'code': 'CRY',
        'type': 'club',
        'teamType': 'default',
        'countryId': '1fk5l4hkqk12i7zske6mcqju6',
        'country': 'England',
        'status': 'active',
        'city': 'London',
        'postalAddress': 'Selhurst Park, London',
        'addressZip': 'SE25 6PU',
        'founded': '1861',
        'lastUpdated': '2025-01-09T08:21:12Z'
    }
    """
    team_id: str  # maps to 'id'
    name: str
    short_name: str  # maps to 'shortName'
    official_name: str  # maps to 'officialName'
    code: str
    type: str
    team_type: str  # maps to 'teamType'
    country_id: str  # maps to 'countryId'
    country: str
    status: str
    city: str
    postal_address: Optional[str]  # maps to 'postalAddress'
    address_zip: Optional[str]  # maps to 'addressZip'
    founded: Optional[str]
    last_updated: str  # maps to 'lastUpdated'
    players: List[Player] = field(default_factory=list)  # Add this field

    @classmethod
    def from_dict(cls, data: dict) -> 'Team':
        """Create a Team instance from a dictionary."""
        return cls(
            team_id=data['id'],
            name=data['name'],
            short_name=data['shortName'],
            official_name=data['officialName'],
            code=data['code'],
            type=data['type'],
            team_type=data['teamType'],
            country_id=data['countryId'],
            country=data['country'],
            status=data['status'],
            city=data['city'],
            postal_address=data.get('postalAddress', ''),
            address_zip=data.get('addressZip', ''),
            founded=data.get('founded', ''),
            last_updated=data['lastUpdated']
        )

    def to_dict(self) -> dict:
        """Convert Team instance to a dictionary."""
        return {
            'id': self.team_id,
            'name': self.name,
            'shortName': self.short_name,
            'officialName': self.official_name,
            'code': self.code,
            'type': self.type,
            'teamType': self.team_type,
            'countryId': self.country_id,
            'country': self.country,
            'status': self.status,
            'city': self.city,
            'postalAddress': self.postal_address,
            'addressZip': self.address_zip,
            'founded': self.founded,
            'lastUpdated': self.last_updated
        }

    def add_player(self, player: Player) -> None:
        """Add a player to the team and update the player's team information."""
        self.players.append(player)
        player.team_id = self.team_id
        player.team_name = self.name

    def remove_player(self, player: Player) -> None:
        """Remove a player from the team and clear their team information."""
        if player in self.players:
            self.players.remove(player)
            player.team_id = ''
            player.team_name = ''

    def get_players(self) -> List[Player]:
        """Get all players in the team."""
        return self.players
