# Directory: src/backend_streaming/providers/opta/infra/models.py
from sqlalchemy import Column, String, Integer, ForeignKey, DateTime, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class TeamModel(Base):
    __tablename__ = 'teams'

    team_id = Column(String, primary_key=True)
    name = Column(String)
    short_name = Column(String)
    official_name = Column(String)
    code = Column(String)
    type = Column(String)
    team_type = Column(String)
    country_id = Column(String)
    country = Column(String)
    status = Column(String)
    city = Column(String)
    postal_address = Column(String)
    address_zip = Column(String)
    founded = Column(String)
    last_updated = Column(String)
    
    players = relationship("PlayerModel", back_populates="team")

class PlayerModel(Base):
    __tablename__ = 'players'

    player_id = Column(String, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    short_first_name = Column(String)
    short_last_name = Column(String)
    gender = Column(String)
    match_name = Column(String)
    nationality = Column(String)
    nationality_id = Column(String)
    position = Column(String)
    type = Column(String)
    date_of_birth = Column(String)
    place_of_birth = Column(String)
    country_of_birth = Column(String)
    country_of_birth_id = Column(String)
    height = Column(Integer)
    weight = Column(Integer)
    foot = Column(String)
    shirt_number = Column(Integer)
    status = Column(String)
    active = Column(String)
    team_id = Column(String, ForeignKey('teams.team_id'))
    team_name = Column(String)
    last_updated = Column(String)

    team = relationship("TeamModel", back_populates="players")
    
    
class DomainEventModel(Base):
    """
    Represents a row in the 'domain_events' table, storing an immutable
    record of a domain event (if you're doing event sourcing).
    """
    __tablename__ = 'domain_events'
    
    domain_event_id = Column(String, primary_key=True)   # e.g. a UUID string
    aggregate_id    = Column(String, nullable=False, index=True)
    event_type      = Column(String, nullable=False)
    occurred_on     = Column(DateTime, nullable=False)
    payload         = Column(JSON, nullable=False)

    def __repr__(self):
        return (f"<DomainEventModel(domain_event_id='{self.domain_event_id}', "
                f"event_type='{self.event_type}', aggregate_id='{self.aggregate_id}')>")
