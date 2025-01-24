# Directory: src/backend_streaming/providers/opta/infra/db.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
from backend_streaming.providers.opta.infra.models import Base

DATABASE_URL = "postgresql://jlee@localhost:5432/opta_test"
# ↑ Adjust this connection string to your actual Postgres details.

# 1. Create Engine
engine = create_engine(DATABASE_URL, echo=False)  
# echo=True just logs SQL statements for debugging; you can set it to False in production.

# 2. Check if the database exists
if not database_exists(engine.url):
    create_database(engine.url)  # Creates the actual 'opta_test' if it doesn't exist
    
# 3. Create a configured "Session" class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 4. Create all tables (if they don't exist)
def init_db():
    Base.metadata.create_all(bind=engine)

# Optional: a quick helper to get a session
def get_session():
    return SessionLocal()



