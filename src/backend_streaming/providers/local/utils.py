import os
import json
import logging
from datetime import datetime

from typing import List
from db.postgres.constants import Operations
from db.core.factory import DatabaseClientFactory as db_factory
from backend_streaming.constants import GAMES_DIR

logger = logging.getLogger(__name__)

def get_event_time(event: dict) -> int:
    return event['minute'] * 60 + event['second']

def get_events_for_game(game_id):
    """
    Query game events from database and save as sorted JSON file.
    Call if you want more games to simulate for LocalDataProvider.
    """
    file_path = f'{GAMES_DIR}/{game_id}.json'
    
    # Check if file already exists
    if os.path.exists(file_path):
        logger.info(f"Events file already exists for game {game_id}")
        # Load and return existing events as JSON array
        with open(file_path, 'r') as f:
            return json.load(f)
    
    # If file doesn't exist, query database and save
    try:    
        db = db_factory.get_sql_client('postgres-local')
        events_df = db.query(
            conditions=[('game_id', game_id, 'eq')],
            table_name='events',
        )
    except Exception as e:
        logger.error(f"Error querying events for game {game_id}: {e}")
        return []
    
    # Convert RealDictRow objects to regular dictionaries and handle datetime
    events_list = []
    for event in events_df:
        event_dict = dict(event)
        if 'created_at' in event_dict and isinstance(event_dict['created_at'], datetime):
            event_dict['created_at'] = event_dict['created_at'].isoformat()
        events_list.append(event_dict)

    # sort by event_id (assumes this represents the order of events)
    events_list.sort(key=lambda x: x['event_id'])
    os.makedirs(GAMES_DIR, exist_ok=True)
    
    # Write events as a proper JSON array
    with open(file_path, 'w') as f:
        f.write('[\n')  # Start JSON array
        for i, event in enumerate(events_list):
            f.write(json.dumps(event))
            if i < len(events_list) - 1:  # Add comma for all but last item
                f.write(',')
            f.write('\n')
        f.write(']')  # End JSON array
    
    logger.info(f"Saved sorted events for game {game_id} to {file_path}")
    return events_list


def _reset_table(table_name: str, additional_updates: dict = {}) -> bool:
    """Helper function to reset hit status in a table."""
    try:
        db = db_factory.get_sql_client('postgres-local')
        db.update(
            table_name=table_name,
            updates={
                'hit': False,
                'hit_updated_at': None,
                **additional_updates
            },
            # always true condition. update all rows by default
            conditions=[('1', '1', Operations.EQ)]
        )
        return True
    except Exception as e:
        logger.error(f"Error resetting table {table_name}: {e}")
        return False

def reset_playerprops() -> bool:
    """Reset testplayerprops table to initial state."""
    return _reset_table('playerprops')

def reset_betslipbets() -> bool:
    """Reset testbetslipbets table to initial state."""
    return _reset_table('betslipbets')
    
def reset_userbethistory() -> bool:
    """Reset testuserbethistory table to initial state."""
    return _reset_table('userbethistory')

def reset_all() -> bool:
    """Reset all tables to initial state."""
    logger.info("Resetting all tables to initial state")
    return reset_playerprops() and reset_betslipbets() and reset_userbethistory()