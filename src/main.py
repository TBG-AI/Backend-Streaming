import argparse
import os
import logging
from typing import List
from db.core.factory import DatabaseClientFactory as db_factory

from src.config.logging import setup_logging
from src.shared.streamer import SingleGameStreamer
from src.providers.local.local import LocalDataProvider
from src.providers.local.utils import reset_all
from src.constants import GAMES_DIR

# Configure logging first, before other imports
setup_logging()
logger = logging.getLogger(__name__)

def run_game(game_id: int, provider: str):
    """Run a single game instance"""
    if provider == "local":
        reset_all()
        provider = LocalDataProvider(game_id)
        db_client = db_factory.get_sql_client("postgres-local")
        sqs_client = db_factory.get_sqs_client("sqs-local", group_id=game_id)
    else:
        raise ValueError(f"Invalid provider: {provider}")

    game = SingleGameStreamer(
        game_id=game_id,
        provider=provider,
        db_client=db_client,
        sqs_client=sqs_client,
    )
    game.run()


def main():
    parser = argparse.ArgumentParser(description='Process game events')
    parser.add_argument('--all', action='store_true', help='Run all available games simultaneously')
    parser.add_argument('game_id', type=int, nargs='?', help='Game ID to process (e.g., 1821102 for Chelsea v Brighton)')
    parser.add_argument('--provider', type=str, help='Data provider to use. For test, use "local"')
    args = parser.parse_args()
    
    
    if args.all:
        game_ids = [f.split('.')[0] for f in os.listdir(GAMES_DIR)]
        for game_id in game_ids:
            run_game(int(game_id), args.provider)
    else:
        run_game(args.game_id, args.provider)

if __name__ == "__main__":
    main() 