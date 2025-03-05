#!/usr/bin/env python3
import argparse
from typing import List
from backend_streaming.providers.whoscored.app.services.scraper import SingleGameScraper
from backend_streaming.providers.whoscored.app.services.run_scraper import process_game
from backend_streaming.providers.whoscored.infra.config.config import paths

def fetch_games(game_ids: List[str]) -> None:
    """Fetch events for specified game IDs"""
    for game_id in game_ids:
        print(f"\nProcessing game {game_id}...")
        try:
            scraper = SingleGameScraper(game_id)
            process_game(game_id, scraper)
        except Exception as e:
            raise e

def main():
    parser = argparse.ArgumentParser(description='Manually fetch WhoScored games')
    parser.add_argument('--all-games', action='store_true', help='Fetch all games')
    parser.add_argument(
        '--game-ids', 
        nargs='+', 
        help='Space-separated list of game IDs to fetch'
    )
    args = parser.parse_args()
    if args.all_games:
        game_ids = [game.stem for game in paths.raw_pagesources_dir.glob('*.txt')]
    else:
        game_ids = args.game_ids
    fetch_games(game_ids)

if __name__ == "__main__":
    import os
    print(f"db before init is: {os.getenv('DATABASE_URL')}")
    main()
