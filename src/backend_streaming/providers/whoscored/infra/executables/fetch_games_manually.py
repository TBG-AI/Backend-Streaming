#!/usr/bin/env python3
import argparse
from typing import List
from backend_streaming.providers.whoscored.app.services.scraper import SingleGameScraper
from backend_streaming.providers.whoscored.app.services.run_scraper import process_game

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
    parser.add_argument(
        '--game-ids', 
        nargs='+', 
        help='Space-separated list of game IDs to fetch'
    )
   
    args = parser.parse_args()
    fetch_games(args.game_ids)

if __name__ == "__main__":
    main()
