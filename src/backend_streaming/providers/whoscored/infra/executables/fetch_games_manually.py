#!/usr/bin/env python3
import argparse
from typing import List
from backend_streaming.providers.whoscored.app.services.scraper import ManualGameScraper
from backend_streaming.deploy.logs.log_processor import GameLogProcessor
from pathlib import Path

def fetch_games(game_ids: List[str]) -> None:
    """Fetch events for specified game IDs"""
    for game_id in game_ids:
        print(f"\nProcessing game {game_id}...")
        try:
            scraper = ManualGameScraper(game_id)
            scraper.fetch_events()
            print(f"Successfully processed game {game_id}")
        except Exception as e:
            print(f"Failed to process game {game_id}: {str(e)}")

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
