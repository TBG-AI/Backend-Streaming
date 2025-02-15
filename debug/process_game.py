import argparse
from typing import Optional, Tuple
import sys
import logging
from datetime import datetime

from backend_streaming.providers.whoscored.app.services.scraper import SingleGamesScraper
from backend_streaming.providers.whoscored.domain.ws import setup_whoscored
from backend_streaming.utils.logging import setup_logger
import time

def process_game(game_id: str) -> Tuple[str, bool]:
    """
    Process a single game in its own process.
    
    Args:
        game_id: WhoScored game ID to process
        
    Returns:
        Tuple of (game_id, success_status)
        success_status is True if all iterations successfully fetched events
    """
    logger = setup_logger(game_id)
    try:
        logger.info(f"Starting game at {datetime.now()}")
        scraper = SingleGamesScraper(setup_whoscored(game_id=game_id))
        
        success = True  # Track if all iterations were successful
        
        # TODO: hacky way to emulate a live game
        for i in range(5):
            events = scraper.fetch_events(ws_game_id=game_id)
            if not events:  # If no events found in any iteration
                success = False
                logger.warning(f"Fetch {i+1}/10: No events found")
            else:
                logger.info(f"Fetch {i+1}/10: Found {len(events)} events")
            logger.debug(f"Timestamp: {datetime.now()}")
            time.sleep(5)
            
        return game_id, success
            
    except Exception as e:
        logger.error(f"Error processing game: {str(e)}", exc_info=True)
        return game_id, False

def main(args: Optional[list[str]] = None) -> int:
    """
    Main entry point for the script.
    
    Args:
        args: Command line arguments (defaults to sys.argv[1:])
        
    Returns:
        0: Success (all events fetched successfully)
        1: Partial failure (some events missing)
        2: Complete failure (error occurred)
    """
    parser = argparse.ArgumentParser(description='Process a WhoScored game')
    parser.add_argument('game_id', help='WhoScored game ID to process')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    parsed_args = parser.parse_args(args)
    
    if parsed_args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        game_id, success = process_game(parsed_args.game_id)
        if success:
            logging.info(f"Game {game_id} processed successfully")
            return 0  # Complete success
        else:
            logging.warning(f"Game {game_id} processed with missing events")
            return 1  # Partial failure
    except Exception as e:
        logging.error(f"Failed to process game: {e}", exc_info=True)
        return 2  # Complete failure

if __name__ == "__main__":
    sys.exit(main())