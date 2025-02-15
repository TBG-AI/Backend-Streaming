import sys
from datetime import datetime, timedelta
from backend_streaming.providers.whoscored.domain.ws import setup_whoscored
from backend_streaming.providers.whoscored.app.services.scraper import SingleGamesScraper
from backend_streaming.providers.whoscored.infra.logs.logger import setup_game_logger
import time

def process_game(game_id: str):
    """
    Process a single game, continuously fetching events until game completion
    or maximum duration reached.
    """
    logger = setup_game_logger(game_id)
    start_time = datetime.now()
    fetch_stats = {
        'total_fetches': 0,
        'successful_fetches': 0,
        'total_events': 0,
        'last_fetch_time': None,
        'last_event_count': 0
    }
    
    try:
        logger.info(f"Starting game processor at {start_time}")
        scraper = SingleGamesScraper(setup_whoscored(game_id=game_id))
        
        # Continue until max duration (e.g., 3 hours) or game completion
        # while datetime.now() - start_time < timedelta(hours=3):
        # TODO: THIS IS HACKY!
        for _ in range(1):
            fetch_stats['total_fetches'] += 1
            current_time = datetime.now()
            
            try:
                events = scraper.fetch_events(ws_game_id=game_id)
                fetch_stats['last_fetch_time'] = current_time
                
                if events:
                    fetch_stats['successful_fetches'] += 1
                    fetch_stats['total_events'] = len(events)
                    fetch_stats['last_event_count'] = len(events)
                    logger.info(
                        f"Fetch {fetch_stats['total_fetches']}: "
                        f"Found {len(events)} events. "
                        f"Total successful fetches: {fetch_stats['successful_fetches']}"
                    )
                else:
                    logger.warning(
                        f"Fetch {fetch_stats['total_fetches']}: "
                        f"No events found. "
                        f"Total successful fetches: {fetch_stats['successful_fetches']}"
                    )
                
            except Exception as fetch_error:
                logger.error(f"Error during fetch: {fetch_error}", exc_info=True)
            
            # Log summary every 15 minutes
            if fetch_stats['total_fetches'] % 180 == 0:  # Assuming 5-second intervals
                logger.info(
                    f"Summary after {fetch_stats['total_fetches']} fetches:\n"
                    f"Successful fetches: {fetch_stats['successful_fetches']}\n"
                    f"Total events: {fetch_stats['total_events']}\n"
                    f"Running for: {datetime.now() - start_time}"
                )
            
            time.sleep(5)  # Poll interval
            
        logger.info(f"Game processor completed. Final stats: {fetch_stats}")
        
    except Exception as e:
        logger.error(f"Fatal error in game processor: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    # Handle command line argument
    if len(sys.argv) != 2:
        print("Usage: python scraper.py <game_id>")
        sys.exit(1)
    
    game_id = sys.argv[1]
    process_game(game_id)