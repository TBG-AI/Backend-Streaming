import sys
import asyncio
import logging
import json
from typing import List
from datetime import datetime
from backend_streaming.streamer.streamer import SingleGameStreamer
from backend_streaming.providers.whoscored.app.services.scraper import SingleGameScraper
from backend_streaming.providers.whoscored.infra.config.logger import setup_game_logger
from backend_streaming.providers.whoscored.infra.config.config import paths


def process_game(game_id: str, scraper: SingleGameScraper):
    """
    Process a single game, continuously fetching events until game completion
    or maximum duration reached.

    # NOTE: scraper is only passed in when running manually
    """
    with open(paths.ws_to_opta_match_mapping_path, 'r') as f:
        ws_to_opta_match_mapping = json.load(f)

    # setup
    logger = setup_game_logger(game_id)
    streamer = SingleGameStreamer(ws_to_opta_match_mapping[game_id])
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
        is_eog = False
        while not is_eog:
            fetch_stats['total_fetches'] += 1
            current_time = datetime.now()

            # TODO: implement proper condition to check for eog...
            # For now, just running once to prevent infinite loop.
            is_eog = True
           
            try:
                # get the events
                events = scraper.fetch_events()
                fetch_stats['last_fetch_time'] = current_time
                # log useful stats
                if events:
                    fetch_stats['successful_fetches'] += 1
                    fetch_stats['total_events'] = len(events)
                    fetch_stats['last_event_count'] = len(events)
                    logger.info(
                        f"Fetch {fetch_stats['total_fetches']}: "
                        f"Found {len(events)} events. "
                        f"Total successful fetches: {fetch_stats['successful_fetches']}"
                    )
                    # stream into designated queue
                    asyncio.run(stream_events(streamer, events, logger, is_eog=is_eog))
            except Exception as fetch_error:
                logger.error(f"Error during fetch: {fetch_error}", exc_info=True)
        
        # final send through streamer to dictate end of game. 
        logger.info(f"Game processor completed. Final stats: {fetch_stats}")
        
    except Exception as e:
        logger.error(f"Fatal error in game processor: {e}", exc_info=True)
        raise


async def stream_events(
    streamer: SingleGameStreamer, 
    events: List[dict],
    logger: logging.Logger,
    is_eog: bool = False,
):
    """Helper function to stream events asynchronously"""
    try:
        message_type = streamer.STOP_MESSAGE_TYPE if is_eog else streamer.PROGRESS_MESSAGE_TYPE
        await streamer.connect()
        await streamer.send_message(message_type, events)
        logger.info(f"Streamed {len(events)} events")
    except Exception as e:
        logger.error(f"Failed to stream events: {e}")
        raise
    finally:
        await streamer.close()


if __name__ == "__main__":
    # Handle command line argument
    if len(sys.argv) != 2:
        print("Usage: python scraper.py <game_id>")
        sys.exit(1)
    
    game_id = sys.argv[1]
    process_game(game_id)