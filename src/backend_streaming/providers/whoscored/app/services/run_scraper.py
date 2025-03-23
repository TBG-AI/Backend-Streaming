import sys
import asyncio
import logging
import json
import re
from typing import List, Tuple
from datetime import datetime
from backend_streaming.streamer.streamer import SingleGameStreamer
from backend_streaming.providers.whoscored.app.services.scraper import SingleGameScraper
from backend_streaming.providers.whoscored.infra.config.logger import setup_game_logger
from backend_streaming.providers.whoscored.infra.config.config import paths

def parse_game_txt(game_txt: str) -> Tuple[str, str]:
    """
    Parse the game_txt and extract the game_id and matchCentreData.
    """
    # Extract matchId using regex
    match_id_pattern = r'matchId:(\d+)'
    match_id_match = re.search(match_id_pattern, game_txt)
    if not match_id_match:
        raise ValueError("Could not find matchId in the provided text")
    
    match_id = match_id_match.group(1)
    
    # Extract matchCentreData using regex
    match_centre_pattern = r'matchCentreData: (\{.*?}),\s*matchCentreEventTypeJson'
    match_centre_match = re.search(match_centre_pattern, game_txt, re.DOTALL)
    if not match_centre_match: 
        raise ValueError("Could not find matchCentreData in the provided text")
    
    match_centre_data = match_centre_match.group(1)
    return match_id, match_centre_data


def save_game_txt(match_id: str, match_centre_data: str) -> None:
    file_path = paths.raw_pagesources_dir / f"{match_id}.txt"
    with open(file_path, "w") as f:
        f.write(match_centre_data)


async def process_game(
    game_id: str,
    scraper: SingleGameScraper,
    send_via_stream: bool = True
) -> dict:
    """
    Process a single game, continuously fetching events until game completion
    or maximum duration reached.
    NOTE: for manual fetches, 
    # NOTE: scraper is only passed in when running manually
    """
    assert send_via_stream != scraper._is_manual_scraper, \
        "Manual scrapers should not send via stream. Too error prone..."

    # setup
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
        opta_game_id = scraper.ws_to_opta_mapping[game_id]
        streamer = SingleGameStreamer(opta_game_id)
        payloads = []
        is_eog = False
        while not is_eog:
            # TODO: implement proper condition to check for eog...
            # For now, just running once to prevent infinite loop.
            is_eog = True
            try:
                # this populates the json_data attribute in the scraper
                # NOTE: the ORDER of operations for fetching and updating mappings is important.
                events = scraper.fetch_events()
                player_data = scraper.update_player_data()
                lineup_info = scraper.extract_lineup()
                projections = scraper.save_projections(events)

                # construct payload and store in memory (this is in case we want to see previous data)
                payload = {
                    'projections': projections,
                    'player_data': player_data,
                    'lineup_info': lineup_info
                }
                payloads.append(payload)

                # By default, we always send but for manual fetch we choose not to.
                # Doesn't matter if we send tbh, but just in case things break
                if send_via_stream:
                    await stream(
                        streamer = streamer,
                        data = payload,
                        logger = logger, 
                        is_eog = is_eog
                    )
                
                # log useful stats
                fetch_stats['total_events'] = len(events)
                fetch_stats['total_fetches'] += 1
                fetch_stats['last_fetch_time'] = datetime.now()
                fetch_stats['last_event_count'] = len(events)
                fetch_stats['successful_fetches'] += 1
                
            except Exception as fetch_error:
                logger.error(f"Error during fetch: {fetch_error}", exc_info=True)
        
        # final send through streamer to dictate end of game. 
        logger.info(f"Game processor completed. Final stats: {fetch_stats}")
        scraper.file_repo.save(
            file_type='payloads', 
            data=payload, 
            file_name=f"{game_id}.json"
        )
        return {
            'opta_game_id': opta_game_id,
            'payloads': payloads
        }
        
    except Exception as e:
        logger.error(f"Fatal error in game processor: {e}", exc_info=True)
        raise


async def stream(
    streamer: SingleGameStreamer, 
    data: List[dict],
    logger: logging.Logger,
    is_eog: bool = False,
):
    """
    Helper function to stream data into main server asynchronously.
    Each data fields to have it's own message type.
    """
    try:
        message_type = streamer.STOP_MESSAGE_TYPE if is_eog else streamer.PROGRESS_MESSAGE_TYPE
        await streamer.connect()
        await streamer.send_message(message_type, data)
        logger.info(f"Streamed fields: {data.keys()}")
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