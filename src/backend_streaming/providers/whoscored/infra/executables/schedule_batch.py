#!/usr/bin/env python3
from datetime import datetime, timedelta
import pytz
import argparse
from backend_streaming.providers.whoscored.app.services.ws_provider import WhoScoredProvider

def schedule_batch(
    start_date: datetime, 
    end_date: datetime, 
    force_cache: bool=False, 
    debug: bool=False
):
    """
    Schedule a new batch of games.
    NOTE: the start and end dates need to be UTC aware!
    """
    provider = WhoScoredProvider()
    try:
        # Schedule the batch
        provider.schedule_batch(
            batch_start=start_date,
            batch_end=end_date,
            force_cache=force_cache,
            debug=debug
        )
        
        # Keep main thread alive while there are scheduled jobs
        while True:
            status = provider.get_scheduler_status()
            if status['scheduled_jobs'] == 0 and status['running_jobs'] == 0:
                provider.logger.info('All jobs completed. Exiting...')
                break
            provider.logger.info(
                f'Status: {status["scheduled_jobs"]} scheduled, '
                f'{status["running_jobs"]} running. '
                f'Next run at: {status["next_runtime"]}'
            )
            import time
            time.sleep(10)
            
    finally:
        provider._cleanup(terminate=True)
        provider.scheduler.shutdown()

def main():
    parser = argparse.ArgumentParser(description='Schedule WhoScored game batch')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    parser.add_argument('--force-cache', action='store_true', help='Force cache')
    parser.add_argument('--debug', action='store_true', help='Debug mode')
    args = parser.parse_args()

    timezone = pytz.timezone('UTC')
    
    # Default to next 7 days if no dates provided
    if args.start_date:
        start_date = timezone.localize(datetime.strptime(args.start_date, '%Y-%m-%d'))
    else:
        start_date = timezone.localize(datetime.now())
        
    if args.end_date:
        end_date = timezone.localize(datetime.strptime(args.end_date, '%Y-%m-%d'))
    else:
        end_date = start_date + timedelta(days=7)

    print(f"Scheduling batch from {start_date.date()} to {end_date.date()}")
    schedule_batch(start_date, end_date, force_cache=args.force_cache, debug=args.debug)

if __name__ == "__main__":
    main()
