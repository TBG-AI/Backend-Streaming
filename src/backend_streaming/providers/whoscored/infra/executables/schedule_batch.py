#!/usr/bin/env python3
from datetime import datetime, timedelta
import pytz
import argparse
import time
from backend_streaming.providers.whoscored.app.services.ws_provider import WhoScoredProvider

def schedule_batch(
    start_date: datetime, 
    days_delta: int,  # Changed from end_date to days_delta
    force_cache: bool=False, 
    debug: bool=False
):
    """
    Schedule a rolling batch of games.
    NOTE: the start date needs to be UTC aware!
    
    Args:
        start_date: UTC aware datetime to start scheduling from
        days_delta: Number of days to schedule in each batch
        force_cache: Whether to force cache refresh
        debug: Enable debug logging
    """
    provider = WhoScoredProvider()
    try:
        while True:  # Continuous scheduling loop
            # Calculate end date for current batch
            end_date = start_date + timedelta(days=days_delta)
            provider.logger.info(f'Scheduling batch from {start_date} to {end_date}')
            
            # Schedule the batch
            provider.schedule_batch(
                batch_start=start_date,
                batch_end=end_date,
                force_cache=force_cache,
                debug=debug
            )
            
            # Wait for current batch to complete
            while True:
                status = provider.get_scheduler_status()
                if status['scheduled_jobs'] == 0 and status['running_jobs'] == 0:
                    provider.logger.info('Current batch completed.')
                    break
                provider.logger.info(
                    f'Status: {status["scheduled_jobs"]} scheduled, '
                    f'{status["running_jobs"]} running. '
                    f'Next run at: {status["next_runtime"]}'
                )
                time.sleep(10)
            
            # Update start_date for next batch
            # Adding 1 day to avoid any timezone edge cases
            start_date = end_date + timedelta(days=1)
            
    except KeyboardInterrupt:
        provider.logger.info('Received interrupt signal. Cleaning up...')
    finally:
        provider._cleanup(terminate=True)
        provider.scheduler.shutdown()

def main():
    parser = argparse.ArgumentParser(description='Schedule WhoScored game batch')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--days-delta', help='Number of days to schedule in each batch')
    parser.add_argument('--force-cache', action='store_true', help='Force cache')
    parser.add_argument('--debug', action='store_true', help='Debug mode')
    args = parser.parse_args()

    timezone = pytz.timezone('UTC')
    
    # Default to next 7 days if no dates provided
    if args.start_date:
        start_date = timezone.localize(datetime.strptime(args.start_date, '%Y-%m-%d'))
    else:
        start_date = timezone.localize(datetime.now())
        
    if args.days_delta:
        days_delta = int(args.days_delta)
    else:
        days_delta = 7

    print(f"Scheduling batch from {start_date.date()} to {start_date.date() + timedelta(days=days_delta)}")
    schedule_batch(start_date, days_delta, force_cache=args.force_cache, debug=args.debug)

if __name__ == "__main__":
    main()
