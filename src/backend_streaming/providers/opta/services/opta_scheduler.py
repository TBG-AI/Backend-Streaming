# src/backend_streaming/providers/opta/services/opta_scheduler_async.py

import asyncio
import datetime
from datetime import timezone, timedelta

from backend_streaming.providers.opta.infra.api import get_tournament_schedule
from backend_streaming.providers.opta.services.opta_provider import OptaStreamer
from backend_streaming.providers.opta.constants import EPL_TOURNAMENT_ID

async def start_stream(match_id: str, interval: int = 30):
    """
    The actual streaming logic, wrapped as an async function.
    Calls the async run_live_stream on the OptaStreamer.
    
    Args:
        match_id: The match ID to stream
        interval: Polling interval in seconds (default: 30)
    """
    print(f"[{datetime.datetime.now(timezone.utc)}] Starting live stream for match {match_id}.")
    provider = OptaStreamer(match_id=match_id)
    await provider.run_live_stream(interval=interval)

async def schedule_task(delay_seconds: float, match_id: str, interval: int = 30):
    """
    Sleep for `delay_seconds`, then call `start_stream`.
    This creates a "delayed async task" for each match.
    """
    await asyncio.sleep(delay_seconds)
    await start_stream(match_id, interval=interval)

async def schedule_matches_for_tournament(tournament_id=EPL_TOURNAMENT_ID, interval: int = 30):
    """
    1) Fetch the tournament calendar.
    2) For each match's date/time, schedule stream start 10 mins before kickoff.
    3) Schedule only if the match is within the next 7 days.
    """
    data = await get_tournament_schedule(tournament_id)
    if not data or "matchDate" not in data:
        print("No matchDate info in the response.")
        return
    
    match_dates = data["matchDate"]
    now = datetime.datetime.now(timezone.utc)
    one_week_later = now + timedelta(days=7)

    for md in match_dates:
        match_list = md.get("match", [])
        for m in match_list:
            match_id = m["id"]
            # e.g. date="2024-08-17Z", time="14:00:00Z"
            date_str = m["date"]
            time_str = m["time"]
            
            # Construct a datetime, parse
            datetime_str = f"{date_str.replace('Z', '')}T{time_str.replace('Z', '')}+00:00"
            match_datetime = datetime.datetime.fromisoformat(datetime_str)
            
            # Start streaming 10 minutes before official match time
            stream_start = match_datetime - timedelta(minutes=10)
            
            # Check if in past or beyond 7 days
            if stream_start < now:
                continue
            if match_datetime > one_week_later:
                continue
            
            # Otherwise schedule the stream start
            delay_seconds = (stream_start - now).total_seconds()
            print(
                f"[{datetime.datetime.now(timezone.utc)}] "  # Add timestamp
                f"Scheduled match {match_id} | "
                f"Stream starts in: {timedelta(seconds=delay_seconds)} | "
                f"Kickoff time: {match_datetime.strftime('%Y-%m-%d %H:%M:%S')} UTC"
            )
            
            # Create the async task
            asyncio.create_task(schedule_task(delay_seconds, match_id, interval=interval))

async def run_scheduler(interval: int = 30):
    """
    Main entry point for scheduling all the matches and then keeping the
    program running so the tasks have time to trigger.
    
    Args:
        interval: Polling interval in seconds for the live stream (default: 30)
    """
    await schedule_matches_for_tournament(EPL_TOURNAMENT_ID, interval=interval)
    
    # Keep the loop running so scheduled tasks can execute
    while True:
        await asyncio.sleep(1)

def main():
    """
    Entry point if you want to run it as a script: calls the async run_scheduler function.
    """
    interval = 30  # Hardcoded for now
    asyncio.run(run_scheduler(interval=interval))

if __name__ == "__main__":
    main()
