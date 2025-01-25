# src/backend_streaming/providers/opta/services/opta_scheduler.py
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
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    await start_stream(match_id, interval=interval)

async def schedule_matches_for_tournament(tournament_id=EPL_TOURNAMENT_ID, interval: int = 30):
    """
    1) Fetch the tournament calendar.
    2) For each match's date/time, schedule stream start 10 mins before kickoff.
    3) If the match has already started (now past the scheduled start), begin streaming immediately
       as long as the match is still within 90 minutes of its official kickoff.
    4) Schedule only if the match is within the next 7 days.
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
            date_str = m.get("date") or ""
            time_str = m.get("time") or ""

            # If time is missing or empty, skip:
            if not time_str or time_str.strip() in ("Z", ""):
                print(f"Skipping match {match_id} due to missing time.")
                continue

            datetime_str = f"{date_str.replace('Z', '')}T{time_str.replace('Z', '')}+00:00"
            try:
                match_datetime = datetime.datetime.fromisoformat(datetime_str)
            except ValueError as e:
                print(f"Skipping match {match_id} because of invalid date/time {datetime_str}: {e}")
                continue
            
            # Skip matches beyond 7 days:
            if match_datetime > one_week_later:
                continue

            # Calculate the normal streaming start time (10 minutes before kickoff)
            stream_start = match_datetime - timedelta(minutes=10)

            # If the stream start time is in the future, schedule normally.
            if stream_start > now:
                delay_seconds = (stream_start - now).total_seconds()
                print(
                    f"[{datetime.datetime.now(timezone.utc)}] "
                    f"Scheduled match {match_id} | "
                    f"Stream starts in: {timedelta(seconds=delay_seconds)} | "
                    f"Kickoff time: {match_datetime.strftime('%Y-%m-%d %H:%M:%S')} UTC"
                )
            
            else:
                # stream_start is in the past
                # We check if we are still within 90 minutes of the official match time
                late_start_threshold = match_datetime + timedelta(minutes=180)
                if now <= late_start_threshold:
                    # We can still start streaming immediately
                    delay_seconds = 0
                    print(
                        f"[{datetime.datetime.now(timezone.utc)}] "
                        f"Match {match_id} already started, but within 90-minute window. "
                        f"Starting stream immediately."
                    )
                else:
                    # The match is probably over (beyond 90 mins from kickoff)
                    continue

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
