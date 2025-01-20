# Directory: src/backend_streaming/providers/opta/services/opta_scheduler.py
import sched
import time
import datetime
from datetime import timezone, timedelta

from backend_streaming.providers.opta.infra.api import get_tournament_schedule
from backend_streaming.providers.opta.services.opta_provider import OptaStreamer
from backend_streaming.providers.opta.constants import EPL_TOURNAMENT_ID

# Initialize the scheduler
scheduler = sched.scheduler(time.time, time.sleep)

def schedule_matches_for_tournament(tournament_id=EPL_TOURNAMENT_ID):
    """
    1) Fetch the tournament calendar.
    2) For each match's date/time, schedule stream start 10 mins before kickoff.
    3) Schedule only if the match is within the next 7 days.
    """
    data = get_tournament_schedule(tournament_id)
    if not data or "matchDate" not in data:
        print("No matchDate info in the response.")
        return
    
    match_dates = data["matchDate"]
    now = datetime.datetime.now(timezone.utc)
    one_week_later = now + timedelta(days=7)  # boundary for scheduling

    for md in match_dates:
        match_list = md.get("match", [])
        for m in match_list:
            match_id = m["id"]
            
            # Extract date/time (assumed to be in UTC with "Z")
            date_str = m["date"]  # e.g. "2024-08-17Z"
            time_str = m["time"]  # e.g. "14:00:00Z"
            
            # Construct a full datetime string and parse
            datetime_str = f"{date_str.replace('Z', '')}T{time_str.replace('Z', '')}+00:00"
            match_datetime = datetime.datetime.fromisoformat(datetime_str)
            
            # We want to start streaming 10 minutes before the official match time
            stream_start = match_datetime - timedelta(minutes=10)
            
            # Define the streaming job
            def start_stream(match_id=match_id):
                print(f"Starting live stream for match {match_id} at {datetime.datetime.now(timezone.utc)} UTC")
                provider = OptaStreamer(match_id=match_id)
                provider.run_live_stream(interval=30)
            
            # Check if this match is in the past or beyond 7 days
            if stream_start < now:
                # This means the start time is in the past; skip
                print(f"Match {match_id} has a stream start time in the past; skipping.")
                continue
            
            if match_datetime > one_week_later:
                # If the match is more than 7 days away, skip
                print(f"Match {match_id} is more than 7 days away; skipping.")
                continue
            
            # Otherwise, schedule the stream start
            delay_seconds = (stream_start - now).total_seconds()
            scheduler.enter(delay_seconds, 1, start_stream)
            print(f"Scheduled match {match_id} to start streaming at {stream_start} UTC "
                  f"(10 mins before kickoff at {match_datetime} UTC).")

def run_scheduler():
    schedule_matches_for_tournament(EPL_TOURNAMENT_ID)
    print("All matches scheduled. Running scheduler...")

    # In a simple scenario, you can just block here until events are done
    # If you need to keep the process alive continuously, you can do:
    while True:
        # (For Python 3.10+, you can do `scheduler.run(blocking=False)`)
        scheduler.run(blocking=False)
        time.sleep(1)

if __name__ == "__main__":
    run_scheduler()
