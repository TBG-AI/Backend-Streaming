#!/usr/bin/env python3
from pathlib import Path
import time
from backend_streaming.deploy.monitoring.alerts.game_monitor import GameMonitor
from backend_streaming.deploy.monitoring.alerts.batch_scheduler import BatchScheduleMonitor

# TODO (PRODUCTION): fill this up properly
CHECK_EVERY_SECONDS = 3600

def main():
    # Initialize monitors (no email params needed)
    game_monitor = GameMonitor()
    batch_monitor = BatchScheduleMonitor()
    
    while True:
        # Check both types of issues
        batch_monitor.check_batch_schedule()
        game_monitor.check_and_alert()
        print(f"Checked both monitors. Sleeping for {CHECK_EVERY_SECONDS} seconds...")
        time.sleep(CHECK_EVERY_SECONDS)  # Check every hour

if __name__ == "__main__":
    main()
