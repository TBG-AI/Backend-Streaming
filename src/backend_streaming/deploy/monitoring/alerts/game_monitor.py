#!/usr/bin/env python3
from pathlib import Path
import time
from backend_streaming.deploy.logs.log_processor import GameLogProcessor
from backend_streaming.deploy.monitoring.alerts.email_config import send_alert

class GameMonitor:
    def __init__(self):
        self.processor = GameLogProcessor(
            Path(__file__).parents[3] / "providers" / "whoscored"
        )

    def check_and_alert(self):
        """Check for issues and send alerts if needed"""
        issues = self.processor.get_issues()
        print(f"Issues: {issues}")
        
        if issues.failed_game_ids or issues.games_missing_players:
            self.alert(issues)

    def alert(self, issues):
        content = (
            f"***** STREAMING SERVICE ALERT *****\n"
            f"Fetch Failed Games: {', '.join(issues.failed_game_ids) or 'None'}\n"
            f"Stream Failed Games: {', '.join(issues.stream_failed_games) or 'None'}\n"
            f"Games with Missing Players: {', '.join(issues.games_missing_players) or 'None'}\n"
            f"Missing Player IDs: {', '.join(issues.missing_player_ids) or 'None'}\n\n"
        )
        send_alert(content)

def main():
    monitor = GameMonitor()
    
    while True:
        monitor.check_and_alert()
        time.sleep(3600)  # Check every hour

if __name__ == "__main__":
    main()
