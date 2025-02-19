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
            self._send_alert(issues)

    def _send_alert(self, issues):
        content = (
            f"WhoScored Processing Issues:\n\n"
            f"Failed Games: {', '.join(issues.failed_game_ids)}\n"
            f"Games with Missing Players: {', '.join(issues.games_missing_players)}\n"
            f"Missing Player IDs: {', '.join(issues.missing_player_ids)}\n\n"
            f"Actions Required:\n"
            f"1. For failed games: Run fetch_games_manually.py\n"
            f"2. For missing players: Update player mappings\n"
        )
        send_alert(content)

def main():
    monitor = GameMonitor()
    
    while True:
        monitor.check_and_alert()
        time.sleep(3600)  # Check every hour

if __name__ == "__main__":
    main()
