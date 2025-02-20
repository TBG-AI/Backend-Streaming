#!/usr/bin/env python3
from pathlib import Path
import time
from backend_streaming.deploy.logs.log_processor import LogProcessor
from backend_streaming.deploy.monitoring.alerts.email_config import send_alert

class GameMonitor:
    def __init__(self):
        self.processor = LogProcessor()

    def check_logs(self):
        """
        Check for issues and send alerts if needed
        NOTE: choosing not to remove the warnings so we don't miss it.
        """
        game_warnings = self.processor.get_warnings()
        
        # Send separate alert for each game's warnings
        for game in game_warnings:
            content = (
                f"***** WARNINGS for game {game.game_id} *****\n"
            )
            # Add each warning
            for warning in game.warnings:
                content += f"• {warning}\n"
            
            # Send alert with game-specific subject
            send_alert(
                content=content,
                subject=f"Game {game.game_id} Warnings"
            )

def main():
    monitor = GameMonitor()
    
    while True:
        monitor.check_and_alert()
        time.sleep(3600)  # Check every hour

if __name__ == "__main__":
    main()
