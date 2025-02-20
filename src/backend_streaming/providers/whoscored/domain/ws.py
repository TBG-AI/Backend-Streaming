import psutil
import filelock
from pathlib import Path
import time
import undetected_chromedriver as uc
import soccerdata as sd
import os
from dataclasses import dataclass
from typing import Dict
import json
from backend_streaming.providers.whoscored.infra.logs.logger import setup_game_logger
LEAGUE = "ENG-Premier League"
SEASON = '24-25'


class WhoScored(sd.WhoScored):
    def __init__(self, game_id: str, *args, **kwargs):
        """
        Custom WhoScored class to handle unique ChromeDriver setup for each game
        NOTE: keeping the same name as the base class to properly inherit class properties
        """
        self.game_id = game_id
        # Calculate unique port for ChromeDriver
        self.driver_port = 9515 + (int(self.game_id) % 484)
        self.logger = setup_game_logger(self.game_id, overwrite=True)
        super().__init__(*args, **kwargs)

    def _init_webdriver(self) -> "uc.Chrome":
        """
        Start the Selenium driver with unique port and file lock to prevent race conditions.
        """
        # NOTE: this is the first time the game log is created!
        if hasattr(self, "_driver"):
            self._driver.quit()

        # waiting up to 2 minutes for lock but we won't have that many concurrent games running. 
        # NOTE: maybe only for tests...
        lock_file = Path(__file__).parent / ".chromedriver_setup.lock"
        lock = filelock.FileLock(str(lock_file), timeout=120)  
        
        try:
            with lock:
                self.logger.info(f"Acquired lock for game {self.game_id}")
                return uc.Chrome(port=self.driver_port)
        except filelock.Timeout:
            self.logger.warning(f"chromedriver setup timeout")
            raise


def setup_whoscored(game_id: str = None) -> sd.WhoScored:
    """
    Set up the WhoScored scraper with isolated resources if game_id is provided
    """    
    if game_id is not None:
        return WhoScored(
            game_id=game_id,
            leagues=LEAGUE, 
            seasons=SEASON,
        )
    else:
        return sd.WhoScored(
            leagues=LEAGUE, 
            seasons=SEASON,
        )

