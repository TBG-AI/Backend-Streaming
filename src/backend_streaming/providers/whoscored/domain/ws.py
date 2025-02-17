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
        super().__init__(*args, **kwargs)

    def _init_webdriver(self) -> "uc.Chrome":
        """
        Start the Selenium driver with unique port and file lock to prevent race conditions.
        """
        if hasattr(self, "_driver"):
            self._driver.quit()

        lock_file = Path(__file__).parent / ".chromedriver_setup.lock"
        lock = filelock.FileLock(str(lock_file), timeout=30)  # Wait up to 30 seconds
        
        try:
            with lock:
                return uc.Chrome(port=self.driver_port)
        except filelock.Timeout:
            self.logger.error("Timeout waiting for chromedriver setup lock")
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
    


def wait_for_chromedriver_setup(chrome_data_dir: str, timeout: int = 30) -> bool:
    """
    Wait until ChromeDriver is fully set up
    # TODO: need this to avoid race conditions with undetected-chromedriver setup
    """
    start_time = time.time()
    chrome_dir = Path(chrome_data_dir)
    
    while time.time() - start_time < timeout:
        # Check for key Chrome directories and files
        if not (chrome_dir / "Default").exists():
            time.sleep(0.5)
            continue
            
        # Check for running ChromeDriver process
        for proc in psutil.process_iter(['name', 'cmdline']):
            if proc.info['name'] == 'chromedriver':
                if str(chrome_data_dir) in ' '.join(proc.info['cmdline'] or []):
                    return True
                    
        time.sleep(0.5)
    
    return False
