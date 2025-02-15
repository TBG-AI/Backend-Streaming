import psutil
from pathlib import Path
import time
import undetected_chromedriver as uc
import soccerdata as sd

LEAGUE = "ENG-Premier League"
SEASON = '24-25'

class WhoScored(sd.WhoScored):
    def __init__(self, game_id: str, *args, **kwargs):
        """
        Keeping it the same name as the base
        """
        self.game_id = game_id
        # Calculate unique port for ChromeDriver
        self.driver_port = 9515 + (int(self.game_id) % 484)
        super().__init__(*args, **kwargs)

    def _init_webdriver(self) -> "uc.Chrome":
        """
        Start the Selenium driver with unique port.
        TODO: Add the proxy setup if necessary. Currently removed for clarity
        """
        if hasattr(self, "_driver"):
            self._driver.quit()

        return uc.Chrome(port=self.driver_port)

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
