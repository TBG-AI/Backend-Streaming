import soccerdata as sd
import platform

LEAGUE = "ENG-Premier League"
SEASON = '24-25'

# TODO: add different leagues and seasons

def setup_whoscored() -> sd.WhoScored:
    """
    setup the whoscored scraper object
    """
    # get platform
    system = platform.system().lower()
    # os.environ['CHROME_VERSION'] = CHROME_VERSION    
    return sd.WhoScored(
        leagues=LEAGUE, 
        seasons=SEASON,
        # path_to_browser=CHROME_PATHS[system]
    )