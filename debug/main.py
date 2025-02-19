import soccerdata as sd
from backend_streaming.providers.whoscored.domain.ws import setup_whoscored
import pandas as pd
import pytz
from datetime import datetime

scraper = setup_whoscored()
# schedule = scraper.read_schedule(force_cache=True)

# # this looks like 'date home-away' e.g. '2024-10-29 Luton-Burnley'
# schedule['full_game'] = schedule.index.get_level_values('game') 
# date_index = pd.to_datetime(schedule.index.get_level_values('game').str.split(' ').str[0])

# # proper conversion to UTC
# if date_index.tz is None:
#     schedule.index = date_index.tz_localize(pytz.UTC)
# elif date_index.tz != pytz.UTC:
#     schedule.index = date_index.tz_convert(pytz.UTC)

# # filter schedule 
# # return schedule[batch_start: batch_end]

# timezone = pytz.timezone('UTC') 
# batch_start=timezone.localize(datetime(2025, 2, 19))
# batch_end=timezone.localize(datetime(2025, 2, 20))

# batch = schedule[batch_start: batch_end]

scraper.read_events()





# TODO: are all games in utc???
import ipdb; ipdb.set_trace()