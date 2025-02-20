import soccerdata as sd

ws = sd.WhoScored(leagues="ENG-Premier League", seasons='24-25')
# loader = ws.read_events(
#     output_fmt='loader', 
#     match_id=1821424, 
#     force_cache=True,
#     live=True
# )
schedule = ws.read_schedule(force_cache=True)
import ipdb; ipdb.set_trace()