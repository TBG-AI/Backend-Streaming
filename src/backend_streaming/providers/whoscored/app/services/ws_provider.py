import pytz
import pandas as pd
import soccerdata as sd
import time
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Dict
import logging
import argparse

from io import TextIOWrapper
from datetime import datetime, timedelta
from backend_streaming.providers.whoscored.domain.ws import setup_whoscored
from apscheduler.schedulers.background import BackgroundScheduler
from backend_streaming.providers.whoscored.infra.logs.logger import setup_provider_logger

# TODO: constantly upsdate the mappings for new players. or fill with new mappings

    
class WhoScoredProvider:
    def __init__(self, max_concurrent_games: int = 7):
        """
        This is the provider class for whoscored data.
        Methods:
            - schedule_batch: schedules scraping of games for time range
            - get_events: the main scraping method
            - stream_events: publishes events to designated queue
        """
        self.scheduler = BackgroundScheduler(timezone=pytz.UTC)
        self.max_concurrent_games = max_concurrent_games
        self.script_dir = Path(__file__).parent
        self.scraper_script = self.script_dir / "run_scraper.py"
        self.active_processes = {}  # game_id -> process mapping
        self.logger = setup_provider_logger()
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def schedule_batch(
        self,
        batch_start: datetime,
        batch_end: datetime,
        force_cache: bool=False
    ):
        """Schedule multiple game processors"""
        if not self.scheduler.running:
            self.scheduler.start()
        
        try:
            scraper = setup_whoscored()
            schedule = scraper.read_schedule(force_cache=force_cache)
            batch = self._filter_schedule(schedule, batch_start, batch_end)
            
            for start_time, row in batch.iterrows():
                game_id = row['game_id']
                self.logger.info(f"Scheduling game {game_id} for {start_time}")
                
                self.scheduler.add_job(
                    self.launch_game_processor,
                    'date',
                    run_date=start_time,
                    args=[game_id],
                    id=f"game_{game_id}",
                    name=f"Game {game_id}"
                )
            
            self.logger.info(f"Scheduled {len(batch)} games")
            
        except Exception as e:
            self.logger.error(f"Batch scheduling failed: {e}", exc_info=True)
            raise
    
    
    def launch_game_processor(self, game_id: str) -> bool:
        """Launch a single game processor with output going to a log file"""
        temp_script = self.script_dir / f"game_{game_id}_launcher.sh"
        temp_log_file = self.script_dir / f"game_{game_id}.log"
        
        try:
            # Generate single-game script
            script_content = self._generate_game_script(game_id)
            temp_script.write_text(script_content)
            temp_script.chmod(0o755)
            
            self.logger.info(f"Launching processor for game {game_id}")
            
            # Launch with output redirected to log file
            with open(temp_log_file, 'w') as f:
                process = subprocess.Popen(
                    [str(temp_script)],
                    stdout=f,
                    stderr=subprocess.STDOUT,  # Combine stderr into stdout
                    universal_newlines=True
                )
            
            self.active_processes[game_id] = {
                'process': process,
                'log_file': temp_log_file
            }
            return True
                
        except Exception as e:
            self.logger.error(f"Failed to launch game {game_id}: {e}")
            return False
        # TODO: don't remove since we need logs..?
        # finally:
        #     if temp_script.exists():
        #         temp_script.unlink()

    def monitor_logs(self, game_id: str):
        """
        Monitor logs for a specific game.
        This is a blocking call so need to launch in a thread.
        """
        proc_info = self.active_processes.get(game_id)
        if not proc_info:
            return
            
        with open(proc_info['log_file'], 'r') as f:
            f.seek(0, 2)  # Seek to end
            while proc_info['process'].poll() is None:  # While process is running
                line = f.readline()
                if line:
                    self.logger.info(f"Game {game_id}: {line.strip()}")
                else:
                    time.sleep(0.1)

    def _generate_game_script(self, game_id: str) -> str:
        """
        Generate shell script for a single game
        TODO: this indentation is a bit ugly.
        """
        return f'''#!/bin/bash
                # Activate conda environment
                source ~/miniconda3/etc/profile.d/conda.sh
                conda activate streaming

                # Run the processor
                python3 "{self.scraper_script}" "{game_id}"
                '''

    def _filter_schedule(self, schedule: pd.DataFrame, batch_start: datetime, batch_end: datetime) -> pd.DataFrame:
        """
        Filter schedule for a given date range and optional game_ids.
        ASSUMES schedule is result of read_schedule() function.
        """
        # this looks like 'date home-away' e.g. '2024-10-29 Luton-Burnley'
        schedule['full_game'] = schedule.index.get_level_values('game') 
        date_index = pd.to_datetime(schedule.index.get_level_values('game').str.split(' ').str[0])

        # proper conversion to UTC
        if date_index.tz is None:
            schedule.index = date_index.tz_localize(pytz.UTC)
        elif date_index.tz != pytz.UTC:
            schedule.index = date_index.tz_convert(pytz.UTC)

        # filter schedule 
        return schedule[batch_start: batch_end]
    
    
if __name__ == "__main__":
    from threading import Thread
    provider = WhoScoredProvider()
    
    # Launch each game and its monitoring thread
    for game_id in ['1821417', '1821389']:
        # Launch the game
        if provider.launch_game_processor(game_id):
            # # Start a dedicated monitoring thread for this game
            # monitor_thread = Thread(
            #     target=provider.monitor_logs,
            #     args=(game_id,),  # Pass game_id to monitor specific game
            #     name=f"monitor_{game_id}",
            #     daemon=True
            # )
            # monitor_thread.start()
            pass
    
    # Keep main thread alive while daemon threads run
    while any(p['process'].poll() is None for p in provider.active_processes.values()):
        time.sleep(1)