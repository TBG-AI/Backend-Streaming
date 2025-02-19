import pytz
import pandas as pd
import soccerdata as sd
import os
import subprocess

from pathlib import Path
from typing import List, Dict
from datetime import datetime, timedelta
from backend_streaming.providers.whoscored.domain.ws import setup_whoscored
from apscheduler.schedulers.background import BackgroundScheduler
from backend_streaming.providers.whoscored.domain.batch_schedule import BatchSchedule
from backend_streaming.providers.whoscored.infra.logs.logger import setup_provider_logger

    
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

    def get_scheduler_status(self) -> dict:
        """
        Get current status of scheduled jobs.
        
        Returns:
            dict: Contains counts of jobs in different states
        """
        if not self.scheduler.running:
            return {
                'status': 'stopped',
                'scheduled_jobs': 0,
                'running_jobs': 0,
                'next_runtime': None
            }
        
        self._cleanup()
        jobs = self.scheduler.get_jobs()
        running_processes = len(self.active_processes)
        next_job = min((job.next_run_time for job in jobs if job.next_run_time), default=None)

        return {
            'status': 'running',
            'scheduled_jobs': len(jobs),
            'running_jobs': running_processes,
            'next_runtime': next_job
        }

    def schedule_batch(
        self,
        batch_start: datetime,
        batch_end: datetime,
        force_cache: bool=False,
        # will launch first 5 games in parallel immediately
        debug: bool=False
    ):
        """
        Schedule multiple game processors
        """
        if debug: 
            self.logger = setup_provider_logger(file_name="provider_debug")
            la_tz = pytz.timezone('America/Los_Angeles')
            start_time = datetime.now(pytz.UTC).astimezone(la_tz) + timedelta(seconds=30)
            force_cache = True
        
        if not self.scheduler.running:
            self.scheduler.start()
        
        try:
            scraper = setup_whoscored()
            schedule = scraper.read_schedule(force_cache=force_cache)
            batch = self._filter_schedule(schedule, batch_start, batch_end)
            if debug:
                batch = batch[:3]
            
            for _, row in batch.iterrows():
                game_id = row['game_id']
                # TODO: make sure to convert our deployment timezone to UTC
                start_time = start_time if debug else row['start_time']
                self.logger.info(f"Scheduling game {game_id} for {start_time}")
                
                self.scheduler.add_job(
                    self.launch_game_processor,
                    'date',
                    run_date=start_time,
                    args=[game_id],
                    id=f"game_{game_id}",
                    name=f"Game {game_id}"
                )
            # After scheduling games, save the batch end date            
            # NOTE: this is for monitoring purposes
            self.logger.info(f"Scheduled {len(batch)} games")
            BatchSchedule(batch_end=batch_end).save()
            
        except Exception as e:
            self.logger.error(f"Batch scheduling failed: {e}", exc_info=True)
            raise
    
    
    def launch_game_processor(self, game_id: str) -> bool:
        """Launch a single game processor"""
        temp_script = self.script_dir / f"game_{game_id}_launcher.sh"
        
        try:
            script_content = self._generate_game_script(game_id)
            temp_script.write_text(script_content)
            temp_script.chmod(0o755)
            
            self.logger.info(f"Launching processor for game {game_id}")
            process = subprocess.Popen(
                [str(temp_script)],
                # Force unbuffered output. 
                # this is to see the logs in real time
                env={**os.environ, 'PYTHONUNBUFFERED': '1'},
                universal_newlines=True
            )
            
            self.active_processes[game_id] = process
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to launch game {game_id}: {e}")
            return False

    def _cleanup(self, terminate: bool=False):
        """
        Cleanup finished processes and temp scripts.
        If terminate is True, will terminate all processes.
        """
        # Clean up completed processes
        completed_games = []
        for game_id, process in self.active_processes.items():
            if terminate or process.poll() is not None:  # Process has finished
                completed_games.append(game_id)
                # Clean up the temp script
                temp_script = self.script_dir / f"game_{game_id}_launcher.sh"
                if temp_script.exists():
                    temp_script.unlink()
                    
        # Remove completed processes from active_processes
        for game_id in completed_games:
            del self.active_processes[game_id]

    def _generate_game_script(self, game_id: str) -> str:
        """Generate shell script for a single game"""
        return f'''#!/bin/bash
                # Activate conda environment
                source ~/miniconda3/etc/profile.d/conda.sh
                conda activate streaming

                # Run the processor in unbuffered mode
                python3 -u "{self.scraper_script}" "{game_id}"
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
    import time
    ###################################################
    # Testing if we can launch multiple games at once #
    ###################################################
    # provider = WhoScoredProvider()
    
    # # Launch each game
    # for game_id in ['1821417', '1821389']:
    #     provider.launch_game_processor(game_id)

    # while any(p.poll() is None for p in provider.active_processes.values()):
    #     time.sleep(1)
    # # Cleanup
    # provider.cleanup()

    ####################################################
    # Testing to see if we can schedule multiple games #
    ####################################################
    timezone = pytz.timezone('UTC') 
    provider = WhoScoredProvider()
    
    try:
        # Schedule the batch
        provider.schedule_batch(
            batch_start=timezone.localize(datetime(2025, 1, 25)),
            batch_end=timezone.localize(datetime(2025, 1, 25)),
            force_cache=True,
            debug=True
        )
        
        # Keep main thread alive while there are scheduled or running jobs
        while True:
            status = provider.get_scheduler_status()
            
            # Exit if no more scheduled jobs and no running processes
            if status['scheduled_jobs'] == 0 and status['running_jobs'] == 0:
                provider.logger.info("All jobs completed. Exiting...")
                break
                
            provider.logger.info(
                f"Status: {status['scheduled_jobs']} scheduled, "
                f"{status['running_jobs']} running. "
                f"Next run at: {status['next_runtime']}"
            )
            time.sleep(10)  # Check every 10 seconds
            
    except KeyboardInterrupt:
        provider.logger.info("Received interrupt signal. Cleaning up...")

    finally:
        provider._cleanup(terminate=True)
        provider.scheduler.shutdown()

   
