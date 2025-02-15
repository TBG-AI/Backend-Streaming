import pytz
import pandas as pd
import soccerdata as sd
import time
import os
import subprocess
import sys
from pathlib import Path
from typing import List
import logging

from io import TextIOWrapper
from datetime import datetime, timedelta
from backend_streaming.providers.whoscored.domain.ws import setup_whoscored
from apscheduler.schedulers.background import BackgroundScheduler

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
        self.scraper_script = self.script_dir / "scraper.py"
        self.active_processes: dict[str, subprocess.Popen] = {}
    
    def schedule_batch(
        self,
        batch_start: datetime,
        batch_end: datetime,
        force_cache: bool=False
    ):
        """
        Instead of directly scheduling the scraping, this will schedule the launch 
        of dedicated game processors (containers) for each game.
        """
        if not self.scheduler.running:
            self.scheduler.start()
        
        try:
            # default epl 24-25 season
            scraper = setup_whoscored()
            schedule = scraper.read_schedule(force_cache=force_cache)
            batch = self._filter_schedule(schedule, batch_start, batch_end)
            
            for start_time, row in batch.iterrows():
                game_id = row['game_id']
                print(f"Scheduling game processor for game {game_id} to start at {start_time}")
                
                try:
                    # Schedule the launch of a dedicated game processor
                    self.scheduler.add_job(
                        self._launch_game_processor,  # This will launch the container/process
                        'date',
                        run_date=start_time,
                        args=[game_id],
                        id=f"launch_processor_{game_id}",
                        name=f"Launch processor for game {game_id}"
                    )
                    print(f"Successfully scheduled processor launch for game {game_id}")
                except Exception as e:
                    print(f"Failed to schedule processor launch for game {game_id}: {str(e)}")
            
            print(f"Scheduled processors for {len(batch)} games")
                
        except Exception as e:
            print(f"Failed to schedule batch: {str(e)}")
            raise

    def launch_games(self, game_ids: List[str]):
        """Launch multiple games in separate terminals"""
        # Create temporary shell script with the provided game IDs
        import ipdb; ipdb.set_trace()
        script_content = self._generate_shell_script(game_ids)
        temp_script_path = self.script_dir / "temp_run_scrapers.sh"
        
        try:
            # Write temporary script
            with open(temp_script_path, 'w') as f:
                f.write(script_content)
            
            # Make script executable
            os.chmod(temp_script_path, 0o755)
            
            # Execute the script
            logging.info(f"Launching scrapers for games: {game_ids}")
            process = subprocess.Popen(
                [str(temp_script_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            # Store process for monitoring
            for game_id in game_ids:
                self.active_processes[game_id] = process
                
            return True
            
        except Exception as e:
            logging.error(f"Error launching games: {e}")
            return False
        finally:
            # Clean up temporary script
            if temp_script_path.exists():
                temp_script_path.unlink()

    def check_status(self) -> dict:
        """Check status of running games"""
        status = {}
        for game_id, process in list(self.active_processes.items()):
            if process.poll() is not None:
                # Process has finished
                stdout, stderr = process.communicate()
                status[game_id] = {
                    'status': 'completed',
                    'return_code': process.returncode,
                    'output': stdout,
                    'errors': stderr
                }
                del self.active_processes[game_id]
            else:
                status[game_id] = {'status': 'running'}
        return status

    def cleanup(self):
        """Terminate all active processes"""
        for process in self.active_processes.values():
            process.terminate()
        self.active_processes.clear()

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
    
    def _generate_shell_script(self, game_ids: List[str]) -> str:
        """Generate shell script content with provided game IDs"""
        return f'''#!/bin/bash
                # Array of game IDs
                GAMES=({" ".join(f'"{gid}"' for gid in game_ids)})

                # Get the absolute path to the scraper script
                SCRAPER_SCRIPT="{self.scraper_script}"

                # Activate the conda environment
                source ~/miniconda3/etc/profile.d/conda.sh
                conda activate streaming

                # Launch a terminal for each game
                for game_id in "${{GAMES[@]}}"; do
                    echo "Launching scraper for game $game_id"
                    
                    # For macOS
                    osascript -e "tell app \\"Terminal\\" 
                        do script \\"conda activate streaming && python3 '$SCRAPER_SCRIPT' '$game_id'\\"
                    end tell"
                    
                    sleep 1
                done

                echo "All scrapers launched"
                '''


if __name__ == "__main__":
    # Example of scheduling games
    provider = WhoScoredProvider()
    try:
        # Launch some simultaneous games
        simultaneous_games = ['1821417', '1821389']
        provider.launch_games(simultaneous_games)
        
        # Monitor their status
        while provider.active_processes:
            status = provider.check_status()
            for game_id, info in status.items():
                print(f"Game {game_id}: {info['status']}")
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\nGracefully shutting down...")
    finally:
        provider.cleanup()
