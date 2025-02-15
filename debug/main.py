import subprocess
from pathlib import Path
from typing import List
import argparse

def generate_shell_script(game_ids: List[str], scraper_script: Path) -> str:
    """Generate shell script content with provided game IDs"""
    return f'''#!/bin/bash

# Array of game IDs
GAMES=({" ".join(f'"{gid}"' for gid in game_ids)})

# Get the absolute path to the scraper script
SCRAPER_SCRIPT="{scraper_script}"

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

def test_run_scrapers(game_ids: List[str]):
    """
    Run scrapers for specified game IDs
    
    Args:
        game_ids: List of WhoScored game IDs to process
    """
    # Get the paths
    script_dir = Path(__file__).parent
    scraper_script = script_dir / "process_game.py"
    temp_script = script_dir / "temp_run_scrapers.sh"
    
    print(f"Setting up script for games: {game_ids}")
    
    try:
        # Generate and write shell script
        script_content = generate_shell_script(game_ids, scraper_script)
        temp_script.write_text(script_content)
        
        # Make script executable
        temp_script.chmod(0o755)
        
        print(f"Launching script: {temp_script}")
        
        # Launch the script
        process = subprocess.Popen(
            [str(temp_script)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        # Wait for completion and get output
        stdout, stderr = process.communicate()
        
        print("\nProcess completed")
        print(f"Return code: {process.returncode}")
        print("\nStandard output:")
        print(stdout)
        
        if stderr:
            print("\nStandard error:")
            print(stderr)
            
    except Exception as e:
        print(f"Error running script: {e}")
    finally:
        # Clean up temporary script
        if temp_script.exists():
            temp_script.unlink()

def main(args: List[str] = None):
    parser = argparse.ArgumentParser(description='Run WhoScored scrapers for multiple games')
    parser.add_argument('game_ids', nargs='+', help='One or more WhoScored game IDs to process')
    
    parsed_args = parser.parse_args(args)
    test_run_scrapers(parsed_args.game_ids)

if __name__ == "__main__":
    main()