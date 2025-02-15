#!/bin/bash

# Array of game IDs
# GAMES=("1821417" "1821389")
GAMES=("1821417")

# Get the absolute path to the scraper script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRAPER_SCRIPT="$SCRIPT_DIR/scraper.py"

# Activate the conda environment if needed
source ~/miniconda3/etc/profile.d/conda.sh
conda activate streaming

# Launch a terminal for each game
for game_id in "${GAMES[@]}"; do
    echo "Launching scraper for game $game_id"
    
    # For macOS
    osascript -e "tell app \"Terminal\" 
        do script \"conda activate streaming && python3 '$SCRAPER_SCRIPT' '$game_id'\"
    end tell"
    
    # Small delay to prevent terminal window overlap
    sleep 1
done

echo "All scrapers launched"
