#!/bin/bash
set -e

# Load environment variables
if [ -f .env.prod ]; then
    set -a; source .env.prod; set +a
else
    echo "Error: .env.prod file not found"
    exit 1
fi

# Forward all arguments to the Python script
python -m backend_streaming.providers.whoscored.infra.executables.fetch_games_manually "$@"
