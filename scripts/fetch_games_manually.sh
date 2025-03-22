#!/bin/bash
set -e

# Check if an argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <env>"
    echo "Please specify the environment: 'prod' or 'local'"
    exit 1
fi

# Determine which environment file to load
ENV_FILE=".env.$1"

# Load the appropriate environment variables
if [ -f "$ENV_FILE" ]; then
    echo "Loading environment from $ENV_FILE"
    set -a; source "$ENV_FILE"; set +a
else
    echo "Error: $ENV_FILE file not found"
    exit 1
fi

# Forward all arguments to the Python script, excluding the first one
shift
python -m backend_streaming.providers.whoscored.infra.executables.fetch_games_manually "$@"
