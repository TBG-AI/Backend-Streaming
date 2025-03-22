#!/bin/bash

# Check if environment file is provided
cd "$(dirname "$0")/.."
echo "Current directory: $(pwd)"
if [ -z "$1" ]; then
    echo "Error: No environment file specified"
    echo "Usage: ./run.sh <path-to-env-file>"
    echo "Example: ./run.sh .env.test"
    exit 1
fi

# Check if container already exists
if [ "$(docker ps -aq -f name=tbg-streamer)" ]; then
    echo "Removing existing 'tbg-streamer' container..."
    docker rm -f tbg-streamer
fi

echo "Running server with environment file: $1"
docker run \
  --env-file $1 \
  -d \
  --name tbg-streamer \
  -p 8001:8001 \
  tbg-streamer

echo "Container 'tbg-streamer' is now running!"