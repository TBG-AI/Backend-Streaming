#!/bin/bash

# Check if environment file is provided
if [ -z "$1" ]; then
    echo "Error: No environment file specified"
    echo "Usage: ./build_and_run_docker.sh <path-to-env-file>"
    echo "Example: ./build_and_run_docker.sh .env.docker"
    exit 1
fi

# Store the environment file path
ENV_FILE="$1"

# Execute the build script
echo "====== Step 1: Building Docker image ======"
./scripts/build.sh

# Check if build was successful
if [ $? -ne 0 ]; then
    echo "Error: Docker build failed. Exiting."
    exit 1
fi

# Execute the run script with the provided environment file
echo "====== Step 2: Running Docker container ======"
./scripts/run.sh "$ENV_FILE"

echo "====== Build and run process completed! ======"
