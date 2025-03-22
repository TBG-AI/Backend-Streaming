#!/bin/bash

echo "====== Building Docker image for TBG Streamer ======"

# Check if Docker daemon is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker daemon is not running."
    echo "Please start Docker Desktop and try again."
    exit 1
fi

# Change to the root directory (where Dockerfile, requirements.txt, and setup.py are)
cd "$(dirname "$0")/.."
echo "Current directory: $(pwd)"
echo "requirements.txt: $(ls requirements.txt)"

# Build the image with no cache
docker buildx build \
  --platform linux/arm64 \
  -t tbg-streamer \
  --load .

echo "====== Docker image 'tbg-streamer' built successfully! ======" 