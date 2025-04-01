#!/bin/bash
# tests/run_test_environment.sh
# Purpose: Manages the testing environment setup and teardown for both main and WebSocket services

set -e

# Process command arguments
if [ "$1" == "--stop" ]; then
  echo "Stopping Docker containers..."
  docker compose -f tests/docker-compose.yml down -v --remove-orphans
  echo "Environment stopped successfully."
  exit 0
fi

# Check if we need to rebuild
if [ "$1" == "--rebuild" ]; then
  echo "Rebuilding Docker containers..."
  docker compose -f tests/docker-compose.yml build --no-cache
fi

echo "Starting testing environment with PostgreSQL..."

# Create logs directory if it doesn't exist
mkdir -p ./tests/logs

# Tables for the main database
MAIN_TABLES="teams,players"

# Step 1: Dump the database schema if it doesn't exist or force flag is provided
if [ ! -f "./tests/db_schema.sql" ] || [ "$1" == "--force-dump" ]; then
  echo "Step 1: Dumping main database schema and test data..."
  chmod +x tests/dump_database.sh
  
  # Use the -t parameter to specify exactly which tables to include
  ./tests/dump_database.sh -t "$MAIN_TABLES"
else
  echo "Step 1: Using existing main database schema dump. Use --force-dump to refresh."
fi

# Step 2: Clean and start Docker environment
echo "Step 2: Starting Docker environment..."
echo "Creating test_network if it doesn't exist..."
docker network create test_network || true

docker compose -f tests/docker-compose.yml down -v # Clean start
docker compose -f tests/docker-compose.yml up -d

# Step 3: Wait for the main application to be ready
echo "Step 3: Waiting for the main application to be ready..."
timeout=60
counter=0
until $(curl --output /dev/null --silent --fail http://localhost:8001/health || [ $counter -eq $timeout ]); do
    if [ $counter -eq $timeout ]; then
        echo "WARNING: Could not verify main application health endpoint. Continuing anyway as it might not have a health check."
        break
    fi
    echo "Waiting for main application to be ready... ($counter/$timeout seconds)"
    sleep 1
    counter=$((counter+1))
done

# Step 4: Wait for the streaming service to be ready
echo "Step 4: Waiting for the streaming service to be ready..."
timeout=60
counter=0
until $(curl --output /dev/null --silent --fail http://localhost:8001/health || [ $counter -eq $timeout ]); do
    if [ $counter -eq $timeout ]; then
        echo "WARNING: Could not verify streaming service health endpoint. Continuing anyway as it might not have a health check."
        break
    fi
    echo "Waiting for streaming service to be ready... ($counter/$timeout seconds)"
    sleep 1
    counter=$((counter+1))
done

# Allow more time for services to initialize
echo "Giving services additional time to initialize..."
sleep 2

echo "Environment setup complete!"
echo "The streaming service is available at: http://localhost:8001"
echo "To see application logs:"
echo "  - Streaming service: docker logs streaming_microservice"
echo "To stop the environment, use: ./tests/run_test_environment.sh --stop" 