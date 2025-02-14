#!/bin/bash

# Activate virtual environment if needed (uncomment and modify path as needed)
# source /path/to/your/venv/bin/activate

# Set environment variables if needed
# export RABBITMQ_URL="amqp://guest:guest@localhost:5672/"
# export QUEUE_NAME="game_events"

# Run the Python module
python -m src.backend_streaming.providers.opta.replay

# Add error handling
if [ $? -eq 0 ]; then
    echo "Replay completed successfully"
else
    echo "Replay failed with exit code $?"
fi