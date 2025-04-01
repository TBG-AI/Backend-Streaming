#!/bin/bash

echo "Setting PYTHONPATH"

# Set PYTHONPATH
export PYTHONPATH=/app:${PYTHONPATH}
export PYTHONPATH=/app/src:${PYTHONPATH}

# Run whatever command was passed to docker run
exec "$@" 