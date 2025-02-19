FROM python:3.9-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application code
COPY . .

# Make scripts executable
RUN chmod +x deploy/scripts/*

# Default command (will be overridden by docker-compose)
CMD ["python", "-m", "backend_streaming.providers.whoscored.app.main"]
