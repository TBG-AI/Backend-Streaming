# 1) Use Python 3.10 slim as the base
FROM python:3.10-slim

# 2) (Optional) Avoid buffering Python output
ENV PYTHONUNBUFFERED=1

# 3) Install any necessary system packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

# 4) Set a working directory inside the container
WORKDIR /app

# 5) Copy in ONLY files needed to install dependencies (for caching)
COPY requirements.txt ./

# 6) Install dependencies (caches this layer if requirements.txt doesn’t change)
RUN pip install --no-cache-dir -r requirements.txt

# 7) Now copy in the rest of the project
COPY . .

# 8) (Optional) If you want to install your code as a Python package:
RUN pip install --no-cache-dir .

# 9) Expose FastAPI’s default port
EXPOSE 8001

# 10) Default command to run your FastAPI app with Uvicorn
RUN chmod +x scripts/entrypoint.sh
ENTRYPOINT ["./scripts/entrypoint.sh"]
CMD ["uvicorn", "src.backend_streaming.main:app", "--host", "0.0.0.0", "--port", "8001"]