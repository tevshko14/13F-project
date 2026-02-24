FROM python:3.12-slim

WORKDIR /app

# Install build dependencies for native packages (numpy/pandas from edgartools)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ && \
    rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml README.md ./
COPY src/ ./src/

# Install the package (reads pyproject.toml, builds with hatchling)
RUN pip install --no-cache-dir hatchling && \
    pip install --no-cache-dir .

# Railway injects PORT env var
ENV PORT=8000

EXPOSE 8000

# Default: run the web server.
# Cron services override by setting START_COMMAND env var
# (e.g. START_COMMAND=filings-insider-sync).
CMD ["sh", "-c", "if [ -n \"$START_COMMAND\" ]; then echo \"Running: $START_COMMAND\" && exec $START_COMMAND; else exec gunicorn filings.web:app --bind 0.0.0.0:${PORT:-8000} --workers ${WEB_WORKERS:-1} --worker-class uvicorn.workers.UvicornWorker --timeout 120 --graceful-timeout 30 --preload --access-logfile -; fi"]
