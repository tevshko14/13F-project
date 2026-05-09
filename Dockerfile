FROM python:3.12-slim

WORKDIR /app

# Install build dependencies for native packages (numpy/pandas from edgartools)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ && \
    rm -rf /var/lib/apt/lists/*

# Copy pinned requirements (exact versions from working local env)
COPY requirements.txt ./

# Install ALL dependencies from pinned versions — no resolution, no surprises
RUN pip install --no-cache-dir -r requirements.txt

# Copy project source and install (deps already installed, just need build tool + our code)
COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN pip install --no-cache-dir hatchling && \
    pip install --no-cache-dir --no-deps .

# Print key versions for debugging
RUN python -c "import jinja2, starlette, fastapi; print(f'jinja2={jinja2.__version__} starlette={starlette.__version__} fastapi={fastapi.__version__}')"

# Railway injects PORT env var
ENV PORT=8000

# Cap glibc malloc arenas (default scales with CPU count, each up to 64MB
# of fragmentation potential).  Set BEFORE Python starts -- glibc reads
# this once at process init.  Knocks 100-300MB off RSS at steady state.
ENV MALLOC_ARENA_MAX=2

EXPOSE 8000

# Default to a single uvicorn worker.  Multiple workers each carry their
# own caches + heap, multiplying RSS by the worker count.  At our
# traffic level one worker w/ async event loop has plenty of headroom;
# bump WEB_WORKERS env var if any specific deploy needs redundancy.
CMD ["sh", "-c", "if [ -n \"$START_COMMAND\" ]; then echo \"Running: $START_COMMAND\" && exec $START_COMMAND; else exec gunicorn filings.web:app --bind 0.0.0.0:${PORT:-8000} --workers ${WEB_WORKERS:-1} --worker-class uvicorn.workers.UvicornWorker --timeout 120 --graceful-timeout 30 --max-requests 1000 --max-requests-jitter 50 --preload --access-logfile -; fi"]
