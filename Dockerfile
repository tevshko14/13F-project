FROM python:3.12-slim

WORKDIR /app

# Install build dependencies for native packages (numpy/pandas from edgartools)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ && \
    rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml ./
COPY src/ ./src/

# Install the package (reads pyproject.toml, builds with hatchling)
RUN pip install --no-cache-dir hatchling && \
    pip install --no-cache-dir .

# Railway injects PORT env var
ENV PORT=8000

EXPOSE ${PORT}

CMD uvicorn filings.web:app --host 0.0.0.0 --port ${PORT}
