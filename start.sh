#!/bin/sh
exec gunicorn filings.web:app \
  --bind "0.0.0.0:${PORT:-8000}" \
  --workers "${WEB_WORKERS:-2}" \
  --worker-class uvicorn.workers.UvicornWorker \
  --timeout 120 \
  --graceful-timeout 30 \
  --access-logfile -
