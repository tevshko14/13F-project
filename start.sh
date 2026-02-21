#!/bin/sh

# Allow cron services to override the start command via env var.
# Set START_COMMAND in Railway env vars for each cron service, e.g.:
#   START_COMMAND=filings-insider-sync
#   START_COMMAND=filings-sync
#   START_COMMAND=filings-youtube-sync
if [ -n "$START_COMMAND" ]; then
  echo "Running custom command: $START_COMMAND"
  exec $START_COMMAND
fi

exec gunicorn filings.web:app \
  --bind "0.0.0.0:${PORT:-8000}" \
  --workers "${WEB_WORKERS:-2}" \
  --worker-class uvicorn.workers.UvicornWorker \
  --timeout 120 \
  --graceful-timeout 30 \
  --access-logfile -
