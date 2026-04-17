web: uvicorn filings.web:app --host 0.0.0.0 --port $PORT --limit-max-requests 1000
# Sync worker: Railway Cron Job -> uv run filings-sync (every 12h, or 2h during filing season)
# Note: railway.toml is the primary deploy spec and uses gunicorn with --max-requests 1000
# --max-requests-jitter 50. This Procfile is a fallback for environments that only read
# Procfile (local dev via `heroku local`, old Railway configs). Match the recycle cadence.
