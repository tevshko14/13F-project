"""Background worker process for PaperPanda.

Runs the periodic tasks that previously lived in the web process
(stock-bundle warmer, fund-refresh sweep, market-data prefetch,
Reddit scanner, feature-announcement scanner, L2 warmer, retention
cleanup).  Pulled out so a slow yfinance / SEC EDGAR / ApeWisdom
upstream can no longer saturate the web tier's thread pool.

Architecture
------------
* Web tier: HTTP routes only.  Set ``WORKER_MODE=web`` to skip the
  periodic tasks this worker owns.
* Worker tier: this process.  Real work happens via the asyncio
  task loop; a tiny stdlib HTTP server runs in a daemon thread on
  ``$PORT`` solely so Railway's healthcheck (which is set globally
  in ``railway.toml`` and can't be overridden per-service in the
  dashboard) can probe ``/health`` and see the worker is alive.
  Communicates with web via Supabase (caches + L2 + the
  ``stock_overview_cache`` table all act as the data plane).
* Both processes share the same code/repo.  Differ only in entry
  point + ``WORKER_MODE``.

Started via:
    uv run filings-worker
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from types import SimpleNamespace
from typing import Any

logger = logging.getLogger(__name__)

_started_at = time.time()


class _HealthHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler -- only responds to ``/health``.

    Returns 200 + a small JSON payload so Railway's healthcheck
    passes; everything else 404s.  Intentionally stdlib-only so we
    don't pull in aiohttp / FastAPI just for one endpoint.
    """

    def do_GET(self):  # noqa: N802 (stdlib API)
        if self.path == "/health":
            body = json.dumps({
                "status": "ok",
                "service": "worker",
                "uptime_s": int(time.time() - _started_at),
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format, *args):  # noqa: A002 (stdlib API)
        # Silence the default per-request stderr line; the healthcheck
        # fires every few seconds and would dominate the log.
        return


def _start_health_server(port: int) -> None:
    """Spin up the healthcheck HTTP server in a daemon thread.

    Daemon thread = process exits when the main asyncio loop exits,
    no separate shutdown bookkeeping needed.  Single-threaded
    HTTPServer is fine: Railway's healthcheck hits us at most once
    every few seconds and one handler handles it in microseconds.
    """
    try:
        server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    except OSError as exc:
        logger.warning(
            "worker: failed to bind health server on port %d: %s "
            "(continuing without healthcheck endpoint -- Railway "
            "may restart us)",
            port, exc,
        )
        return
    thread = threading.Thread(
        target=server.serve_forever, name="health", daemon=True,
    )
    thread.start()
    logger.info("worker: health server listening on 0.0.0.0:%d", port)


def _configure_logging() -> None:
    """Match web.py log format so Railway log search works on both."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)-8s %(name)s — %(message)s',
        stream=sys.stdout,
    )
    # Quiet noisy libraries -- match web.py.
    for noisy in ("httpx", "httpcore", "urllib3", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


async def _initial_fund_cache_load() -> dict[str, Any]:
    """Load fund_cache from Supabase once at startup.

    Worker maintains its own in-memory copy so the fund-refresh sweep
    can populate it; subsequent refreshes go via SEC EDGAR and write
    back to Supabase L2 (which web reads on its own next startup).
    """
    from filings import cache
    try:
        data = await asyncio.wait_for(
            asyncio.to_thread(cache.load_cache_from_supabase),
            timeout=60,
        )
    except (asyncio.TimeoutError, Exception) as exc:
        logger.warning(
            "worker: Supabase fund_cache load failed (%s) — starting empty",
            exc,
        )
        data = {}
    return data or {}


async def _run() -> None:
    _configure_logging()
    logger.info("worker: starting (WORKER_MODE=%s)",
                os.environ.get("WORKER_MODE", "<unset>"))

    # Bring up the healthcheck endpoint FIRST so Railway's probe
    # passes during the (potentially slow) init that follows.  PORT
    # is set by Railway; default to 8081 for local dev so we don't
    # collide with a local web instance on 8000.
    port_str = os.environ.get("PORT", "8081")
    try:
        port = int(port_str)
    except ValueError:
        logger.warning("worker: invalid PORT=%r, defaulting to 8081", port_str)
        port = 8081
    _start_health_server(port)

    # Initialize the same thread-pool topology as web -- some tasks
    # (e.g. fund-refresh) call to_heavy()/to_supabase() and need pools
    # initialised on the running loop.
    from filings.concurrency import init_pools, shutdown_pools
    init_pools()

    # Eager-init async Supabase client so first call doesn't pay TLS
    # handshake + auth latency (matches web.py lifespan).
    from filings import supabase_cache
    await supabase_cache.init_async_client()

    # Load fund_cache (some tasks like the fund-refresh sweep + the
    # stock tier seeder read it).  Worker owns this in-memory copy;
    # the periodic refresh updates it AND writes back to Supabase L2.
    fund_cache = await _initial_fund_cache_load()
    logger.info("worker: fund_cache loaded (%d entries)", len(fund_cache))

    # Web's helpers expect a FastAPI-app-shaped object with `.state.X`.
    # Build a minimal shim that satisfies that contract without
    # actually constructing a FastAPI app or binding any port.
    fake_app = SimpleNamespace(
        state=SimpleNamespace(
            fund_cache=fund_cache,
            refresh_status="idle",
            refresh_progress={"done": 0, "failed": 0, "total": 0},
        ),
    )

    # Import the periodic-task bodies + the wrapper from web.py.  This
    # incidentally constructs the FastAPI app instance, which is fine:
    # we never serve it, never bind a port, never accept a connection.
    # The cost is some module-level setup (route registration, env
    # parsing) -- O(50ms) of imports we'd pay anyway.
    from filings import web as _web

    tasks: set[asyncio.Task] = set()

    def _track(coro, name: str) -> asyncio.Task:
        t = asyncio.create_task(coro, name=name)
        tasks.add(t)
        t.add_done_callback(tasks.discard)
        return t

    # ── Tasks: same set the lifespan gates behind WORKER_MODE!=web ──

    _track(_web._prefetch_market_data(fake_app), "prefetch_market")

    _track(
        asyncio.to_thread(supabase_cache.run_retention_cleanup),
        "retention_cleanup",
    )

    if _web._ENABLE_BACKGROUND_REFRESH:
        fake_app.state.refresh_status = "pending"
        _track(_web._delayed_refresh_sweep(fake_app), "refresh_sweep")

    _track(
        _web._periodic_task(
            name="Reddit velocity scan", startup_delay=60,
            interval=_web._REDDIT_SCAN_INTERVAL,
            body=_web._run_reddit_velocity_scan,
        ),
        "reddit_scanner",
    )

    _track(
        _web._periodic_task(
            name="Feature announcement scan", startup_delay=45,
            interval=_web._FEATURE_ANNOUNCE_INTERVAL,
            body=_web._run_feature_announcement_scan,
        ),
        "feature_announce_scanner",
    )

    if _web._redesign_preview_router.is_enabled():
        _track(
            _web._periodic_task(
                name="Redesign L2 warmer", startup_delay=30,
                interval=_web._REDESIGN_L2_WARMER_INTERVAL,
                body=_web._run_redesign_l2_warm,
            ),
            "redesign_l2_warmer",
        )

    _track(
        _web._periodic_task(
            name="stock warmer", startup_delay=180,
            interval=_web._STOCK_WARMER_INTERVAL_S,
            body=_web._run_stock_warmer,
        ),
        "stock_warmer",
    )

    # _run_initial_tier_seed reads from app.state.fund_cache so it
    # must run AFTER our load above.  Idempotent (Supabase upsert
    # with on_conflict ignore_duplicates), safe across restarts.
    _track(_web._run_initial_tier_seed(), "stock_tier_seeder")

    logger.info("worker: %d periodic tasks armed", len(tasks))

    # Wait for SIGTERM/SIGINT.  When it arrives, cancel outstanding
    # tasks and let asyncio drain.  Railway's graceful-shutdown
    # window is 30s by default; tasks should respect cancellation.
    stop = asyncio.Event()

    def _on_signal(signame: str) -> None:
        if not stop.is_set():
            logger.info("worker: %s received -- shutting down", signame)
            stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _on_signal, sig.name)
        except NotImplementedError:
            # Windows / unusual envs don't support add_signal_handler.
            pass

    try:
        await stop.wait()
    finally:
        logger.info("worker: cancelling %d tasks", len(tasks))
        for t in tasks:
            t.cancel()
        # Best-effort drain; bound by Railway's 30s graceful window.
        await asyncio.gather(*tasks, return_exceptions=True)
        shutdown_pools()
        logger.info("worker: clean exit")


def main() -> None:
    """CLI entry point (registered in pyproject.toml)."""
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
