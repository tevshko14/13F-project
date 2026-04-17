"""Watchlist daily digest email worker — sends personalized digests via Resend.

Designed to run as a Railway Cron Job once per hour.
For each user with digest_enabled=TRUE and a non-empty watchlist:
  1. Check if past their preferred digest_time in their timezone
  2. Check if digest already sent today (watchlist_digest_log)
  3. Gather last 24h notifications matching their watchlist tickers
  4. Skip if 0 events (no empty emails)
  5. Render HTML email, send via Resend
  6. Log to watchlist_digest_log

Usage:
    uv run filings-digest
"""

import logging
import os
import time
from datetime import datetime, timedelta, timezone

from filings import supabase_cache
from filings.log_config import setup_worker_logging

logger = logging.getLogger(__name__)

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
FROM_EMAIL = os.environ.get("DIGEST_FROM_EMAIL", "PaperPanda <digest@paperpanda.io>")
BASE_URL = os.environ.get("BASE_URL", "https://paperpanda.io")

# Signal type icons for email
SIGNAL_ICONS = {
    "13f_change": "📊",
    "insider_trade": "👤",
    "congress_trade": "🏛️",
    "reddit_velocity": "🔥",
    "youtube": "🎬",
    "options_activity": "📈",
    "convergence": "🎯",
}


def _user_local_now(tz_name: str) -> datetime:
    """Get the current time in the user's timezone."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(tz_name))
    except Exception:
        # Fallback to UTC
        return datetime.now(timezone.utc)


def _already_sent_today(user_id: str, today_date: str) -> bool:
    """Check if we already sent a digest for this user today."""
    return supabase_cache.check_digest_sent_today(user_id, today_date)


def _log_digest(user_id: str, today_date: str, event_count: int, status: str = "sent") -> None:
    """Log a sent digest to watchlist_digest_log."""
    supabase_cache.log_digest_result(user_id, today_date, event_count, status)


def _gather_signals(tickers: list[str], hours: int = 24) -> dict[str, list[dict]]:
    """Gather recent notifications for the given tickers, grouped by ticker."""
    signals: dict[str, list[dict]] = {t: [] for t in tickers}
    try:
        all_notifs = supabase_cache.get_recent_notifications(500)
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        for n in all_notifs:
            if n.get("created_at", "") < cutoff:
                continue
            meta = n.get("metadata") or {}
            nticker = (meta.get("ticker") or "").upper()
            if nticker in signals:
                signals[nticker].append(n)
    except Exception as exc:
        logger.warning("Failed to gather signals: %s", exc)
    return signals


def _render_email(user_signals: dict[str, list[dict]], tickers: list[str]) -> tuple[str, str]:
    """Render the digest email HTML and subject line."""
    total = sum(len(v) for v in user_signals.values())
    active_tickers = [t for t in tickers if user_signals.get(t)]

    subject = f"PaperPanda Daily Digest — {total} signal{'s' if total != 1 else ''} across your watchlist"

    rows_html = ""
    for ticker in active_tickers:
        sigs = user_signals[ticker]
        rows_html += f"""
        <tr>
            <td style="padding: 16px; border-bottom: 1px solid #e2e8f0;">
                <div style="font-weight: 700; font-size: 16px; color: #0f172a; margin-bottom: 8px;">
                    <a href="{BASE_URL}/stock/{ticker}" style="color: #0d9488; text-decoration: none;">{ticker}</a>
                </div>
        """
        for s in sigs[:5]:
            icon = SIGNAL_ICONS.get(s.get("type", ""), "📌")
            title = s.get("title", "Signal")
            msg = s.get("message", "")
            link = s.get("link", f"/stock/{ticker}")
            if not link.startswith("http"):
                link = BASE_URL + link
            rows_html += f"""
                <div style="margin-bottom: 6px; font-size: 14px; color: #334155;">
                    {icon} <a href="{link}" style="color: #0f766e; text-decoration: none;">{title}</a>
                    <span style="color: #94a3b8;"> — {msg[:100]}</span>
                </div>
            """
        rows_html += """
            </td>
        </tr>
        """

    html = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
    <body style="margin: 0; padding: 0; background: #f8fafc; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
        <div style="max-width: 600px; margin: 0 auto; padding: 24px 16px;">
            <div style="text-align: center; margin-bottom: 24px;">
                <h1 style="font-size: 24px; color: #0f172a; margin: 0 0 4px;">🐼 PaperPanda</h1>
                <p style="color: #64748b; font-size: 14px; margin: 0;">Daily Watchlist Digest</p>
            </div>

            <div style="background: #fff; border-radius: 12px; border: 1px solid #e2e8f0; overflow: hidden;">
                <div style="background: linear-gradient(135deg, #0d9488, #0f766e); padding: 16px 20px; color: #fff;">
                    <strong>{total} signal{'s' if total != 1 else ''}</strong> across {len(active_tickers)} ticker{'s' if len(active_tickers) != 1 else ''} in the last 24 hours
                </div>
                <table style="width: 100%; border-collapse: collapse;">
                    {rows_html}
                </table>
            </div>

            <div style="text-align: center; margin-top: 24px; padding: 16px;">
                <a href="{BASE_URL}/watchlist" style="display: inline-block; padding: 10px 24px; background: #0d9488; color: #fff; border-radius: 8px; text-decoration: none; font-weight: 600;">View Full Watchlist</a>
            </div>

            <div style="text-align: center; margin-top: 16px; font-size: 12px; color: #94a3b8;">
                <a href="{BASE_URL}/watchlist" style="color: #64748b;">Manage preferences</a>
                &nbsp;·&nbsp;
                <a href="{BASE_URL}/api/watchlist/preferences?unsubscribe=digest" style="color: #64748b;">Unsubscribe</a>
            </div>
        </div>
    </body>
    </html>
    """

    return subject, html


def _send_email(to: str, subject: str, html: str) -> bool:
    """Send an email via Resend API."""
    if not RESEND_API_KEY:
        logger.warning("RESEND_API_KEY not set — skipping email send")
        return False
    try:
        import resend
        resend.api_key = RESEND_API_KEY
        resend.Emails.send({
            "from": FROM_EMAIL,
            "to": [to],
            "subject": subject,
            "html": html,
        })
        return True
    except Exception as exc:
        logger.error("Failed to send email to %s: %s", to, exc)
        return False


def run_digest_cycle() -> None:
    """Main digest cycle — check all eligible users, send digests."""
    logger.info("Starting digest cycle")
    start = time.time()

    users = supabase_cache.get_watchlist_users_for_digest()
    if not users:
        logger.info("No eligible users for digest")
        return

    sent = 0
    skipped = 0

    for user in users:
        user_id = user["user_id"]
        email = user["email"]
        tz = user.get("digest_timezone", "America/New_York")
        digest_time_str = user.get("digest_time", "18:00")

        # Check if it's past their preferred time
        local_now = _user_local_now(tz)
        try:
            hour, minute = map(int, digest_time_str.split(":")[:2])
        except (ValueError, AttributeError):
            hour, minute = 18, 0

        if local_now.hour < hour or (local_now.hour == hour and local_now.minute < minute):
            skipped += 1
            continue

        today_str = local_now.strftime("%Y-%m-%d")
        if _already_sent_today(user_id, today_str):
            skipped += 1
            continue

        # Gather signals for this user's watchlist
        tickers = user["tickers"]
        signals = _gather_signals(tickers)
        total_events = sum(len(v) for v in signals.values())

        if total_events == 0:
            _log_digest(user_id, today_str, 0, "skipped_empty")
            skipped += 1
            continue

        subject, html = _render_email(signals, tickers)
        ok = _send_email(email, subject, html)

        if ok:
            _log_digest(user_id, today_str, total_events, "sent")
            sent += 1
            logger.info("Sent digest to %s (%d events)", email, total_events)
        else:
            _log_digest(user_id, today_str, total_events, "failed")

    elapsed = time.time() - start
    logger.info("Digest cycle done: %d sent, %d skipped (%.1fs)", sent, skipped, elapsed)


def main() -> None:
    """Entry point for the digest worker."""
    setup_worker_logging()
    logger.info("Watchlist digest worker starting")

    if not RESEND_API_KEY:
        logger.warning("RESEND_API_KEY not set — emails will be logged but not sent")

    run_digest_cycle()
    logger.info("Watchlist digest worker finished")
