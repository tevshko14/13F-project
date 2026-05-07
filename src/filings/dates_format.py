"""Shared date formatters — single source of truth for user-facing dates.

The product's display convention is **MMM DD YYYY** — zero-padded day,
no comma, no abbreviated year.  Examples::

    May 06 2026     ← format_date("2026-05-06")
    Apr 13 2024     ← format_date(datetime(2024, 4, 13))
    Jan 02 2026     ← format_date("2026-01-02T15:30:00")
    May 06          ← format_date_short("2026-05-06")
    Wed, May 06 2026 ← format_date_with_dow("2026-05-06")

Internal/data formats (ISO strings in cache keys, JSON output, log
timestamps) are NOT touched by these helpers — keep using
``strftime("%Y-%m-%d")`` for those.

Accepts:
    - ``datetime`` / ``date`` objects
    - ISO strings: ``"2026-05-06"`` or ``"2026-05-06T12:30:00"``
    - Empty / None → returns ``""``
    - Already-formatted strings (``"May 06 2026"``) → returned as-is
      via best-effort parse-and-reformat fallback.
"""

from __future__ import annotations

from datetime import date, datetime


_PASSTHROUGH_PARSERS: tuple[str, ...] = (
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    # Already-formatted variants we want to recognise + re-emit canonically.
    "%b %d, %Y",
    "%b %-d, %Y",
    "%B %d, %Y",
    "%B %-d, %Y",
    "%b %d %Y",
    "%b %-d %Y",
)


def _to_date(value) -> date | None:
    """Best-effort coercion of an arbitrary value into a `date`.

    Returns ``None`` when the value is empty or unparseable; callers
    should treat that as "render as empty string".
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    # Fast path: ISO date prefix.
    head = s[:10]
    try:
        return datetime.strptime(head, "%Y-%m-%d").date()
    except ValueError:
        pass
    # Slow path: try each known format.
    for fmt in _PASSTHROUGH_PARSERS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def format_date(value, *, fallback: str = "") -> str:
    """Render *value* as ``"MMM DD YYYY"`` (e.g. ``"May 06 2026"``).

    Empty / unparseable input returns ``fallback`` (default empty string).
    Day is zero-padded.  No comma between any pieces.
    """
    d = _to_date(value)
    if d is None:
        return fallback
    return d.strftime("%b %d %Y")


def format_date_short(value, *, fallback: str = "") -> str:
    """Render *value* as ``"MMM DD"`` (e.g. ``"May 06"``) — no year.

    For compact contexts where the year is implied (axis ticks, current-
    week tabs, sparkline endpoint labels).  Day is zero-padded.
    """
    d = _to_date(value)
    if d is None:
        return fallback
    return d.strftime("%b %d")


def format_date_with_dow(value, *, fallback: str = "") -> str:
    """Render *value* as ``"Wed, May 06 2026"`` — keeps the abbreviated
    weekday for calendar contexts that lean on day-of-week orientation.
    """
    d = _to_date(value)
    if d is None:
        return fallback
    return d.strftime("%a, %b %d %Y")


# Jinja-filter convenience aliases — mounted in app_state.py so templates
# can do ``{{ entry.date | dt_long }}`` etc.
def _filter_dt_long(value):
    """Jinja filter — `{{ value | dt_long }}` → 'May 06 2026'."""
    return format_date(value, fallback=value or "")


def _filter_dt_short(value):
    """Jinja filter — `{{ value | dt_short }}` → 'May 06'."""
    return format_date_short(value, fallback=value or "")


def _filter_dt_dow(value):
    """Jinja filter — `{{ value | dt_dow }}` → 'Wed, May 06 2026'."""
    return format_date_with_dow(value, fallback=value or "")


def register_jinja_filters(env) -> None:
    """Register the date filters on a Jinja environment.

    Called from `app_state.py` once at app startup so every template
    inherits ``dt_long`` / ``dt_short`` / ``dt_dow`` without per-page
    imports.
    """
    env.filters.setdefault("dt_long",  _filter_dt_long)
    env.filters.setdefault("dt_short", _filter_dt_short)
    env.filters.setdefault("dt_dow",   _filter_dt_dow)
