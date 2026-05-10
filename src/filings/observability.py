"""Shared Sentry init for web + worker tiers.

Both processes tag events with `service=web|worker` so the Sentry UI
filters cleanly by tier.  No-op when ``SENTRY_DSN`` is unset.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

# Errors are always captured; this knob only affects performance traces,
# which we mostly don't need.  Override via SENTRY_TRACES_SAMPLE_RATE.
_DEFAULT_TRACES_SAMPLE_RATE = 0.1


def init_sentry(service: str) -> bool:
    """Initialise Sentry for the given service tier.

    ``service`` is a short tag (typically "web" or "worker") attached
    to every event via ``sentry_sdk.set_tag``.  Returns True on
    successful init, False when DSN unset or sentry-sdk unavailable.

    NOT idempotent -- ``sentry_sdk.init`` replaces the global Hub on
    each call, so don't call twice with different services from the
    same process.  Call once per process from the entry point.
    """
    dsn = os.environ.get("SENTRY_DSN", "").strip()
    if not dsn:
        logger.info("Sentry: SENTRY_DSN not set, skipping init (service=%s)", service)
        return False

    try:
        import sentry_sdk
    except ImportError:
        logger.warning(
            "Sentry: SENTRY_DSN set but sentry-sdk not installed (service=%s)",
            service,
        )
        return False

    try:
        sample_rate = float(os.environ.get(
            "SENTRY_TRACES_SAMPLE_RATE", _DEFAULT_TRACES_SAMPLE_RATE,
        ))
    except ValueError:
        sample_rate = _DEFAULT_TRACES_SAMPLE_RATE

    sentry_sdk.init(
        dsn=dsn,
        environment=os.environ.get("RAILWAY_ENVIRONMENT", "development"),
        traces_sample_rate=sample_rate,
    )
    # Use set_tag on the global scope rather than a `before_send` hook --
    # idiomatic for static tagging and avoids re-allocating a closure on
    # every event.
    sentry_sdk.set_tag("service", service)
    logger.info(
        "Sentry initialised (service=%s, traces=%.0f%%)",
        service, sample_rate * 100,
    )
    return True
