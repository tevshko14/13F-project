"""FastAPI routers for PaperPanda — split from web.py by domain.

Each router module exports an ``APIRouter`` instance mounted by web.py
via ``app.include_router(...)``.

Routers import shared helpers from ``filings.app_state`` (templates,
validators).  They access app-level state (caches, semaphores, pools)
via ``request.app.state`` or by importing lazily from ``filings.web``
inside handler bodies to avoid circular import at load time.
"""
