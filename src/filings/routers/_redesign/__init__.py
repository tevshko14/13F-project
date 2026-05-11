"""Per-feature sub-routers for the redesign preview.

The parent ``redesign_preview.py`` composes these sub-routers into the
single ``router`` it exports to ``web.py``.  Splitting them out keeps
each feature's handlers + helpers under ~2K LOC instead of cohabiting
the 13K-LOC monolith they grew out of.

Shared helpers live in :mod:`helpers` -- anything used by 2+ feature
modules.  Feature-specific helpers live alongside their routes.
"""
