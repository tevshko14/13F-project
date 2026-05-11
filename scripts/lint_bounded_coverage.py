"""Lint: every ``await to_X(...)`` inside a redesign route handler must be
wrapped in ``_bounded`` / ``_bounded_call``.

Why
───

The bounded() wrappers convert upstream timeouts / exceptions into a
shaped fallback so the page renders.  An ungated ``await to_X(...)``
raises ``TimeoutError`` up to the route handler and crashes the page.
This is exactly the bug we hit on /retail today (``to_light(...)``
unwrapped inside the route handler -> /retail 500 when ApeWisdom was
slow).

This script walks the AST of every file in
``src/filings/routers/_redesign/`` and reports any ``await to_X(...)``
inside a function decorated with ``@router.get`` /
``@_placeholder_route`` that isn't passed as an argument to
``_bounded`` / ``_bounded_call``.

Exit code 0 if clean, 1 if violations found.

Run locally:
    uv run python scripts/lint_bounded_coverage.py

CI uses the same invocation (see ``.github/workflows/ci.yml``).
"""

from __future__ import annotations

import ast
import pathlib
import sys

# Wrappers that must guard the upstream calls.
GUARDS = {"_bounded", "_bounded_call"}

# Functions that produce blocking upstream work + raise on timeout.
# If you add a new pool wrapper to concurrency.py, add it here.
GATED_FNS = {"to_heavy", "to_light", "to_supabase", "to_upstream"}

# Decorators that mark a function as a route handler.  We only enforce
# the rule inside these — internal helpers (`_fetch_home_X`, etc.) can
# legitimately raise; the gating happens at the route handler.
ROUTE_DECORATORS = {"router.get", "router.post", "_placeholder_route"}


def _decorator_name(dec: ast.expr) -> str:
    """Extract the dotted-name for a decorator AST node."""
    if isinstance(dec, ast.Call):
        return _decorator_name(dec.func)
    if isinstance(dec, ast.Attribute):
        parent = _decorator_name(dec.value)
        return f"{parent}.{dec.attr}" if parent else dec.attr
    if isinstance(dec, ast.Name):
        return dec.id
    return ""


def _is_route_handler(fn: ast.AsyncFunctionDef | ast.FunctionDef) -> bool:
    """True if any decorator on this function is a route registration."""
    for dec in fn.decorator_list:
        if _decorator_name(dec) in ROUTE_DECORATORS:
            return True
    return False


def _called_fn_name(node: ast.Call) -> str:
    """Extract the function name being called -- ``to_heavy(...)`` -> ``to_heavy``."""
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return ""


def _find_violations(tree: ast.AST, source_path: str) -> list[str]:
    """Walk the AST and return human-readable violation strings."""
    # Build a parent-map so we can ask "what ancestors does this node have?"
    parent: dict[int, ast.AST] = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parent[id(child)] = node

    def ancestors(n: ast.AST):
        cur = parent.get(id(n))
        while cur is not None:
            yield cur
            cur = parent.get(id(cur))

    violations: list[str] = []

    # Only enforce inside route handlers.
    for node in ast.walk(tree):
        if not isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
            continue
        if not _is_route_handler(node):
            continue

        route_name = node.name

        # Find every `await to_X(...)` inside this function's body.
        # Skip awaits inside nested function defs (those are their own
        # scope and can legitimately raise).
        for sub in ast.walk(node):
            if not isinstance(sub, ast.Await):
                continue
            # Skip if this Await is inside a nested function.
            inside_nested = False
            for anc in ancestors(sub):
                if anc is node:
                    break
                if isinstance(anc, (ast.AsyncFunctionDef, ast.FunctionDef, ast.Lambda)):
                    inside_nested = True
                    break
            if inside_nested:
                continue

            # Is the awaited expression a call to a gated fn?
            inner = sub.value
            if not isinstance(inner, ast.Call):
                continue
            inner_name = _called_fn_name(inner)
            if inner_name not in GATED_FNS:
                continue

            # Check: is the `await to_X(...)` passed as an arg to a guard?
            # Walk ancestors of the inner Call; if any ancestor is a Call
            # to one of GUARDS, this is properly wrapped.
            guarded = False
            for anc in ancestors(sub):
                if anc is node:
                    break
                if isinstance(anc, ast.Call):
                    anc_name = _called_fn_name(anc)
                    if anc_name in GUARDS:
                        guarded = True
                        break

            if not guarded:
                violations.append(
                    f"{source_path}:{sub.lineno}: "
                    f"`await {inner_name}(...)` inside route handler "
                    f"`{route_name}` is not wrapped in {sorted(GUARDS)}"
                )

    return violations


def main() -> int:
    root = pathlib.Path(__file__).resolve().parent.parent
    target = root / "src" / "filings" / "routers" / "_redesign"
    if not target.exists():
        print(f"lint_bounded_coverage: target dir not found: {target}", file=sys.stderr)
        return 1

    all_violations: list[str] = []
    for py_file in sorted(target.rglob("*.py")):
        try:
            tree = ast.parse(py_file.read_text())
        except SyntaxError as e:
            print(f"{py_file}: syntax error: {e}", file=sys.stderr)
            return 1
        all_violations.extend(_find_violations(tree, str(py_file.relative_to(root))))

    if all_violations:
        print(
            "lint_bounded_coverage: found ungated upstream calls in route handlers.\n"
            "Each `await to_X(...)` inside a route handler must be passed as an\n"
            "argument to `_bounded(...)` or `_bounded_call(...)` so a slow upstream\n"
            "renders the fallback shape instead of crashing the page.\n",
            file=sys.stderr,
        )
        for v in all_violations:
            print(v, file=sys.stderr)
        print(
            f"\nTotal: {len(all_violations)} violation(s).  Fix by wrapping each "
            f"`await to_X(...)` in `_bounded_call(...)` with a complete-shape fallback.",
            file=sys.stderr,
        )
        return 1

    print("lint_bounded_coverage: 0 violations")
    return 0


if __name__ == "__main__":
    sys.exit(main())
