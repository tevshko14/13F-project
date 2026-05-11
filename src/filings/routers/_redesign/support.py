"""Support page — Panda Fund + Stripe checkout (v2 design).

Two routes:
  * ``/support``           — the funding-history chart + Stripe checkout
  * ``/support/thank-you`` — post-checkout return URL (same template, flag)

Page context (raised cents, monthly history, etc.) is shared with the v1
support page via ``filings.web._support_page_context`` -- we just re-shape
the funding history into SVG bar-chart geometry instead of leaning on
ECharts.  All non-feature-specific helpers come from
:mod:`filings.routers._redesign.helpers`.
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from filings.app_state import templates
from filings.routers._redesign.helpers import (
    _nice_axis_step,
    _shell_context,
    GracefulRoute,
)

router = APIRouter(route_class=GracefulRoute)


def _support_history_chart(months: list[str], raised: list[int], goal: int) -> dict:
    """Server-render bar-chart geometry for the funding history panel.

    Avoids the ECharts dependency the v1 page leaned on — every other v2
    chart uses inline SVG with `vector-effect="non-scaling-stroke"` and
    HTML axis labels, so this matches the established pattern.
    """
    n = max(len(months), 1)
    vb_w, vb_h = 1500.0, 320.0
    pad_top, pad_bot, pad_left = 18.0, 36.0, 64.0
    plot_w = vb_w - pad_left - 24
    plot_h = vb_h - pad_top - pad_bot

    if not months:
        return {"have_data": False, "bars": [], "y_labels": [], "grid_ys": [],
                "vb_width": vb_w, "vb_height": vb_h}

    # Y axis: nice round step up to (and beyond) the goal so the dashed
    # goal line always lands on a labelled tick.
    y_top: float = max(goal, max(raised) if raised else 1)
    step = _nice_axis_step(y_top, target_steps=4)
    import math
    y_top = math.ceil(y_top / step) * step

    def _y_for(v: float) -> float:
        return pad_top + (1.0 - v / y_top) * plot_h

    bar_gap = 14.0
    bar_w = max(40.0, (plot_w - bar_gap * (n + 1)) / n)

    bars: list[dict] = []
    for i, (label, v) in enumerate(zip(months, raised)):
        x = pad_left + bar_gap + i * (bar_w + bar_gap)
        y = _y_for(v)
        bars.append({
            "label":  label,
            "value":  v,
            "x":      round(x, 1),
            "y":      round(y, 1),
            "w":      round(bar_w, 1),
            "h":      round(_y_for(0) - y, 1),
            "label_x": round(x + bar_w / 2, 1),
            "is_funded": v >= goal,
            "value_str": f"${v}",
        })

    y_labels: list[dict] = []
    grid_ys:  list[float] = []
    yv: float = 0.0
    while yv <= y_top + step / 2:
        y_pos = round(_y_for(yv), 1)
        y_labels.append({"label": f"${int(yv)}", "y": y_pos})
        grid_ys.append(y_pos)
        yv += step

    return {
        "have_data": True,
        "bars":      bars,
        "y_labels":  y_labels,
        "grid_ys":   grid_ys,
        "goal_y":    round(_y_for(goal), 1),
        "goal":      goal,
        "vb_width":  vb_w,
        "vb_height": vb_h,
        "left_pad":  pad_left,
    }


@router.get("/support", response_class=HTMLResponse)
async def preview_support(request: Request):
    """Support page — Panda Fund + Stripe checkout, v2 design."""
    from filings.web import _support_page_context  # reuse v1 helper

    base = await _support_page_context(request)
    base["chart"] = _support_history_chart(
        base.get("funding_history_months") or [],
        base.get("funding_history_raised") or [],
        base.get("monthly_goal") or 200,
    )
    base.update(await _shell_context(request, "Support"))
    return templates.TemplateResponse("_redesign/support.html", base)


@router.get("/support/thank-you", response_class=HTMLResponse)
async def preview_support_thank_you(request: Request):
    """Post-Stripe-checkout return — same template with thank-you flag."""
    from filings.web import _support_page_context

    base = await _support_page_context(request, extra={"show_thank_you": True})
    base["chart"] = _support_history_chart(
        base.get("funding_history_months") or [],
        base.get("funding_history_raised") or [],
        base.get("monthly_goal") or 200,
    )
    base.update(await _shell_context(request, "Support"))
    return templates.TemplateResponse("_redesign/support.html", base)
