from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Params:
    # Volume anchor: trust crowd more when volume is high
    vol_anchor_usd: float = 500_000.0

    # Overconfidence detector
    pm_extreme_high: float = 0.94
    pm_extreme_low: float = 0.06

    # Caps applied only at extremes
    cap_high: float = 0.91
    floor_low: float = 0.09

    # Extra regression for extreme prices
    regression_factor: float = 0.20


P = Params()


def _clamp(x: float, lo: float, hi: float) -> float:
    try:
        v = float(x)
    except Exception:
        v = 0.5
    return max(lo, min(hi, v))


def compute_machine_p(crowd_p: float, volume24hr: float = 0.0, title: str = "") -> float:
    """
    Deterministic Phase-1 auditor:
    - Volume anchoring
    - Overconfidence crusher at extremes
    - Extra regression for extreme odds
    """
    p_pm = _clamp(crowd_p, 0.001, 0.999)
    vol = max(float(volume24hr or 0.0), 0.0)

    # 1) Volume anchor (high vol -> trust crowd)
    wv = min(vol / P.vol_anchor_usd, 1.0)
    p = wv * p_pm + (1.0 - wv) * 0.5

    # 2) Overconfidence crusher (only when PM is extreme)
    if p_pm >= P.pm_extreme_high:
        p = min(p, P.cap_high)
    elif p_pm <= P.pm_extreme_low:
        p = max(p, P.floor_low)

    # 3) Extra regression only when extreme (helps avoid "sure-thing worship")
    deviation = abs(p_pm - 0.5)
    if deviation > 0.40:  # > 0.90 or < 0.10
        p = p + P.regression_factor * (0.5 - p)

    return _clamp(p, 0.01, 0.99)