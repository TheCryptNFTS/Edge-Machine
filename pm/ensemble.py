# pm/ensemble.py
# Phase-1 deterministic auditor

REGRESSION_FACTOR = 0.2

def compute_machine_p(crowd_p: float) -> float:
    # Basic conservative correction (you can evolve later)
    p = float(crowd_p)
    if p <= 0 or p >= 1:
        return 0.5

    # mild pull toward 0.5 when extreme
    if abs(p - 0.5) > 0.4:
        p = p + REGRESSION_FACTOR * (0.5 - p)

    return max(min(p, 0.99), 0.01)