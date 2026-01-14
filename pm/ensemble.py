# Deterministic forecast logic (Phase-1)
# Goal: correct the present (crowd bias), not predict the future.

REGRESSION_FACTOR = 0.20   # Tunable: higher = stronger pull to 0.5 on extremes
VOL_ANCHOR_USD = 500_000.0 # Above this, trust crowd more

def compute_machine_p(crowd_p: float, volume24hr: float = 0.0) -> float:
    crowd_p = float(crowd_p)
    volume24hr = float(volume24hr or 0.0)

    # clamp inputs
    if crowd_p < 0.001: crowd_p = 0.001
    if crowd_p > 0.999: crowd_p = 0.999
    if volume24hr < 0: volume24hr = 0.0

    # Volume anchor: high vol => trust crowd more
    vol_weight = min(volume24hr / VOL_ANCHOR_USD, 1.0)
    p = vol_weight * crowd_p + (1.0 - vol_weight) * 0.5

    # Extra regression if extreme
    deviation = abs(crowd_p - 0.5)
    if deviation > 0.40:  # crowd >0.90 or <0.10
        p = p + REGRESSION_FACTOR * (0.5 - p)

    # Final clamp
    if p < 0.01: p = 0.01
    if p > 0.99: p = 0.99
    return float(p)