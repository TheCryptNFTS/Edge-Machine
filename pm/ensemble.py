# Deterministic probability auditor logic (Phase 1)
# Goal: correct the present, not predict the future.

REGRESSION_FACTOR = 0.2  # Tune only after audit

def compute_machine_p(crowd_p: float, volume: float) -> float:
    """
    Adjust crowd probability toward base rate (0.5)
    with trust weighted by liquidity.
    """

    # Guardrails
    try:
        crowd_p = float(crowd_p)
        volume = float(volume or 0.0)
    except Exception:
        return 0.5

    if crowd_p <= 0 or crowd_p >= 1:
        return 0.5

    # Volume confidence: more volume = trust crowd more
    vol_weight = min(volume / 500_000.0, 1.0)

    # Base regression to mean
    machine_p = (vol_weight * crowd_p) + ((1.0 - vol_weight) * 0.5)

    # Extra correction for extreme confidence
    deviation = abs(crowd_p - 0.5)
    if deviation > 0.4:  # >90% or <10%
        machine_p += REGRESSION_FACTOR * (0.5 - machine_p)

    # Clamp for sanity
    return max(min(machine_p, 0.99), 0.01)