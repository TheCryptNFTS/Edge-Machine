def compute_machine_p(crowd_p: float) -> float:
    """
    Phase-1 deterministic auditor.
    Crush extremes, regress to mean.
    """
    try:
        p = float(crowd_p)
    except Exception:
        return 0.5

    p = max(0.0, min(1.0, p))

    # Overconfidence crusher
    if p > 0.94:
        return 0.91
    if p < 0.06:
        return 0.09

    # Mild regression
    return round(0.85 * p + 0.15 * 0.5, 4)