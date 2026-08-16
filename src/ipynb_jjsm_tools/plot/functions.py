import matplotlib.colors as mcolors


def darken(color: str, factor: float = 0.6) -> tuple:
    """Return a darker shade of color. factor < 1 = darker."""
    r, g, b, a = mcolors.to_rgba(color)
    return (r * factor, g * factor, b * factor, a)