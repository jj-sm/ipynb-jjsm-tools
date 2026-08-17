"""
Color-magnitude, HR diagram plotting, a generic scatter function
covers both cases: a CMD plots (color, apparent mag); an HR diagram is
the same plot with an absolute magnitude or temperature/color axis.
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import AutoMinorLocator


def magnitude_diagram(
    color,
    mag,
    color_label,
    mag_label,
    title="",
    ax=None,
    invert_mag=True,
    invert_color=False,
    s=15,
    alpha=0.7,
    point_color="black",
    rasterized=True,
    figsize=(4, 5),
    minor_ticks=True,
    isochrone=None,
    isochrone_label="Isócrona",
    isochrone_color="crimson",
    isochrone_kwargs=None,
    filename=None,
    graphs_path=None,
    dpi=350,
    **scatter_kwargs,
):
    """
    Generic color-magnitude scatter plot. Used for both CMDs (color vs.
    apparent magnitude) and HR diagrams (color/temperature vs. absolute
    magnitude/luminosity) — pass `invert_mag=True` (the default) so
    brighter points land at the top, as is conventional for both.

    Parameters
    ----------
    color, mag : array-like
        Color index (x-axis) and magnitude (y-axis), same length.
    color_label, mag_label : str
        Axis labels, e.g. r'$G-R$ [mag]' and r'$R$ [mag]'.
    ax : matplotlib Axes, optional
        Plot into an existing axes instead of creating a new figure.
    invert_mag : bool
        Flip the y-axis so smaller (brighter) magnitudes are on top.
    invert_color : bool
        Flip the x-axis (rarely needed, but mirrors invert_mag for
        temperature-like color axes plotted in reverse).
    isochrone : (array-like, array-like), optional
        `(iso_color, iso_mag)` — an isochrone (or any other reference
        track) already converted into this diagram's color/magnitude
        space (i.e. already shifted by distance modulus, extinction,
        reddening, etc. — this function just draws the line). Pass a
        pair of equal-length arrays; already-filtered to the range you
        want plotted.
    isochrone_label, isochrone_color : str
        Legend label and line color for the isochrone track.
    isochrone_kwargs : dict, optional
        Extra kwargs forwarded to `ax.plot` for the isochrone line
        (e.g. `{"lw": 2.5, "zorder": 5}`).
    scatter_kwargs : forwarded to `ax.scatter`.

    Returns
    -------
    (fig, ax) — fig is None if an existing `ax` was passed in.
    """
    color = np.asarray(color)
    mag = np.asarray(mag)

    fig = None
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)

    ax.scatter(color, mag, s=s, alpha=alpha, color=point_color, rasterized=rasterized, **scatter_kwargs)

    if isochrone is not None:
        iso_color, iso_mag = isochrone
        iso_kwargs = {"lw": 2.5, "zorder": 5, **(isochrone_kwargs or {})}
        ax.plot(iso_color, iso_mag, color=isochrone_color, label=isochrone_label, **iso_kwargs)
        ax.legend(fontsize=9, loc="best")

    ax.set_xlabel(color_label, fontsize=13)
    ax.set_ylabel(mag_label, fontsize=13)
    if title:
        ax.set_title(title, fontsize=13)

    if invert_mag:
        ax.invert_yaxis()
    if invert_color:
        ax.invert_xaxis()

    if minor_ticks:
        ax.xaxis.set_minor_locator(AutoMinorLocator())
        ax.yaxis.set_minor_locator(AutoMinorLocator())

    if fig is not None:
        fig.tight_layout()

    if filename and fig is not None:
        base = Path(graphs_path) / filename if graphs_path else Path(filename)
        fig.savefig(f"{base}.png", dpi=dpi, bbox_inches="tight")
        fig.savefig(f"{base}.pdf", bbox_inches="tight")

    return fig, ax


def color_magnitude_diagram(color, mag, color_label, mag_label, **kwargs):
    """Alias of `magnitude_diagram`, kept for readability at call sites building a CMD."""
    return magnitude_diagram(color, mag, color_label, mag_label, **kwargs)


def hr_diagram(color_or_temp, abs_mag_or_lum, color_label=r"$B-V$ [mag]", mag_label=r"$M_V$ [mag]", **kwargs):
    """
    Alias of `magnitude_diagram` for the classic HR-diagram orientation
    (bright/blue toward the upper area). Pass `invert_color=True` if
    plotting against temperature directly (hot stars on the left).
    """
    return magnitude_diagram(color_or_temp, abs_mag_or_lum, color_label, mag_label, **kwargs)


def shift_isochrone(iso_color, iso_mag, distance_modulus=0.0, extinction=0.0, reddening=0.0, mag_range=None):
    """
    Shift a theoretical isochrone from absolute color/magnitude into the
    observed plane, ready to hand to `magnitude_diagram(..., isochrone=...)`:

        mag_obs   = iso_mag   + distance_modulus + extinction
        color_obs = iso_color + reddening

    Generalizes the notebook's per-panel `(m - M) + A_r`, `E(g-r)`, etc.
    bookkeeping into one call.

    Parameters
    ----------
    iso_color, iso_mag : array-like
        Isochrone color index and magnitude in absolute/theoretical
        units (e.g. straight from a PARSEC/MIST table), same length.
    distance_modulus : float
        `m - M`, added to `iso_mag`.
    extinction : float
        `A_band` for the magnitude's band, added to `iso_mag`.
    reddening : float
        `E(color)` for the color index, added to `iso_color`.
    mag_range : (low, high), optional
        If given, only isochrone points whose *observed* magnitude
        falls in `[low, high]` are kept — trims the overlaid line to
        the range actually visible in your plot instead of running off
        the axes.

    Returns
    -------
    (color_obs, mag_obs) : ndarray, ndarray
    """
    iso_color = np.asarray(iso_color, dtype=float)
    iso_mag = np.asarray(iso_mag, dtype=float)

    mag_obs = iso_mag + distance_modulus + extinction
    color_obs = iso_color + reddening

    if mag_range is not None:
        lo, hi = mag_range
        mask = (mag_obs >= lo) & (mag_obs <= hi)
        return color_obs[mask], mag_obs[mask]

    return color_obs, mag_obs
