"""
Display FITS-like image arrays (2D data or RGB stacks)
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import LogNorm

try:
    from cmcrameri import cm as _cmc

    _DEFAULT_CMAP = _cmc.batlowK
except Exception:  # pragma: no cover - cmcrameri is optional
    _DEFAULT_CMAP = "viridis"


def _save_fig(fig, filename, graphs_path, dpi):
    base = Path(graphs_path) / filename if graphs_path else Path(filename)
    fig.savefig(f"{base}.png", dpi=dpi, bbox_inches="tight")
    fig.savefig(f"{base}.pdf", format="pdf", bbox_inches="tight")


def plot_image(
    image_data,
    figsize=(4, 4),
    title="",
    cmap=None,
    norm=None,
    colorbar_label="ADU",
    filename=None,
    graphs_path=None,
    dpi=350,
    axes=True,
    wcs=None,
    axes_in=True,
    all_axis=True,
    vmin_pct=10,
    vmax_pct=99.9,
    colorbar_kwargs: dict | None = None,
    **kwargs,
):
    """
    Display a 2D image array (or an (ny, nx, 3) RGB stack) with a log
    stretch by default and WCS-aware RA/DEC axes when a `wcs` is given.

    Parameters
    ----------
    image_data : ndarray
        2D data array, or an RGB stack with shape (ny, nx, 3).
    cmap : matplotlib colormap, optional
        Defaults to cmcrameri's batlowK if available, else 'viridis'.
        Ignored for RGB input.
    norm : matplotlib Normalize, optional
        Defaults to a LogNorm built from the `vmin_pct`/`vmax_pct`
        percentiles of the finite pixels. Ignored for RGB input.
    filename : str, optional
        If given, saves both a PNG (at `dpi`) and a PDF next to
        `graphs_path` (or the current directory if not given).
    axes_in : bool
        Tick direction on the *displayed* (non-hidden) sides: 'in' when
        True, 'out' when False.
    all_axis : bool
        If True, ticks/labels appear on all four sides; if False, only
        bottom-left.
    kwargs : forwarded to `ax.imshow`.

    Returns
    -------
    (fig, ax)
    """
    image_data = np.asarray(image_data)
    is_rgb = image_data.ndim == 3
    cmap = cmap or _DEFAULT_CMAP

    subplot_kw = {"projection": wcs} if wcs is not None else {}
    fig, ax = plt.subplots(figsize=figsize, subplot_kw=subplot_kw)

    major_tick_dir = "in" if axes_in else "out"
    minor_tick_dir = "out"

    if wcs is not None:
        ax.coords[0].set_ticks(direction=major_tick_dir)
        ax.coords[1].set_ticks(direction=major_tick_dir)
        tick_pos = "bltr" if all_axis else "bl"
        ax.coords[0].set_ticks_position(tick_pos)
        ax.coords[1].set_ticks_position(tick_pos)
        ax.coords[0].display_minor_ticks(False)
        ax.coords[1].display_minor_ticks(False)
    else:
        ax.tick_params(axis="both", which="major", direction=major_tick_dir, top=all_axis, right=all_axis)
        ax.tick_params(axis="both", which="minor", direction=minor_tick_dir, top=all_axis, right=all_axis)

    if norm is None and not is_rgb:
        finite = image_data[np.isfinite(image_data)]
        vmin = np.nanpercentile(finite, vmin_pct)
        vmax = np.nanpercentile(finite, vmax_pct)
        vmin = max(vmin, 1e-3)
        norm = LogNorm(vmin=vmin, vmax=vmax)

    if is_rgb:
        image = ax.imshow(image_data, origin="lower", **kwargs)
    else:
        image = ax.imshow(image_data, cmap=cmap, norm=norm, origin="lower", **kwargs)
        fig.colorbar(image, label=colorbar_label, **colorbar_kwargs if colorbar_kwargs else {})

    if wcs is not None:
        ax.set_xlabel("RA", fontsize=15)
        ax.set_ylabel("DEC", fontsize=15)
        ax.coords[0].set_major_formatter("hh:mm:ss")
        ax.coords[1].set_major_formatter("dd:mm:ss")
        ax.grid(color="white", ls="dotted", alpha=0.3)
    else:
        ax.set_xlabel("pix", fontsize=15)
        ax.set_ylabel("pix", fontsize=15)

    ax.set_title(title)

    if not axes:
        ax.set_xticks([])
        ax.set_yticks([])

    if filename:
        _save_fig(fig, filename, graphs_path, dpi)

    return fig, ax


def plot_image_both_styles(image_data, filename=None, graphs_path=None, **plot_kwargs):
    """
    Fallback to check whether TeX is available, and plot the image depending on it.
    """
    from ..setup.plot import activate_tex

    figs = {}
    for enabled, key in ((True, "tex"), (False, "mathtext")):
        status = activate_tex(enabled)
        fname = f"{filename}_{key}" if filename else None
        fig, ax = plot_image(image_data, filename=fname, graphs_path=graphs_path, **plot_kwargs)
        figs[key] = (fig, ax, status)
    return figs
