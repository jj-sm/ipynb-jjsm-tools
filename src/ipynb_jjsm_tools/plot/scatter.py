"""Generic scatter plot with an optional fitted curve and a residuals
sub-panel — the 'data on top, residuals below, shared x-axis' layout
used for Sérsic-profile fits, isochrone fits, zero-point fits, etc.
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import AutoMinorLocator


def scatter_with_fit(
    x,
    y,
    fit=None,
    fit_x=None,
    xlabel="",
    ylabel="",
    data_label="Datos",
    fit_label="Ajuste",
    residual_label="Residuo",
    data_color="tab:blue",
    fit_color="tab:red",
    residual_ylim=None,
    invert_y=False,
    figsize=(4, 4),
    height_ratios=(3, 1),
    s=14,
    alpha=0.7,
    grid_alpha=0.2,
    label_x_coord=-0.12,
    filename=None,
    graphs_path=None,
    dpi=350,
    scatter_kwargs=None,
    fit_kwargs=None,
):
    """
    Scatter plot with an optional fitted curve and a residuals panel
    underneath (shared x-axis) — e.g. `mu_V` vs `r^(1/4)` with a Sérsic
    fit and its residuals, an isochrone fit, a zero-point fit, etc.

    Parameters
    ----------
    x, y : array-like
        Data points to scatter.
    fit : callable(x) -> y, or array-like, optional
        The fitted model. Either a callable, evaluated both at `x`
        (for residuals) and at `fit_x` (for a smooth plotted curve —
        useful when `fit_x` is a finer grid than the data), or a
        precomputed array of fitted y values already matching `x`
        one-to-one (used directly for both the curve and residuals).
        If None, no fit line or residual panel is drawn: just a plain
        scatter is made and `(fig, ax)` is returned.
    fit_x : array-like, optional
        Only used when `fit` is callable — x values to evaluate/plot
        the smooth fit curve at. Defaults to `x`.
    invert_y : bool
        Invert the y-axis on both panels (e.g. for a magnitude axis).
    residual_ylim : (low, high), optional
        y-limits for the residual panel.
    scatter_kwargs, fit_kwargs : dict, optional
        Extra kwargs forwarded to `ax.scatter` (data points) / `ax.plot`
        (fit curve) respectively.

    Returns
    -------
    (fig, ax)              if `fit` is None
    (fig, (ax, ax_r))       if `fit` is given
    """
    x = np.asarray(x)
    y = np.asarray(y)
    scatter_kwargs = scatter_kwargs or {}
    fit_kwargs = fit_kwargs or {}
    has_fit = fit is not None

    if has_fit:
        fig, (ax, ax_r) = plt.subplots(
            2, 1, figsize=figsize, gridspec_kw={"height_ratios": list(height_ratios)}, sharex=True
        )
    else:
        fig, ax = plt.subplots(figsize=figsize)
        ax_r = None

    ax.scatter(x, y, s=s, alpha=alpha, color=data_color, zorder=10, label=data_label, **scatter_kwargs)
    ax.set_ylabel(ylabel)
    ax.xaxis.set_minor_locator(AutoMinorLocator())
    ax.yaxis.set_minor_locator(AutoMinorLocator())
    ax.grid(True, linestyle="--", alpha=grid_alpha)

    if has_fit:
        if callable(fit):
            fx = np.asarray(fit_x) if fit_x is not None else x
            fy_curve = fit(fx)
            fy_at_x = fit(x)
        else:
            # Precomputed array: assumed already aligned with `x`, one-to-one.
            fy_at_x = np.asarray(fit)
            fx, fy_curve = x, fy_at_x

        ax.plot(fx, fy_curve, color=fit_color, label=fit_label, zorder=15, **fit_kwargs)
        ax.legend(framealpha=0.9)

        residuals = y - fy_at_x
        ax_r.scatter(x, residuals, s=s, alpha=alpha, color=fit_color, zorder=10, label=residual_label)
        ax_r.axhline(0, color=fit_color, linestyle="-", zorder=5)
        ax_r.set_ylabel(residual_label)
        ax_r.set_xlabel(xlabel)
        ax_r.xaxis.set_minor_locator(AutoMinorLocator())
        ax_r.yaxis.set_minor_locator(AutoMinorLocator())
        ax_r.grid(True, alpha=grid_alpha, linestyle="--")
        if residual_ylim is not None:
            ax_r.set_ylim(*residual_ylim)

        ax.yaxis.set_label_coords(label_x_coord, 0.5)
        ax_r.yaxis.set_label_coords(label_x_coord, 0.5)

        if invert_y:
            ax.invert_yaxis()
            ax_r.invert_yaxis()
    else:
        ax.set_xlabel(xlabel)
        ax.legend(framealpha=0.9)
        if invert_y:
            ax.invert_yaxis()

    fig.tight_layout()

    if filename:
        base = Path(graphs_path) / filename if graphs_path else Path(filename)
        fig.savefig(f"{base}.png", dpi=dpi, bbox_inches="tight")
        fig.savefig(f"{base}.pdf", bbox_inches="tight")

    return (fig, ax) if not has_fit else (fig, (ax, ax_r))
