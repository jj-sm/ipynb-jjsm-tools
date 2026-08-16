"""Photometric zero-point calibration: sigma-clipped ZP estimation from a
set of (catalog mag, instrumental mag) pairs, and a plot of the fit.
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from astropy.stats import sigma_clipped_stats
from matplotlib.ticker import AutoMinorLocator


def compute_zeropoint(cat_mag, inst_mag, sigma=2.0):
    """
    Sigma-clipped zero point: ZP = cat_mag - inst_mag, averaged over the
    stars in common between a detected source list and a reference
    catalog.

    Parameters
    ----------
    cat_mag, inst_mag : array-like
        Catalog and instrumental magnitudes for the same matched stars,
        same order.
    sigma : float
        Sigma-clipping threshold passed to `sigma_clipped_stats`.

    Returns
    -------
    dict with:
        'zp'     : sigma-clipped mean zero point (NaN if no valid stars)
        'sigma'  : sigma-clipped std of the zero point
        'n'      : number of finite (cat_mag - inst_mag) values used
        'values' : the raw per-star (cat_mag - inst_mag) array, same
                   length/order as the input (NaN where either input was
                   non-finite)
    """
    cat_mag = np.asarray(cat_mag, dtype=float)
    inst_mag = np.asarray(inst_mag, dtype=float)
    zp_arr = cat_mag - inst_mag
    valid = zp_arr[np.isfinite(zp_arr)]

    if len(valid) == 0:
        return {"zp": np.nan, "sigma": np.nan, "n": 0, "values": zp_arr}
    if len(valid) == 1:
        return {"zp": float(valid[0]), "sigma": 0.0, "n": 1, "values": zp_arr}

    _, mean, std = sigma_clipped_stats(valid, sigma=sigma)
    return {"zp": float(mean), "sigma": float(std), "n": len(valid), "values": zp_arr}


def plot_zeropoint_fit(
    inst_mag,
    cat_mag,
    zp,
    sigma,
    band="",
    color=None,
    cat_label="cat",
    figsize=(4, 2.5),
    filename=None,
    graphs_path=None,
    dpi=350,
):
    """
    Plot instrumental vs. catalog magnitude for the matched calibration
    stars, with the fitted `m_cat = m_inst + ZP` line overlaid.

    Parameters
    ----------
    inst_mag, cat_mag : array-like
        Matched instrumental and catalog magnitudes (same stars, same
        order) for a single band.
    zp, sigma : float
        Zero point and its uncertainty, e.g. from `compute_zeropoint`.
    band : str
        Band name, used in the axis labels and legend.
    color : matplotlib color, optional
    cat_label : str
        Name of the reference catalog, shown on the y-axis.

    Returns
    -------
    (fig, ax)
    """
    inst_mag = np.asarray(inst_mag, dtype=float)
    cat_mag = np.asarray(cat_mag, dtype=float)

    fig, ax = plt.subplots(figsize=figsize)
    m_rng = np.linspace(np.nanmin(inst_mag) - 0.5, np.nanmax(inst_mag) + 0.5, 100)
    color = color or "tab:blue"

    ax.scatter(inst_mag, cat_mag, s=80, zorder=5, ec="k", lw=0.5, color=color, label="stars")
    ax.plot(
        m_rng,
        m_rng + zp,
        "r--",
        lw=1.5,
        label=rf"$ZP_{{{band}}} = {zp:.3f} \pm {sigma:.3f}$",
        zorder=10,
    )

    ax.set_xlabel(rf"$m_{{\rm inst,\,{band}}}$", fontsize=12)
    ax.set_ylabel(rf"$m_{{\rm {cat_label},\,{band}}}$", fontsize=12)
    ax.invert_xaxis()
    ax.invert_yaxis()
    ax.legend(fontsize=10)
    ax.xaxis.set_minor_locator(AutoMinorLocator())
    ax.yaxis.set_minor_locator(AutoMinorLocator())
    ax.grid(True, linestyle="--", alpha=0.5, zorder=-1)
    fig.tight_layout()

    if filename:
        base = Path(graphs_path) / filename if graphs_path else Path(filename)
        fig.savefig(f"{base}.png", dpi=dpi, bbox_inches="tight")
        fig.savefig(f"{base}.pdf", bbox_inches="tight")

    return fig, ax
