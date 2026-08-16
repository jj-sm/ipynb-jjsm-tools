"""Aperture photometry: flux extraction with a local sigma-clipped
background annulus, and a radius-optimization sweep + plot to pick the
aperture radius that minimizes photometric scatter per band.
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from astropy.stats import sigma_clipped_stats
from photutils.aperture import CircularAnnulus, CircularAperture, aperture_photometry


def make_apertures(x, y, r, r_in, r_out):
    """Build a (CircularAperture, CircularAnnulus) pair for a set of star positions."""
    positions = np.transpose((x, y))
    aperture = CircularAperture(positions, r=r)
    annulus = CircularAnnulus(positions, r_in=r_in, r_out=r_out)
    return aperture, annulus


def aperture_flux(data, apertures, sigma=3.0, min_annulus_px=5):
    """
    Background-subtracted aperture flux for every position in `apertures`.

    The local sky background is estimated per star as the sigma-clipped
    median of its annulus, then scaled by the aperture area and
    subtracted from the raw aperture sum.

    Parameters
    ----------
    data : CCDData or ndarray
        Image data. `aperture_photometry` is run on `data` directly so
        units propagate when `data` is a `CCDData`/`Quantity`.
    apertures : (CircularAperture, CircularAnnulus)
        Output of `make_apertures`.
    sigma : float
        Sigma-clipping threshold for the annulus background estimate.
    min_annulus_px : int
        Minimum finite pixels required in an annulus to trust it; annuli
        with fewer are treated as background=0 rather than discarding
        the star.

    Returns
    -------
    ndarray
        One background-subtracted flux per position. NaN for positions
        where the photometry call itself fails.
    """
    aperture, annulus = apertures
    raw = data.data if hasattr(data, "data") else data
    annulus_masks = annulus.to_mask(method="center")

    bkg_median = []
    for mask in annulus_masks:
        ann_data = mask.multiply(raw)
        if ann_data is None:
            bkg_median.append(0.0)
            continue
        ann_1d = ann_data[mask.data > 0]
        ann_1d = ann_1d[np.isfinite(ann_1d)]
        if len(ann_1d) < min_annulus_px:
            bkg_median.append(0.0)
            continue
        _, med, _ = sigma_clipped_stats(ann_1d, sigma=sigma)
        bkg_median.append(float(med))
    bkg_median = np.array(bkg_median)

    try:
        phot = aperture_photometry(data, aperture)
        unit = phot["aperture_sum"].unit
        bkg_total = bkg_median * aperture.area * unit if unit is not None else bkg_median * aperture.area
        phot["aper_bkg"] = bkg_total
        phot["aper_sum_bkgsub"] = phot["aperture_sum"] - phot["aper_bkg"]
        result = phot["aper_sum_bkgsub"]
        return result.value if hasattr(result, "value") else np.asarray(result)
    except Exception:
        n_pos = len(aperture.positions) if hasattr(aperture, "positions") else 1
        return np.full(n_pos, np.nan)


def instrumental_mag(flux):
    """-2.5 log10(flux); NaN wherever flux <= 0."""
    flux = np.asarray(flux, dtype=float)
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.where(flux > 0, -2.5 * np.log10(flux), np.nan)


def scan_aperture_radii(radii_pix, evaluate_fn, bands):
    """
    Sweep candidate aperture radii and collect a scatter curve per band,
    so the radius that minimizes photometric scatter can be picked.

    This is deliberately agnostic to *how* the scatter is computed
    (zero-point sigma against a reference catalog is the typical case,
    see `zeropoint.compute_zeropoint`) — you supply `evaluate_fn`.

    Parameters
    ----------
    radii_pix : iterable of float
        Candidate aperture radii, in pixels, to test.
    evaluate_fn : callable(radius_pix) -> dict | None
        Called once per radius. Must return a dict containing at least
        `{'std': {band: sigma, ...}}` (e.g. the zero-point sigma achieved
        at that radius for each band), or `None` if that radius couldn't
        be evaluated (e.g. no matched sources).
    bands : iterable of str
        Band identifiers to track, e.g. ('g', 'r', 'i').

    Returns
    -------
    results : dict[band] -> {'radii': [...], 'stds': [...]}
        The full scatter curve per band (only finite points).
    best_radii : dict[band] -> float | None
        The radius minimizing scatter for each band; `None` for a band
        that never produced a finite value anywhere in the sweep.
    """
    results = {b: {"radii": [], "stds": []} for b in bands}

    for r in radii_pix:
        res = evaluate_fn(r)
        if res is None:
            continue
        for b in bands:
            std = res.get("std", {}).get(b, np.nan)
            if np.isfinite(std):
                results[b]["radii"].append(r)
                results[b]["stds"].append(std)

    best_radii = {}
    for b in bands:
        stds = results[b]["stds"]
        best_radii[b] = results[b]["radii"][int(np.argmin(stds))] if stds else None

    return results, best_radii


def plot_radius_optimization(
    results,
    best_radii=None,
    bands=None,
    colors=None,
    xlabel="Aperture radius [pix]",
    ylabel=r"$\sigma_{\rm ZP}$ [mag]",
    figsize=(4, 4),
    filename=None,
    graphs_path=None,
    dpi=350,
):
    """
    Plot the per-band scatter-vs-radius curves from `scan_aperture_radii`,
    marking the best radius per band if given.

    Parameters
    ----------
    results : dict[band] -> {'radii': [...], 'stds': [...]}
        As returned by `scan_aperture_radii`.
    best_radii : dict[band] -> float, optional
        If given, draws a dashed vertical line at each band's optimum.
    bands : iterable of str, optional
        Defaults to `results.keys()`.
    colors : dict[band] -> color, optional
        Defaults to {'g': 'blue', 'r': 'green', 'i': 'red'} for those
        bands, tab10 cycling for anything else.

    Returns
    -------
    (fig, ax)
    """
    bands = list(bands) if bands is not None else list(results.keys())
    default_colors = {"g": "tab:blue", "r": "tab:green", "i": "tab:red"}
    cmap_fallback = plt.get_cmap("tab10")
    colors = colors or {b: default_colors.get(b, cmap_fallback(i % 10)) for i, b in enumerate(bands)}

    fig, ax = plt.subplots(figsize=figsize)
    for b in bands:
        if not results[b]["radii"]:
            continue
        ax.plot(results[b]["radii"], results[b]["stds"], "o-", color=colors[b], label=b.upper())
        if best_radii and best_radii.get(b) is not None:
            ax.axvline(best_radii[b], color=colors[b], ls="--", lw=1, alpha=0.6)

    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    if filename:
        base = Path(graphs_path) / filename if graphs_path else Path(filename)
        fig.savefig(f"{base}.png", dpi=dpi, bbox_inches="tight")
        fig.savefig(f"{base}.pdf", bbox_inches="tight")

    return fig, ax
