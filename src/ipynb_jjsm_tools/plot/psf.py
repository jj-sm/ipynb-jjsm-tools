"""Point-spread-function inspection: a joint plot of a star's cutout
plus its X and Y pixel-value profiles, each with an optional 1D
Gaussian fit — useful for eyeballing FWHM/sigma and sanity-checking
aperture-photometry radii.
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.gridspec import GridSpec
from matplotlib.ticker import AutoMinorLocator, FuncFormatter

try:
    from scipy.optimize import curve_fit as _curve_fit
except Exception:  # pragma: no cover - scipy is an optional dependency here
    _curve_fit = None


def gaussian_1d(x, amp, cen, sigma, offset):
    """1D Gaussian on a constant offset: amp * exp(-0.5*((x-cen)/sigma)^2) + offset."""
    return amp * np.exp(-0.5 * ((x - cen) / sigma) ** 2) + offset


def _fit_profile(pixel_coords, profile, sigma_guess):
    if _curve_fit is None:
        return None
    p0 = [profile.max(), pixel_coords[np.argmax(profile)], sigma_guess, profile.min()]
    try:
        popt, pcov = _curve_fit(gaussian_1d, pixel_coords, profile, p0=p0, maxfev=5000)
        return {"popt": popt, "perr": np.sqrt(np.diag(pcov))}
    except RuntimeError:
        return None


def plot_star_profile(
    img,
    cx,
    cy,
    label="",
    window=20,
    sigma_guess=3.0,
    true_popt=None,
    cmap="inferno",
    figsize=(4, 4),
    filename=None,
    graphs_path=None,
    dpi=350,
):
    """
    Joint plot of a star's cutout plus its X and Y pixel-value profiles
    (with an optional 1D Gaussian fit on each) — the notebook's
    `plot_star_profile_joint`, generalized to not depend on any global
    `SAVE_PLOTS`/`GRAPHS_PATH`/`DPI` names.

    Parameters
    ----------
    img : ndarray
        Full 2D image array to crop from.
    cx, cy : float
        Star centroid, in pixel coordinates.
    window : int
        Half-width (in pixels) of the cutout and of each 1D slice.
    sigma_guess : float
        Initial guess for the Gaussian sigma (pixels) fed to
        `scipy.optimize.curve_fit` on both axes.
    true_popt : (amp, cen, sigma, offset), optional
        A reference/injected profile to overplot for comparison, e.g.
        when checking recovery on a simulated star.
    filename, graphs_path, dpi :
        If `filename` is given, saves PNG (at `dpi`) + PDF to
        `graphs_path/filename` (or the current directory if
        `graphs_path` is None).

    Returns
    -------
    fig, fit_results
        `fit_results` has keys 'x' and 'y', each either None (fit
        failed, or scipy isn't available) or
        `{'popt': [amp, cen, sigma, offset], 'perr': [...]}`.
    """
    px, py = int(round(cx)), int(round(cy))

    x_slice = img[py, max(0, px - window): px + window + 1]
    y_slice = img[max(0, py - window): py + window + 1, px]
    x_px = np.arange(len(x_slice)) + max(0, px - window)
    y_px = np.arange(len(y_slice)) + max(0, py - window)

    fit_results = {
        "x": _fit_profile(x_px, x_slice, sigma_guess),
        "y": _fit_profile(y_px, y_slice, sigma_guess),
    }

    fig = plt.figure(figsize=figsize)
    gs = GridSpec(2, 2, width_ratios=[1, 2.5], height_ratios=[2.5, 1], hspace=0.05, wspace=0.05)
    ax_img = fig.add_subplot(gs[0, 1])
    ax_left = fig.add_subplot(gs[0, 0], sharey=ax_img)
    ax_bot = fig.add_subplot(gs[1, 1], sharex=ax_img)
    ax_void = fig.add_subplot(gs[1, 0])
    ax_void.axis("off")

    x0_img, x1_img = max(0, px - window), min(img.shape[1], px + window + 1)
    y0_img, y1_img = max(0, py - window), min(img.shape[0], py + window + 1)
    crop = img[y0_img:y1_img, x0_img:x1_img]

    ax_img.imshow(
        crop, origin="lower", cmap=cmap, aspect="auto",
        extent=[x0_img - 0.5, x1_img - 0.5, y0_img - 0.5, y1_img - 0.5],
    )
    ax_img.axhline(py, color="steelblue", linestyle="--", linewidth=2, alpha=0.75)
    ax_img.axvline(px, color="tomato", linestyle="--", linewidth=2, alpha=0.75)
    ax_img.set_title(label, fontsize=12)
    ax_img.tick_params(labelbottom=False, labelleft=False)

    # --- X profile (bottom) ---
    ax_bot.step(x_px, x_slice, where="mid", color="tomato", linewidth=1.2)
    if fit_results["x"] is not None:
        popt = fit_results["x"]["popt"]
        xf = np.linspace(x_px[0], x_px[-1], 300)
        ax_bot.plot(
            xf, gaussian_1d(xf, *popt), color="tomato", linewidth=1.5, alpha=0.8,
            label=rf"$\sigma={popt[2]:.2f}$ px",
        )
        ax_bot.axvline(popt[1], color="tomato", linestyle="--", linewidth=2, alpha=0.75)
        if true_popt is not None:
            ax_bot.plot(
                xf, gaussian_1d(xf, *true_popt), color="lime", linewidth=1.3,
                linestyle=":", alpha=0.9, label="Original",
            )
        ax_bot.legend(fontsize=6, loc="upper right")
    ax_bot.set_xlabel(r"Píxel $x$", fontsize=10)
    ax_bot.grid(True, alpha=0.2, linestyle="--")
    ax_bot.xaxis.set_minor_locator(AutoMinorLocator())
    plt.setp(ax_bot.get_xticklabels(), fontsize=8)
    plt.setp(ax_bot.get_yticklabels(), fontsize=8)

    # --- Y profile (left) ---
    ax_left.step(y_slice, y_px, where="mid", color="steelblue", linewidth=1.2)
    if fit_results["y"] is not None:
        popt = fit_results["y"]["popt"]
        yf = np.linspace(y_px[0], y_px[-1], 300)
        ax_left.plot(
            gaussian_1d(yf, *popt), yf, color="steelblue", linewidth=1.5, alpha=0.8,
            label=rf"$\sigma={popt[2]:.2f}$ px",
        )
        ax_left.axhline(popt[1], color="steelblue", linestyle="--", linewidth=2, alpha=0.75)
        if true_popt is not None:
            true_popt_y = [true_popt[0], float(py), true_popt[2], true_popt[3]]
            ax_left.plot(
                gaussian_1d(yf, *true_popt_y), yf, color="lime", linewidth=1.3,
                linestyle=":", alpha=0.9, label="Original",
            )
        ax_left.legend(fontsize=6, loc="upper left")
    ax_left.set_ylabel(r"Píxel $y$", fontsize=10)
    ax_left.margins(x=0.15)
    ax_left.grid(True, alpha=0.2, linestyle="--")
    ax_left.invert_xaxis()
    ax_bot.invert_yaxis()
    ax_left.yaxis.set_minor_locator(AutoMinorLocator())
    ax_left.xaxis.set_major_formatter(FuncFormatter(lambda x, _: "" if x == 0 else f"{x:.0f}"))
    plt.setp(ax_left.get_xticklabels(), fontsize=8, rotation=45, ha="right")
    plt.setp(ax_left.get_yticklabels(), fontsize=8)

    ax_void.text(0.3, 0.3, "Cuentas", ha="center", va="center", fontsize=10, transform=ax_void.transAxes)

    if filename:
        base = Path(graphs_path) / filename if graphs_path else Path(filename)
        fig.savefig(f"{base}.png", dpi=dpi, bbox_inches="tight")
        fig.savefig(f"{base}.pdf", format="pdf", bbox_inches="tight")

    return fig, fit_results
