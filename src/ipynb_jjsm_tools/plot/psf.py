"""Point-spread-function inspection: a joint plot of a star's cutout
plus its X and Y pixel-value profiles, each with an optional 1D
Gaussian fit — useful for eyeballing FWHM/sigma and sanity-checking
aperture-photometry radii.
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.gridspec import GridSpec
from matplotlib.patches import Circle
from matplotlib.ticker import AutoMinorLocator, FuncFormatter

try:
    from scipy.optimize import curve_fit as _curve_fit
except Exception:  # pragma: no cover - scipy is an optional dependency here
    _curve_fit = None


# FWHM = 2 * sqrt(2 * ln 2) * sigma
_FWHM_FACTOR = 2.0 * np.sqrt(2.0 * np.log(2.0))


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
    r_in=None,
    r_out=None,
    r_aperture=None,
    cmap="inferno",
    figsize=(4, 4),
    filename=None,
    graphs_path=None,
    dpi=350,
    flux_label="Flujo (nJy)",
    xlabel=r"Píxel $x$",
    ylabel=r"Píxel $y$",
    show_fwhm=True,
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
    r_aperture : float, optional
        Photometry aperture radius (pixels). Drawn as a circle on the
        cutout and as vertical guides on the X/Y profiles. Labelled
        only on the cutout.
    r_in, r_out : float, optional
        Inner / outer radii (pixels) of the background annulus. Drawn
        as concentric circles on the cutout and as vertical guides on
        the X/Y profiles. Labelled only on the cutout.
    flux_label : str
        Label for the colorbar (units of the image). Default 'Flujo (nJy)'.
    xlabel, ylabel : str
        Axis labels for the pixel profiles. Default 'Píxel x' / 'Píxel y'.
    show_fwhm : bool
        If True, draws a segment at half-max spanning the FWHM on each
        profile, with the FWHM value in the legend.
    filename, graphs_path, dpi :
        If `filename` is given, saves PNG (at `dpi`) + PDF to
        `graphs_path/filename` (or the current directory if
        `graphs_path` is None).

    Returns
    -------
    fig, fit_results
        `fit_results` has keys 'x' and 'y', each either None (fit
        failed, or scipy isn't available) or
        `{'popt': [amp, cen, sigma, offset], 'perr': [...], 'fwhm': float}`.
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
    # FWHM Calc
    for k in ("x", "y"):
        if fit_results[k] is not None:
            fit_results[k]["fwhm"] = _FWHM_FACTOR * abs(fit_results[k]["popt"][2])

    fig = plt.figure(figsize=figsize)
    # Colorbar
    gs = GridSpec(
        2, 3,
        width_ratios=[1, 2.5, 0.08],
        height_ratios=[2.5, 1],
        hspace=0.05, wspace=0.05,
    )
    ax_img = fig.add_subplot(gs[0, 1])
    ax_left = fig.add_subplot(gs[0, 0], sharey=ax_img)
    ax_bot = fig.add_subplot(gs[1, 1], sharex=ax_img)
    ax_void = fig.add_subplot(gs[1, 0])
    ax_cbar = fig.add_subplot(gs[0, 2])
    ax_void.axis("off")

    x0_img, x1_img = max(0, px - window), min(img.shape[1], px + window + 1)
    y0_img, y1_img = max(0, py - window), min(img.shape[0], py + window + 1)
    crop = img[y0_img:y1_img, x0_img:x1_img]

    im = ax_img.imshow(
        crop, origin="lower", cmap=cmap, aspect="auto",
        extent=[x0_img - 0.5, x1_img - 0.5, y0_img - 0.5, y1_img - 0.5],
    )
    ax_img.axhline(py, color="steelblue", linestyle="--", linewidth=2, alpha=0.75)
    ax_img.axvline(px, color="tomato", linestyle="--", linewidth=2, alpha=0.75)
    ax_img.set_title(label, fontsize=12)
    ax_img.tick_params(labelbottom=False, labelleft=False)

    # Aperture and annulus guides on the cutout
    if r_aperture is not None:
        ax_img.add_patch(Circle(
            (px, py), r_aperture, fill=False, color="white", linewidth=1.5,
            linestyle="-", label=rf"$r_{{ap}}={r_aperture:.1f}$ px",
        ))
    if r_in is not None:
        ax_img.add_patch(Circle(
            (px, py), r_in, fill=False, color="white", linewidth=1.2,
            linestyle="--", label=rf"$r_{{in}}={r_in:.1f}$ px",
        ))
    if r_out is not None:
        ax_img.add_patch(Circle(
            (px, py), r_out, fill=False, color="white", linewidth=1.2,
            linestyle=":", label=rf"$r_{{out}}={r_out:.1f}$ px",
        ))
    if any(r is not None for r in (r_aperture, r_in, r_out)):
        ax_img.legend(fontsize=6, loc="upper right", framealpha=0.7)

    # colorbar for flux on the far right
    cbar = fig.colorbar(im, cax=ax_cbar)
    cbar.set_label(flux_label, fontsize=8)
    cbar.ax.tick_params(labelsize=7)

    # X Profile
    ax_bot.step(x_px, x_slice, where="mid", color="tomato", linewidth=1.2)
    if fit_results["x"] is not None:
        popt = fit_results["x"]["popt"]
        fwhm_x = fit_results["x"]["fwhm"]
        amp, cen, sigma, offset = popt
        xf = np.linspace(x_px[0], x_px[-1], 300)
        ax_bot.plot(
            xf, gaussian_1d(xf, *popt), color="tomato", linewidth=1.5, alpha=0.8,
            label=rf"$\sigma={sigma:.2f}$ px",
        )
        ax_bot.axvline(cen, color="tomato", linestyle="--", linewidth=2, alpha=0.75)

        # FWHM marker: horizontal segment at half-max spanning the FWHM
        if show_fwhm:
            half_max = offset + amp / 2.0
            ax_bot.hlines(
                y=half_max, xmin=cen - fwhm_x / 2, xmax=cen + fwhm_x / 2,
                color="red", linewidth=2, alpha=0.9, linestyle="-",
                # label=rf"$\textup{{FWHM}}={fwhm_x:.2f}$ px",
                label=rf"$\textup{{FWHM}}$",
            )

        if true_popt is not None:
            ax_bot.plot(
                xf, gaussian_1d(xf, *true_popt), color="lime", linewidth=1.3,
                linestyle=":", alpha=0.9, label="Original",
            )
        ax_bot.legend(fontsize=6, loc="upper right")

    # aperture / annulus guides on X 
    for r, ls in ((r_aperture, "-"), (r_in, "--"), (r_out, ":")):
        if r is not None:
            ax_bot.axvline(px - r, color="0.4", linestyle=ls, linewidth=1, alpha=0.6)
            ax_bot.axvline(px + r, color="0.4", linestyle=ls, linewidth=1, alpha=0.6)

    ax_bot.set_xlabel(xlabel, fontsize=10)
    ax_bot.grid(True, alpha=0.2, linestyle="--")
    ax_bot.xaxis.set_minor_locator(AutoMinorLocator())
    plt.setp(ax_bot.get_xticklabels(), fontsize=8)
    plt.setp(ax_bot.get_yticklabels(), fontsize=8)

    # Y Profile
    ax_left.step(y_slice, y_px, where="mid", color="steelblue", linewidth=1.2)
    if fit_results["y"] is not None:
        popt = fit_results["y"]["popt"]
        fwhm_y = fit_results["y"]["fwhm"]
        amp, cen, sigma, offset = popt
        yf = np.linspace(y_px[0], y_px[-1], 300)
        ax_left.plot(
            gaussian_1d(yf, *popt), yf, color="steelblue", linewidth=1.5, alpha=0.8,
            label=rf"$\sigma={sigma:.2f}$ px",
        )
        ax_left.axhline(cen, color="steelblue", linestyle="--", linewidth=2, alpha=0.75)

        # FWHM marker
        if show_fwhm:
            half_max = offset + amp / 2.0
            ax_left.vlines(
                x=half_max, ymin=cen - fwhm_y / 2, ymax=cen + fwhm_y / 2,
                color="blue", linewidth=2, alpha=0.9, linestyle="-",
                # label=rf"$\textup{{FWHM}}={fwhm_y:.2f}$ px",
                label=rf"$\textup{{FWHM}}$",
            )

        if true_popt is not None:
            true_popt_y = [true_popt[0], float(py), true_popt[2], true_popt[3]]
            ax_left.plot(
                gaussian_1d(yf, *true_popt_y), yf, color="lime", linewidth=1.3,
                linestyle=":", alpha=0.9, label="Original",
            )
        ax_left.legend(fontsize=6, loc="upper left")

    # aperture / annulus guides on Y
    for r, ls in ((r_aperture, "-"), (r_in, "--"), (r_out, ":")):
        if r is not None:
            ax_left.axhline(py - r, color="0.4", linestyle=ls, linewidth=1, alpha=0.6)
            ax_left.axhline(py + r, color="0.4", linestyle=ls, linewidth=1, alpha=0.6)

    ax_left.set_ylabel(ylabel, fontsize=10)
    ax_left.margins(x=0.15)
    ax_left.grid(True, alpha=0.2, linestyle="--")
    ax_left.invert_xaxis()
    ax_bot.invert_yaxis()
    ax_left.yaxis.set_minor_locator(AutoMinorLocator())
    ax_left.xaxis.set_major_formatter(FuncFormatter(lambda x, _: "" if x == 0 else f"{x:.0f}"))
    plt.setp(ax_left.get_xticklabels(), fontsize=8, rotation=45, ha="right")
    plt.setp(ax_left.get_yticklabels(), fontsize=8)

    ax_void.text(0.3, 0.3, flux_label, ha="center", va="center", fontsize=9, transform=ax_void.transAxes, rotation=45)

    if filename:
        base = Path(graphs_path) / filename if graphs_path else Path(filename)
        fig.savefig(f"{base}.png", dpi=dpi, bbox_inches="tight")
        fig.savefig(f"{base}.pdf", format="pdf", bbox_inches="tight")

    return fig, fit_results


def plot_star_profile_3d(
    img,
    cx,
    cy,
    label="",
    window=20,
    sigma_guess=3.0,
    r_in=None,
    r_out=None,
    r_aperture=None,
    cmap="inferno",
    figsize=(6, 5),
    filename=None,
    graphs_path=None,
    dpi=350,
    flux_label="Flujo (nJy)",
    xlabel=r"Píxel $x$",
    ylabel=r"Píxel $y$",
    show_fwhm=True,
    view_elev=25,
    view_azim=-60,
    surface_alpha=0.85,
):
    """
    3D surface plot of a star's PSF, with the X-slice (fixed y = cy) and
    Y-slice (fixed x = cx) projected onto the back walls of the axes,
    each with an optional 1D Gaussian fit.

    Aperture and annulus radii are drawn as circles on the base plane of
    the 3D box (z = z_floor).

    Parameters
    ----------
    img : ndarray
        Full 2D image array to crop from.
    cx, cy : float
        Star centroid, in pixel coordinates.
    window : int
        Half-width (in pixels) of the cutout and of each 1D slice.
    sigma_guess : float
        Initial guess for the Gaussian sigma (pixels) fed to curve_fit.
    r_aperture, r_in, r_out : float, optional
        Radii (px) drawn as circles on the base plane. Labelled in the
        figure legend.
    view_elev, view_azim : float
        Camera elevation / azimuth for the 3D view.
    surface_alpha : float
        Alpha for the surface plot (lower = easier to see the back walls).

    Returns
    -------
    fig, fit_results
        Same shape as the 2D version: fit_results has keys 'x' and 'y',
        each None or a dict {'popt', 'perr', 'fwhm'}.
    """
    px, py = int(round(cx)), int(round(cy))

    x0, x1 = max(0, px - window), min(img.shape[1], px + window + 1)
    y0, y1 = max(0, py - window), min(img.shape[0], py + window + 1)
    crop = img[y0:y1, x0:x1]

    x_axis = np.arange(x0, x1)
    y_axis = np.arange(y0, y1)
    XX, YY = np.meshgrid(x_axis, y_axis)

    x_slice = img[py, x0:x1]
    y_slice = img[y0:y1, px]

    fit_results = {
        "x": _fit_profile(x_axis, x_slice, sigma_guess),
        "y": _fit_profile(y_axis, y_slice, sigma_guess),
    }
    for k in ("x", "y"):
        if fit_results[k] is not None:
            fit_results[k]["fwhm"] = _FWHM_FACTOR * abs(fit_results[k]["popt"][2])

    fig = plt.figure(figsize=figsize)
    ax = fig.add_subplot(111, projection="3d")

    # sfc
    surf = ax.plot_surface(
        XX, YY, crop, cmap=cmap, alpha=surface_alpha,
        linewidth=0, antialiased=True, rcount=60, ccount=60,
    )

    # base plane z
    z_min = float(np.nanmin(crop))
    z_max = float(np.nanmax(crop))
    z_span = z_max - z_min
    z_floor = z_min - 0.05 * z_span
    ax.set_zlim(z_floor, z_max + 0.05 * z_span)

    # z plane projected image
    ax.contourf(
        XX, YY, crop,
        zdir="z", offset=z_floor,
        cmap=cmap, alpha=0.7, levels=20,
    )

    # X profile
    y_back = y_axis[-1]
    ax.plot(x_axis, np.full_like(x_axis, y_back, dtype=float), x_slice,
            color="tomato", linewidth=1.4, alpha=0.9, label="Perfil X")
    if fit_results["x"] is not None:
        popt = fit_results["x"]["popt"]
        amp, cen, sigma, offset = popt
        xf = np.linspace(x_axis[0], x_axis[-1], 300)
        ax.plot(xf, np.full_like(xf, y_back), gaussian_1d(xf, *popt),
                color="tomato", linewidth=1.2, linestyle="--", alpha=0.9,
                label=rf"Ajuste X: $\sigma={sigma:.2f}$ px")
        if show_fwhm:
            fwhm_x = fit_results["x"]["fwhm"]
            half_max = offset + amp / 2.0
            ax.plot(
                [cen - fwhm_x / 2, cen + fwhm_x / 2],
                [y_back, y_back],
                [half_max, half_max],
                color="tomato", linewidth=2.5, alpha=0.95,
                label=rf"FWHM$_x={fwhm_x:.2f}$ px",
            )

    # Y profile
    x_back = x_axis[0]
    ax.plot(np.full_like(y_axis, x_back, dtype=float), y_axis, y_slice,
            color="steelblue", linewidth=1.4, alpha=0.9, label="Perfil Y")
    if fit_results["y"] is not None:
        popt = fit_results["y"]["popt"]
        amp, cen, sigma, offset = popt
        yf = np.linspace(y_axis[0], y_axis[-1], 300)
        ax.plot(np.full_like(yf, x_back), yf, gaussian_1d(yf, *popt),
                color="steelblue", linewidth=1.2, linestyle="--", alpha=0.9,
                label=rf"Ajuste Y: $\sigma={sigma:.2f}$ px")
        if show_fwhm:
            fwhm_y = fit_results["y"]["fwhm"]
            half_max = offset + amp / 2.0
            ax.plot(
                [x_back, x_back],
                [cen - fwhm_y / 2, cen + fwhm_y / 2],
                [half_max, half_max],
                color="steelblue", linewidth=2.5, alpha=0.95,
                label=rf"FWHM$_y={fwhm_y:.2f}$ px",
            )

    # aperture, annulus projection on the z plane
    if r_aperture is not None:
        cx_c, cy_c = _circle_xy(px, py, r_aperture)
        ax.plot(cx_c, cy_c, np.full_like(cx_c, z_floor),
                color="k", linewidth=1.5, linestyle="-",
                label=rf"$r_{{ap}}={r_aperture:.1f}$ px")
    if r_in is not None:
        cx_c, cy_c = _circle_xy(px, py, r_in)
        ax.plot(cx_c, cy_c, np.full_like(cx_c, z_floor),
                color="k", linewidth=1.2, linestyle="--",
                label=rf"$r_{{in}}={r_in:.1f}$ px")
    if r_out is not None:
        cx_c, cy_c = _circle_xy(px, py, r_out)
        ax.plot(cx_c, cy_c, np.full_like(cx_c, z_floor),
                color="k", linewidth=1.2, linestyle=":",
                label=rf"$r_{{out}}={r_out:.1f}$ px")

    # centroid marker
    ax.plot([px], [py], [z_floor], marker="+", color="k", markersize=10, mew=1.5)

    ax.set_xlabel(xlabel, fontsize=9, labelpad=6)
    ax.set_ylabel(ylabel, fontsize=9, labelpad=6)
    ax.set_zlabel(flux_label, fontsize=9, labelpad=6)
    ax.set_title(label, fontsize=11)

    ax.view_init(elev=view_elev, azim=view_azim)
    ax.tick_params(labelsize=7)

    cbar = fig.colorbar(surf, ax=ax, shrink=0.6, pad=0.1)
    cbar.set_label(flux_label, fontsize=8)
    cbar.ax.tick_params(labelsize=7)

    ax.legend(fontsize=6, loc="upper left", bbox_to_anchor=(0.0, 1.0), framealpha=0.75)

    fig.tight_layout()

    if filename:
        base = Path(graphs_path) / filename if graphs_path else Path(filename)
        fig.savefig(f"{base}.png", dpi=dpi, bbox_inches="tight")
        fig.savefig(f"{base}.pdf", format="pdf", bbox_inches="tight")

    return fig, fit_results