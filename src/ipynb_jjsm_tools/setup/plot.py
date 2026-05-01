import shutil
import matplotlib.pyplot as plt

_PLOT_BASE_STYLE = {
    "font.family": "sans-serif",
    "axes.labelsize": 12,
    "axes.titlesize": 12,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "legend.fontsize": 9,
}


def _latex_toolchain_status():
    required = ["latex"]
    optional = ["dvipng", "dvisvgm", "gs"]
    missing_required = [cmd for cmd in required if shutil.which(cmd) is None]
    has_render_backend = any(shutil.which(cmd) is not None for cmd in optional)
    return {
        "ok": not missing_required and has_render_backend,
        "missing_required": missing_required,
        "has_render_backend": has_render_backend,
    }


def activate_tex(enabled=True):
    """
    Configure matplotlib text/math rendering.

    Parameters
    ----------
    enabled : bool
        If False, forces mathtext regardless of LaTeX availability.

    Returns
    -------
    dict with keys 'backend' and 'message'.
    """
    plt.rcParams.update(_PLOT_BASE_STYLE)

    if not enabled:
        plt.rcParams["text.usetex"] = False
        return {"backend": "mathtext", "message": "TeX disabled."}

    status = _latex_toolchain_status()
    if not status["ok"]:
        missing = ", ".join(status["missing_required"]) or "none"
        plt.rcParams["text.usetex"] = False
        return {
            "backend": "mathtext",
            "message": f"TeX unavailable (missing: {missing}; need dvipng/dvisvgm/gs).",
        }

    plt.rcParams.update(
        {
            "text.usetex": True,
            "mathtext.default": "sf",
            "text.latex.preamble": r"\usepackage{sfmath}",
        }
    )

    try:
        fig, ax = plt.subplots(figsize=(2, 1))
        ax.text(0.1, 0.5, r"$E=\frac{1}{2}CV^2$")
        fig.canvas.draw()
        plt.close(fig)
        return {"backend": "usetex", "message": "LaTeX rendering active."}
    except Exception as exc:
        plt.rcParams["text.usetex"] = False
        plt.close("all")
        return {
            "backend": "mathtext",
            "message": f"LaTeX probe failed, fallback to mathtext: {exc.__class__.__name__}",
        }
