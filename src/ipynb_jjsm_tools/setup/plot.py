import functools
import io
import threading
import os
import platform
import shutil
import subprocess
import urllib.request
import warnings
import zipfile
from pathlib import Path
import matplotlib
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.backend_bases import FigureCanvasBase
from matplotlib.figure import Figure
from matplotlib.text import Text

CM_PER_INCH = 2.54


def cm_to_in(*values):
    """Convert one or more cm values to inches."""
    out = tuple(v / CM_PER_INCH for v in values)
    return out[0] if len(out) == 1 else out


def size(width_cm, height_cm=None, *, aspect=3 / 4):
    """
    Return a (width, height) tuple in inches, ready for figsize=lab.size(...).

    width_cm : figure width in cm (e.g. match a LaTeX \\includegraphics width)
    height_cm : figure height in cm; if omitted, derived from width_cm * aspect
    aspect : height/width ratio used when height_cm is not given (default 3/4)
    """
    w = width_cm / CM_PER_INCH
    h = (height_cm / CM_PER_INCH) if height_cm is not None else w * aspect
    return (w, h)


def set_font_sizes(base=10, *, label=None, title=None, tick=None, legend=None):
    """
    Set default font sizes (in points) for figure text, matching your document's body size.

    base : point size matching your LaTeX body text (e.g. \\normalsize -> 10/11/12 depending
        on document class). Used for axes.labelsize and axes.titlesize unless overridden.
    label, title, tick, legend : optional per-element overrides (points).
        tick and legend default to base - 1, matching typical \\footnotesize-ish convention.
    """
    plt.rcParams.update(
        {
            "font.size": base,
            "axes.labelsize": label if label is not None else base,
            "axes.titlesize": title if title is not None else base,
            "xtick.labelsize": tick if tick is not None else base - 1,
            "ytick.labelsize": tick if tick is not None else base - 1,
            "legend.fontsize": legend if legend is not None else base - 1,
        }
    )


_PLOT_BASE_STYLE = {
    "font.family": "sans-serif",
    "axes.labelsize": 12,
    "axes.titlesize": 12,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "legend.fontsize": 9,
    "xtick.direction": "in",
    "ytick.direction": "in",
    "xtick.top": True,
    "ytick.right": True,
    "xtick.minor.visible": True,
    "ytick.minor.visible": True,
}


_FONT_PRESETS = {
    "bright": {
        "cmu_family": "CMU Bright",
        "tex_packages": ["cmbright"],
        "usetex": {
            "font.family": "sans-serif",
            "font.sans-serif": ["Computer Modern Sans Serif"],
            "text.latex.preamble": r"\usepackage{amsmath}\usepackage{cmbright}",
        },
        "mathtext": {
            "font.family": "CMU Bright",
            "mathtext.fontset": "custom",
            "mathtext.rm": "CMU Bright",
            "mathtext.it": "CMU Bright:italic",
            "mathtext.bf": "CMU Bright:bold",
            "mathtext.sf": "CMU Bright",
            "mathtext.fallback": "stixsans",
            "mathtext.default": "it",
        },
        "mathtext_nf": {
            "font.family": "DejaVu Sans",
            "mathtext.fontset": "dejavusans",
            "mathtext.default": "regular",
        },
    },
    "roman": {
        "cmu_family": "CMU Serif",
        "tex_packages": ["type1cm"],
        "usetex": {
            "font.family": "serif",
            "font.serif": ["Computer Modern Roman"],
            "text.latex.preamble": r"\usepackage{amsmath}",
        },
        "mathtext": {
            "font.family": "CMU Serif",
            "mathtext.fontset": "cm",
            "mathtext.default": "it",
        },
        "mathtext_nf": {
            "font.family": "DejaVu Serif",
            "mathtext.fontset": "cm",
            "mathtext.default": "it",
        },
    },
}

# CMU (Computer Modern Unicode) OpenType fonts for non-LaTeX text
_CMU_URL = "https://mirrors.ctan.org/fonts/cm-unicode.zip"
_CMU_CACHE = Path(matplotlib.get_cachedir()) / "cmu_fonts"
_CMU_DOWNLOAD_FAILED = False  # don't retry the download for every figure


def _font_available(family):
    return any(f.name == family for f in font_manager.fontManager.ttflist)


def _register_font_dir(directory):
    for path in Path(directory).glob("cmun*.otf"):
        font_manager.fontManager.addfont(str(path))


def _download_cmu(dest=_CMU_CACHE):
    dest.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(_CMU_URL, timeout=60) as resp:
        data = resp.read()
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for name in zf.namelist():
            base = os.path.basename(name)
            if base.startswith("cmun") and base.endswith(".otf"):
                (dest / base).write_bytes(zf.read(name))


def ensure_cmu_fonts(families=("CMU Bright", "CMU Serif"), download=True):
    """
    Make the CMU fonts usable by matplotlib for normal (non-LaTeX) text.

    Looks in: fonts matplotlib already knows -> TeX Live's cm-unicode -> local cache,
    and downloads cm-unicode from CTAN into the matplotlib cache dir as a last resort.
    Returns True if every requested family is available.
    """

    def ok():
        return all(_font_available(f) for f in families)

    if ok():
        return True

    search_dirs = []
    _add_user_tex_to_path()
    if shutil.which("kpsewhich"):
        hit = subprocess.run(
            ["kpsewhich", "cmunrm.otf"], capture_output=True, text=True
        ).stdout.strip()
        if hit:
            search_dirs.append(Path(hit).parent)
    search_dirs.append(_CMU_CACHE)

    for d in search_dirs:
        if d.is_dir():
            _register_font_dir(d)
    global _CMU_DOWNLOAD_FAILED
    if ok() or not download or _CMU_DOWNLOAD_FAILED:
        return ok()

    try:
        _download_cmu()
        _register_font_dir(_CMU_CACHE)
    except Exception as exc:
        _CMU_DOWNLOAD_FAILED = True
        warnings.warn(
            f"Could not download CMU fonts ({exc.__class__.__name__}: {exc})."
        )
    return ok()


# LaTeX detection + activation
# User-local TeX Live installed by install_tex() when there's no root
_TINYTEX_DIR = Path.home() / (
    "Library/TinyTeX" if platform.system() == "Darwin" else ".TinyTeX"
)
_TINYTEX_URL = "https://yihui.org/tinytex/install-bin-unix.sh"


def _add_user_tex_to_path():
    # Jupyter kernels often don't inherit ~/bin, where TinyTeX links its binaries
    path = os.environ.get("PATH", "").split(os.pathsep)
    for bindir in sorted(_TINYTEX_DIR.glob("bin/*")):
        if (bindir / "latex").exists() and str(bindir) not in path:
            os.environ["PATH"] = os.pathsep.join([str(bindir), *path])
            return


def _has_tex_package(pkg):
    if shutil.which("kpsewhich") is None:
        return False
    out = subprocess.run(["kpsewhich", f"{pkg}.sty"], capture_output=True, text=True)
    return bool(out.stdout.strip())


def _latex_toolchain_status(packages=()):
    _add_user_tex_to_path()
    required = ["latex"]
    optional = ["dvipng", "dvisvgm", "gs"]
    missing_required = [cmd for cmd in required if shutil.which(cmd) is None]
    has_render_backend = any(shutil.which(cmd) is not None for cmd in optional)
    missing_packages = (
        [] if missing_required else [p for p in packages if not _has_tex_package(p)]
    )
    return {
        "ok": not missing_required and has_render_backend and not missing_packages,
        "missing_required": missing_required,
        "has_render_backend": has_render_backend,
        "missing_packages": missing_packages,
    }


def _use_mathtext(preset, font, message, download_fonts):
    plt.rcParams["text.usetex"] = False
    if ensure_cmu_fonts((preset["cmu_family"],), download=download_fonts):
        plt.rcParams.update(preset["mathtext"])
        text_font = preset["cmu_family"]
    else:
        plt.rcParams.update(preset["mathtext_nf"])
        text_font = "fallback (CMU fonts unavailable)"
        message += " CMU fonts unavailable, using DejaVu for text."
    return {
        "backend": "mathtext",
        "font": font,
        "text_font": text_font,
        "message": message,
    }


def activate_tex(enabled=True, serif=False, download_fonts=True, auto_fallback=True):
    """
    Configure matplotlib text/math rendering for all plot text
    (labels, ticks, titles, legends, annotations and math).

    Parameters
    ----------
    enabled : bool
        If False, forces mathtext regardless of LaTeX availability.
    serif : bool
        False -> CMU Bright (sans).  True -> Computer Modern Roman / CMU Serif.
    download_fonts : bool
        In mathtext mode, download the CMU fonts from CTAN if they aren't found.
    auto_fallback : bool
        If a figure later fails to compile with LaTeX (bad string, missing package...),
        redraw that figure with mathtext and emit a LaTeXFallbackWarning.

    Returns
    -------
    dict with keys 'backend', 'font', 'text_font' and 'message'.
    """
    font = "roman" if serif else "bright"
    preset = _FONT_PRESETS[font]
    _FALLBACK.update(font=font, download_fonts=download_fonts, enabled=False)

    # Reset so switching back and forth never leaves stale settings behind
    plt.rcParams.update(_PLOT_BASE_STYLE)
    plt.rcParams["text.latex.preamble"] = ""

    if not enabled:
        return _use_mathtext(preset, font, "TeX disabled.", download_fonts)

    status = _latex_toolchain_status(preset["tex_packages"])
    if not status["ok"]:
        parts = []
        if status["missing_required"]:
            parts.append("missing: " + ", ".join(status["missing_required"]))
        if not status["has_render_backend"]:
            parts.append("need dvipng/dvisvgm/gs")
        if status["missing_packages"]:
            parts.append(
                "missing TeX packages: " + ", ".join(status["missing_packages"])
            )
        return _use_mathtext(
            preset,
            font,
            f"TeX unavailable ({'; '.join(parts)}). Run install_tex() for the install command.",
            download_fonts,
        )

    plt.rcParams.update(preset["usetex"])
    plt.rcParams["text.usetex"] = True

    try:
        fig, ax = plt.subplots(figsize=(2, 1))
        ax.text(0.1, 0.5, r"Text $E=\frac{1}{2}CV^2$")
        fig.canvas.draw()
        plt.close(fig)
        if auto_fallback:
            _install_fallback_hooks()
            _FALLBACK["enabled"] = True
        name = "CMU Bright" if font == "bright" else "Computer Modern Roman"
        return {
            "backend": "usetex",
            "font": font,
            "text_font": name,
            "message": "LaTeX rendering active.",
        }
    except Exception as exc:
        plt.close("all")
        return _use_mathtext(
            preset,
            font,
            f"LaTeX probe failed, fallback to mathtext: {exc.__class__.__name__}.",
            download_fonts,
        )


# Per-figure fallback: LaTeX fails -> redraw with mathtext + alert
class LaTeXFallbackWarning(UserWarning):
    """Emitted when a figure could not be compiled with LaTeX and was redrawn without it."""


warnings.simplefilter("always", LaTeXFallbackWarning)  # alert for every failing figure

_FALLBACK = {"enabled": False, "font": "bright", "download_fonts": True}
_state = threading.local()


def _figure_uses_tex(fig):
    return any(t.get_usetex() for t in fig.findobj(Text))


def _summarize_latex_error(exc):
    lines = str(exc).splitlines()
    out = []
    for i, line in enumerate(lines):
        if "following string" in line and i + 1 < len(lines):
            out.append("  string: " + lines[i + 1].strip())
        if line.startswith("!"):
            out.append("  error:  " + line.strip())
    if not out:
        out.append(f"  {exc.__class__.__name__}: {lines[0] if lines else ''}")
    return "\n".join(dict.fromkeys(out))


def _alert(exc):
    warnings.warn(
        "\u26a0 LaTeX failed; figure rendered WITHOUT LaTeX (mathtext).\n"
        + _summarize_latex_error(exc),
        LaTeXFallbackWarning,
        stacklevel=4,
    )


def disable_tex_in_figure(fig):
    """Switch every text in `fig` from usetex to mathtext, keeping the chosen font style."""
    preset = _FONT_PRESETS[_FALLBACK["font"]]
    have_cmu = ensure_cmu_fonts(
        (preset["cmu_family"],), download=_FALLBACK["download_fonts"]
    )
    rc = preset["mathtext"] if have_cmu else preset["mathtext_nf"]
    # mathtext.rm/it/bf/... are read at draw time; they don't affect usetex figures
    plt.rcParams.update(
        {
            k: v
            for k, v in rc.items()
            if k.startswith("mathtext.") and k != "mathtext.fontset"
        }
    )
    for t in fig.findobj(Text):
        t.set_usetex(False)
        t.set_fontfamily(rc["font.family"])
        t.get_fontproperties().set_math_fontfamily(rc["mathtext.fontset"])
    return fig


def _install_fallback_hooks():
    # Guard against double-wrapping (e.g. module reloaded in Jupyter)
    if getattr(FigureCanvasBase.print_figure, "_tex_fallback", False):
        return
    orig_print = FigureCanvasBase.print_figure
    orig_draw = Figure.draw

    # savefig, Jupyter inline display, exports: clean retry with a fresh renderer
    @functools.wraps(orig_print)
    def print_figure(self, filename, *args, **kwargs):
        fig = self.figure
        pos = None
        if hasattr(filename, "seek") and hasattr(filename, "tell"):
            try:
                pos = filename.tell()
            except Exception:
                pos = None
        _state.printing = getattr(_state, "printing", 0) + 1
        try:
            return orig_print(self, filename, *args, **kwargs)
        except Exception as exc:
            if not _FALLBACK["enabled"] or not _figure_uses_tex(fig):
                raise
            if pos is not None:  # discard partial output in buffers
                filename.seek(pos)
                filename.truncate()
            disable_tex_in_figure(fig)
            _alert(exc)
            try:
                return orig_print(self, filename, *args, **kwargs)
            except Exception as exc2:
                raise RuntimeError(
                    "LaTeX failed and the mathtext fallback also failed "
                    "(the string may use LaTeX-only commands)."
                ) from exc2
        finally:
            _state.printing -= 1

    # Interactive GUI windows (plt.show with Qt/Tk/...): retry the draw in place
    @functools.wraps(orig_draw)
    def draw(self, renderer, *args, **kwargs):
        try:
            return orig_draw(self, renderer, *args, **kwargs)
        except Exception as exc:
            if (
                getattr(_state, "printing", 0)
                or not _FALLBACK["enabled"]
                or not _figure_uses_tex(self)
            ):
                raise  # print_figure handles it, or it isn't a LaTeX problem
            disable_tex_in_figure(self)
            _alert(exc)
            return orig_draw(self, renderer, *args, **kwargs)

    print_figure._tex_fallback = True
    draw._tex_fallback = True
    FigureCanvasBase.print_figure = print_figure
    Figure.draw = draw


# TeX installation helper
_TL_PACKAGES = [
    "type1cm",
    "cm-super",
    "cmbright",
    "hfbright",
    "cm-unicode",
    "dvipng",
    "underscore",
    "geometry",
]


def _can_sudo():
    """True if we're root or can plausibly use sudo (so system package managers work)."""
    if os.geteuid() == 0:  # e.g. Colab runs as root
        return True
    if shutil.which("sudo") is None:
        return False
    if subprocess.run(["sudo", "-n", "true"], capture_output=True).returncode == 0:
        return True
    import grp

    groups = set()
    for gid in os.getgroups():
        try:
            groups.add(grp.getgrgid(gid).gr_name)
        except KeyError:  # LDAP/cluster groups without a local name
            pass
    return bool(groups & {"sudo", "wheel", "admin"})


def _tinytex_tlmgr():
    hits = sorted(_TINYTEX_DIR.glob("bin/*/tlmgr"))
    return hits[0] if hits else None


def _tinytex_command(full=False):
    """User-local TeX Live (no root needed): https://yihui.org/tinytex/"""
    pkgs = " ".join(_TL_PACKAGES)
    tlmgr = _tinytex_tlmgr()
    if tlmgr is not None:  # already installed; re-running the installer would wipe it
        return f'"{tlmgr}" install {pkgs}'
    if shutil.which("curl"):
        fetch = f"curl -fsSL {_TINYTEX_URL}"
    elif shutil.which("wget"):
        fetch = f"wget -qO- {_TINYTEX_URL}"
    else:
        return "# Neither curl nor wget found; install TeX Live from https://tug.org/texlive/"
    installer = "TINYTEX_INSTALLER=TinyTeX-2 " if full else ""
    return f'{fetch} | {installer}sh && "{_TINYTEX_DIR}"/bin/*/tlmgr install {pkgs}'


def tex_install_command(full=False):
    """
    Return a shell command that installs a TeX setup good enough for activate_tex().
    full=True installs the complete distribution instead (several GB).

    Without root/sudo (clusters, Data Lab, JupyterHub) this installs TinyTeX into
    your home directory instead of using the system package manager.
    """
    system = platform.system()

    if system == "Darwin":
        if shutil.which("brew") is None:
            return _tinytex_command(full)
        if full:
            return "brew install --cask mactex-no-gui"
        return (
            "brew install --cask basictex && "
            'export PATH="/Library/TeX/texbin:$PATH" && '
            f"sudo tlmgr update --self && sudo tlmgr install {' '.join(_TL_PACKAGES)}"
        )

    if system == "Windows":
        # MiKTeX installs missing packages (cmbright etc.) on first use
        return "winget install -e --id MiKTeX.MiKTeX && winget install -e --id ArtifexSoftware.GhostScript"

    # Linux
    if _tinytex_tlmgr() is not None:
        return _tinytex_command(full)
    if _can_sudo():
        sudo = "" if os.geteuid() == 0 else "sudo "
        if shutil.which("apt-get"):
            pkgs = (
                "texlive-full"
                if full
                else "texlive-latex-extra texlive-fonts-extra cm-super dvipng fonts-cmu"
            )
            return f"{sudo}apt-get update && {sudo}apt-get install -y {pkgs}"
        if shutil.which("dnf"):
            pkgs = (
                "texlive-scheme-full"
                if full
                else (
                    "texlive-scheme-basic "
                    + " ".join(f"texlive-{p}" for p in _TL_PACKAGES)
                )
            )
            return f"{sudo}dnf install -y {pkgs}"
        if shutil.which("pacman"):
            return f"{sudo}pacman -S --needed texlive-latexextra texlive-fontsextra texlive-binextra"
    tlmgr = shutil.which("tlmgr")
    if tlmgr and os.access(Path(tlmgr).resolve().parent, os.W_OK):  # user-owned TeX Live
        return f"tlmgr install {' '.join(_TL_PACKAGES)}"
    return _tinytex_command(full)


def install_tex(run=False, full=False):
    """Print the TeX install command for this machine; with run=True, also execute it."""
    cmd = tex_install_command(full=full)
    print(cmd)
    if run:
        subprocess.run(cmd, shell=True, check=True)
        _add_user_tex_to_path()
    return cmd
