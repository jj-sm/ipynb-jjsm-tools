"""
Post-install setup for ipynb-jjsm-tools.

Run after `pip install ipynb-jjsm-tools`:

    jjsm-setup                      # install LaTeX only (system-wide)
    jjsm-setup --extra lab          # add an optional-dependency group to the current env, then LaTeX
    jjsm-setup --venv               # create ./.venv with uv, install the package there, then LaTeX
    jjsm-setup --venv myenv --extra full
    jjsm-setup --no-tex             # skip the LaTeX step entirely
"""
import argparse
import shutil
import subprocess
import sys
from pathlib import Path

PACKAGE_NAME = "ipynb-jjsm-tools"
PACKAGE_SOURCE = "git+https://github.com/jj-sm/ipynb-jjsm-tools.git"  # not on PyPI
VALID_EXTRAS = ("none", "notebook", "lab", "full")


def _venv_python(venv_dir: Path) -> Path:
    if sys.platform == "win32":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _require_uv() -> None:
    if shutil.which("uv") is None:
        sys.exit(
            "uv not found. Install it first: "
            "https://docs.astral.sh/uv/getting-started/installation/"
        )


def create_venv(venv_dir: Path) -> Path:
    _require_uv()
    python = _venv_python(venv_dir)
    if python.exists():
        print(f"==> Reusing existing venv at {venv_dir}")
        return python
    print(f"==> Creating uv venv at {venv_dir}")
    subprocess.run(["uv", "venv", str(venv_dir)], check=True)
    return python


def install_package(python: Path, extra: str) -> None:
    name = PACKAGE_NAME if extra == "none" else f"{PACKAGE_NAME}[{extra}]"
    spec = f"{name} @ {PACKAGE_SOURCE}"
    print(f"==> Installing {spec}")
    if shutil.which("uv") is not None:
        cmd = ["uv", "pip", "install", "--python", str(python), spec]
    else:
        cmd = [str(python), "-m", "pip", "install", spec]
    subprocess.run(cmd, check=True)


def install_latex() -> None:
    # TeX is installed system-wide, so it doesn't matter which env runs this
    from .plot import install_tex

    print("==> Installing LaTeX")
    try:
        install_tex(run=True)
    except subprocess.CalledProcessError:
        print(
            "LaTeX install failed or needed sudo you don't have.\n"
            "On a shared/managed system (clusters, Data Lab, JupyterHub), try instead:\n"
            "    conda install -c conda-forge texlive-core cm-super\n"
            "This needs no root and installs into your own conda env."
        )


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--venv", nargs="?", const=".venv", default=None, metavar="DIR",
        help="create a uv venv (default name .venv if DIR omitted) and install into it; "
             "if not given, installs into the current environment instead",
    )
    parser.add_argument(
        "--extra", choices=VALID_EXTRAS, default="none",
        help="optional-dependency group to install alongside the package (default: none)",
    )
    parser.add_argument(
        "--no-tex", action="store_true", help="skip the LaTeX install step",
    )
    args = parser.parse_args(argv)

    venv_python = None
    if args.venv is not None:
        venv_python = create_venv(Path(args.venv))
        install_package(venv_python, args.extra)
    elif args.extra != "none":
        # The package is already installed here (this command comes from it); only add extras
        install_package(Path(sys.executable), args.extra)

    if not args.no_tex:
        install_latex()
    else:
        print("==> Skipping LaTeX install (--no-tex)")

    if venv_python is not None:
        if sys.platform != "win32":
            print(f"==> Done. Activate with: source {venv_python.parent / 'activate'}")
        else:
            print(f"==> Done. Activate with: {venv_python.parent / 'activate.bat'}")
    else:
        print("==> Done.")


if __name__ == "__main__":
    main()
