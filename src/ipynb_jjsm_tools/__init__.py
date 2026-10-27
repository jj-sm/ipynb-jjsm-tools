"""
ipynb_jjsm_tools
Custom toolkit for astronomical data analysis and plotting.
"""

__author__ = "Juan José Sánchez Medina"

import importlib

from . import setup
from . import download
from . import plot

__all__ = ["setup", "download", "plot", "photometry"]


def __getattr__(name):
    # `photometry` depends on astropy/photutils, which are only installed
    # via the `astro` extra. Import it lazily so the rest of the package
    # works in repos that don't have (or need) those astronomy deps.
    # (Using importlib directly, not `from . import photometry`, avoids
    # infinite recursion if that import itself raises an error.)
    if name == "photometry":
        module = importlib.import_module(f"{__name__}.photometry")
        globals()["photometry"] = module
        return module
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")