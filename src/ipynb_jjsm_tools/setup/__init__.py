from . import dirs
from .dirs import create_dirs, add_project_root, set_project_root
from .plot import (
    activate_tex,
    size,
    set_font_sizes,
    cm_to_in,
    ensure_cmu_fonts,
    disable_tex_in_figure,
    install_tex,
    tex_install_command,
    LaTeXFallbackWarning,
)
from .logs import info_log

__all__ = [
    "dirs",
    "create_dirs",
    "add_project_root",
    "set_project_root",
    "activate_tex",
    "size",
    "set_font_sizes",
    "cm_to_in",
    "ensure_cmu_fonts",
    "disable_tex_in_figure",
    "install_tex",
    "tex_install_command",
    "LaTeXFallbackWarning",
    "info_log",
]
