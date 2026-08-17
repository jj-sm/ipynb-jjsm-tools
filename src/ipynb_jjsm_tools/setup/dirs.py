import os
import sys
from pathlib import Path

def create_dirs(path, data="data", output="out", cache="cache"):
    path = Path(path)
    data = path / data
    cache = path / cache
    output = path / output
    notebooks = path / "notebooks"
    os.makedirs(data, exist_ok=True)
    os.makedirs(cache, exist_ok=True)
    os.makedirs(output, exist_ok=True)
    os.makedirs(notebooks, exist_ok=True)

def add_project_root(start=None, marker=".root_ident", verbose=True, chdir=True):
    """
    Walk up from `start` looking for `marker`, prepend the first
    matching directory to `sys.path`, and (by default) `os.chdir()`
    into it — so both imports and relative paths (`Path("data")`,
    `create_dirs(".")`, ...) work the same way regardless of which
    subfolder the notebook itself lives in.

    Parameters
    ----------
    start : str or Path, optional
        Directory to start the search from. Defaults to the current
        working directory (`os.getcwd()`) — which for a Jupyter kernel
        is *not always* the notebook's own folder, and isn't
        guaranteed to be inside your project tree at all (e.g. a
        shared course JupyterHub often starts kernels in your home
        directory, one level above every course repo you have
        cloned). If `add_project_root()` keeps returning None, check
        the printed trace below (or pass `start` explicitly to point
        straight at your repo, e.g.
        `add_project_root(start="~/AST221-1/UC-AST221")`).
    marker : str
        Filename to look for in each candidate directory.
    verbose : bool
        If True (default), print every directory that was actually
        checked (and whether `marker` was present in it), plus the
        kernel's real cwd — so a cwd mismatch, a typo'd marker, or a
        broken symlink is easy to spot instead of failing silently.
        Also confirms when the cwd is changed.
    chdir : bool
        If True (default) and `marker` is found, also change the
        process's working directory to that folder. Set to False to
        only prepend to `sys.path` without touching the cwd.

    Returns
    -------
    Path to the first directory (at or above `start`) containing
    `marker` — or None if `marker` isn't found anywhere between
    `start` and the filesystem root.
    """
    kernel_cwd = Path(os.getcwd()).resolve()
    start = Path(start).resolve() if start is not None else kernel_cwd

    candidates = [start, *start.parents]
    trace = []
    for parent in candidates:
        candidate = parent / marker

        present = os.path.lexists(candidate)
        trace.append((parent, present))
        if present and candidate.exists():
            if str(parent) not in sys.path:
                sys.path.insert(0, str(parent))
            if chdir and Path(os.getcwd()).resolve() != parent:
                os.chdir(parent)
                if verbose:
                    print(f">> add_project_root: found '{marker}' at {parent} — cwd changed to it.")
            elif verbose:
                print(f">> add_project_root: found '{marker}' at {parent}.")
            return parent

    if verbose:
        print(f">> add_project_root: no usable '{marker}' found. Checked, from {start}:")
        for parent, present in trace:
            flag = "found (broken symlink?)" if present else ""
            print(f"   [{'x' if present else ' '}] {parent}  {flag}")
        print(f">> kernel cwd (os.getcwd()): {kernel_cwd}")
        if start != kernel_cwd:
            print(f">> search actually started from (explicit `start`): {start}")
        print(
            ">> If a directory above is marked 'found (broken symlink?)', the file "
            "exists as a name but doesn't resolve — check with `ls -la` in that "
            "folder. Otherwise, none of the checked directories contain the "
            "marker at all — pass add_project_root(start='/path/to/your/repo') "
            "to point directly at the folder that has it."
        )

    return None