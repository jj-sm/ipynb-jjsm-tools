import os
import sys
from pathlib import Path

def create_dirs(path, data="data", output="graphics"):
    path = Path(path)
    data = path / data
    output = path / output
    os.makedirs(data, exist_ok=True)
    os.makedirs(output, exist_ok=True)

def add_project_root():
    current = Path(os.getcwd()).resolve()

    for parent in [current, *current.parents]:
        if (parent / ".root_ident").exists():
            if str(parent) not in sys.path:
                sys.path.insert(0, str(parent))
            return parent