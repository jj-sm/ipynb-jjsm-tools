import os

def create_dirs(data="data", output="graphics"):
    os.makedirs(data, exist_ok=True)
    os.makedirs(output, exist_ok=True)
