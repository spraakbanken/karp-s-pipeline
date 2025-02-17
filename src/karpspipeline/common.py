from pathlib import Path


def create_output_dir():
    Path("output").mkdir(exist_ok=True)
