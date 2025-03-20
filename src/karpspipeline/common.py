from pathlib import Path

type Map = dict[str, object]


def create_output_dir():
    _create_dir("output")


def create_error_dir():
    _create_dir("error")


def _create_dir(dir: str) -> None:
    Path(dir).mkdir(exist_ok=True)
