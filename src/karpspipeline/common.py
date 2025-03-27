from pathlib import Path

type Map = dict[str, object]


def create_output_dir() -> Path:
    return _create_dir("output")


def get_output_dir() -> Path:
    return Path("output")


def create_log_dir() -> Path:
    return _create_dir("log")


def _create_dir(dir: str) -> Path:
    path = Path(dir)
    path.mkdir(exist_ok=True)
    return path
