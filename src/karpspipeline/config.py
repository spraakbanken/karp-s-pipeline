import copy
from dataclasses import dataclass
import logging
import os
from pathlib import Path
from typing import Iterator
from karpspipeline.common import Map
from karpspipeline.models import PipelineConfig
from karpspipeline.util import yaml

logger = logging.getLogger(__name__)


def _merge_configs(orig_parent_config: Map | None, child_config: Map) -> Map:
    """
    Overwrites main_config with values from resource_config
    """
    if not orig_parent_config:
        return child_config
    parent_config = copy.deepcopy(orig_parent_config)
    for key, value in child_config.items():
        main_val = parent_config.get(key)
        if value is None:
            continue
        elif main_val and isinstance(main_val, dict) and isinstance(value, dict):
            tmp = _merge_configs(main_val, value)
            parent_config[key] = tmp
        else:
            parent_config[key] = value
    return parent_config


@dataclass
class ConfigHandle:
    workdir: Path
    config_dict: Map


def load_config(config_handle) -> PipelineConfig:
    config_dict = config_handle.config_dict
    config_dict["workdir"] = config_handle.workdir
    return PipelineConfig.model_validate(config_dict)


def _find_configs() -> Iterator[ConfigHandle]:
    def read_config(dir_path: Path) -> Map | None:
        config_path = dir_path / "config.yaml"
        if config_path.exists():
            with open(config_path) as fp:
                logger.info(f"Reading {config_path}")
                return yaml.load(fp)
        return None

    start_path = Path(os.getcwd())
    parent_configs = []
    config = read_config(start_path)
    path = start_path
    # recusively find all parents until there is not config.yaml OR it contains root: true
    while config and not config.get("root", False):
        parent_configs.append(config)
        path = path / ".."
        config = read_config(path)
    if config:
        parent_configs.append(config)
    parent_configs = list(reversed(parent_configs))
    left = parent_configs[0]
    for right in reversed(parent_configs[1:]):
        left = _merge_configs(left, right)

    # now all parents configs have been merged, parent_config can still be None
    parent_config = left

    def find_children(path: Path, parent: Map | None):
        children = []
        for dir in path.iterdir():
            if dir.is_dir():
                config = read_config(dir)
                if config:
                    new_config = _merge_configs(parent, config)
                    new_children = find_children(dir, new_config)
                    if new_children:
                        children.extend(new_children)
                    else:
                        # insert workdir so we can find the correct place later
                        new_config["workdir"] = dir
                        children.append(new_config)
        return children

    children = find_children(start_path, parent_config)

    if children:
        for child in children:
            yield ConfigHandle(workdir=child["workdir"], config_dict=child)
    elif parent_config:
        yield ConfigHandle(workdir=start_path, config_dict=parent_config)


def find_configs() -> list[ConfigHandle]:
    return list(_find_configs())
