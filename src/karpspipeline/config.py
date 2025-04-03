from karpspipeline.common import Map
from karpspipeline.models import PipelineConfig
from karpspipeline.util import yaml


def _merge_configs(parent_config: Map, child_config: Map) -> Map:
    """
    Overwrites main_config with values from resource_config
    """
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


def load_config(main_config_path: str | None) -> PipelineConfig:
    if main_config_path:
        with open(main_config_path) as fp:
            print(f"Reading main config from: {main_config_path}")
            main_config = yaml.load(fp)
    else:
        main_config = {}
    with open("config.yaml") as fp:
        print("Reading config.yaml")
        resource_config = yaml.load(fp)
        merged_config = _merge_configs(main_config, resource_config)
        config = PipelineConfig.model_validate(merged_config)
        return config
