from typing import Iterable

from karpspipeline.common import create_output_dir
from karpspipeline.karps.install import add_config, add_to_db
from karpspipeline.karps.models import KarpsConfig
from karpspipeline.models import Entry, PipelineConfig, FieldConfig
from karpspipeline.karps.export import create_karps_backend_config, create_karps_sql

__all__ = ["export", "install"]


def _get_karps_config(config):
    return KarpsConfig.model_validate(config.export["karps"])


def export(config: PipelineConfig, resource_config: FieldConfig, entries: Iterable[Entry]):
    create_output_dir()
    karps_config = _get_karps_config(config)

    size = create_karps_sql(config, resource_config, entries)
    create_karps_backend_config(config, karps_config, resource_config, size)


def install(pipeline_config: PipelineConfig):
    karps_config = _get_karps_config(pipeline_config)

    add_to_db(pipeline_config.resource_id, karps_config)
    add_config(karps_config, pipeline_config.resource_id)
