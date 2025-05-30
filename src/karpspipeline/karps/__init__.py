from typing import Iterable

from karpspipeline.common import create_output_dir
import karpspipeline.karps.install as backend_install
from karpspipeline.karps.models import KarpsConfig
from karpspipeline.models import Entry, PipelineConfig, FieldConfig
import karpspipeline.karps.export as backend_export

__all__ = ["export", "install"]


def _get_karps_config(config):
    return KarpsConfig.model_validate(config.export["karps"])


def export(
    config: PipelineConfig, resource_config: FieldConfig, entries: Iterable[Entry], fields: list[dict[str, str]]
):
    create_output_dir()
    karps_config = _get_karps_config(config)

    size = backend_export.create_karps_sql(config, resource_config, entries)
    backend_export.create_karps_backend_config(config, karps_config, resource_config, size, fields)


def install(pipeline_config: PipelineConfig):
    karps_config = _get_karps_config(pipeline_config)

    backend_install.add_to_db(pipeline_config.resource_id, karps_config)
    backend_install.add_config(pipeline_config, karps_config, pipeline_config.resource_id)
