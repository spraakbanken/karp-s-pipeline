from typing import Callable

from karpspipeline.common import create_output_dir
import karpspipeline.karps.install as backend_install
from karpspipeline.karps.models import KarpsConfig
from karpspipeline.models import ConfiguredField, Entry, EntrySchema, InferredField, PipelineConfig
import karpspipeline.karps.export as backend_export

__all__ = ["export", "install"]


def _get_karps_config(config):
    return KarpsConfig.model_validate(config.modules["karps"])


def _compare_to_current_fields(config: PipelineConfig, entry_schema: EntrySchema) -> list[dict[str, str]]:
    """
    Looks in the main config file for presets about the fields, mainly label but could also be tagset
    """

    def to_dict(elems: list[ConfiguredField]) -> dict[str, ConfiguredField]:
        return {elem.name: elem for elem in elems}

    main_fields: dict[str, ConfiguredField] = to_dict(config.fields)
    new_fields = []
    for key, field in entry_schema.items():
        field: InferredField
        if key in main_fields:
            main_field = main_fields[key]
            # TODO other settings, like values for enums are not taken into account
            if main_field.collection != field.collection or main_field.type != field.type:
                raise ImportError(
                    f"{key} is configured, but it is not the same as in this resource, must rename or add alias."
                )
            new_fields.append(main_field.model_dump(exclude_unset=True))
        else:
            new_field = field.model_dump(exclude_unset=True)
            new_field["name"] = key
            new_fields.append(new_field)
    return new_fields


def export(
    config: PipelineConfig,
    entry_schema: EntrySchema,
    source_order: list[str],
    size: int,
) -> list[Callable[[Entry], Entry]]:
    fields: list[dict[str, str]] = _compare_to_current_fields(config, entry_schema)
    create_output_dir(config.workdir)
    karps_config = _get_karps_config(config)

    # sql_gen is a coroutine for creating the SQL file for backend
    sql_gen = backend_export.create_karps_sql(config, karps_config, entry_schema)
    backend_export.create_karps_backend_config(config, karps_config, entry_schema, source_order, size, fields)

    next(sql_gen)

    def task(entry: Entry) -> Entry:
        sql_gen.send(entry)
        return entry

    return [task]


def install(pipeline_config: PipelineConfig):
    karps_config = _get_karps_config(pipeline_config)

    backend_install.add_to_db(pipeline_config, karps_config)
    backend_install.add_config(pipeline_config, karps_config, pipeline_config.resource_id)
