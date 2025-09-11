import glob
from collections.abc import Iterator
import importlib
from typing import Any

from karpspipeline import karps
from karpspipeline.common import ImportException
from karpspipeline.read import read_data
from karpspipeline.util import json
from karpspipeline.util.terminal import bold
from karpspipeline.models import (
    ConfiguredField,
    Entry,
    EntrySchema,
    PipelineConfig,
    InferredField,
)


type_lookup: dict[type, str] = {int: "integer", str: "text", bool: "bool", float: "float"}


def run(config: PipelineConfig, subcommand: str = "all") -> None:
    # import the resource
    entry_schema, source_order, entries = _import_resource(config)
    # augument data with for example UD-tags
    entries = list(_convert_entries(config, entry_schema, iter(entries)))
    fields = _compare_to_current_fields(config, entry_schema)
    run_all = False
    if subcommand == "all":
        run_all = True
    cmd_found = False
    print("Using entry schema: " + json.dumps(entry_schema))

    if run_all or (subcommand == "karps" and "karps" in config.export):
        karps.export(config, entry_schema, source_order, entries, fields)
        cmd_found = True

    if not cmd_found:
        raise ImportException(f"Subcommand '{subcommand}' not available.")


def _check(key: str, field: InferredField, values: object) -> None:
    if values is None:
        return
    if bool(field.collection) != isinstance(values, list):
        raise ImportException(f'Mismatch, field: "{key}"')
    if not isinstance(values, list):
        values = [values]
    field_type = field.type
    for value in values:
        expected_type_name = type_lookup[type(value)]
        if field_type != expected_type_name:
            raise ImportException(f'Mismatch, field: "{key}"')


def _create_fields(entries: Iterator[Entry]) -> tuple[EntrySchema, list[Entry]]:
    schema = {}
    res = []
    for entry in entries:
        res.append(entry)
        for key in entry:
            values = entry[key]
            if key in schema:
                _check(key, schema[key], values)
            else:
                # not previously seen field
                field = {}
                if isinstance(values, list):
                    field["collection"] = True
                    # check that all values have the same type by using type and counting
                    x = [type(value) for value in values]
                    if x.count(x[0]) != len(x):
                        raise ImportException("Not all values in collection have the same type")
                    typ = x[0]
                else:
                    typ = type(values)
                if values is None:
                    # defer type inference until a concrete value occurs
                    continue
                field["type"] = type_lookup[typ]
                print(f"Adding {key} = {json.dumps(field)}")
                schema[key] = InferredField.model_validate(field)

    return schema, res


def _import_resource(pipeline_config: PipelineConfig) -> tuple[EntrySchema, list[str], list[Entry]]:
    """
    Checks that the source-files contain entries adhering to resource_config
    Moves the file to output/<resource_id>.jsonl
    If the file is already there, do nothing
    """
    files = glob.glob("source/*")
    if len(files) != 1:
        # we only support one input file
        print(f"pipeline supports {bold('one')} input file in source/")
    else:
        print(f"Reading source file: {files[0]}")

    source_order, entries = read_data(pipeline_config)

    # generate schema from entries
    fields, res = _create_fields(entries)
    return fields, source_order, res


def _compare_to_current_fields(config: PipelineConfig, entry_schema: EntrySchema) -> list[dict[str, str]]:
    """
    Looks in the main config file for presets about this field, mainly label but could also be tagset
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


def _convert_entries(config: PipelineConfig, entry_schema: EntrySchema, entries: Iterator[Entry]) -> Iterator[Entry]:
    """
    Check if config contains any renames or conversions
    Update the entry schema and each entry with this information
    """

    def _convert_value(converter: str | None, val: Any) -> Any:
        if converter:
            [module, func] = converter.split(".")
            mod = importlib.import_module("karpspipeline.converters." + module)
            func_obj = getattr(mod, func)
            return func_obj(val)
        return val

    add_all = False
    converted_fields = []
    if len(config.export.fields) == 0:
        # add all the fields from the source to the target if there are no field settings
        add_all = True
    for field in config.export.fields:
        if field.root == "...":
            add_all = True
        else:
            converted_fields.append(field)

    for field in converted_fields:
        if field.exclude:
            entry_schema.pop(field.target, None)
        else:
            entry_schema[field.target] = entry_schema[field.name]

    for entry in entries:
        if add_all:
            new_entry = dict(entry)
        else:
            new_entry = {}
        for field in converted_fields:
            if field.exclude:
                new_entry.pop(field.name, None)
            else:
                val = entry[field.name]
                new_entry[field.target] = _convert_value(field.converter, val)

        yield new_entry
