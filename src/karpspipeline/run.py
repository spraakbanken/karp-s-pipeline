import csv
import glob
from collections.abc import Iterator
from typing import cast

from karpspipeline import karps
from karpspipeline import csvmetadata
from karpspipeline.common import ImportException
from karpspipeline.util import json
from karpspipeline.util.terminal import bold
from karpspipeline.models import (
    ConfiguredField,
    Entry,
    EntrySchema,
    PipelineConfig,
    InferredField,
    FieldConfig,
)


type_lookup: dict[type, str] = {int: "integer", str: "text", bool: "bool", float: "float"}


def run(config: PipelineConfig, subcommand: str = "all") -> None:
    entry_schema, entries = import_resource(config)
    field_config = FieldConfig(fields=entry_schema)
    fields = compare_to_current_fields(config, field_config)
    print("Using entry schema: " + json.dumps(entry_schema))
    run_all = False
    if subcommand == "all":
        run_all = True
    cmd_found = False
    if run_all or (subcommand == "karps" and "karps" in config.export):
        karps.export(config, field_config, entries, fields)
        cmd_found = True
    if run_all or subcommand == "csvmetadata":
        csvmetadata.export(config, field_config, entries, fields)
        cmd_found = True

    if not cmd_found:
        raise ImportException(f"Subcommand '{subcommand}' not available.")


def check(key: str, field: InferredField, values: object) -> None:
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


def create_fields(entries: Iterator[Entry]) -> tuple[EntrySchema, list[Entry]]:
    schema = {}
    res = []
    for entry in entries:
        res.append(entry)
        for key in entry:
            values = entry[key]
            if key in schema:
                check(key, schema[key], values)
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


def validate_entry(fields: EntrySchema, entry: Entry) -> None:
    for key in entry:
        if key not in fields:
            raise ImportException(f'entry contains field: "{key}" that is not in config: "{json.dumps(entry)}"')
        check(key, fields[key], entry[key])


def import_resource(pipeline_config: PipelineConfig) -> tuple[EntrySchema, list[Entry]]:
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

    csv_files = glob.glob("source/*csv")
    tsv_files = glob.glob("source/*tsv")
    if csv_files or tsv_files:
        fp = open((csv_files + tsv_files)[0], encoding="utf-8-sig")
        if csv_files:
            reader = csv.reader(fp)
        else:
            reader = csv.reader(fp, dialect="excel-tab")
        headers: list[str] = next(reader, None) or []
        import_settings = cast(dict[str, dict[str, list[dict[str, str]]]], pipeline_config.import_settings)
        # type information for parsing values
        cast_fields: list[dict[str, str]] = import_settings["csv"]["cast_fields"]

        def get_entries() -> Iterator[Entry]:
            for row in reader:
                entry: dict[str, str | int | float] = dict(zip(headers, row))
                # parse values
                for field in cast_fields:
                    if field["type"] == "int":
                        entry[field["name"]] = int(entry[field["name"]])
                    elif field["type"] == "float":
                        entry[field["name"]] = float(entry[field["name"]])
                    else:
                        raise RuntimeError(f"Uknown type: {field['type']}, given in CSV import")
                yield entry
            fp.close()

        entries = get_entries()
    else:
        jsonl_files = glob.glob("source/*jsonl")
        fp = open(jsonl_files[0])

        def get_entries() -> Iterator[Entry]:
            for line in fp:
                entry = json.loads(line)
                yield entry
            fp.close()

        entries = get_entries()

    # generate schema from entries
    fields, res = create_fields(entries)
    return fields, res


def compare_to_current_fields(config: PipelineConfig, field_config: FieldConfig) -> list[dict[str, str]]:
    """
    Looks in the main config file for presets about this field, mainly label but could also be tagset
    """

    def to_dict(elems: list[ConfiguredField]) -> dict[str, ConfiguredField]:
        return {elem.name: elem for elem in elems}

    main_fields: dict[str, ConfiguredField] = to_dict(config.fields)
    new_fields = []
    for key, field in field_config.fields.items():
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
