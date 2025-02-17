import glob
import json
import os
import sys
from typing import Iterator

import yaml

from karpspipeline.util.terminal import bold
from karpspipeline.models import (
    Entry,
    EntrySchema,
    PipelineConfig,
    Field,
    ResourceConfig,
)
from karpspipeline import karps


class ImportException(Exception):
    pass


type_lookup = {int: "integer", str: "text", bool: "bool"}


def check(key: str, field: Field, values: object) -> None:
    if bool(field.get("collection")) != isinstance(values, list):
        raise ImportException(f'Mismatch, field: "{key}"')
    if not isinstance(values, list):
        values = [values]
    field_type = field["type"]
    for value in values:
        expected_type_name = type_lookup[type(value)]
        if field_type != expected_type_name:
            raise ImportException(f'Mismatch, field: "{key}"')


def create_fields(entries: Iterator[Entry]) -> EntrySchema:
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
                    x.count(x[0]) == len(x)
                    typ = x[0]
                else:
                    typ = type(values)
                field["type"] = type_lookup[typ]
                print(f"Adding {key} = {json.dumps(field, ensure_ascii=False)}")
                schema[key] = field

    return schema, res


def validate_entry(fields: EntrySchema, entry: Entry) -> None:
    for key in entry:
        if key not in fields:
            raise ImportException(
                f'entry contains field: "{key}" that is not in config: "{json.dumps(entry)}"'
            )
        check(key, fields[key], entry[key])


def import_resource(resource_config: ResourceConfig | None) -> EntrySchema:
    """
    Checks that the source-files contain entries adhering to resource_config
    Moves the file to output/<resource_id>.jsonl
    If the file is already there, do nothing
    """
    files = glob.glob("source/*jsonl")
    if len(files) != 1:
        # we only support one input file
        print(f"pipeline supports {bold('one')} input file in source/")

    print(f"Reading source file: {files[0]}")
    with open(files[0]) as fp:

        def get_entries(fp) -> Iterator[Entry]:
            for line in fp:
                entry = json.loads(line)
                print("Processing: ", json.dumps(entry, ensure_ascii=False))
                yield entry

        entries = get_entries(fp)
        fields = resource_config.get("fields") if resource_config else None
        if fields:
            res = []
            for entry in entries:
                validate_entry(fields, entry)
                res.append(entry)
            return fields, res
        else:
            # if fields are omitted, generate schema from entries
            fields, res = create_fields(entries)
            return fields, res


def run(config: PipelineConfig) -> None:
    resource_config = config.resource_config
    try:
        entry_schema, entries = import_resource(resource_config)
        # TODO overwrites...
        config.resource_config = ResourceConfig(fields=entry_schema)
    except ImportException as e:
        print(str(e))
        sys.exit(1)
    print("Using entry schema: " + json.dumps(entry_schema, ensure_ascii=False))
    if "karps" in config.export:
        karps.export(config, entries)


def install(config: PipelineConfig) -> None:
    """
    TODO here we assume that the run is done
    """
    if "karps" in config:
        karps.install(config)


def main() -> None:
    """
    run - prepares the material
    install - adds the material to the requested system
        karps-pipeline install karps (karps-backend)
        karps-pipeline install sbx-repo (add resources to some repo, don't know where yet)
        karps-pipeline install sbx-metadata (add resources to some repo, don't know where yet)
        karps-pipeline install all (do all of the above)
    """
    os.system("")
    if len(sys.argv) != 2:
        print(f"{bold('Usage:')} karps-pipeline run")
        sys.exit(1)

    with open("config.yaml") as fp:
        config = PipelineConfig(**yaml.safe_load(fp))
        print("Reading config.yaml")

    if sys.argv[1] == "run":
        run(config)

    if sys.argv[1] == "install":
        install(config)

    print("done.")
