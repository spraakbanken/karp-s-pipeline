from datetime import datetime
import glob
import os
import shutil
import sys
import traceback
from typing import Iterator

import yaml

from karpspipeline.common import Map, create_log_dir, create_output_dir
from karpspipeline.util import json
from karpspipeline.util.terminal import bold
from karpspipeline.models import (
    Entry,
    EntrySchema,
    PipelineConfig,
    Field,
    FieldConfig,
)
from karpspipeline import karps

__all__ = ["main"]


class ImportException(Exception):
    pass


type_lookup: dict[type, str] = {int: "integer", str: "text", bool: "bool"}


def check(key: str, field: Field, values: object) -> None:
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
                schema[key] = Field.model_validate(field)

    return schema, res


def validate_entry(fields: EntrySchema, entry: Entry) -> None:
    for key in entry:
        if key not in fields:
            raise ImportException(f'entry contains field: "{key}" that is not in config: "{json.dumps(entry)}"')
        check(key, fields[key], entry[key])


def import_resource() -> tuple[EntrySchema, list[Entry]]:
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
                yield entry

        entries = get_entries(fp)

        # generate schema from entries
        fields, res = create_fields(entries)
        return fields, res


def run(config: PipelineConfig) -> None:
    entry_schema, entries = import_resource()
    resource_config = FieldConfig(fields=entry_schema)
    print("Using entry schema: " + json.dumps(entry_schema))
    if "karps" in config.export:
        karps.export(config, resource_config, entries)


def install(config: PipelineConfig) -> None:
    """
    TODO here we assume that the run is done
    """
    if "karps" in config.export:
        karps.install(config)


def clean() -> None:
    shutil.rmtree(create_log_dir())
    shutil.rmtree(create_output_dir())


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


def load_config() -> PipelineConfig:
    if main_config_path := os.getenv("KARPSPIPELINE_CONFIG"):
        with open(main_config_path) as fp:
            print(f"Reading main config from: {main_config_path}")
            main_config = yaml.safe_load(fp)
    else:
        main_config = {}
    with open("config.yaml") as fp:
        print("Reading config.yaml")
        resource_config = yaml.safe_load(fp)
        merged_config = _merge_configs(main_config, resource_config)
        config = PipelineConfig.model_validate(merged_config)
        return config


def main() -> int:
    os.system("")
    if len(sys.argv) != 2:
        print(f"{bold('Usage:')} karps-pipeline run/install")
        print()
        print(f"{bold('run')} - prepares the material")
        print(f"{bold('install')} - adds the material to the requested system")
        print(f"{bold('clean')} - remove genereated files")
        print()
        print("karps-pipeline install karps (karps-backend)")
        print("karps-pipeline install sbx-repo (add resources to some repo, don't know where yet)")
        print("karps-pipeline install sbx-metadata (add resources to some repo, don't know where yet)")
        print("karps-pipeline install all (do all of the above)")
        print()
        print(
            "Set environment variable KARPSPIPELINE_CONFIG to a config.yaml which will be merged with the project ones"
        )
        return 1

    try:
        if sys.argv[1] == "clean":
            clean()
        else:
            config = load_config()

            if sys.argv[1] == "run":
                run(config)

            if sys.argv[1] == "install":
                install(config)

    except Exception:
        print("error.")
        log_dir = create_log_dir()
        err_file = log_dir / (f"err_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.log")
        with open(err_file, "w") as f:
            traceback.print_exc(file=f)
        return 1

    print("done.")
    return 0
