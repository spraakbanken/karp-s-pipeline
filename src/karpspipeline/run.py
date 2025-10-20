import glob
from collections.abc import Iterator
import importlib
from typing import Any, Callable, cast

from karpspipeline import karps, sbxrepo

from karpspipeline.common import ImportException, create_output_dir
from karpspipeline.read import read_data
from karpspipeline.util import json
from karpspipeline.util.terminal import bold
from karpspipeline.models import Entry, EntrySchema, PipelineConfig, InferredField


type_lookup: dict[type, str] = {int: "integer", str: "text", bool: "bool", float: "float"}


def run(config: PipelineConfig, subcommand: str = "all") -> None:
    # pre-import tasks, invoke conversions to know which fields *will* be there
    # TODO collect number of items
    entry_schema, source_order, [size] = _pre_import_resource(config)

    # generator for entries
    entries = _import_resource(config, entry_schema, source_order)

    # callables added to tasks will be called for each entry
    tasks: list[Callable[[Entry], Entry]] = []

    # modifies entry_schema based on config and returns modification task for entries
    entry_converter = get_entry_converter(config, entry_schema)
    # add task to include, exclude, rename or update fields in enries (based on export.fields)
    tasks.append(entry_converter)

    print("Using entry schema: " + json.dumps(entry_schema))

    run_all = False
    if subcommand == "all":
        run_all = True
    cmd_found = False

    if run_all or subcommand == "dump":

        def json_dump():
            with open(create_output_dir() / f"{config.resource_id}.jsonl", "w") as fp:
                while True:
                    entry = yield
                    if not entry:
                        break
                    fp.write(json.dumps(entry) + "\n")

        gen = json_dump()
        next(gen)

        def task(entry: Entry, /) -> Entry:
            gen.send(entry)
            return entry

        # add task for dumping data as jsonl (with all modifications)
        tasks.append(task)
        cmd_found = True

    if (run_all and "karps" in config.export.default) or subcommand == "karps":
        new_tasks = karps.export(config, entry_schema, source_order, size)
        tasks.extend(new_tasks)
        cmd_found = True
    if (run_all and "sbxrepo" in config.export.default) or subcommand == "sbxrepo":
        sbxrepo.export(config, size)
        cmd_found = True

    # for each entry, do the needed tasks
    for entry in entries:
        updated_entry = entry
        for task in tasks:
            updated_entry = task(updated_entry)

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


def _create_fields(entries: Iterator[Entry]) -> EntrySchema:
    def _add_max_length(field: InferredField):
        if field.type == "text":
            if field.collection:
                if values:
                    field_maxlen = max((len(value) for value in values))
                else:
                    field_maxlen = 0
            else:
                field_maxlen = len(cast(str, values))
            field.extra["length"] = max(cast(int, field.extra.get("length", 0)), field_maxlen)

    schema = {}
    for entry in entries:
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
            _add_max_length(schema[key])
    return schema


def _pre_import_resource(pipeline_config: PipelineConfig) -> tuple[EntrySchema, list[str], list[int]]:
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

    source_order, size, entries = read_data(pipeline_config)

    # generate schema from entries - _create_field will exaust the generator and make size updated
    fields = _create_fields(entries)
    return (fields, source_order, size)


def _import_resource(
    pipeline_config: PipelineConfig, entry_schema: EntrySchema, source_order: list[str]
) -> Iterator[Entry]:
    _, _, entries = read_data(pipeline_config)
    return entries


def get_entry_converter(config: PipelineConfig, entry_schema: EntrySchema) -> Callable[[Entry], Entry]:
    """
    Check if config contains any renames or conversions
    Update the entry schema and each entry with this information
    """

    def _get_converter(converter: str) -> dict[str, Callable[[object], object]]:
        [module, func] = converter.split(".")
        mod = importlib.import_module("karpspipeline.converters." + module)
        func_obj = getattr(mod, func)
        update_schema = getattr(mod, func + "_update_schema")
        return {"update_schema": update_schema, "convert": func_obj}

    add_all = False
    converted_fields = []
    converters = {}
    if len(config.export.fields) == 0:
        # add all the fields from the source to the target if there are no field settings
        add_all = True
    for field in config.export.fields:
        if field.root == "...":
            add_all = True
        else:
            converted_fields.append(field)
    if not add_all:
        entry_schema.clear()
    for field in converted_fields:
        if field.exclude:
            entry_schema.pop(field.name, None)
        else:
            # TODO here we copy the schema from source field, but length may be different
            entry_schema[field.target] = entry_schema[field.name].model_copy(deep=True)
        # pre-import each converter
        if field.converter:
            converters[field.converter] = _get_converter(field.converter)
            converters[field.converter]["update_schema"](entry_schema[field.target])

    def _convert_value(converter: str | None, val: Any) -> Any:
        if converter:
            return converters[converter]["convert"](val)
        return val

    def convert(entry: Entry) -> Entry:
        new_entry = {}
        for key in entry_schema.keys():
            if key in entry:
                new_entry[key] = entry[key]

        for field in converted_fields:
            if not field.exclude:
                val = entry[field.name]
                new_entry[field.target] = _convert_value(field.converter, val)

        return new_entry

    return convert
