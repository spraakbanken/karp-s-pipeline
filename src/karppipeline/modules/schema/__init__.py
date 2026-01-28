import importlib
import logging
from pathlib import Path
import pickle
from typing import Any, Iterator, cast, Callable
import unicodedata
from karppipeline.common import ImportException, create_output_dir
from karppipeline.models import EntrySchema, PipelineConfig, Entry, InferredField
from karppipeline.util import json
from karppipeline.util.terminal import bold
from karppipeline.read import read_data

logger = logging.getLogger(__name__)

__all__ = ["export", "dependencies"]


# generate schema, source_order and size, TODO sbxmetadata should be an optional dependency
dependencies = ["sbxmetadata"]


def export(config, _):
    """
    Loads the input data and deduces schema, source order and size.

    Returns the task for doing all field conversions.
    """
    # pre-import tasks, invoke conversions to know which fields *will* be there
    entry_schema, source_order, [size] = _pre_import_resource(config)

    # modifies entry_schema based on config and returns modification task for entries
    entry_converter = _get_entry_converter(config, entry_schema)

    logger.info("Using entry schema: " + json.dumps(entry_schema))

    with open(_get_data_path(config), "wb") as fp:
        pickle.dump({"entry_schema": entry_schema, "source_order": source_order, "size": size}, fp)

    # return task to include, exclude, rename or update fields in enries (based on export.fields)
    return (entry_converter,)


def load(config) -> dict[str, object]:
    with open(_get_data_path(config), "rb") as fp:
        return pickle.load(fp)


def _get_data_path(config) -> Path:
    module_dir = create_output_dir(config.workdir) / "schema"
    module_dir.mkdir(exist_ok=True)
    return module_dir / "schema.pickle"


def _pre_import_resource(pipeline_config: PipelineConfig) -> tuple[EntrySchema, list[str], list[int]]:
    """
    Checks that the source-files contain entries adhering to resource_config
    Moves the file to output/<resource_id>.jsonl
    If the file is already there, do nothing
    """
    files = list(pipeline_config.workdir.glob("source/*"))
    if len(files) != 1:
        # we only support one input file
        logger.warning(f"pipeline supports {bold('one')} input file in source/")
    else:
        logger.info(f"Reading source file: {files[0]}")

    source_order, size, entries = read_data(pipeline_config)

    # generate schema from entries - _create_field will exaust the generator and make size updated
    fields = _create_fields(entries)
    return (fields, source_order, size)


type_lookup: dict[type, str] = {int: "integer", str: "text", bool: "bool", float: "float", dict: "object"}


def _check(key: str, field: InferredField, values: object) -> None:
    if values is None:
        return
    if bool(field.collection) != isinstance(values, list):
        raise ImportException(f'Mismatch, field: "{key}"')
    if not isinstance(values, list):
        values = [values]
    field_type = field.type
    for value in values:
        actual_type_name = type_lookup[type(value)]

        # it is fine to first infer float and then seeing integer values
        if not (actual_type_name == "integer" and field_type == "float") and field_type != actual_type_name:
            raise ImportException(f'Mismatch, field: "{key}". Was {actual_type_name}, expected {field_type}.')


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
                logger.debug(f"Adding {key} = {json.dumps(field)}")
                schema[key] = InferredField(**field)
            _add_max_length(schema[key])
    return schema


def _get_entry_converter(config: PipelineConfig, entry_schema: EntrySchema) -> Callable[[Entry], Entry]:
    """
    Check if config contains any renames or conversions
    Update the entry schema and each entry with this information
    """

    def _get_converter(converter: str) -> dict[str, Callable[[object], object]]:
        [module, func] = converter.split(".")
        mod = importlib.import_module("karppipeline.converters." + module)
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
            if field.name == "*":
                # the converter must set the schema, placeholder value
                entry_schema[field.target] = InferredField(type="str")
            else:
                # TODO here we copy the schema from source field, but length may be different
                entry_schema[field.target] = entry_schema[field.name].copy()
        # pre-import each converter
        if field.converter:
            converters[field.converter] = _get_converter(field.converter)
            entry_schema[field.target] = converters[field.converter]["update_schema"](entry_schema[field.target])

    def _convert_value(converter: str | None, val: Any) -> Any:
        if converter:
            return converters[converter]["convert"](config.resource_id, val)
        return val

    def convert(entry: Entry) -> Entry:
        logger.debug("schema entry task")
        new_entry = {}

        # initialize data
        for key in entry_schema.keys():
            if key in entry:
                new_entry[key] = entry[key]

        # convert or rename fields
        for field in converted_fields:
            if not field.exclude:
                if field.name == "*":
                    new_entry[field.target] = _convert_value(field.converter, entry)
                else:
                    val = entry[field.name]
                    new_entry[field.target] = _convert_value(field.converter, val)

        # clean up all text fields
        for key in entry_schema.keys():
            if entry_schema[key].type == "text" and key in new_entry:
                if not entry_schema[key].collection:
                    new_entry[key] = _clean_text(new_entry[key])
                else:
                    # this also causes all None to be []
                    new_entry[key] = [_clean_text(text) for text in new_entry[key] or []]

        return new_entry

    return convert


def _clean_text(text: str) -> str:
    """
    Removes control characters, formatting characters, unassigned characters and makes all spaces into "normal" space
    """

    def inner(text) -> Iterator[str]:
        for c in text:
            cat = unicodedata.category(c)
            if c == "\n":
                yield c
            # remove all control characters (Cc), formatting characters (Cf), unassigned characters(Cn)
            elif cat not in {"Cc", "Cf", "Cn"}:
                if cat == "Zs":
                    # normalize space separators
                    yield " "
                else:
                    yield c

    return "".join(inner(text))
