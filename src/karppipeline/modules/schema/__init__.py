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
type_lookup: dict[type, str] = {int: "integer", str: "text", bool: "bool", float: "float"}


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
    reads source file and generates a schema, return (source order, size of resource, schema)
    source order is roughly the order that fields occur in source file
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


def _create_fields(entries: Iterator[Entry]) -> EntrySchema:
    """
    Goes through the entries and each key in the entries and populates schema
    """
    schema = {}
    for idx, entry in enumerate(entries):
        for key in entry:
            values = entry[key]
            try:
                _check_or_create_field(schema, key, values)
            except ImportException as e:
                raise ImportException(f"Error for entry on row: {idx + 1}: " + e.args[0])
    return schema


def _check_or_create_field(schema, key, values):
    """
    Called for each key and value in each entry

    For unknown fields, initializes the field, for known fields, check that the given values
    match the field.
    """
    field = schema.get(key)
    collection = False
    if not isinstance(values, list):
        values = (values,)
    elif field and not field.collection:
        raise ImportException(f'Mismatch, field: "{key}"')
    else:
        collection = True
    for value in values:
        if not isinstance(value, dict):
            value = ((key, value, field),)
        elif field and not field.type == "table":
            raise ImportException(f'Mismatch, field: "{key}"')
        else:
            # sub-fields do not have collection: true although they could be seen as such...
            collection = False
            if not field:
                # first time this table field is found
                fields = {}
                field = InferredField(type="table", collection=True, name=key, fields=fields)
                schema[key] = field

            # use fields from the parent field as schema, will add sub-fields to the correct level
            schema = field.fields
            value = [(key, val, schema.get(key)) for (key, val) in value.items()]

        for inner_key, inner_value, inner_field in value:
            if inner_value is None:
                break
            if isinstance(inner_value, list) or isinstance(inner_value, dict):
                raise ImportException("Level of nesting not allowed.")
            if inner_field:
                _check_type(inner_key, inner_field, inner_value)
            else:
                # not previously seen field, initializes type and name
                inner_field = InferredField(type=type_lookup[type(inner_value)], name=inner_key)
                inner_field.collection = collection
                schema[inner_key] = inner_field

            if inner_field and inner_field.type == "text":
                _add_max_length(inner_field, inner_value)


def _check_type(key: str, field: InferredField, value: str | float | int | bool) -> None:
    field_type = field.type
    actual_type_name = type_lookup[type(value)]
    # it is fine to first infer float and then seeing integer values
    if not (actual_type_name == "integer" and field_type == "float") and field_type != actual_type_name:
        raise ImportException(f'Mismatch, field: "{key}". Was {actual_type_name}, expected {field_type}.')


def _add_max_length(field: InferredField, value: str):
    """
    Sets or update the longest value seen for this field, only works for text fields
    """
    field.extra["length"] = max(cast(int, field.extra.get("length", 0)), len(value))


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
                entry_schema[field.target] = InferredField(name=field.target, type="str")
            else:
                # TODO here we copy the schema from source field, but length may be different
                field_copy = entry_schema[field.name].copy()
                field_copy.name = field.target
                entry_schema[field.target] = field_copy
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
