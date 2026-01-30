import csv
import logging
from typing import Iterator, cast

from karppipeline.models import Entry, PipelineConfig
from karppipeline.util import json
from karppipeline.util.terminal import bold

logger = logging.getLogger(__name__)


def _update_json_source_order(source_order: list[str], new_keys: list[str]) -> list[str]:
    """
    Tries to merge two lists so that the order of original list is preserved, while new
    elements are added in between in appropriate places. If the order is conflicting
    we don't really care what happens, order should be hard coded in cofig for those cases.
    """
    source_place = 0
    for i, key in enumerate(new_keys):
        if key in source_order:
            source_place = source_order.index(key)
            continue

        # find anchor - find the next elment in keys that are already in
        source_order_from_current = source_order[source_place:]
        for future_key in new_keys[i:]:
            if future_key in source_order_from_current:
                # but get the index  from source_order
                anchor_idx = source_order.index(future_key)
                # splice in the new element immediately before anchor
                source_order.insert(anchor_idx, key)
                source_place = anchor_idx
                break
        else:
            # anchor not found - add
            source_order.append(key)
    return source_order


def _find_source_file(pipeline_config: PipelineConfig):
    files = list(pipeline_config.workdir.glob("source/*"))
    if len(files) != 1:
        # we only support one input file
        logger.warning(f"pipeline supports {bold('one')} input file in source/ and will select the first file.")
    logger.info(f"Reading source file: {files[0]}")
    return files[0]


def read_data(pipeline_config: PipelineConfig) -> tuple[list[str], list[int], Iterator[Entry]]:
    """
    When reading CSV data, we know the fields and their order beforehand, but not for JSON
    (unless hard coded in configuration). We prepare source order here, but it is not usable
    until after the generators have been consumed, same as size.
    """
    input_file = _find_source_file(pipeline_config)

    # size, array because generator needs mutable object
    size = [0]
    if input_file.suffix in [".csv", ".tsv"]:
        fp = open((input_file), encoding="utf-8-sig")
        if input_file.suffix == ".csv":
            reader = csv.reader(fp)
        else:
            reader = csv.reader(fp, dialect="excel-tab")
        source_order = next(reader, None) or []
        import_settings = cast(dict[str, dict[str, list[dict[str, str]]]], pipeline_config.import_settings)
        # type information for parsing values
        cast_fields: list[dict[str, str]] = import_settings["csv"]["cast_fields"]

        def get_entries() -> Iterator[Entry]:
            for row in reader:
                entry: dict[str, str | int | float] = dict(zip(source_order, row))
                # parse values
                for field in cast_fields:
                    if field["type"] == "int":
                        entry[field["name"]] = int(entry[field["name"]])
                    elif field["type"] == "float":
                        entry[field["name"]] = float(entry[field["name"]])
                    else:
                        raise RuntimeError(f"Uknown type: {field['type']}, given in CSV import")
                size[0] += 1
                yield entry
            fp.close()

    else:
        fp = open(input_file)
        source_order = []

        def get_entries() -> Iterator[Entry]:
            for line in fp:
                entry = json.loads(line)

                # get the sort order from the input JSON
                # this could be configurable to speed up
                keys = list(entry.keys())
                _update_json_source_order(source_order, keys)
                size[0] += 1
                yield entry
            fp.close()

    return source_order, size, get_entries()
