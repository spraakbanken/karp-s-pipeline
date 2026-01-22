import csv
from typing import Iterator, cast

from karpspipeline.models import Entry, PipelineConfig
from karpspipeline.util import json


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


def read_data(pipeline_config: PipelineConfig) -> tuple[list[str], list[int], Iterator[Entry]]:
    """
    When reading CSV data, we know the fields and their order beforehand, but not for JSON
    (unless hard coded in configuration). We prepare source order here, but it is not usable
    until after the generators have been consumed.
    """
    csv_files = list(pipeline_config.workdir.glob("source/*csv"))
    tsv_files = list(pipeline_config.workdir.glob("source/*tsv"))
    # size, array because generator needs mutable object
    size = [0]
    if csv_files or tsv_files:
        fp = open((csv_files + tsv_files)[0], encoding="utf-8-sig")
        if csv_files:
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

        return source_order, size, get_entries()
    else:
        jsonl_files = pipeline_config.workdir.glob("source/*jsonl")
        fp = open(next(jsonl_files))

        source_order = []
        size = [0]

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
