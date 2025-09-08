import csv
import glob
import json
from typing import Iterator, cast

from karpspipeline.models import Entry, PipelineConfig


def read_data(pipeline_config: PipelineConfig):
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

        return get_entries()
    else:
        jsonl_files = glob.glob("source/*jsonl")
        fp = open(jsonl_files[0])

        def get_entries() -> Iterator[Entry]:
            for line in fp:
                entry = json.loads(line)
                yield entry
            fp.close()

        return get_entries()
