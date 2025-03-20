from typing import Iterable, Iterator

import yaml

from karpspipeline.common import create_output_dir
from karpspipeline.models import Entry, EntrySchema, PipelineConfig
from karpspipeline.util import json


def create_karps_backend_config(pipeline_config: PipelineConfig):
    create_output_dir()

    backend_config = {
        "resource_id": pipeline_config.resource_id,
        "label": pipeline_config.label,
        "fields": [],
    }
    for key, val in pipeline_config.resource_config.fields.items():
        field = {"name": key, "type": val.type, "collection": val.collection}
        backend_config["fields"].append(field)

    with open(f"output/{pipeline_config.resource_id}_karps.yaml", "w") as fp:
        yaml.dump(backend_config, fp)


def create_karps_sql(pipeline_config: PipelineConfig, entries: Iterable[Entry]):
    def schema(table_name: str, structure: EntrySchema) -> str:
        """
        Find schema automatically by going through all elements
        """

        def inner():
            for field_name, field in structure.items():
                if field.collection:
                    column_type = "JSON"
                elif field.type == "integer":
                    column_type = "INT"
                elif field.type == "text":
                    column_type = "TEXT"
                else:
                    raise Exception("unknown column type", field.type)
                yield f"{field_name} {column_type}"

        return f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name} (
            {",\n".join(inner())}
        )
        CHARACTER SET utf8mb4
        COLLATE utf8mb4_swedish_ci;
        """

    def sql() -> Iterator[str]:
        yield schema(
            pipeline_config.resource_id, pipeline_config.resource_config.fields
        )
        for entry in entries:
            columns = entry.keys()

            def format_str(val):
                """
                Wrap string in single quotes, escape single quotes and newlines
                """
                return f"'{val.replace("'", "\\'").replace('\n', '\\n')}'"

            def sqlify_values(values):
                for val in values:
                    if isinstance(val, list):
                        yield format_str(json.dumps(val))
                    elif val is None:
                        yield "NULL"
                    elif isinstance(val, str):
                        yield format_str(val)
                    elif isinstance(val, int):
                        yield str(val)
                    else:
                        raise Exception("unknown type")

            values = sqlify_values(entry.values())

            yield f"INSERT INTO {pipeline_config.resource_id} ({', '.join(columns)}) VALUES ({', '.join(values)});\n"

    with open(f"output/{pipeline_config.resource_id}.sql", "w") as fp:
        for line in sql():
            fp.write(line)
