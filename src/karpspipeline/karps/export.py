import time
from typing import Iterable, Iterator


from karpspipeline.common import create_output_dir
from karpspipeline.karps.models import KarpsConfig
from karpspipeline.models import Entry, EntrySchema, FieldConfig, PipelineConfig
from karpspipeline.util import json, yaml


def create_karps_backend_config(
    pipeline_config: PipelineConfig,
    karps_config: KarpsConfig,
    field_config: FieldConfig,
    size: int,
    fields: list[dict[str, str]],
):
    # these fields might already be present in backend config, install must merge this file and backend fields.yaml
    with open(create_output_dir() / "fields.yaml", "w") as fp:
        yaml.dump(fields, fp)

    backend_config = {
        "resource_id": pipeline_config.resource_id,
        "label": pipeline_config.name.model_dump(),
        # TODO sort
        "fields": list(field_config.fields.keys()),
        "word": karps_config.word,
        "size": size,
        "link": karps_config.link,
        "updated": int(time.time()),
    }
    if karps_config.word not in backend_config["fields"]:
        raise ImportError(f"word: {karps_config.word}, but {karps_config.word} is not available in the resource")
    if karps_config.tags:
        backend_config["tags"] = karps_config.tags
    if pipeline_config.description:
        backend_config["description"] = pipeline_config.description.model_dump()
    if karps_config.word_description:
        backend_config["word_description"] = karps_config.word_description.model_dump()

    with open(f"output/{pipeline_config.resource_id}_karps.yaml", "w") as fp:
        yaml.dump(backend_config, fp)


def create_karps_sql(
    pipeline_config: PipelineConfig,
    resource_config: FieldConfig,
    entries: Iterable[Entry],
) -> int:
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
                yield f"`{field_name}` {column_type}"

        return f"""
        DROP TABLE IF EXISTS `{table_name}`;
        CREATE TABLE `{table_name}` (
            {",\n".join(inner())}
        )
        CHARACTER SET utf8mb4
        COLLATE utf8mb4_swedish_ci;
        """

    def entries_sql() -> Iterator[str]:
        for entry in entries:
            columns = entry.keys()

            def json_escape(val):
                return val.replace('"', '\\"')

            def format_str(val):
                """
                Wrap string in single quotes, escape single quotes and newlines
                """
                # replace all single quotes not already escaped
                return f"'{val.replace("'", "\\'").replace('\n', '\\n')}'"

            def sqlify_values(values):
                for val in values:
                    if isinstance(val, list):
                        fmted = json.dumps([json_escape(x) for x in val])
                        yield format_str(fmted)
                    elif val is None:
                        yield "NULL"
                    elif isinstance(val, str):
                        yield format_str(val)
                    elif isinstance(val, int):
                        yield str(val)
                    else:
                        raise Exception("unknown type")

            values = sqlify_values(entry.values())

            yield f"INSERT INTO `{pipeline_config.resource_id}` ({', '.join(f'`{column}`' for column in columns)}) VALUES ({', '.join(values)});\n"

    size = 0
    with open(f"output/{pipeline_config.resource_id}.sql", "w") as fp:
        schema_sql = schema(pipeline_config.resource_id, resource_config.fields)
        fp.write(schema_sql)
        for line in entries_sql():
            fp.write(line)
            size += 1
    return size
