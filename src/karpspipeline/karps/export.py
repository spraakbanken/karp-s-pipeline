import time
from typing import Iterable, Iterator


from karpspipeline.common import create_output_dir
from karpspipeline.karps.models import KarpsConfig
from karpspipeline.models import Entry, EntrySchema, FieldConfig, PipelineConfig, InferredField
from karpspipeline.util import yaml


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
        "entry_word": karps_config.entry_word.model_dump(),
        "size": size,
        "link": karps_config.link,
        "updated": int(time.time()),
    }
    if karps_config.entry_word.field not in backend_config["fields"]:
        raise ImportError(f"entry_word: {karps_config.entry_word.field}, but field is not available in the resource")
    if karps_config.tags:
        backend_config["tags"] = karps_config.tags
    if pipeline_config.description:
        backend_config["description"] = pipeline_config.description.model_dump()

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

        def delete_statement(table_name) -> str:
            """
            Each resource with collections produces multiple tables, prefixed with {resource_id}__ and these statements
            removed them dynamically
            """
            return f"""
            SELECT CONCAT('DROP TABLE IF EXISTS `', GROUP_CONCAT(TABLE_NAME SEPARATOR '`, `'), '`;')
            INTO @drop_stmt FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = 'karps_local' AND TABLE_NAME LIKE '{table_name}__%';
            SET @run_stmt = IF(@drop_stmt IS NOT NULL, @drop_stmt, 'SELECT "No tables to drop";');
            PREPARE stmt FROM @run_stmt;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
            DROP TABLE IF EXISTS `{table_name}`;
            """

        def inner(_structure):
            tables = []
            fields = []
            for field_name, field in _structure.items():
                if field.collection:
                    field_copy = InferredField(type=field.type, collection=False)
                    # currently single field, but could support collection: true & type: object in future
                    _, inner_fields = inner({"value": field_copy})
                    inner_table_name = f"{table_name}__{field_name}"
                    tables.append(f"""
                    CREATE TABLE `{inner_table_name}` (
                        {",\n".join(inner_fields)},
                        __parent_id INT,
                        FOREIGN KEY (__parent_id) REFERENCES `{table_name}`(__id)
                    )
                    CHARACTER SET utf8mb4
                    COLLATE utf8mb4_swedish_ci;
                    """)
                else:
                    if field.type == "integer":
                        column_type = "INT"
                    elif field.type == "text":
                        column_type = "TEXT"
                    elif field.type == "float":
                        column_type = "FLOAT"
                    else:
                        raise Exception("unknown column type", field.type)
                    fields.append(f"`{field_name}` {column_type}")
            return tables, fields

        tables, fields = inner(structure)

        return f"""
        {delete_statement(table_name)}
        CREATE TABLE `{table_name}` (
            __id INT PRIMARY KEY,
            {",\n".join(fields)}
        )
        CHARACTER SET utf8mb4
        COLLATE utf8mb4_swedish_ci;
        """ + "".join(tables)

    def entries_sql() -> Iterator[str]:
        for idx, entry in enumerate(entries):

            def format_str(val):
                """
                Wrap string in single quotes, escape backslashes and single quotes
                """
                return f"'{val.replace('\\', '\\\\').replace("'", "\\'").replace('\n', '\\n')}'"

            def format_value(val):
                if val is None:
                    return "NULL"
                elif isinstance(val, str):
                    return format_str(val)
                elif isinstance(val, int) or isinstance(val, float):
                    return str(val)
                else:
                    raise Exception("unknown type")

            def sqlify_values(entry):
                """
                if values are scalar, they must be formatted/encoded in a wway that makes sense for MySQL
                if values are lists, they must be transformed into a separate INSERT statement with a ref to parent (idx from closure)
                """
                inserts = []
                columns = []
                main_values = []
                for field_name, val in entry.items():
                    if isinstance(val, list):
                        for x in val:
                            inserts.append(
                                f"INSERT INTO `{pipeline_config.resource_id}__{field_name}` (__parent_id, value) VALUES ({idx}, {format_value(x)});\n"
                            )
                    elif val is not None:
                        columns.append(field_name)
                        main_values.append(format_value(val))
                return inserts, columns, main_values

            inserts, columns, values = sqlify_values(entry)

            # first emit the main entry
            yield f"INSERT INTO `{pipeline_config.resource_id}` (`__id`, {', '.join(f'`{column}`' for column in columns)}) VALUES ({idx}, {', '.join(values)});\n"
            # then emit rows depending on main entry
            yield from inserts

    size = 0
    with open(f"output/{pipeline_config.resource_id}.sql", "w") as fp:
        schema_sql = schema(pipeline_config.resource_id, resource_config.fields)
        fp.write(schema_sql)
        for line in entries_sql():
            fp.write(line)
            size += 1
    return size
