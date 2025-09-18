import time
from typing import Generator, Iterator


from karpspipeline.common import create_output_dir
from karpspipeline.karps.models import KarpsConfig
from karpspipeline.models import Entry, EntrySchema, PipelineConfig, InferredField
from karpspipeline.util import yaml

VARCHAR_CUTOFF = 200  # if a field contains values larger than this, use TEXT type and skip indexing


def create_karps_backend_config(
    pipeline_config: PipelineConfig,
    karps_config: KarpsConfig,
    entry_schema: EntrySchema,
    source_order: list[str],
    size: int,
    fields: list[dict[str, str]],
):
    # these fields might already be present in backend config, install must merge this file and backend fields.yaml
    with open(create_output_dir() / "fields.yaml", "w") as fp:
        yaml.dump(fields, fp)

    def order_fields(fields: Iterator[str]):
        # initialize main sort order
        order_map = {name: i for i, name in enumerate([field.name for field in pipeline_config.fields])}

        # order by apperance in input objects for non-configured fields
        for i, name in enumerate(source_order):
            if name not in order_map:
                order_map[name] = len(pipeline_config.fields) + i

        # should be no unknown fields at this point
        sorted_keys = sorted(fields, key=lambda x: order_map[x])
        return sorted_keys

    backend_config = {
        "resource_id": pipeline_config.resource_id,
        "label": pipeline_config.name.model_dump(),
        "fields": order_fields(iter(entry_schema.keys())),
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
    pipeline_config: PipelineConfig, karps_config: KarpsConfig, resource_config: EntrySchema
) -> Generator[None, Entry | None, None]:
    def schema(table_name: str, structure: EntrySchema) -> tuple[str, str]:
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
            WHERE TABLE_SCHEMA = '{karps_config.db_database}' AND TABLE_NAME LIKE '{table_name}__%';
            SET @run_stmt = IF(@drop_stmt IS NOT NULL, @drop_stmt, 'SELECT "No tables to drop";');
            PREPARE stmt FROM @run_stmt;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
            DROP TABLE IF EXISTS `{table_name}`;
            """

        def inner(_structure):
            tables = []
            fields = []
            indices = []
            for field_name, field in _structure.items():
                if field.collection:
                    field_copy = InferredField(type=field.type, collection=False, extra=field.extra)
                    # currently single field, but could support collection: true & type: object in future
                    _, inner_fields, _ = inner({"value": field_copy})
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
                    if field.type == "text" and field.extra["length"] <= VARCHAR_CUTOFF:
                        indices.append(
                            f"CREATE INDEX `{inner_table_name}_idx` ON `{inner_table_name}`(value({field.extra['length']}));"
                        )
                else:
                    if field.type == "integer":
                        column_type = "INT"
                    elif field.type == "text":
                        if field.extra["length"] > VARCHAR_CUTOFF:
                            column_type = "TEXT"
                        else:
                            column_type = f"VARCHAR({field.extra['length']})"
                            indices.append(
                                f"CREATE INDEX `{table_name}__{field_name}_idx` ON `{table_name}`(`{field_name}`({field.extra['length']}));"
                            )
                    elif field.type == "float":
                        column_type = "FLOAT"
                    else:
                        raise Exception("unknown column type", field.type)
                    fields.append(f"`{field_name}` {column_type}")
            return tables, fields, indices

        tables, fields, indices = inner(structure)

        return (
            f"""
        {delete_statement(table_name)}
        CREATE TABLE `{table_name}` (
            __id INT PRIMARY KEY,
            {",\n".join(fields)}
        )
        CHARACTER SET utf8mb4
        COLLATE utf8mb4_swedish_ci;
        """
            + "".join(tables)
        ), "\n".join(indices) + "\n"

    def entries_sql() -> Generator[list[str], Entry | None, None]:
        idx = 0
        lines = []
        while True:
            entry = yield lines
            if entry is None:
                break

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

            # main entry
            lines = [
                f"INSERT INTO `{pipeline_config.resource_id}` (`__id`, {', '.join(f'`{column}`' for column in columns)}) VALUES ({idx}, {', '.join(values)});\n"
            ] + inserts

            idx += 1

    sql_gen = entries_sql()
    next(sql_gen)
    with open(f"output/{pipeline_config.resource_id}.sql", "w") as fp:
        schema_sql, indices = schema(pipeline_config.resource_id, resource_config)
        fp.write(schema_sql)
        fp.write(indices)
        while True:
            entry = yield
            if not entry:
                # TODO it this needed or can sql_gen be killed/gc:ed when the outer generator is done
                sql_gen.send(None)
                break
            for line in sql_gen.send(entry):
                fp.write(line)
