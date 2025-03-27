from contextlib import contextmanager

from io import TextIOWrapper
from pathlib import Path
import shutil
from typing import Iterable, Iterator

import mysql.connector
from mysql.connector.abstracts import MySQLCursorAbstract

from karpspipeline.common import Map, get_output_dir
from karpspipeline.karps.models import KarpsConfig
from karpspipeline.util.git import GitRepo
import yaml


def add_to_db(resource_id, karps_config):
    @contextmanager
    def get_db_cursor(karps_config: KarpsConfig) -> Iterator[MySQLCursorAbstract]:
        connection = mysql.connector.connect(
            user=karps_config.db_user,
            password=karps_config.db_password,
            database=karps_config.db_database,
        )
        cursor = None
        try:
            cursor = connection.cursor()
            yield cursor
        finally:
            if cursor:
                cursor.close()
            connection.commit()
            connection.close()

    sql_filename = f"output/{resource_id}.sql"
    with open(sql_filename) as sql_file:
        with get_db_cursor(karps_config) as cursor:
            buffer = []
            for line in sql_file:
                line = line.rstrip()
                if line:
                    buffer.append(line)
                    if line[-1] == ";":
                        cursor.execute(" ".join(buffer))
                        buffer = []


def add_config(karps_config: KarpsConfig, resource_id: str):
    config_dir = karps_config.output_config_dir
    repo = GitRepo(config_dir)
    main_dir = Path(config_dir)
    output_dir = get_output_dir()
    resource_dir = main_dir / "resources"

    if not main_dir.is_dir():
        main_dir.mkdir()

        field_config = main_dir / "fields.yaml"
        field_config.touch()
        main_config = main_dir / "config.yaml"
        main_config.touch()
        resource_dir.mkdir()
        repo.init()

    # resource-yaml contains a list of fields
    karps_resource_config = output_dir / f"{resource_id}_karps.yaml"
    shutil.copy(karps_resource_config, resource_dir / f"{resource_id}.yaml")
    # this updates config.yaml with new information from the resource
    _update_config(main_dir / "config.yaml", karps_resource_config, karps_config)
    # this merges all the current resource field configs into one big file, taking into account
    # that fields.yaml may already contain translated labels etc
    _update_fields(karps_config)

    repo.commit_all()


def _merge_fields(resource_fields_filename: str, config_fields_filename: str):
    """
    This function takes the temporary fields-config for a specific resource and merges it with the main fields resource
    """
    ...


def _get_iterable(resource_obj, key) -> Iterable:
    tags = resource_obj.get(key, ())
    if isinstance(tags, Iterable):
        return tags
    return ()


def _add_tags(
    config_obj: dict[str, object],
    resource_obj: dict[str, object],
    karps_config: KarpsConfig,
    fp_out: TextIOWrapper,
) -> None:
    """
    Takes a  resource-config file and updates Karp-S backend configuration file if needed.
    """
    current_tags = config_obj.get("tags", {})
    for tag in _get_iterable(resource_obj, "tags"):
        if tag not in current_tags:
            if not isinstance(current_tags, dict):
                # TODO do this better, make config_obj into a dataclass
                raise Exception("wrong format for tags in config.yaml")
            if not current_tags:
                config_obj["tags"] = {}
                current_tags = config_obj["tags"]
            current_tags[tag] = karps_config.tags_description[tag].model_dump()
    yaml.dump(config_obj, fp_out, allow_unicode=True)


def _read(filename: Path) -> Map:
    with open(filename) as fp:
        config = yaml.safe_load(fp)
        print(f"Reading input file: {filename}")
        return config or {}


def _update_config(config_filename: Path, resource_filename: Path, karps_config: KarpsConfig):
    # read the input yaml fields
    config_obj = _read(config_filename)
    resource_obj = _read(resource_filename)
    # open config.yaml for writing
    with open(config_filename, "w") as fp_out:
        _add_tags(config_obj, resource_obj, karps_config, fp_out)


def _update_fields(karps_config: KarpsConfig):
    fields_path = Path(karps_config.output_config_dir) / "fields.yaml"
    with open(fields_path) as fp:
        main_fields = yaml.safe_load(fp)
    for field in karps_config.fields:
        print("####")
        print(field)
        print(main_fields)
