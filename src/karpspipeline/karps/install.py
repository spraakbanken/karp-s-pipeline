from contextlib import contextmanager

from io import TextIOWrapper
from pathlib import Path
import shutil
from typing import Iterable, Iterator

import mysql.connector
from mysql.connector.abstracts import MySQLCursorAbstract

from karpspipeline.common import Map, get_output_dir, InstallException
from karpspipeline.karps.models import KarpsConfig
from karpspipeline.models import PipelineConfig
from karpspipeline.util import yaml
from karpspipeline.util.git import GitRepo


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
                        cursor.fetchall()
                        buffer = []


def add_config(pipeline_config: PipelineConfig, karps_config: KarpsConfig, resource_id: str):
    config_dir = karps_config.output_config_dir
    repo = GitRepo(config_dir)
    main_dir = Path(config_dir)
    output_dir = get_output_dir()
    resource_dir = main_dir / "resources"

    if not main_dir.is_dir():
        main_dir.mkdir()
        repo.init()

    field_config = main_dir / "fields.yaml"
    field_config.touch()
    main_config = main_dir / "config.yaml"
    main_config.touch()
    if not resource_dir.is_dir():
        resource_dir.mkdir()

    # resource-yaml contains a list of fields
    karps_resource_config = output_dir / f"{resource_id}_karps.yaml"
    shutil.copy(karps_resource_config, resource_dir / f"{resource_id}.yaml")
    # this updates config.yaml with new information from the resource
    _update_config(main_dir / "config.yaml", karps_resource_config, karps_config)
    # this merges all the current resource field configs into one big file, taking into account
    # that fields.yaml may already contain translated labels etc
    _update_fields(karps_config, pipeline_config)

    repo.commit_all(msg=f"add {pipeline_config.resource_id}")


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
    yaml.dump(config_obj, fp_out)


def _read(filename: Path) -> Map:
    with open(filename) as fp:
        config = yaml.load(fp)
        print(f"Reading input file: {filename}")
        return config or {}


def _update_config(config_filename: Path, resource_filename: Path, karps_config: KarpsConfig):
    # read the input yaml fields
    config_obj = _read(config_filename)
    resource_obj = _read(resource_filename)
    # open config.yaml for writing
    with open(config_filename, "w") as fp_out:
        _add_tags(config_obj, resource_obj, karps_config, fp_out)


def _update_fields(karps_config: KarpsConfig, pipeline_config: PipelineConfig):
    """
    when running, fields.yaml are created with information about the
    fields that are not already present in the backend. Take this file
    and merge it with <export.karps.output_config_dir>/fields.yaml
    There should be no conflicts.
    """

    # first check the current backend config for fields
    fields_file = Path(karps_config.output_config_dir) / "fields.yaml"
    if not fields_file.exists():
        current_fields = []
    else:
        with open(fields_file) as fp:
            current_fields = yaml.load_array(fp) or []
    field_lookup = {field["name"]: field for field in current_fields}
    new_fields = []
    with open(get_output_dir() / "fields.yaml") as fp:
        fields = yaml.load_array(fp)
        for new_field in fields:
            new_label = new_field.get("label")
            if new_field["name"] in field_lookup:
                # update resource list
                field_resources = field_lookup[new_field["name"]]["resource_id"]
                if isinstance(field_resources, list):  # this is for typechecking
                    field_resources.append(pipeline_config.resource_id)
                    field_resources = list(set(field_resources))
                    field_lookup[new_field["name"]]["resource_id"] = field_resources
                if field_resources == [pipeline_config.resource_id]:
                    # if the field is used only by current resource, allow overwrites
                    field_lookup[new_field["name"]].update(new_field)
                else:
                    # no changes to other resources are allowed
                    if (
                        new_field["type"] != field_lookup[new_field["name"]]["type"]
                        or new_field.get("collection", False)
                        != field_lookup[new_field["name"]].get("collection", False)
                        or (new_label and new_label != field_lookup[new_field["name"]].get("label"))
                    ):
                        raise InstallException(
                            f"There already exists a field called {new_field['name']} with different settings"
                        )
            else:
                new_field["resource_id"] = [pipeline_config.resource_id]
                new_fields.append(new_field)

    current_fields.extend(new_fields)

    with open(Path(karps_config.output_config_dir) / "fields.yaml", "w") as fp:
        yaml.dump(current_fields, fp)
