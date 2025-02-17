from io import TextIOWrapper
from pathlib import Path
import shutil

from karpspipeline.util.git import GitRepo
import yaml


def install(config_dir: str, resource_id: str):
    repo = GitRepo(config_dir)
    main_dir = Path(config_dir)
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
    shutil.copy(Path(f"{resource_id}.yaml"), resource_dir)
    # this updates config.yaml with new information from the resource
    _update_config(f"{config_dir}/config.yaml", f"{resource_id}.yaml")
    # this merges all the current resource field configs into one big file, taking into account
    # that fields.yaml may already contain translated labels etc
    _merge_fields(config_dir)

    repo.commit_all()


def _merge_fields(resource_fields_filename: str, config_fields_filename: str):
    """
    This function takes the temporary fields-config for a specific resource and merges it with the main fields resource
    """
    ...


def _add_tags(
    resource_obj: dict[str, object],
    config_obj: dict[str, object],
    fp_out: TextIOWrapper,
) -> None:
    """
    Takes a  resource-config file and updates Karp-S backend configuration file if needed.
    """
    for tag in resource_obj.get("tags", ()):
        current_tags = config_obj.get("tags")
        if tag not in current_tags:
            if not current_tags:
                config_obj["tags"] = {}
                current_tags = config_obj["tags"]
            current_tags[tag] = {
                "label": {"eng": tag, "swe": tag},
                "description": {
                    "eng": tag,
                    "swe": tag,
                },
            }
            yaml.dump(config_obj, fp_out)


def _read(filename: str):
    with open(filename) as fp:
        config = yaml.safe_load(fp)
        print(f"Reading input file: {filename}")
        return config


def _update_config(resource_filename: str, config_filename: str):
    # read the input yaml fields
    [resource_obj, config_obj] = (
        _read(filename) for filename in [resource_filename, config_filename]
    )
    # open config.yaml for writing
    with open(config_filename, "w") as fp_out:
        _add_tags(resource_obj, config_obj, fp_out)
