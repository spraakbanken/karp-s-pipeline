import logging
import subprocess
from typing import Callable, cast
from karpspipeline.common import create_output_dir, get_output_dir
from karpspipeline.models import Entry, EntrySchema, PipelineConfig
from karpspipeline.util import yaml

logger = logging.getLogger("karp")


def export(
    config: PipelineConfig,
    entry_schema: EntrySchema,
) -> list[Callable[[Entry], Entry]]:
    create_karp_backend_config(config, entry_schema)
    return []


def create_karp_backend_config(config: PipelineConfig, entry_schema: EntrySchema):
    karp_config = {"resource_id": config.resource_id, "resource_name": config.name.model_dump()["swe"], "fields": {}}
    for field_name, field in entry_schema.items():
        dumped = field.model_dump(exclude_defaults=True, exclude_unset=True)
        if field.type == "text":
            dumped["type"] = "string"
        karp_config["fields"][field_name] = dumped

    output_dir = create_output_dir(config.workdir) / "karp"
    output_dir.mkdir(exist_ok=True)
    with open(output_dir / f"{config.resource_id}.yaml", "w") as fp:
        yaml.dump(karp_config, fp)


def install(config: PipelineConfig):
    config_file = get_output_dir(config.workdir) / "karp" / f"{config.resource_id}.yaml"

    # adding a resurce in Karp is done in three steps
    # creating resource with config
    karps_config = cast(dict, config.modules["karp"])
    _karp_cli_runner(karps_config, ["resource", "create", str(config_file)])
    # adding entries
    data_file = get_output_dir(config.workdir) / f"{config.resource_id}.jsonl"
    _karp_cli_runner(karps_config, ["entries", "add", config.resource_id, str(data_file)])
    # publish the resource
    _karp_cli_runner(karps_config, ["resource", "publish", config.resource_id])


def _karp_cli_runner(config: dict[str, str], cmds):
    karp_cli = config["cli"]
    cwd = config["cwd"]
    try:
        result = subprocess.run(
            [karp_cli, *cmds],
            check=True,
            capture_output=True,
            text=True,
            cwd=cwd,
        )
        logger.info(result.stdout)
        logger.info(result.stderr)
    except subprocess.CalledProcessError as e:
        logger.error(e.stdout)
        logger.error(e.stderr)
        raise
