from datetime import datetime
import urllib.request

import jsonschema_rs


from karpspipeline.models import PipelineConfig
from karpspipeline.sbxrepo.models import SBXRepoConfig
from karpspipeline.util import json, yaml
from karpspipeline.sbxrepo.common import _get_config, _get_metadata_file


def _create_sb_metadata_file(pipeline_config: PipelineConfig, size):
    sbmetadata_config: SBXRepoConfig = _get_config(pipeline_config)

    # target object
    metadata: dict[str, object] = {"size": {"entries": size}}
    configured_metadata = sbmetadata_config.metadata.model_dump(
        exclude_none=True, exclude={"yaml_export_path", "schema_", "license"}
    )

    # add configured values to target
    metadata.update(configured_metadata)

    metadata["name"] = pipeline_config.name.model_dump(exclude_none=True)
    if not pipeline_config.description:
        raise RuntimeError("'description' is mandatory when generating SBX metadata.")
    metadata["short_description"] = pipeline_config.description.model_dump(exclude_none=True)

    # add derived values to target
    date_str = _get_current_date_string()
    if not sbmetadata_config.metadata.created:
        metadata["created"] = date_str
    if not sbmetadata_config.metadata.updated:
        metadata["updated"] = date_str

    if not sbmetadata_config.metadata.downloads:
        metadata["downloads"] = [
            {
                "url": sbmetadata_config.data.download_url_template.format(resource_id=pipeline_config.resource_id),
                "license": sbmetadata_config.metadata.license,
                "format": "jsonl",
            }
        ]
    if not sbmetadata_config.metadata.interfaces:
        metadata["interfaces"] = [
            {
                "url": sbmetadata_config.data.interface_url_template.format(resource_id=pipeline_config.resource_id),
                "license": sbmetadata_config.metadata.license,
            }
        ]
    metadata["type"] = "lexicon"

    # load and test against JSON schema for SBX metadata
    with urllib.request.urlopen(sbmetadata_config.metadata.schema_) as response:
        content = response.read().decode("utf-8")
        schema = json.loads(content)
    try:
        jsonschema_rs.validate(schema, metadata)
    except jsonschema_rs.ValidationError as exc:
        raise ImportError("metadata file not valid") from exc

    with open(_get_metadata_file(pipeline_config.resource_id), "w") as fp:
        yaml.dump(metadata, fp)


def _get_current_date_string():
    return datetime.now().strftime("%Y-%m-%d")
