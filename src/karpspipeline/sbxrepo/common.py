from pathlib import Path


from karpspipeline.common import create_output_dir
from karpspipeline.models import PipelineConfig
from karpspipeline.sbxrepo.models import SBXRepoConfig


def _get_config(pipeline_config: PipelineConfig):
    return SBXRepoConfig.model_validate(pipeline_config.modules["sbxrepo"])


def _get_metadata_file(resource_id: str) -> Path:
    output_dir = create_output_dir() / "sbxrepo"
    output_dir.mkdir(exist_ok=True)
    return output_dir / _get_metadata_filename(resource_id)


def _get_metadata_filename(resource_id: str) -> str:
    return f"{resource_id}.yaml"
