from typing import Callable, Sequence


from karpspipeline.models import Entry, PipelineConfig


def export(
    config: PipelineConfig,
    size: int,
) -> Sequence[Callable[[Entry], Entry]]:
    from karpspipeline.sbxrepo.export import _create_sb_metadata_file

    # create and validate file, save it in output directory
    _create_sb_metadata_file(config, size)
    return ()


def install(pipeline_config: PipelineConfig):
    from karpspipeline.sbxrepo.common import _get_config
    from karpspipeline.sbxrepo.install import _upload_data, _install_metadata_file
    from karpspipeline.sbxrepo.models import SBXRepoConfig

    sbmetadata_config: SBXRepoConfig = _get_config(pipeline_config)
    _upload_data(pipeline_config, sbmetadata_config)
    _install_metadata_file(pipeline_config, sbmetadata_config)
