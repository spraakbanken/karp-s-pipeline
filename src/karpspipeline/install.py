from karpspipeline import karps
from karpspipeline.models import PipelineConfig


def install(config: PipelineConfig) -> None:
    """
    TODO here we assume that the run is done
    """
    if "karps" in config.export:
        karps.install(config)
