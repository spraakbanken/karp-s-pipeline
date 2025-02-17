from typing import Iterator
from karpspipeline.models import Entry, PipelineConfig
from karpspipeline.karps.export import create_karps_backend_config, create_karps_sql


def export(config: PipelineConfig, entries: Iterator[Entry]):
    create_karps_backend_config(config)
    create_karps_sql(config, entries)
    export_config = config.export
    if export_config["karps"].get("install"):
        # 1. get db credentials
        # 2. run "cat karps/data.sql > mysql -u <user> -p <password> <database>
        # 3. get backend config directory <bcd>
        # 4. mv karps/<resource_id>.yaml <bcd>
        pass


def install(): ...
