import importlib
import logging
from typing import Callable

from karpspipeline.common import ImportException
from karpspipeline.read import read_data

from karpspipeline.models import Entry, PipelineConfig


logger = logging.getLogger(__name__)


def run(config: PipelineConfig, subcommand: str = "all") -> None:
    if subcommand == "all":
        invoked_cmds = config.export.default
    else:
        invoked_cmds = [subcommand]

    resolved_cmds = []
    mods = {}

    def resolve(invoked_cmds):
        """
        Traverses the dependency tree and adds dependencies to resolved_cmds in the order they need to run
        """
        for cmd in invoked_cmds:
            try:
                mod = importlib.import_module("karpspipeline.modules." + cmd)
                mods[cmd] = mod
            except ModuleNotFoundError as e:
                raise ImportException(f"{cmd} not found") from e
            resolve(mod.dependencies)
            if cmd not in resolved_cmds:
                resolved_cmds.append(cmd)

    resolve(invoked_cmds)

    entry_tasks: list[Callable[[Entry], Entry]] = []
    module_data = {}
    for cmd in resolved_cmds:
        mod = mods[cmd]
        dependencies = mod.dependencies
        for dependency in dependencies:
            if dependency not in module_data:
                # fetch the result from cmd's dependency
                if hasattr(mods[dependency], "load"):
                    module_data[dependency] = mods[dependency].load(config)
                else:
                    # add dependency so we don't have to look for the load method again
                    module_data[dependency] = None
        new_tasks = mod.export(config, module_data)

        # callables added to entry_tasks will be called for each entry
        entry_tasks.extend(new_tasks)

    # for each entry, do the needed tasks
    # TODO read_data actually loads the entire file, but here we should read one line at a time
    for entry in read_data(config)[2]:
        updated_entry = entry
        for task in entry_tasks:
            updated_entry = task(updated_entry)
