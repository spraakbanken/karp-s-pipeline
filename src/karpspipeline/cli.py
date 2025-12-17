import os
import sys
from typing import TYPE_CHECKING

from karpspipeline.common import ImportException, InstallException
from karpspipeline.util.terminal import bold, green_box, red_box


if TYPE_CHECKING:
    from karpspipeline.config import ConfigHandle


def clean(configs: list["ConfigHandle"]) -> None:
    import shutil
    import os
    from karpspipeline.common import get_log_dir, get_output_dir

    """
    remove log and output directories for the given resources
    """
    for resource in configs:
        clean_paths = get_log_dir(resource.workdir), get_output_dir(resource.workdir)
        for path in clean_paths:
            if os.path.exists(path):
                print(f"Remove {path}")
                shutil.rmtree(path)


def cli():
    os.system("")
    if len(sys.argv) > 3:
        help_text = []
        help_text.append(f"{bold('Usage:')} karps-pipeline run/install")
        help_text.append("")
        help_text.append(f"{bold('run')} - prepares the material")
        help_text.append(f"{bold('install')} - adds the material to the requested system")
        help_text.append(f"{bold('clean')} - remove genereated files")
        help_text.append("")
        help_text.append("Subcommands:")
        help_text.append("")
        help_text.append("karps-pipeline install karps")
        help_text.append("karps-pipeline install sbxrepo")
        help_text.append("")
        help_text.append(
            "Automatically picks up a config.yaml in current directory, checks for parents and children and runs the command on all resources this level and below."
        )
        print("\n".join(help_text))
        return 1

    import logging
    from karpspipeline.config import find_configs, load_config
    from karpspipeline.install import install
    from karpspipeline.run import run
    import karpspipeline.logging as karps_logging

    configs = find_configs()

    if sys.argv[1] == "clean":
        clean(configs)
        return 0

    do_run = sys.argv[1] == "run"
    do_install = sys.argv[1] == "install"

    kwargs = {}
    if len(sys.argv) > 2:
        kwargs["subcommand"] = sys.argv[2]

    silent = False
    if len(configs) > 1:
        silent = True
    for config_handle in configs:
        karps_logging.setup_resource_logging(config_handle.workdir, silent=silent)
        try:
            config = load_config(config_handle)
            # run calls importers and exporters
            if not silent:
                if do_run:
                    task_output = "Running"
                elif do_install:
                    task_output = "Installing"
                else:
                    task_output = "Unknown action"
                print(task_output, config.resource_id)
            if do_install:
                install(config, **kwargs)
            elif do_run:
                run(config, **kwargs)
            if silent:
                # TODO inform user if there was warnings
                print(f"{green_box()} {config.resource_id}\t success")
        except Exception as e:
            if isinstance(e, InstallException) or isinstance(e, ImportException):
                logging.getLogger("karpspipeline").error(f"Exception for resource: {e.args[0]}")
            else:
                logging.getLogger("karpspipeline").error("Exception for resource", exc_info=True)
            if silent:
                print(f"{red_box()} {config_handle.workdir}\t fail")

    return 0
