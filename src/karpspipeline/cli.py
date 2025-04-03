from datetime import datetime
import os
import shutil
import sys
import traceback

from karpspipeline.common import create_log_dir, get_log_dir, get_output_dir
from karpspipeline.config import load_config
from karpspipeline.install import install
from karpspipeline.run import run
from karpspipeline.util.terminal import bold


def clean() -> None:
    clean_paths = get_log_dir(), get_output_dir()
    for path in clean_paths:
        if os.path.exists(path):
            shutil.rmtree(path)


def cli():
    os.system("")
    if len(sys.argv) != 2:
        print(f"{bold('Usage:')} karps-pipeline run/install")
        print()
        print(f"{bold('run')} - prepares the material")
        print(f"{bold('install')} - adds the material to the requested system")
        print(f"{bold('clean')} - remove genereated files")
        print()
        print("karps-pipeline install karps (karps-backend)")
        print("karps-pipeline install sbx-repo (add resources to some repo, don't know where yet)")
        print("karps-pipeline install sbx-metadata (add resources to some repo, don't know where yet)")
        print("karps-pipeline install all (do all of the above)")
        print()
        print(
            "Set environment variable KARPSPIPELINE_CONFIG to a config.yaml which will be merged with the project ones"
        )
        return 1

    try:
        if sys.argv[1] == "clean":
            clean()
        else:
            config = load_config(os.getenv("KARPSPIPELINE_CONFIG"))

            if sys.argv[1] == "run":
                # run calls importers and exporters
                run(config)

            if sys.argv[1] == "install":
                # install calls installers (always application specific, like Karp-S backend or resource repo)
                install(config)

    except Exception:
        print("error.")
        log_dir = create_log_dir()
        err_file = log_dir / (f"err_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.log")
        with open(err_file, "w") as f:
            traceback.print_exc(file=f)
        return 1

    print("done.")
    return 0
