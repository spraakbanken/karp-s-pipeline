from datetime import datetime
import logging
import sys

from karppipeline.common import create_log_dir


logger = logging.getLogger("karppipeline")


def setup_resource_logging(path, silent=False):
    # remove previous handlers
    logger.handlers.clear()

    logger.setLevel("INFO")

    # either log to file or console
    if silent:
        # create log file for resource if it does not exist
        log_dir = create_log_dir(path)
        log_file = log_dir / "run.log"
        # write header to know if a new run has started
        with open(log_file, "a") as f:
            f.write("-------------------------------\n")
            f.write(f"pipeline run, {datetime.now().strftime('%Y-%m-%d_%H%M%S')}\n")
        logger.addHandler(logging.FileHandler(log_file))
    else:
        logger.addHandler(logging.StreamHandler(stream=sys.stdout))


def get_logger():
    return logger
