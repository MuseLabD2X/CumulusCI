""" CLI logger """
import logging
from logging.handlers import RotatingFileHandler
import os
import sys
import string
import requests
from random import choice
from pathlib import Path

import coloredlogs

try:
    import colorama
except ImportError:
    # coloredlogs only installs colorama on Windows
    pass


def init_logger(log_requests=False):
    """ Initialize the logger """

    logger = logging.getLogger(__name__.split(".")[0])
    for handler in logger.handlers:  # pragma: no cover
        logger.removeHandler(handler)

    if os.name == "nt" and "colorama" in sys.modules:  # pragma: no cover
        colorama.init()

    formatter = coloredlogs.ColoredFormatter(fmt="%(asctime)s: %(message)s")
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    if log_requests:
        requests.packages.urllib3.add_stderr_logger()


def get_tempfile_logger():
    """Creates and returns a logger tha writes to a
    temporary logfile. Returns the logger and path to tempfile"""
    logger = logging.getLogger("tempfile_logger")
    temp_logfile_dir = Path.home() / ".cumulusci" / "logs"
    temp_logfile_dir.mkdir(parents=True, exist_ok=True)

    filepath = temp_logfile_dir / get_tempfile_name(15)
    handler = logging.FileHandler(filepath, encoding="utf-8")
    handler.terminator = ""
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger, filepath


def get_tempfile_name(len):
    """Gneerates a logfile name comprised of lowercase letters
    of the specified length ending in .log"""
    letters = string.ascii_letters
    filename = "".join(choice(letters) for i in range(len))
    return f"{filename}.log"


def get_gist_logger():
    """Determines the appropriate filepath for logfile
    and name for the logger. Returns a logger with
    RotatingFileHandler attached."""
    logfile_dir = Path.home() / ".cumulusci" / "logs"
    logfile_dir.mkdir(parents=True, exist_ok=True)
    logfile_path = logfile_dir / "cci.log"

    return get_rot_file_logger("stdout/stderr", logfile_path)


def get_rot_file_logger(name, path):
    """Returns a logger with a rotating file handler"""
    logger = logging.getLogger(name)

    handler = RotatingFileHandler(path, backupCount=5, encoding="utf-8")
    handler.doRollover()  # rollover existing log files
    handler.terminator = ""  # click.echo already adds a newline
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger
