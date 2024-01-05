"""pyWWA local module."""
# stdlib
import json
import os

# Local
from .cmdline import parse_cmdline

# Shared configuration
SETTINGS = {}
# Eventually updated by command line parsing
CTX = parse_cmdline([])
# Eventually updated to be a JABBER instance
JABBER = None


def get_table_filepath(filename):
    """Return full path to a given filename resource."""
    testfn = os.path.join(get_basedir(), "tables", filename)
    if not os.path.isfile(testfn):
        raise FileNotFoundError(f"could not locate table file {testfn}")
    return testfn


def get_basedir() -> str:
    """Since I am a hack, we need to compute the base folder of this repo."""
    thisdir = os.path.dirname(__file__)
    # up two folders
    return os.path.abspath(os.path.join(thisdir, "../.."))


def load_config() -> dict:
    """Attempt to locate our configuration file."""
    testfn = os.path.join(get_basedir(), "settings.json")
    if not os.path.isfile(testfn):
        return {}
    with open(testfn, encoding="utf-8") as fh:
        res = json.load(fh)
    return res


CONFIG = load_config()
