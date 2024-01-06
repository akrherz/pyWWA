"""pyWWA local module."""
# stdlib
import json
import os

# Shared configuration
SETTINGS = {}
# Eventually updated by command line parsing
CTX_DEFAULTS = {
    "disable_email": False,
    "disable_dbwrite": False,
    "replace": False,
    "shutdown_delay": 5,
    "stdout_logging": False,
    "utcnow": None,
}
CTX = {}
CTX.update(CTX_DEFAULTS)
# Eventually updated to be a JABBER instance
JABBER = None


def get_data_filepath(filename):
    """Return full path to a given filename resource."""
    testfn = os.path.join(os.path.dirname(__file__), "data", filename)
    if not os.path.isfile(testfn):
        raise FileNotFoundError(f"could not locate table file {testfn}")
    return testfn


def load_config() -> dict:
    """Attempt to locate our configuration file."""
    basedir = os.environ.get("PYWWA_HOME", os.getcwd())
    testfn = os.path.join(basedir, "settings.json")
    if not os.path.isfile(testfn):
        return {}
    with open(testfn, encoding="utf-8") as fh:
        res = json.load(fh)
    return res


CONFIG = load_config()
