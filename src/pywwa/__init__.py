"""pyWWA local module."""

# stdlib
import json
import logging
import os

from twisted.internet import reactor

# Default logging configuration
LOG = logging.getLogger("pywwa")
LOG.addHandler(logging.NullHandler())

# Shared configuration
SETTINGS = {}
SETTINGS_FN = "pywwa_settings.json"
# Eventually updated by command line parsing
CTX_DEFAULTS = {
    "disable_email": False,
    "disable_dbwrite": False,
    "replace": False,
    "shutdown_delay": 5,
    "stdout_logging": False,
    "utcnow": None,
}
# hold reference to jabber client
CTX = {
    "JABBER": None,  # Jabber client
    "LCLIST": [],  # List of twisted LoopingCall objects
}
CTX.update(CTX_DEFAULTS)


def shutdown():
    """Shutdown method in given number of seconds."""
    delay = CTX["shutdown_delay"]
    LOG.info("Shutting down in %s seconds...", delay)
    reactor.callLater(delay, reactor.stop)


def get_data_filepath(filename):
    """Return full path to a given filename resource."""
    testfn = os.path.join(os.path.dirname(__file__), "data", filename)
    if not os.path.isfile(testfn):
        raise FileNotFoundError(f"could not locate table file {testfn}")
    return testfn


def load_settings():
    """Attempt to locate our configuration file."""
    dirpaths = [
        os.getcwd(),
        os.environ.get("PYWWA_HOME"),
        os.path.join(os.path.expanduser("~"), "etc"),
        os.path.join(os.path.expanduser("~"), "pyWWA"),
    ]
    for dirpath in dirpaths:
        if dirpath is None:
            continue
        testfn = os.path.join(dirpath, SETTINGS_FN)
        if not os.path.isfile(testfn):
            continue
        with open(testfn, encoding="utf-8") as fh:
            SETTINGS.update(json.load(fh))
