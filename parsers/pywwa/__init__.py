"""pyWWA local module."""
# stdlib
import inspect
import json
import os

# Shared configuration
SETTINGS = {}


def load_config():
    """Attempt to locate our configuration file."""
    # 1. `cwd`/pyWWA/settings.json
    # 2. `cwd`/settings.json
    # 3. scriptloc/../settings.json
    # 4. scriptloc/../../settings.json
    for relpath in ["pyWWA", ""]:
        testfn = os.path.join(os.getcwd(), relpath, "settings.json")
        if not os.path.isfile(testfn):
            continue
        return json.load(open(testfn))
    scriptpath = os.path.dirname(os.path.abspath(inspect.stack()[-1].filename))
    for relpath in ["..", "../.."]:
        testfn = os.path.join(scriptpath, relpath, "settings.json")
        if not os.path.isfile(testfn):
            continue
        return json.load(open(testfn))
    return {}


CONFIG = load_config()
