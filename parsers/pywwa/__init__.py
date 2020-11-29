"""pyWWA local module."""
# stdlib
import inspect
import json
import os

# Shared configuration
SETTINGS = {}


def get_table_file(filename):
    """Return file pointer for a given table file."""
    for path in get_search_paths():
        testfn = os.path.join(path, "tables", filename)
        if os.path.isfile(testfn):
            return open(testfn)
    raise FileNotFoundError(f"could not locate table file {filename}")


def get_search_paths():
    """Return a list of places to look for auxillary files."""
    # 1. `cwd`/pyWWA
    # 2. `cwd`
    res = ["pyWWA", "."]
    # 3. scriptloc/..
    # 4. scriptloc/../..
    scriptpath = os.path.dirname(os.path.abspath(inspect.stack()[-1].filename))
    for relpath in ["..", "../.."]:
        res.append(os.path.join(scriptpath, relpath))
    return res


def load_config():
    """Attempt to locate our configuration file."""
    for path in get_search_paths():
        testfn = os.path.join(path, "settings.json")
        if not os.path.isfile(testfn):
            continue
        return json.load(open(testfn))
    return {}


CONFIG = load_config()
