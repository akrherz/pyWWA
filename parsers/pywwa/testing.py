"""Utility Functions."""
# stdlib
import inspect
import os


def get_example_file(filename, justfp=False):
    """Return the contents of an example file."""
    # File is relative to calling file?
    callingfn = os.path.abspath(inspect.stack()[1].filename)
    fullpath = os.path.join(
        os.path.dirname(callingfn), "..", "..", "examples", filename
    )
    if justfp:
        return open(fullpath, "rb")
    # Need to ensure that CRCRLF remain intact
    return open(fullpath, "rb").read().decode("utf-8")
