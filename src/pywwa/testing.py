"""Utility Functions."""

# stdlib
import inspect
import os


def get_example_filepath(filename):
    """Return a full filepath."""
    # File is relative to calling file?
    callingfn = os.path.abspath(inspect.stack()[1].filename)
    return os.path.join(
        os.path.dirname(callingfn), "..", "..", "examples", filename
    )


def get_example_file(filename):
    """Return the contents of an example file."""
    fullpath = get_example_filepath(filename)
    # Need to ensure that CRCRLF remain intact
    with open(fullpath, "rb") as fh:
        data = fh.read().decode("utf-8")
    return data
