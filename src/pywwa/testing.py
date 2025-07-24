"""Utility Functions."""

# stdlib
import os


def get_example_filepath(filename):
    """Return a full filepath."""
    return f"{os.getcwd()}/examples/{filename}"


def get_example_file(filename):
    """Return the contents of an example file."""
    fullpath = get_example_filepath(filename)
    # Need to ensure that CRCRLF remain intact
    with open(fullpath, "rb") as fh:
        return fh.read().decode("utf-8")
