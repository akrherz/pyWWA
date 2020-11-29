"""Utility Functions."""
# stdlib
import inspect
import os

# 3rd Party
import pytest
from psycopg2.extras import RealDictCursor

# Locals
from pywwa.common import get_sync_dbconn


def get_example_file(filename):
    """Return the contents of an example file."""
    # File is relative to calling file?
    callingfn = os.path.abspath(inspect.stack()[1].filename)
    fullpath = os.path.join(
        os.path.dirname(callingfn), "..", "..", "examples", filename
    )
    return open(fullpath).read()


@pytest.fixture(scope="function")
def cursor(database):
    """Return a disposable database cursor."""
    pgconn = get_sync_dbconn(database)
    yield pgconn.cursor(cursor_factory=RealDictCursor)
    pgconn.close()
