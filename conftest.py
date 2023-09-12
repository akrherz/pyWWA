"""For testing."""
# 3rd party
import pytest
from pyiem.util import get_dbconnc


@pytest.fixture(scope="function")
def cursor(database):
    """Return a disposable database cursor."""
    pgconn, cursor = get_dbconnc(database)
    yield cursor
    pgconn.close()
