"""For testing."""
# 3rd party
import pytest
import pywwa
from pyiem.util import get_dbconnc


@pytest.fixture(autouse=True)
def setup_and_teardown():
    """Something to fix the hacky global namespace mucking I do."""
    pywwa.CTX.utcnow = None
    yield
    pywwa.CTX.utcnow = None


@pytest.fixture(scope="function")
def cursor(database):
    """Return a disposable database cursor."""
    pgconn, cursor = get_dbconnc(database)
    yield cursor
    pgconn.close()
