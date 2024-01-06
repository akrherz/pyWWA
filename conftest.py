"""For testing."""
# 3rd party
import pytest
import pywwa
from pywwa.database import get_dbconnc


@pytest.fixture(autouse=True)
def setup_and_teardown():
    """Something to fix the hacky global namespace mucking I do."""
    pywwa.CTX.update(pywwa.CTX_DEFAULTS)
    yield
    pywwa.CTX.update(pywwa.CTX_DEFAULTS)


@pytest.fixture(scope="function")
def cursor(database):
    """Return a disposable database cursor."""
    pgconn, cursor = get_dbconnc(database)
    yield cursor
    pgconn.close()
