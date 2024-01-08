"""For testing."""
# 3rd party
import pytest
import pywwa
from pywwa.database import get_dbconnc

# Dragons
# pytest + twisted + click == too much magic for poor me
# So, this effectively disables reactor.stop() from working
pywwa.CTX_DEFAULTS["shutdown_delay"] = 1800


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
