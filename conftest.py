"""For testing."""
# 3rd party
import pytest
from psycopg2.extras import DictCursor

# Local
from pywwa.database import get_sync_dbconn


@pytest.fixture(scope="function")
def cursor(database):
    """Return a disposable database cursor."""
    pgconn = get_sync_dbconn(database)
    yield pgconn.cursor(cursor_factory=DictCursor)
    pgconn.close()
