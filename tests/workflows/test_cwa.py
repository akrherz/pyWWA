"""parse CWA"""

import pytest

from pywwa.database import get_dbconnc
from pywwa.testing import get_example_file
from pywwa.workflows import cwa


@pytest.mark.parametrize("database", ["postgis"])
def test_goodline(cursor):
    """Test goodline"""
    conn, mcursor = get_dbconnc("mesosite")
    cwa.load_database(mcursor)
    conn.close()
    prod = cwa.process_data(cursor, get_example_file("CWA.txt"))
    assert prod is not None
