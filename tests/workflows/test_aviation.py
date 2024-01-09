"""Test aviation."""
# 3rd Party
import pytest

# Local
import pywwa
from pyiem.database import get_dbconnc
from pyiem.util import utc
from pywwa.testing import get_example_file
from pywwa.workflows import aviation


@pytest.mark.parametrize("database", ["postgis"])
def test_processor(cursor):
    """Test basic parsing."""
    mconn, mcursor = get_dbconnc("mesosite")
    data = get_example_file("SIGC.txt")
    pywwa.CTX["utcnow"] = utc(2000, 11, 13, 6, 55)
    aviation.load_database(mcursor)
    mconn.close()
    prod = aviation.process_data(cursor, data)
    # 1. Create a bad loc
    aviation.process_data(cursor, data.replace(" ORL", " ZZZ"))
    # 2. Attempt Jabber
    aviation.final_step(prod)


@pytest.mark.parametrize("database", ["mesosite"])
def test_load_database(cursor):
    """Test our load_database function."""
    aviation.load_database(cursor)
    assert isinstance(aviation.LOCS, dict)
