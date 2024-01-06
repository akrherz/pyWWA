"""Test dsm_parser."""
# 3rd Party
import pytest

# Local
import pywwa
from pyiem.util import utc
from pywwa.testing import get_example_file
from pywwa.workflows import dsm


@pytest.mark.parametrize("database", ["mesosite"])
def test_load_stations(cursor):
    """Test station loading."""
    # Need to set one station to a bad tzname to exercise an exception
    cursor.execute(
        "UPDATE stations SET tzname = 'BoG0S' where id = 'MSP' and "
        "network = 'MN_ASOS'"
    )
    assert cursor.rowcount == 1
    dsm.load_stations(cursor)


@pytest.mark.parametrize("database", ["iem"])
def test_processor(cursor):
    """Test basic parsing."""
    data = get_example_file("DSM.txt")
    pywwa.CTX["utcnow"] = utc(2011, 11, 27, 6, 16)
    dsm.real_parser(cursor, data)
