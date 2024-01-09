"""Test aviation."""
# 3rd Party
import pytest

# Local
import pywwa
from pyiem.util import utc
from pywwa.testing import get_example_file
from pywwa.workflows import aviation


@pytest.mark.parametrize("database", ["mesosite"])
def test_processor(cursor):
    """Test basic parsing."""
    data = get_example_file("SIGC.txt")
    pywwa.CTX["utcnow"] = utc(2000, 11, 13, 6, 55)
    aviation.load_database(cursor)
    prod = aviation.process_data(data)
    # 1. Create a bad loc
    aviation.process_data(data.replace(" ORL", " ZZZ"))
    # 2. Attempt Jabber
    aviation.final_step(None, prod)


@pytest.mark.parametrize("database", ["mesosite"])
def test_load_database(cursor):
    """Test our load_database function."""
    aviation.load_database(cursor)
    assert isinstance(aviation.LOCS, dict)
