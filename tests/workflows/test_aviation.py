"""Test aviation."""
# 3rd Party
from pyiem.util import utc
import pytest

# Local
from pywwa import common
from pywwa.workflows import aviation
from pywwa.testing import get_example_file


@pytest.mark.parametrize("database", ["postgis"])
def test_processor(cursor):
    """Test basic parsing."""
    data = get_example_file("SIGC.txt")
    common.CTX.utcnow = utc(2000, 11, 13, 6, 55)
    aviation.load_database(cursor)
    prod = aviation.process_data(data)
    # 1. Create a bad loc
    aviation.process_data(data.replace(" ORL", " ZZZ"))
    # 2. Attempt Jabber
    aviation.final_step(None, prod)
    # 3. call onready
    aviation.onready(None)
