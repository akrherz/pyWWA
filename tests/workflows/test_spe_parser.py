"""Test spe_parser."""
# 3rd Party
import pytest
from pyiem.util import utc

# Local
from pywwa.workflows import spe_parser
from pywwa.testing import get_example_file


@pytest.mark.parametrize("database", ["postgis"])
def test_processor(cursor):
    """Test basic parsing."""
    data = get_example_file("SPE.txt")
    spe_parser.common.CTX.utcnow = utc(2011, 10, 31, 1, 16)
    spe_parser.real_process(cursor, data)
