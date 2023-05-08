"""Test spe_parser."""
# 3rd Party
import pytest

# Local
import pywwa
from pyiem.util import utc
from pywwa.testing import get_example_file
from pywwa.workflows import spe_parser


@pytest.mark.parametrize("database", ["postgis"])
def test_processor(cursor):
    """Test basic parsing."""
    data = get_example_file("SPE.txt")
    pywwa.CTX.utcnow = utc(2011, 10, 31, 1, 16)
    spe_parser.real_process(cursor, data)
