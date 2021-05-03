"""Test dsm_parser."""
# 3rd Party
import pytest
from pyiem.util import utc

# Local
import pywwa
from pywwa.workflows import dsm_parser
from pywwa.testing import get_example_file


@pytest.mark.parametrize("database", ["mesosite"])
def test_load_stations(cursor):
    """Test station loading."""
    dsm_parser.load_stations(cursor)


@pytest.mark.parametrize("database", ["iem"])
def test_processor(cursor):
    """Test basic parsing."""
    data = get_example_file("DSM.txt")
    pywwa.CTX.utcnow = utc(2011, 11, 27, 6, 16)
    dsm_parser.real_parser(cursor, data)
