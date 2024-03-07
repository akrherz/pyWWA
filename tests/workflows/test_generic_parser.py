"""Test generic_parser."""

# 3rd Party
import pytest

# Local
import pywwa
from pyiem.util import utc
from pywwa.testing import get_example_file
from pywwa.workflows import generic


@pytest.mark.parametrize("database", ["postgis"])
def test_aww(cursor):
    """Test AWW parser."""
    data = get_example_file("AWW.txt")
    pywwa.CTX["utcnow"] = utc(2022, 9, 9, 21, 34)
    prod = generic.process_data({}, cursor, data)
    assert not prod.warnings
    prod = generic.process_data({}, cursor, data.replace("092245", "0z0z0z"))
    assert prod.warnings
