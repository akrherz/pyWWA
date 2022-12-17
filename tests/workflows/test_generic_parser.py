"""Test generic_parser."""
# 3rd Party
import pytest
from pyiem.util import utc

# Local
import pywwa
from pywwa.workflows import generic_parser
from pywwa.testing import get_example_file


@pytest.mark.parametrize("database", ["postgis"])
def test_aww(cursor):
    """Test AWW parser."""
    data = get_example_file("AWW.txt")
    pywwa.CTX.utcnow = utc(2022, 9, 9, 21, 34)
    prod = generic_parser.really_process_data(cursor, data)
    assert not prod.warnings
    prod = generic_parser.really_process_data(
        cursor, data.replace("092245", "0z0z0z")
    )
    assert prod.warnings
