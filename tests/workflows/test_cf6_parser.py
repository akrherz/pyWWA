"""Test cf6_parser."""

# 3rd Party
import pytest
from pyiem.util import utc

# Local
import pywwa
from pywwa.testing import get_example_file
from pywwa.workflows import cf6


@pytest.mark.parametrize("database", ["iem"])
def test_processor(cursor):
    """Test basic parsing."""
    data = get_example_file("CF6.txt")
    pywwa.CTX["utcnow"] = utc(2020, 11, 25, 9, 20)
    prod = cf6.processor(cursor, data)
    assert prod.valid == pywwa.CTX["utcnow"]
