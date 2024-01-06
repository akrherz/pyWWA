"""Test fd_parser."""
# 3rd Party
import pytest

# Local
import pywwa
from pyiem.util import utc
from pywwa.testing import get_example_file
from pywwa.workflows import fd


@pytest.mark.parametrize("database", ["asos"])
def test_processor(cursor):
    """Test basic parsing."""
    data = get_example_file("FD1US1.txt")
    pywwa.CTX["utcnow"] = utc(2023, 3, 9, 1, 59)
    prod = fd.processor(cursor, data)
    assert prod.valid == pywwa.CTX["utcnow"]
