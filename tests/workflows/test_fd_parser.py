"""Test fd_parser."""
# 3rd Party
from pyiem.util import utc
import pytest

# Local
import pywwa
from pywwa.workflows import fd_parser
from pywwa.testing import get_example_file


@pytest.mark.parametrize("database", ["asos"])
def test_processor(cursor):
    """Test basic parsing."""
    data = get_example_file("FD1US1.txt")
    pywwa.CTX.utcnow = utc(2023, 3, 9, 1, 59)
    prod = fd_parser.processor(cursor, data)
    assert prod.valid == pywwa.CTX.utcnow
