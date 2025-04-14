"""Test scp_parser."""

# 3rd Party
import pytest
from pyiem.util import utc

# Local
import pywwa
from pywwa.testing import get_example_file
from pywwa.workflows import scp


@pytest.mark.parametrize("database", ["asos"])
def test_processor(cursor):
    """Test basic parsing."""
    data = get_example_file("SCPPR2.txt")
    pywwa.CTX["utcnow"] = utc(2024, 10, 30, 1, 16)
    scp.real_process(cursor, data)
