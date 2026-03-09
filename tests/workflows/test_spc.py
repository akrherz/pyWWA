"""Test spc."""

import pytest
import pytest_twisted
from pyiem.util import utc

import pywwa
from pywwa.testing import get_example_file
from pywwa.workflows import spc


@pytest.mark.parametrize("database", ["postgis"])
@pytest_twisted.inlineCallbacks
def test_processor(cursor):
    """Test basic parsing."""
    data = get_example_file("PTSDY1.txt")
    pywwa.CTX["utcnow"] = utc(2021, 3, 25, 13, 8)
    pywwa.CTX["disable_xmpp"] = True
    prod = spc.real_parser(cursor, data)
    yield spc.do_jabber(prod)
