"""Test (legacy) ERO parsing."""

import pytest
from pywwa.testing import get_example_file
from pywwa.workflows import ero


@pytest.mark.parametrize("database", ["postgis"])
def test_basic(cursor):
    """Test goodline"""
    prod = ero.real_parser(cursor, get_example_file("RBG94E.txt"))
    ero.do_jabber(prod)
    assert prod is not None
