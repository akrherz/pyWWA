"""XTEUS."""

import pytest
from pywwa.testing import get_example_file
from pywwa.workflows import xteus


@pytest.mark.parametrize("database", ["iem"])
def test_xteus(cursor):
    """Test XTEUS"""
    data = get_example_file("XTEUS.txt")
    res = xteus.process_data(cursor, data.replace("\003", ""))
    assert res is not None
