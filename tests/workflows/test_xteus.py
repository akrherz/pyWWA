"""XTEUS."""

# Local
from pywwa.testing import get_example_file
from pywwa.workflows import xteus


def test_xteus():
    """Test XTEUS"""
    data = get_example_file("XTEUS.txt")
    res = xteus.process_data(data.replace("\003", ""))
    assert res is not None
