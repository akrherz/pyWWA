"""XTEUS."""

# Local
from pywwa.workflows import xteus_parser
from pywwa.testing import get_example_file


def test_xteus():
    """Test XTEUS"""
    data = get_example_file("XTEUS.txt")
    res = xteus_parser.process_data(data.replace("\003", ""))
    assert res is not None
