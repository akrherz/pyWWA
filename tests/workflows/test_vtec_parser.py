"""Test vtec_parser."""

from pywwa.testing import get_example_file
from pywwa.workflows import vtec


def test_parser():
    """Test that we can parse!"""
    vtec.process_data(get_example_file("TOR.txt"))
