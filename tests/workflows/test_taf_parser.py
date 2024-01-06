"""Test taf_parser."""
# 3rd Party
import pytest
from pywwa.testing import get_example_file

# Local
from pywwa.workflows import taf


@pytest.mark.parametrize("database", ["asos"])
def test_real_parser(cursor):
    """Test that we can parse TAFS!"""
    taf.real_process(cursor, get_example_file("TAF.txt"))
