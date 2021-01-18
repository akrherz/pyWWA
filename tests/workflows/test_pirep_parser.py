"""Test pirep_parser."""
# 3rd Party
import pytest

# Local
from pywwa.workflows import pirep_parser
from pywwa.testing import get_example_file


@pytest.mark.parametrize("database", ["postgis"])
def test_real_parser(cursor):
    """Test that we can parse PIREPs!"""
    pirep_parser.real_parser(cursor, get_example_file("PIREP.txt"))
    pirep_parser.cleandb()


@pytest.mark.parametrize("database", ["mesosite"])
def test_load_locs(cursor):
    """Test that we can load our stations table."""
    pirep_parser.load_locs(cursor)


def test_ready():
    """Test our ready method."""
    pirep_parser.ready(None)
