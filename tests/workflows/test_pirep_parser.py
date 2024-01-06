"""Test pirep_parser."""
# 3rd Party
import pytest
from pywwa.testing import get_example_file

# Local
from pywwa.workflows import pirep


@pytest.mark.parametrize("database", ["postgis"])
def test_real_parser(cursor):
    """Test that we can parse PIREPs!"""
    pirep.real_parser(cursor, get_example_file("PIREP.txt"))
    pirep.cleandb()


@pytest.mark.parametrize("database", ["mesosite"])
def test_load_locs(cursor):
    """Test that we can load our stations table."""
    pirep.load_locs(cursor)


def test_ready():
    """Test our ready method."""
    pirep.ready(None)
