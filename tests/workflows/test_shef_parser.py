"""Test shef_parser."""
# 3rd Party
import pytest

# Local
from pywwa.workflows import shef_parser


@pytest.mark.parametrize("database", ["mesosite"])
def test_load_stations(cursor):
    """Test our load_stations function."""
    shef_parser.load_stations(cursor)
    assert isinstance(shef_parser.LOCS, dict)


def test_main2():
    """Exercise the API."""
    shef_parser.main2(None)


def test_servicegaurd():
    """Exercise the API."""
    shef_parser.JOBS.pending = list(range(1001))
    shef_parser.service_guard()
