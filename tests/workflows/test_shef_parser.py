"""Test shef_parser."""
# stdlib
import mock

# 3rd Party
import pytest
from pyiem.nws.product import TextProduct

# Local
from pywwa.workflows import shef_parser
from pywwa.testing import get_example_file


def test_exercise_shefit():
    """Call the API, but not actually test too much of it :("""
    shefit = shef_parser.SHEFIT(TextProduct(get_example_file("RR7.txt")))
    shefit.transport = mock.Mock()
    shefit.deferred = mock.Mock()
    shefit.connectionMade()
    shefit.outReceived(b"Hi")
    shefit.errReceived("hi")
    shefit.outConnectionLost()


def test_clnstr():
    """Test that we clean a string!"""
    assert shef_parser.clnstr("HI") == "HI"


def test_mydict():
    """Test what the custom dict class of MyDict can do."""
    d = shef_parser.MyDict()
    d["HI"] = "HI"
    assert d["HI"] == "HI"
    assert d["TAIRRXZ"] == "max_tmpf"
    assert d["QQQQQQQ"] == ""


@pytest.mark.parametrize("database", ["mesosite"])
def test_load_stations(cursor):
    """Test our load_stations function."""
    # Create an entry with a bad timezone
    cursor.execute(
        "INSERT into stations(id, network, tzname) VALUES "
        "('_X_X_', 'XX_DCP', 'XXX')"
    )
    shef_parser.load_stations(cursor)
    assert isinstance(shef_parser.LOCS, dict)


def test_main2():
    """Exercise the API."""
    shef_parser.main2(None)


def test_servicegaurd():
    """Exercise the API."""
    shef_parser.JOBS.pending = list(range(1001))
    shef_parser.service_guard()
