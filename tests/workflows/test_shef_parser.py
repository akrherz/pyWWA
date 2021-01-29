"""Test shef_parser."""
# stdlib
import mock

# 3rd Party
import pytest
from pyiem.nws.product import TextProduct
from pyiem.util import utc

# Local
from pywwa.workflows import shef_parser
from pywwa.testing import get_example_file


def test_really_process():
    """Test that we can really_process a product!"""
    prod = TextProduct(get_example_file("RR7.txt"))
    # second argument here is what we get from shefit...
    shef_parser.really_process(prod, "")


def test_make_datetime():
    """Test that we can construct datetimes."""
    answer = utc(2021, 1, 18, 12, 43)
    assert shef_parser.make_datetime("2021-01-18", "12:43:00") == answer
    assert shef_parser.make_datetime("0000-00-00", "") is None


def test_exercise_shefit():
    """Call the API, but not actually test too much of it :("""
    text = get_example_file("RR7.txt")
    shefit = shef_parser.SHEFIT(TextProduct(text))
    shefit.transport = mock.Mock()
    shefit.deferred = mock.Mock()
    shefit.connectionMade()
    shefit.outReceived(b"Hi")
    shefit.errReceived("hi")
    shefit.outConnectionLost()

    shef_parser.async_func(text)
    shef_parser.async_func(None)

    shef_parser.process_data(text)


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
    shef_parser.UNKNOWN["_X_X_"] = True
    # Create an entry with a bad timezone
    cursor.execute(
        "INSERT into stations(id, network, tzname) VALUES "
        "('_X_X_', 'XX_DCP', 'XXX')"
    )
    cursor.execute(
        "INSERT into stations(id, network, tzname) VALUES "
        "('_Y_Y_', 'XX_DCP', '')"
    )
    shef_parser.load_stations(cursor)
    assert isinstance(shef_parser.LOCS, dict)
    # Remove station from database
    cursor.execute(
        "DELETE from stations where id = '_X_X_' and network = 'XX_DCP'"
    )
    shef_parser.load_stations(cursor)
    assert "_X_X_" not in shef_parser.LOCS


def test_main2():
    """Exercise the API."""
    shef_parser.main2(None)


def test_servicegaurd():
    """Exercise the API."""
    shef_parser.JOBS.pending = list(range(1001))
    shef_parser.service_guard()
