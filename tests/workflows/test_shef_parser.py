"""Test shef_parser."""

# 3rd Party
import pytest
from pyiem.util import utc

# Local
import pywwa
from pywwa.workflows import shef_parser
from pywwa.testing import get_example_file


def test_restructure_data_future():
    """Ensure that data from the future stays out!"""
    pywwa.CTX.utcnow = utc(2017, 8, 16)
    prod = shef_parser.process_data(get_example_file("RR7.txt"))
    res = shef_parser.restructure_data(prod)
    assert res
    pywwa.CTX.utcnow = utc()
    res = shef_parser.restructure_data(prod)
    assert not res


def test_process_data():
    """Test that we can really_process a product!"""
    pywwa.CTX.utcnow = utc(2020, 9, 15)
    prod = shef_parser.process_data(get_example_file("RR7.txt"))
    assert prod.get_product_id() == "202009151244-KWOH-SRUS27-RRSKRF"


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


def test_api():
    """Exercise the API."""
    shef_parser.log_database_queue_size()
