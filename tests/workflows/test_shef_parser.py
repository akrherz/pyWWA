"""Test shef_parser."""

# 3rd Party
# pylint: disable=no-member-in-module
import pytest

# Local
import pywwa
from psycopg2.errors import DeadlockDetected
from pyiem.util import utc
from pywwa.testing import get_example_file
from pywwa.workflows import shef_parser
from twisted.python.failure import Failure


def sync_workflow(prod, cursor):
    """Common workflow."""
    mydata = shef_parser.restructure_data(prod)
    print(mydata)
    for sid, data in mydata.items():
        times = list(data.keys())
        times.sort()
        for tstamp in times:
            print(tstamp)
            shef_parser.process_site_time(
                cursor, prod, sid, tstamp, data[tstamp]
            )


@pytest.mark.parametrize("database", ["iem"])
def test_omit_report(cursor):
    """Test that the report is omitted..."""
    pywwa.CTX.utcnow = utc(2023, 5, 12, 18)
    cursor.execute(
        "INSERT into stations(id, network, tzname, iemid) VALUES "
        "('CORM6', 'MS_DCP', 'America/Chicago', -99)"
    )

    shef_parser.LOCS["CORM6"] = {
        "MS_COOP": {
            "valid": shef_parser.U1980,
            "iemid": -99,
            "tzname": "America/Chicago",
            "epoc": 1,
            "pedts": 1,
        }
    }
    prod = shef_parser.process_data(get_example_file("CORM6.txt"))
    sync_workflow(prod, cursor)
    cursor.execute(
        "SELECT report, max_tmpf from summary_2023 "
        "where iemid = -99 and day = '2023-05-12'"
    )
    row = cursor.fetchone()
    assert row[1] == 83
    assert row[0] is not None
    ans = row[0]
    prod = shef_parser.process_data(get_example_file("CORM6_RTP.txt"))
    sync_workflow(prod, cursor)
    cursor.execute(
        "SELECT report, max_tmpf from summary_2023 "
        "where iemid = -99 and day = '2023-05-12'"
    )
    row = cursor.fetchone()
    assert row[1] == 84
    assert row[0] == ans


def test_process_site_eb():
    """Test that the errorback works without any side effects."""
    prod = shef_parser.process_data(get_example_file("RR7.txt"))
    shef_parser.process_site_eb(Failure(Exception("Hi Daryl")), prod, "", {})
    shef_parser.process_site_eb(Failure(DeadlockDetected()), prod, "", {})


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
