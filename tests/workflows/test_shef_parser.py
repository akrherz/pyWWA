"""Test shef_parser."""
from functools import partial

# 3rd Party
# pylint: disable=no-member-in-module
import mock
import pytest

# Local
import pywwa
from psycopg.errors import DeadlockDetected
from pyiem.util import utc
from pywwa.testing import get_example_file
from pywwa.workflows import shef_parser
from twisted.python.failure import Failure


def run_interaction(cursor, sql, args):
    """Make runOperation go."""
    cursor.execute(sql, args)
    defer = mock.Mock()
    defer.addErrback = lambda x, y: None
    return defer


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


def test_empty_product():
    """Exercise the API."""
    pywwa.CTX.utcnow = utc(2023, 9, 15, 13)
    prod = shef_parser.process_data(get_example_file("SHEF/RR7KRF.txt"))
    assert not prod.data and not prod.warnings


@pytest.mark.parametrize("database", ["hads"])
def test_enter_unknown(cursor):
    """Insert unknown station."""
    shef_parser.enter_unknown(cursor, "AMSI4", "XX", "XX")
    shef_parser.enter_unknown(cursor, "XX_DCP123456", "XX", "XX")


@pytest.mark.parametrize("database", ["hads"])
def test_230926_rr8krf(cursor):
    """Test a smallint error this found in production."""
    pywwa.CTX.utcnow = utc(2023, 9, 26, 18)
    prod = shef_parser.process_data(get_example_file("SHEF/RR8KRF.txt"))
    for element in prod.data:
        shef_parser.insert_raw_inbound(cursor, element)


@pytest.mark.parametrize("database", ["iem"])
def test_omit_report(cursor):
    """Test that the report is omitted..."""
    shef_parser.CURRENT_QUEUE.clear()
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
    assert row["max_tmpf"] == 83
    assert row["report"] is not None
    ans = row["report"]
    prod = shef_parser.process_data(get_example_file("CORM6_RTP.txt"))
    sync_workflow(prod, cursor)
    cursor.execute(
        "SELECT report, max_tmpf from summary_2023 "
        "where iemid = -99 and day = '2023-05-12'"
    )
    row = cursor.fetchone()
    assert row["max_tmpf"] == 84
    assert row["report"] == ans

    shef_parser.ACCESSDB.runOperation = partial(run_interaction, cursor)
    assert shef_parser.save_current() == 7


@pytest.mark.parametrize("database", ["iem"])
def test_process_site_eb(cursor):
    """Test that the errorback works without any side effects."""
    pywwa.CTX.utcnow = utc(2017, 8, 15, 14)
    prod = shef_parser.process_data(get_example_file("RR7.txt"))
    sync_workflow(prod, cursor)
    shef_parser.process_site_eb(Failure(Exception("Hi Daryl")), prod, "", {})
    shef_parser.process_site_eb(Failure(DeadlockDetected()), prod, "", {})


def test_restructure_data_future():
    """Ensure that data from the future stays out!"""
    pywwa.CTX.utcnow = utc(2017, 8, 14)
    prod = shef_parser.process_data(get_example_file("RR7.txt"))
    res = shef_parser.restructure_data(prod)
    assert len(res.keys()) == 0
    pywwa.CTX.utcnow = utc()
    res = shef_parser.restructure_data(prod)
    assert len(res.keys()) == 0


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
