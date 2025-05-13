"""Test shef_parser."""

import datetime
from functools import partial

# 3rd Party
# pylint: disable=no-member-in-module
from unittest import mock
from zoneinfo import ZoneInfo

import pytest

# Local
from psycopg.errors import DeadlockDetected
from pyiem.util import utc
from twisted.python.failure import Failure

from pywwa import CTX, SETTINGS
from pywwa.testing import get_example_file
from pywwa.workflows import shef

# Ensure that we have a database to work with
shef.build_context()


def run_interaction(cursor, sql, args):
    """Make runOperation go."""
    cursor.execute(sql, args)
    defer = mock.Mock()
    defer.addErrback = lambda x, y: None
    return defer


def sync_workflow(prod, cursor):
    """Common workflow."""
    mydata = shef.restructure_data(prod)
    for sid, data in mydata.items():
        shef.process_site(prod, sid, data)
    # Just exercise the API
    shef.process_accessdb_frontend()

    # Arm the entries for processing
    ts = utc() - datetime.timedelta(days=1)
    for iemid, entry in shef.ACCESSDB_QUEUE.items():
        for localts, record in entry.records.items():
            record["last"] = ts
            shef.write_access_records(
                cursor, [(localts, record)], iemid, entry
            )
    # Just exercise the API
    shef.process_accessdb_frontend()


def test_accessdb_exception():
    """Test some GIGO raises an exception."""
    shef.ACCESSDB_QUEUE[-99] = 0
    shef.process_accessdb_frontend()
    shef.ACCESSDB_QUEUE.pop(-99)


def test_missing_value():
    """Test that this missing value flags a database write to null_ col"""
    shef.LOCS["ALBW3"] = {
        "WI_DCP": {
            "valid": shef.U1980,
            "iemid": -99,
            "tzinfo": ZoneInfo("America/Chicago"),
            "epoc": 1,
            "pedts": 1,
        }
    }
    CTX["utcnow"] = utc(2023, 10, 28, 3, 40)
    shef.process_data(get_example_file("SHEF/RRSNMC.txt"))


def test_exclude():
    """Test the exclusion logic."""
    SETTINGS["pywwa_shef_afos_exclude"] = "RRXXXX"
    shef.build_context()
    payload = get_example_file("SHEF/RR1.txt").replace("RR1DSM", "RRXXXX")
    prod = shef.process_data(payload)
    assert not prod.data


def test_midnight():
    """Test that a midnight report gets moved back one minute."""
    shef.LOCS["AISI4"] = {
        "IA_DCP": {
            "valid": shef.U1980,
            "iemid": -99,
            "tzinfo": ZoneInfo("America/Chicago"),
            "epoc": 1,
            "pedts": 1,
        }
    }
    CTX["utcnow"] = utc(2022, 11, 10, 12)
    payload = get_example_file("SHEF/RR1.txt").replace("1150", "0600")
    shef.process_data(payload)
    assert shef.ACCESSDB_QUEUE[-99].records[utc(2022, 11, 10, 5, 59)]


def test_old_dcpdata():
    """Test that old DCP data is not sent to the database."""
    shef.LOCS["AISI4"] = {
        "IA_DCP": {
            "valid": shef.U1980,
            "iemid": -99,
            "tzinfo": ZoneInfo("America/Chicago"),
            "epoc": 1,
            "pedts": 1,
        }
    }
    CTX["utcnow"] = utc(2022, 11, 10, 12)
    shef.process_data(get_example_file("SHEF/RR1.txt"))
    # rewind to 2021
    CTX["utcnow"] = utc(2021, 11, 10, 12)
    shef.process_data(get_example_file("SHEF/RR1.txt"))


def test_get_localtime():
    """Test when we don't know of a station."""
    tzinfo = ZoneInfo("America/Chicago")
    assert shef.get_localtime("XX_DCP123456", tzinfo) == tzinfo


def test_231027_rr8msr_novariable():
    """Test this found in the wild."""
    CTX["utcnow"] = utc(2023, 10, 19, 15)
    prod = shef.process_data(get_example_file("SHEF/RR8MSR.txt"))
    assert not prod.data


def test_empty_product():
    """Exercise the API."""
    CTX["utcnow"] = utc(2023, 9, 15, 13)
    prod = shef.process_data(get_example_file("SHEF/RR7KRF.txt"))
    assert not prod.data and not prod.warnings


def test_enter_unknown_populates():
    """Test that processing a SHEF file with an unknown actually populates."""
    CTX["utcnow"] = utc(2023, 9, 26, 18)
    payload = get_example_file("SHEF/RR8KRF.txt").replace("PLMW4", "ZZZW4")
    shef.process_data(payload)
    assert shef.UNKNOWN["ZZZW4"]


@pytest.mark.parametrize("database", ["hads"])
def test_enter_unknown(cursor):
    """Insert unknown station."""
    shef.enter_unknown(cursor, "AMSI4", "XX", "XX")
    shef.enter_unknown(cursor, "XX_DCP123456", "XX", "XX")


@pytest.mark.parametrize("database", ["hads"])
def test_230926_rr8krf(cursor):
    """Test a smallint error this found in production."""
    CTX["utcnow"] = utc(2023, 9, 26, 18)
    prod = shef.process_data(get_example_file("SHEF/RR8KRF.txt"))
    for element in prod.data:
        args = (
            element.station,
            element.valid,
            element.varname(),
            element.num_value,
            element.depth,
            element.unit_convention,
            element.dv_interval,
            element.qualifier,
        )
        shef.insert_raw_inbound(cursor, args)


def test_bad_element_in_current_queue():
    """Test GIGO on current_queue."""
    shef.CURRENT_QUEUE.clear()
    shef.CURRENT_QUEUE["HI|BYE|HI"] = {"dirty": True}
    shef.save_current()


@pytest.mark.parametrize("database", ["iem"])
def test_omit_report(cursor):
    """Test that the report is omitted..."""
    shef.CURRENT_QUEUE.clear()
    CTX["utcnow"] = utc(2023, 5, 12, 18)
    cursor.execute(
        "INSERT into stations(id, network, tzname, iemid) VALUES "
        "('CORM6', 'MS_DCP', 'America/Chicago', -99)"
    )

    shef.LOCS["CORM6"] = {
        "MS_COOP": {
            "valid": shef.U1980,
            "iemid": -99,
            "tzinfo": ZoneInfo("America/Chicago"),
            "epoc": 1,
            "pedts": 1,
        }
    }
    prod = shef.process_data(get_example_file("CORM6.txt"))
    sync_workflow(prod, cursor)
    cursor.execute(
        "SELECT report, max_tmpf from summary_2023 "
        "where iemid = -99 and day = '2023-05-12'"
    )
    row = cursor.fetchone()
    assert row["max_tmpf"] == 83
    assert row["report"] is not None
    ans = row["report"]
    prod = shef.process_data(get_example_file("CORM6_RTP.txt"))
    sync_workflow(prod, cursor)
    cursor.execute(
        "SELECT report, max_tmpf from summary_2023 "
        "where iemid = -99 and day = '2023-05-12'"
    )
    row = cursor.fetchone()
    assert row["max_tmpf"] == 84
    assert row["report"] == ans

    CTX["ACCESSDB"].runOperation = partial(run_interaction, cursor)
    assert shef.save_current() == 7
    assert shef.save_current() == 0


def test_process_site_eb():
    """Test that the errorback works without any side effects."""
    CTX["utcnow"] = utc(2017, 8, 15, 14)
    shef.process_data(get_example_file("RR7.txt"))
    entry = shef.ACCESSDB_ENTRY(
        station="", network="", tzinfo=ZoneInfo("America/Chicago"), records={}
    )
    record = {"data": {}, "last": utc(), "product_id": ""}
    shef.write_access_records_eb(
        Failure(Exception("Hi Daryl")), [(utc(), record)], 0, entry
    )
    shef.write_access_records_eb(
        Failure(DeadlockDetected()), [(utc, record)], 0, entry
    )


def test_checkvars():
    """Excerise the checkvars logic with the RR1.txt example"""
    # Define an entry with two networks so to make life choices
    shef.LOCS["AISI4"] = {
        "IA_COOP": {
            "valid": shef.U1980,
            "iemid": -99,
            "tzinfo": ZoneInfo("America/Chicago"),
            "epoc": 1,
            "pedts": 1,
        },
        "ISUSM": {
            "valid": shef.U1980,
            "iemid": -99,
            "tzinfo": ZoneInfo("America/Chicago"),
            "epoc": 1,
            "pedts": 1,
        },
    }
    payload = get_example_file("SHEF/RR1.txt")
    CTX["utcnow"] = utc(2022, 11, 10, 12)
    for repl in ["TX", "HG", "SF", "PPH"]:
        shef.process_data(payload.replace("/TX", f"/{repl}"))
    shef.process_data(payload.replace("RR1", "RR3"))
    shef.LOCS["AISI4"].pop("IA_COOP")
    shef.process_data(payload)


def test_restructure_data_eightchar_id():
    """Test that we omit a greather than 8 char station ID."""
    CTX["utcnow"] = utc(2017, 8, 15, 13)
    prod = shef.process_data(get_example_file("RR7.txt"))
    res = shef.restructure_data(prod)
    assert all(len(x) <= 8 for x in res)


def test_restructure_data_future():
    """Ensure that data from the future stays out!"""
    CTX["utcnow"] = utc(2017, 8, 14)
    prod = shef.process_data(get_example_file("RR7.txt"))
    res = shef.restructure_data(prod)
    assert len(res) == 0
    CTX["utcnow"] = utc()
    res = shef.restructure_data(prod)
    assert len(res) == 0


def test_process_data():
    """Test that we can really_process a product!"""
    CTX["utcnow"] = utc(2020, 9, 15)
    prod = shef.process_data(get_example_file("RR7.txt"))
    assert prod.get_product_id() == "202009151244-KWOH-SRUS27-RRSKRF"


def test_mydict():
    """Test what the custom dict class of MyDict can do."""
    d = shef.MyDict()
    d["HI"] = "HI"
    assert d["HI"] == "HI"
    assert d["TAIRRXZ"] == "max_tmpf"
    assert d["QQQQQQQ"] == ""


@pytest.mark.parametrize("database", ["mesosite"])
def test_load_stations(cursor):
    """Test our load_stations function."""
    shef.UNKNOWN["_X_X_"] = True
    # Create an entry with a bad timezone
    cursor.execute(
        "INSERT into stations(id, network, tzname) VALUES "
        "('_X_X_', 'XX_DCP', 'XXX')"
    )
    cursor.execute(
        "INSERT into stations(id, network, tzname) VALUES "
        "('_Y_Y_', 'XX_DCP', '')"
    )
    shef.load_stations(cursor)
    assert isinstance(shef.LOCS, dict)
    # Remove station from database
    cursor.execute(
        "DELETE from stations where id = '_X_X_' and network = 'XX_DCP'"
    )
    shef.load_stations(cursor)
    assert "_X_X_" not in shef.LOCS


def test_load_stations_fe():
    """Test the blocking frontend."""
    shef.load_stations_fe(True)
    shef.load_stations_fe(False)


def test_api():
    """Exercise the API."""
    shef.log_database_queue_size()


def test_lc_proxy_failure():
    """Call something that fails."""

    def _failure():
        raise Exception("Hi Daryl")

    shef.lc_proxy(_failure)
