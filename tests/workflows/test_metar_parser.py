"""Test metar_parser."""
# 3rd Party
import pytest

# Local
import pywwa
from pyiem.util import utc
from pywwa.testing import get_example_file
from pywwa.workflows import metar


def create_entries(cursor):
    """Return a disposable database cursor."""
    # Create fake station, so we can create fake entry in summary
    # and current tables
    cursor.execute(
        "INSERT into stations(id, network, iemid, tzname) "
        "VALUES ('QQQQ', 'FAKE', -1, 'America/Chicago')"
    )
    cursor.execute(
        "INSERT into current(iemid, valid) VALUES (-1, '2015-09-01 00:00+00')"
    )
    cursor.execute(
        "INSERT into summary_2015(iemid, day) VALUES (-1, '2015-09-01')"
    )


def test_api():
    """Test that we can load things to ignore."""
    metar.load_ignorelist()
    metar.cleandb()


@pytest.mark.parametrize("database", ["iem"])
def test_processor(cursor):
    """Test basic parsing."""
    create_entries(cursor)
    data = get_example_file("METAR.txt")
    pywwa.CTX["utcnow"] = utc(2011, 11, 25, 14)
    metadata = {"iemid": -1, "tzname": "America/Chicago"}
    for mtr in metar.process_data(data).metars:
        metar.do_db(cursor, mtr, metadata)
