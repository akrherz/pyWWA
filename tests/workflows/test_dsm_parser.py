"""Test dsm_parser."""

import pytest
from pyiem.util import utc

import pywwa
from pywwa.testing import get_example_file
from pywwa.workflows.dsm import (
    MEMORY,
    load_stations,
    real_parser,
)


@pytest.mark.parametrize("database", ["iem"])
def test_station_reload(cursor, caplog, disable_email):
    """Test station reload logic."""
    MEMORY["stations_loaded"] = utc(2000, 1, 1)
    data = get_example_file("DSM.txt")
    pywwa.CTX["utcnow"] = utc(2011, 11, 27, 6, 16)
    real_parser(cursor, data)
    MEMORY["stations_loaded"] = utc()
    assert "Reloading station metadata" in caplog.text


@pytest.mark.parametrize("database", ["mesosite"])
def test_load_stations(cursor):
    """Test station loading."""
    # Need to set one station to a bad tzname to exercise an exception
    cursor.execute(
        "UPDATE stations SET tzname = 'BoG0S' where id = 'DSM' and "
        "network = 'IA_ASOS'"
    )
    assert cursor.rowcount == 1
    load_stations(cursor)


@pytest.mark.parametrize("database", ["iem"])
def test_processor(cursor, disable_email):
    """Test basic parsing."""
    data = get_example_file("DSM.txt")
    pywwa.CTX["utcnow"] = utc(2011, 11, 27, 6, 16)
    real_parser(cursor, data)
