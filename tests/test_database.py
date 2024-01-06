"""Test Database Functionality."""

# Third Party
from psycopg.rows import dict_row

# Local
from pywwa import database


def test_database():
    """Test that we can call the API."""
    database.load_nwsli({})


def test_load_metar_stations():
    """Test loading of METAR stations."""
    pgconn = database.get_dbconn("mesosite")
    cursor = pgconn.cursor(row_factory=dict_row)
    database.load_metar_stations(cursor, {})
    pgconn.close()
