"""Test Database Functionality."""

# Third Party
from psycopg2.extras import RealDictCursor

# Local
from pywwa import database


def test_database():
    """Test that we can call the API."""
    database.load_ugcs_nwsli({}, {})


def test_load_metar_stations():
    """Test loading of METAR stations."""
    pgconn = database.get_sync_dbconn("mesosite")
    cursor = pgconn.cursor(cursor_factory=RealDictCursor)
    database.load_metar_stations(cursor, {})
    pgconn.close()
