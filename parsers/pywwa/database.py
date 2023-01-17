"""Database utilities."""

# 3rd Party
from pyiem.util import LOG
from pyiem.nws import nwsli
import psycopg2
from psycopg2.extras import DictCursor
from twisted.enterprise import adbapi

# Local
from pywwa import CONFIG


def get_database(dbname, cp_max=1, module_name="psycopg2"):
    """Get a twisted database connection

    Args:
      dbname (str): The string name of the database to connect to
      cp_max (int): The maximum number of connections to make to the database
      module_name (str): The python module to use for the ConnectionPool
    """
    # Check to see if we have a `settings.json` override
    opts = CONFIG.get(dbname, {})
    return adbapi.ConnectionPool(
        module_name,
        database=opts.get("database", dbname),
        cp_reconnect=True,
        cp_max=cp_max,
        host=opts.get("host", f"iemdb-{dbname}.local"),
        user=opts.get("user", "ldm"),
        port=opts.get("port", 5432),
        gssencmode="disable",  # NOTE: this is problematic with older postgres
        cursor_factory=DictCursor,
    )


def get_sync_dbconn(dbname):
    """Get the synchronous database connection."""
    opts = CONFIG.get(dbname, {})
    return psycopg2.connect(
        database=opts.get("database", dbname),
        host=opts.get("host", f"iemdb-{dbname}.local"),
        user=opts.get("user", "ldm"),
        port=opts.get("port", 5432),
        gssencmode="disable",
    )


def load_nwsli(nwsli_dict):
    """Synchronous load of metadata tables."""
    with get_sync_dbconn("postgis") as pgconn:
        cursor = pgconn.cursor()
        cursor.execute(
            "SELECT nwsli, river_name || ' ' || proximity || ' ' || "
            "name || ' ['||state||']' as rname from hvtec_nwsli"
        )
        for row in cursor:
            nm = row[1].replace("&", " and ")
            nwsli_dict[row[0]] = nwsli.NWSLI(row[0], name=nm)

        LOG.info("nwsli_dict loaded %s entries", len(nwsli_dict))


def load_metar_stations(txn, nwsli_provider):
    """load station metadata to build a xref of stations to networks"""
    txn.execute(
        "SELECT id, iemid, network, tzname, wfo, state, name, "
        "ST_X(geom) as lon, ST_Y(geom) as lat from stations "
        "where network ~* 'ASOS'"
    )
    news = 0
    # Need the fetchall due to non-async here
    for row in txn.fetchall():
        if row["id"] not in nwsli_provider:
            news += 1
            nwsli_provider[row["id"]] = row
    # Load up aliases for stations that changed IDs over the years
    txn.execute(
        "SELECT id, value from stations t JOIN station_attributes a "
        "on (t.iemid = a.iemid) WHERE attr = 'WAS'"
    )
    for row in txn.fetchall():  # Force sync
        if row["id"] not in nwsli_provider:
            LOG.info("Can't remap unknown %s", row["id"])
            continue
        nwsli_provider[row["value"]] = nwsli_provider[row["id"]]

    LOG.info("Loaded %s new stations", news)
