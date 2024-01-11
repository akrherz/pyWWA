"""Database utilities."""

# 3rd Party
from psycopg.rows import dict_row
from pyiem.database import get_dbconn as pyiem_get_dbconn
from pyiem.database import get_dbconnc as pyiem_get_dbconnc
from pyiem.nws import nwsli
from twisted.enterprise import adbapi

# Local
from pywwa import LOG, SETTINGS


def get_dbconnc(dbname):
    """wrapper to pyiem to get the database connection right."""
    opts = SETTINGS.get("dbxref", {}).get(dbname, {})
    return pyiem_get_dbconnc(
        database=opts.get("database", dbname),
        host=opts.get("host", f"iemdb-{dbname}.local"),
        user=opts.get("user", "ldm"),
        port=opts.get("port", 5432),
    )


def get_dbconn(dbname):
    """wrapper to pyiem to get the database connection right."""
    opts = SETTINGS.get("dbxref", {}).get(dbname, {})
    return pyiem_get_dbconn(
        database=opts.get("database", dbname),
        host=opts.get("host", f"iemdb-{dbname}.local"),
        user=opts.get("user", "ldm"),
        port=opts.get("port", 5432),
    )


def get_database(dbname, cp_max=1, module_name="psycopg"):
    """Get a twisted database connection

    Args:
      dbname (str): The string name of the database to connect to
      cp_max (int): The maximum number of connections to make to the database
      module_name (str): The python module to use for the ConnectionPool
    """
    opts = SETTINGS.get("dbxref", {}).get(dbname, {})
    return adbapi.ConnectionPool(
        module_name,
        dbname=opts.get("database", dbname),
        cp_reconnect=True,
        cp_max=cp_max,
        host=opts.get("host", f"iemdb-{dbname}.local"),
        user=opts.get("user", "ldm"),
        port=opts.get("port", 5432),
        gssencmode="disable",  # NOTE: this is problematic with older postgres
        row_factory=dict_row,
    )


def load_nwsli(nwsli_dict):
    """Synchronous load of metadata tables."""
    with get_dbconn("postgis") as pgconn:
        cursor = pgconn.cursor(row_factory=dict_row)
        cursor.execute(
            "SELECT nwsli, river_name || ' ' || proximity || ' ' || "
            "name || ' ['||state||']' as rname from hvtec_nwsli"
        )
        for row in cursor:
            nm = row["rname"].replace("&", " and ")
            nwsli_dict[row["nwsli"]] = nwsli.NWSLI(row["nwsli"], name=nm)

        LOG.info("nwsli_dict loaded %s entries", len(nwsli_dict))


def load_metar_stations(txn, nwsli_provider):
    """load station metadata to build a xref of stations to networks"""
    # if tzname is null, this is likely some newly added station that needs
    # more metadata established prior to full usage
    txn.execute(
        """
        SELECT id, iemid, network, tzname, wfo, state, name,
        ST_X(geom) as lon, ST_Y(geom) as lat from stations
        where network ~* 'ASOS' and tzname is not null
        """
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
