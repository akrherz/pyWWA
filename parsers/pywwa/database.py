"""Database utilities."""
# stdlib
import re

# 3rd Party
from pyiem.util import LOG
from pyiem.nws import ugc
from pyiem.nws import nwsli
import psycopg2
from twisted.enterprise import adbapi

# Local
from pywwa import CONFIG


def get_database(dbname, cp_max=1, module_name="pyiem.twistedpg"):
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


def load_ugcs_nwsli(ugc_dict, nwsli_dict):
    """Synchronous load of metadata tables."""
    with get_sync_dbconn("postgis") as pgconn:
        cursor = pgconn.cursor()
        cursor.execute(
            "SELECT name, ugc, wfo from ugcs WHERE name IS NOT null and "
            "begin_ts < now() and (end_ts is null or end_ts > now())"
        )
        for row in cursor:
            nm = (row[0]).replace("\x92", " ").replace("\xc2", " ")
            wfos = re.findall(r"([A-Z][A-Z][A-Z])", row[2])
            ugc_dict[row[1]] = ugc.UGC(
                row[1][:2], row[1][2], row[1][3:], name=nm, wfos=wfos
            )

        LOG.info("ugc_dict loaded %s entries", len(ugc_dict))

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
        "SELECT id, network, tzname, wfo, state, name, "
        "ST_X(geom) as lon, ST_Y(geom) as lat from stations "
        "where network ~* 'ASOS' or network = 'AWOS'"
    )
    news = 0
    # Need the fetchall due to non-async here
    for row in txn.fetchall():
        if row["id"] not in nwsli_provider:
            news += 1
            nwsli_provider[row["id"]] = row

    LOG.info("Loaded %s new stations", news)
