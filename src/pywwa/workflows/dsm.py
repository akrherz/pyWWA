"""ASOS Daily Summary Message Parser ingestor"""

from zoneinfo import ZoneInfo

# 3rd Party
import click
from pyiem.nws.products.dsm import parser

# Local
from pywwa import LOG, common
from pywwa.database import get_database, get_dbconnc
from pywwa.ldm import bridge

# database timezones to cache
TIMEZONES = {}
STATIONS = {}


def load_stations(txn):
    """load station metadata to build a xref."""
    txn.execute("SELECT id, tzname from stations where network ~* 'ASOS'")
    for row in txn:
        # we need four char station IDs
        station = row["id"] if len(row["id"]) == 4 else f"K{row['id']}"
        tzname = row["tzname"]
        if tzname not in TIMEZONES:
            try:
                TIMEZONES[tzname] = ZoneInfo(tzname)
            except Exception as exp:
                LOG.info("ZoneInfo does not like tzname: %s %s", tzname, exp)
                TIMEZONES[tzname] = ZoneInfo("UTC")
        STATIONS[station] = TIMEZONES[tzname]


def real_parser(txn, data):
    """Please process some data"""
    prod = parser(data, utcnow=common.utcnow(), ugc_provider={})
    prod.tzlocalize(STATIONS)
    if common.dbwrite_enabled():
        prod.sql(txn)
    jres = prod.get_jabbers(
        common.SETTINGS.get("pywwa_product_url", "pywwa_product_url")
    )
    for j in jres:
        common.send_message(j[0], j[1], j[2])

    if prod.warnings:
        common.email_error("\n".join(prod.warnings), data)


@click.command(help=__doc__)
@common.init
def main(*args, **kwargs):
    """build things up."""
    # sync
    pgconn, cursor = get_dbconnc("mesosite")
    load_stations(cursor)
    pgconn.close()
    bridge(real_parser, dbpool=get_database("iem"))
