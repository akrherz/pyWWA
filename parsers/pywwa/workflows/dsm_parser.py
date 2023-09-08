""" ASOS Daily Summary Message Parser ingestor """

# 3rd Party
import pytz
from psycopg2.extras import RealDictCursor
from pyiem.nws.products.dsm import parser
from pyiem.util import LOG, get_dbconn
from twisted.internet import reactor

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge

# database timezones to pytz cache
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
                TIMEZONES[tzname] = pytz.timezone(tzname)
            except Exception as exp:
                LOG.info("pytz does not like tzname: %s %s", tzname, exp)
                TIMEZONES[tzname] = pytz.UTC
        STATIONS[station] = TIMEZONES[tzname]


def real_parser(txn, data):
    """Please process some data"""
    prod = parser(data, utcnow=common.utcnow())
    prod.tzlocalize(STATIONS)
    if common.dbwrite_enabled():
        prod.sql(txn)
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), data)


def main():
    """build things up."""
    common.main(with_jabber=False)
    # sync
    pgconn = get_dbconn("mesosite")
    cursor = pgconn.cursor(cursor_factory=RealDictCursor)
    load_stations(cursor)
    pgconn.close()
    bridge(real_parser, dbpool=get_database("iem"))
    reactor.run()


if __name__ == "__main__":
    main()
