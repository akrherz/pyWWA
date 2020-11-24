""" ASOS Daily Summary Message Parser ingestor """

import pytz
from twisted.internet import reactor
from twisted.python import log
from pyldm import ldmbridge
from pyiem.nws.products.dsm import parser
from pyiem.util import get_dbconn
import common

DBPOOL = common.get_database("iem", cp_max=1)
# database timezones to pytz cache
TIMEZONES = dict()
STATIONS = dict()


def load_stations(txn):
    """load station metadata to build a xref."""
    txn.execute(
        "SELECT id, tzname from stations "
        "where network ~* 'ASOS' or network = 'AWOS'"
    )
    for row in txn:
        # we need four char station IDs
        station = row[0] if len(row[0]) == 4 else "K" + row[0]
        tzname = row[1]
        if tzname not in TIMEZONES:
            try:
                TIMEZONES[tzname] = pytz.timezone(tzname)
            except Exception as exp:
                log.msg("pytz does not like tzname: %s %s" % (tzname, exp))
                TIMEZONES[tzname] = pytz.UTC
        STATIONS[station] = TIMEZONES[tzname]


class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """sys.stdin was closed"""
        common.shutdown()

    def process_data(self, data):
        """ Process the product """
        df = DBPOOL.runInteraction(real_parser, data)
        df.addErrback(common.email_error, data)


def real_parser(txn, data):
    """Please process some data"""
    prod = parser(data)
    prod.tzlocalize(STATIONS)
    if common.dbwrite_enabled():
        prod.sql(txn)
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), data)


def bootstrap():
    """build things up."""
    # sync
    pgconn = get_dbconn("mesosite")
    cursor = pgconn.cursor()
    load_stations(cursor)
    pgconn.close()

    ldmbridge.LDMProductFactory(MyProductIngestor())
    reactor.run()


if __name__ == "__main__":
    bootstrap()
