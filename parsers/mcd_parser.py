"""
 Support SPC's MCD product
 Support WPC's FFG product
"""

from twisted.python import log
from twisted.internet import reactor
from pyiem.nws.products.mcd import parser as mcdparser
from pyldm import ldmbridge
import common

DBPOOL = common.get_database(common.CONFIG["databaserw"]["postgis"], cp_max=2)


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """ Called when the STDIN connection is lost """
        log.msg("connectionLost")
        log.err(reason)
        reactor.callLater(15, reactor.callWhenRunning, reactor.stop)

    def process_data(self, data):
        """ Process a chunk of data """
        # BUG
        data = data.upper()
        df = DBPOOL.runInteraction(real_process, data)
        df.addErrback(common.email_error, data)


def find_cwsus(txn, prod):
    """
    Provided a database transaction, go look for CWSUs that
    overlap the discussion geometry.
    ST_Overlaps do the geometries overlap
    ST_Covers does polygon exist inside CWSU
    """
    wkt = "SRID=4326;%s" % (prod.geometry.wkt,)
    sql = """
        select distinct id from cwsu WHERE st_overlaps('%s', geom) or
        st_covers(geom, '%s') ORDER by id ASC
    """ % (
        wkt,
        wkt,
    )
    txn.execute(sql)
    cwsus = []
    for row in txn.fetchall():
        cwsus.append(row["id"])
    return cwsus


def real_process(txn, raw):
    """ "
    Actually process a single MCD
    """
    prod = mcdparser(raw)
    prod.cwsus = find_cwsus(txn, prod)

    j = prod.get_jabbers(
        common.SETTINGS.get("pywwa_product_url", "pywwa_product_url")
    )
    if len(j) == 1:
        JABBER.send_message(j[0][0], j[0][1], j[0][2])

    prod.database_save(txn)


ldmbridge.LDMProductFactory(MyProductIngestor())
JABBER = common.make_jabber_client("mcd_parser")

reactor.run()
