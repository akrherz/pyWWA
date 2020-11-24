""" Generic NWS Product Parser """
import re

from twisted.python import log
from twisted.internet import reactor
from shapely.geometry import MultiPolygon
from pyldm import ldmbridge
from pyiem.nws.products import parser as productparser
from pyiem.nws import ugc
from pyiem.nws import nwsli
from pyiem.util import utc

import common

ugc_dict = {}
nwsli_dict = {}


def shutdown():
    """ Stop this app """
    log.msg("Shutting down...")
    reactor.callWhenRunning(reactor.stop)


def error_wrapper(exp, buf):
    """Don't whine about known invalid products"""
    if buf.find("HWOBYZ") > -1:
        log.msg("Skipping Error for HWOBYZ")
        return
    common.email_error(exp, buf)


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """ callback when the stdin reader connection is closed """
        log.msg("connectionLost() called...")
        log.err(reason)
        reactor.callLater(7, shutdown)

    def process_data(self, data):
        """ Process the product """
        defer = PGCONN.runInteraction(really_process_data, data)
        defer.addErrback(error_wrapper, data)
        defer.addErrback(log.err)


def really_process_data(txn, buf):
    """ Actually do some processing """
    utcnow = utc() if common.CTX.utcnow is None else common.CTX.utcnow

    # Create our TextProduct instance
    prod = productparser(
        buf, utcnow=utcnow, ugc_provider=ugc_dict, nwsli_provider=nwsli_dict
    )

    # Do the Jabber work necessary after the database stuff has completed
    for (plain, html, xtra) in prod.get_jabbers(
        common.SETTINGS.get("pywwa_product_url", "pywwa_product_url")
    ):
        if xtra.get("channels", "") == "":
            common.email_error("xtra[channels] is empty!", buf)
        jabber.send_message(plain, html, xtra)

    if common.CTX.disable_dbwrite:
        return
    # Insert into database
    product_id = prod.get_product_id()
    sqlraw = buf.replace("\015\015\012", "\n").replace("\000", "").strip()
    giswkt = None
    if prod.segments and prod.segments[0].sbw:
        giswkt = ("SRID=4326;%s") % (MultiPolygon([prod.segments[0].sbw]).wkt,)
    sql = """
        INSERT into text_products(product, product_id, geom)
        values (%s,%s,%s)
    """
    myargs = (sqlraw, product_id, giswkt)
    txn.execute(sql, myargs)


def load_ugc(txn):
    """ load ugc"""
    # Careful here not to load things from the future
    txn.execute(
        """
        SELECT name, ugc, wfo from ugcs WHERE
        name IS NOT Null and begin_ts < now() and
        (end_ts is null or end_ts > now())
    """
    )
    for row in txn.fetchall():
        nm = (row["name"]).replace("\x92", " ").replace("\xc2", " ")
        wfos = re.findall(r"([A-Z][A-Z][A-Z])", row["wfo"])
        ugc_dict[row["ugc"]] = ugc.UGC(
            row["ugc"][:2], row["ugc"][2], row["ugc"][3:], name=nm, wfos=wfos
        )

    log.msg("ugc_dict loaded %s entries" % (len(ugc_dict),))

    sql = """
     SELECT nwsli,
     river_name || ' ' || proximity || ' ' || name || ' ['||state||']' as rname
     from hvtec_nwsli
    """
    txn.execute(sql)
    for row in txn.fetchall():
        nm = row["rname"].replace("&", " and ")
        nwsli_dict[row["nwsli"]] = nwsli.NWSLI(row["nwsli"], name=nm)

    log.msg("nwsli_dict loaded %s entries" % (len(nwsli_dict),))


def ready(_):
    """ cb when our database work is done """
    ldmbridge.LDMProductFactory(MyProductIngestor())


def dbload():
    """ Load up database stuff """
    df = PGCONN.runInteraction(load_ugc)
    df.addCallback(ready)


if __name__ == "__main__":
    # Fire up!
    PGCONN = common.get_database("postgis", cp_max=1)
    dbload()
    jabber = common.make_jabber_client("generic_parser")

    reactor.run()
