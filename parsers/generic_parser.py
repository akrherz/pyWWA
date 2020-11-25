""" Generic NWS Product Parser """
# Local
import re

# 3rd Party
from twisted.internet import reactor
from shapely.geometry import MultiPolygon
from pyldm import ldmbridge
from pyiem.util import LOG
from pyiem.nws.products import parser as productparser
from pyiem.nws import ugc
from pyiem.nws import nwsli

# Local
from pywwa import common
from pywwa.xmpp import make_jabber_client

ugc_dict = {}
nwsli_dict = {}


def error_wrapper(exp, buf):
    """Don't whine about known invalid products"""
    if buf.find("HWOBYZ") > -1:
        LOG.info("Skipping Error for HWOBYZ")
    else:
        common.email_error(exp, buf)


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """ callback when the stdin reader connection is closed """
        common.shutdown()

    def process_data(self, data):
        """ Process the product """
        defer = PGCONN.runInteraction(really_process_data, data)
        defer.addErrback(error_wrapper, data)
        defer.addErrback(LOG.error)


def really_process_data(txn, buf):
    """ Actually do some processing """

    # Create our TextProduct instance
    prod = productparser(
        buf,
        utcnow=common.utcnow(),
        ugc_provider=ugc_dict,
        nwsli_provider=nwsli_dict,
    )

    # Do the Jabber work necessary after the database stuff has completed
    for (plain, html, xtra) in prod.get_jabbers(
        common.SETTINGS.get("pywwa_product_url", "pywwa_product_url")
    ):
        if xtra.get("channels", "") == "":
            common.email_error("xtra[channels] is empty!", buf)
        jabber.send_message(plain, html, xtra)

    if not common.dbwrite_enabled():
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
        "SELECT name, ugc, wfo from ugcs WHERE name IS NOT null and "
        "begin_ts < now() and (end_ts is null or end_ts > now())"
    )
    for row in txn.fetchall():
        nm = (row["name"]).replace("\x92", " ").replace("\xc2", " ")
        wfos = re.findall(r"([A-Z][A-Z][A-Z])", row["wfo"])
        ugc_dict[row["ugc"]] = ugc.UGC(
            row["ugc"][:2], row["ugc"][2], row["ugc"][3:], name=nm, wfos=wfos
        )

    LOG.info("ugc_dict loaded %s entries", len(ugc_dict))

    txn.execute(
        "SELECT nwsli, river_name || ' ' || proximity || ' ' || name || "
        "' ['||state||']' as rname from hvtec_nwsli"
    )
    for row in txn.fetchall():
        nm = row["rname"].replace("&", " and ")
        nwsli_dict[row["nwsli"]] = nwsli.NWSLI(row["nwsli"], name=nm)

    LOG.info("nwsli_dict loaded %s entries", len(nwsli_dict))


def ready(_):
    """ cb when our database work is done """
    ldmbridge.LDMProductFactory(MyProductIngestor())


def errback(err):
    """Called back when initial load fails."""
    LOG.info(err)
    common.shutdown()


def dbload():
    """ Load up database stuff """
    df = PGCONN.runInteraction(load_ugc)
    df.addCallback(ready)
    df.addErrback(errback)


if __name__ == "__main__":
    # Fire up!
    PGCONN = common.get_database("postgis", cp_max=1)
    dbload()
    jabber = make_jabber_client("generic_parser")

    reactor.run()
