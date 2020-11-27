""" Generic NWS Product Parser """
# 3rd Party
from twisted.internet import reactor
from shapely.geometry import MultiPolygon
from pyiem.util import LOG
from pyiem.nws.products import parser as productparser

# Local
from pywwa import common
from pywwa.xmpp import make_jabber_client
from pywwa.ldm import bridge
from pywwa.database import load_ugcs_nwsli

UGC_DICT = {}
NWSLI_DICT = {}
JABBER = make_jabber_client()
PGCONN = common.get_database("postgis")


def error_wrapper(exp, buf):
    """Don't whine about known invalid products"""
    if buf.find("HWOBYZ") > -1:
        LOG.info("Skipping Error for HWOBYZ")
    else:
        common.email_error(exp, buf)


def process_data(data):
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
        ugc_provider=UGC_DICT,
        nwsli_provider=NWSLI_DICT,
    )

    # Do the Jabber work necessary after the database stuff has completed
    for (plain, html, xtra) in prod.get_jabbers(
        common.SETTINGS.get("pywwa_product_url", "pywwa_product_url")
    ):
        if xtra.get("channels", "") == "":
            common.email_error("xtra[channels] is empty!", buf)
        JABBER.send_message(plain, html, xtra)

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


def main():
    """Go Main Go."""
    load_ugcs_nwsli(UGC_DICT, NWSLI_DICT)
    bridge(process_data)

    reactor.run()


if __name__ == "__main__":
    main()
