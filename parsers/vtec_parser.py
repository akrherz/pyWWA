"""
VTEC product ingestor

The warnings table has the following timestamp based columns, this gets ugly
with watches.  Lets try to explain

    issue   <- VTEC timestamp of when this event was valid for
    expire  <- When does this VTEC product expire
    updated <- Product Timestamp of when a product gets updated
    init_expire <- When did this product initially expire
    product_issue <- When was this product issued by the NWS
"""
# stdlib
import re

# 3rd Party
from bs4 import BeautifulSoup
import treq
from twisted.internet import reactor
from twisted.mail.smtp import SMTPSenderFactory
from pyldm import ldmbridge
from pyiem.util import LOG
from pyiem.nws.products.vtec import parser as vtecparser
from pyiem.nws import ugc
from pyiem.nws import nwsli

# Local
from pywwa import common
from pywwa.xmpp import make_jabber_client

JABBER = make_jabber_client()


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """ callback when the stdin reader connection is closed """
        common.shutdown(7)

    def process_data(self, data):
        """ Process the product """
        try:
            really_process_data(data)
        except Exception as myexp:  # pylint: disable=W0703
            common.email_error(myexp, data)


def really_process_data(buf):
    """ Actually do some processing """
    # Make sure we have a trailing $$, if not report error and slap one on
    if buf.find("$$") == -1:
        common.email_error("No $$ Found!", buf)
        buf += "\n\n$$\n\n"

    # Create our TextProduct instance
    text_product = vtecparser(
        buf,
        utcnow=common.utcnow(),
        ugc_provider=ugc_dict,
        nwsli_provider=nwsli_dict,
    )
    # Don't parse these as they contain duplicated information
    if text_product.source == "KNHC" and text_product.afos[:3] == "TCV":
        return
    # Skip spanish products
    if text_product.source == "TJSJ" and text_product.afos[3:] == "SPN":
        return

    # TODO can't disable the database write yet.
    df = PGCONN.runInteraction(text_product.sql)
    df.addCallback(step2, text_product)
    df.addErrback(common.email_error, text_product.unixtext)
    df.addErrback(LOG.error)


def step2(_dummy, text_product):
    """After the SQL is done, lets do other things"""
    if text_product.warnings:
        common.email_error(
            "\n\n".join(text_product.warnings), text_product.text
        )

    # Do the Jabber work necessary after the database stuff has completed
    for (plain, html, xtra) in text_product.get_jabbers(
        common.SETTINGS.get("pywwa_vtec_url", "pywwa_vtec_url"),
        common.SETTINGS.get("pywwa_river_url", "pywwa_river_url"),
    ):
        if xtra.get("channels", "") == "":
            common.email_error("xtra[channels] is empty!", text_product.text)
        send_jabber_message(plain, html, xtra)


def send_jabber_message(plain, html, extra):
    """Some hacky logic to get ahead of web crawlers."""

    def _send(*_args, **_kwargs):
        """Just send it already :("""
        JABBER.send_message(plain, html, extra)

    def _cbBody(body):
        """Finally got the HTML"""
        soup = BeautifulSoup(body, "html.parser")
        url = soup.find("meta", property="og:image")["content"]
        url = url.replace("https", "http").replace(
            "mesonet.agron.iastate.edu", "iem.local"
        )
        d = treq.get(url)
        d.addCallback(_send)
        d.addErrback(_send)

    def _cb(response):
        """Got a response."""
        d = treq.text_content(response)
        d.addCallback(_cbBody)
        d.addErrback(_send)

    url = plain.split()[-1]
    if url.find("/f/") > 0:
        url = url.replace("https", "http").replace(
            "mesonet.agron.iastate.edu", "iem.local"
        )
        d = treq.get(url)
        d.addCallback(_cb)
        d.addErrback(_send)
    else:
        _send()


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

    LOG.info("ugc_dict loaded %s entries", len(ugc_dict))

    sql = """
     SELECT nwsli,
     river_name || ' ' || proximity || ' ' || name || ' ['||state||']' as rname
     from hvtec_nwsli
    """
    txn.execute(sql)
    for row in txn.fetchall():
        nm = row["rname"].replace("&", " and ")
        nwsli_dict[row["nwsli"]] = nwsli.NWSLI(row["nwsli"], name=nm)

    LOG.info("nwsli_dict loaded %s entries", len(nwsli_dict))

    return None


def ready(_dummy):
    """ cb when our database work is done """
    ldmbridge.LDMProductFactory(MyProductIngestor(dedup=True))


def bootstrap():
    """Things to do at startup"""
    df = PGCONN.runInteraction(load_ugc)
    df.addCallback(ready)
    df.addErrback(common.shutdown)


if __name__ == "__main__":
    SMTPSenderFactory.noisy = False
    ugc_dict = {}
    nwsli_dict = {}

    # Fire up!
    PGCONN = common.get_database(common.CONFIG["databaserw"]["postgis"])
    bootstrap()

    reactor.run()
