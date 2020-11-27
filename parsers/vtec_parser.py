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
# 3rd Party
from bs4 import BeautifulSoup
import treq
from twisted.internet import reactor
from twisted.mail.smtp import SMTPSenderFactory
from pyiem.util import LOG
from pyiem.nws.products.vtec import parser as vtecparser

# Local
from pywwa import common
from pywwa.xmpp import make_jabber_client
from pywwa.ldm import bridge
from pywwa.database import load_ugcs_nwsli
from pywwa.database import get_database

SMTPSenderFactory.noisy = False
JABBER = make_jabber_client()
PGCONN = get_database("postgis")
UGC_DICT = {}
NWSLI_DICT = {}


def process_data(data):
    """ Process the product """
    # Make sure we have a trailing $$, if not report error and slap one on
    if data.find("$$") == -1:
        common.email_error("No $$ Found!", data)
        data += "\n\n$$\n\n"

    # Create our TextProduct instance
    text_product = vtecparser(
        data,
        utcnow=common.utcnow(),
        ugc_provider=UGC_DICT,
        nwsli_provider=NWSLI_DICT,
    )
    # Don't parse these as they contain duplicated information
    if text_product.source == "KNHC" and text_product.afos[:3] == "TCV":
        return
    # Skip spanish products
    if text_product.source == "TJSJ" and text_product.afos[3:] == "SPN":
        return

    # This is sort of ambiguous as to what is best to be done when database
    # writing is disabled.  The web workflow will likely fail as well.
    if common.dbwrite_enabled():
        df = PGCONN.runInteraction(text_product.sql)
        df.addCallback(step2, text_product)
        df.addErrback(common.email_error, text_product.unixtext)
        df.addErrback(LOG.error)
    else:
        step2(None, text_product)


def step2(_dummy, text_product):
    """Callback after hopeful database work."""
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


def main():
    """Go Main Go."""
    load_ugcs_nwsli(UGC_DICT, NWSLI_DICT)
    bridge(process_data)
    reactor.run()


if __name__ == "__main__":
    main()
