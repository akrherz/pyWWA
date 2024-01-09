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
import click
from pyiem.nws.products.vtec import parser as vtecparser
from pyiem.nws.ugc import UGCProvider
from pyiem.util import LOG
from twisted.mail.smtp import SMTPSenderFactory

# Local
from pywwa import common
from pywwa.database import get_database, load_nwsli
from pywwa.ldm import bridge

SMTPSenderFactory.noisy = False
PGCONN = get_database("postgis")
UGC_DICT = UGCProvider()
NWSLI_DICT = {}


def process_data(data):
    """Process the product"""
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
    for plain, html, xtra in text_product.get_jabbers(
        common.SETTINGS.get("pywwa_vtec_url", "pywwa_vtec_url"),
        common.SETTINGS.get("pywwa_river_url", "pywwa_river_url"),
    ):
        if xtra.get("channels", "") == "":
            common.email_error("xtra[channels] is empty!", text_product.text)
        common.send_message(plain, html, xtra)


@click.command()
@common.init
def main(*args, **kwargs):
    """Go Main Go."""
    load_nwsli(NWSLI_DICT)
    bridge(process_data)
