"""SPS product ingestor"""
# stdlib
import re

# 3rd Party
from twisted.internet import reactor
from pyiem.util import LOG
from pyiem.nws.products.sps import parser
from pyiem.nws.ugc import UGC
from pyldm import ldmbridge

# Local
from pywwa import common
from pywwa.xmpp import make_jabber_client

POSTGIS = common.get_database("postgis")
PYWWA_PRODUCT_URL = common.SETTINGS.get(
    "pywwa_product_url", "pywwa_product_url"
)
JABBER = make_jabber_client()
ugc_provider = {}


def load_ugc(txn):
    """ load ugc dict """
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
        ugc_provider[row["ugc"]] = UGC(
            row["ugc"][:2], row["ugc"][2], row["ugc"][3:], name=nm, wfos=wfos
        )

    LOG.info("ugc_dict is loaded...")


class myProductIngestor(ldmbridge.LDMProductReceiver):
    """My ingestor"""

    def process_data(self, data):
        """Got data"""
        deffer = POSTGIS.runInteraction(real_process, data)
        deffer.addErrback(common.email_error, data)

    def connectionLost(self, reason):
        """stdin was closed"""
        common.shutdown()


def real_process(txn, raw):
    """ Really process! """
    if raw.find("$$") == -1:
        LOG.info("$$ was missing from this product")
        raw += "\r\r\n$$\r\r\n"
    prod = parser(raw, ugc_provider=ugc_provider)
    if common.dbwrite_enabled():
        prod.sql(txn)
    jmsgs = prod.get_jabbers(PYWWA_PRODUCT_URL)
    for (mess, htmlmess, xtra) in jmsgs:
        JABBER.send_message(mess, htmlmess, xtra)


def ready(_bogus):
    """we are ready to go"""
    ldmbridge.LDMProductFactory(myProductIngestor())


def killer(err):
    """hard stop"""
    LOG.error(err)
    reactor.stop()


df = POSTGIS.runInteraction(load_ugc)
df.addCallback(ready)
df.addErrback(killer)

reactor.run()
