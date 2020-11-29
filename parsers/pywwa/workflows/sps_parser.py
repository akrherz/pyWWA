"""SPS product ingestor"""
# 3rd Party
from twisted.internet import reactor
from pyiem.util import LOG
from pyiem.nws.products.sps import parser

# Local
from pywwa import common
from pywwa.xmpp import make_jabber_client
from pywwa.ldm import bridge
from pywwa.database import load_ugcs_nwsli
from pywwa.database import get_database

POSTGIS = get_database("postgis")
PYWWA_PRODUCT_URL = common.SETTINGS.get(
    "pywwa_product_url", "pywwa_product_url"
)
JABBER = make_jabber_client()
UGC_DICT = {}
NWSLI_DICT = {}


def real_process(txn, raw):
    """ Really process! """
    if raw.find("$$") == -1:
        LOG.info("$$ was missing from this product")
        raw += "\r\r\n$$\r\r\n"
    prod = parser(raw, ugc_provider=UGC_DICT, nwsli_provider=NWSLI_DICT)
    if common.dbwrite_enabled():
        prod.sql(txn)
    jmsgs = prod.get_jabbers(PYWWA_PRODUCT_URL)
    for (mess, htmlmess, xtra) in jmsgs:
        JABBER.send_message(mess, htmlmess, xtra)


def main():
    """Go Main Go."""
    load_ugcs_nwsli(UGC_DICT, NWSLI_DICT)
    bridge(real_process, dbpool=POSTGIS)
    reactor.run()


if __name__ == "__main__":
    main()
