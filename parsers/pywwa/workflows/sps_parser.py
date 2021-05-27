"""SPS product ingestor"""
# 3rd Party
from twisted.internet import reactor
from pyiem.util import LOG
from pyiem.nws.ugc import UGCProvider
from pyiem.nws.products.sps import parser

# Local
from pywwa import common
from pywwa.ldm import bridge
from pywwa.database import load_nwsli
from pywwa.database import get_database

POSTGIS = get_database("postgis")
UGC_DICT = UGCProvider()
NWSLI_DICT = {}


def real_process(txn, raw):
    """Really process!"""
    if raw.find("$$") == -1:
        LOG.info("$$ was missing from this product")
        raw += "\r\r\n$$\r\r\n"
    prod = parser(raw, ugc_provider=UGC_DICT, nwsli_provider=NWSLI_DICT)
    if common.dbwrite_enabled():
        prod.sql(txn)
    baseurl = common.SETTINGS.get("pywwa_product_url", "pywwa_product_url")
    jmsgs = prod.get_jabbers(baseurl)
    for (mess, htmlmess, xtra) in jmsgs:
        common.send_message(mess, htmlmess, xtra)
    if prod.warnings:
        common.email_error("\n\n".join(prod.warnings), prod.text)


def main():
    """Go Main Go."""
    common.main()
    load_nwsli(NWSLI_DICT)
    bridge(real_process, dbpool=POSTGIS)
    reactor.run()


if __name__ == "__main__":
    main()
